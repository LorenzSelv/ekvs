%% lab4kvs_kvsquery
%%
%% This module runs the query in the distributed system.
%% exec(Function, Args) is the only exported function.

-module(lab4kvs_kvsquery).

-export([exec/2]).

-define(FORWARD_TIMEOUT, 200).


exec(Func, Args) ->
    %% Execute a query on the kvs
    lab4kvs_debug:call({exec, Func, Args}),

    Key = case Args of [K,_] -> K; [K,_,_] -> K end,
    
    KeyPartitionID = lab4kvs_viewmanager:get_key_owner_id(Key),
    PartitionNodes = lab4kvs_viewmanager:get_partition_members(KeyPartitionID),

    io:format("Key owned by partition ~p~n", [KeyPartitionID]),
    
    %% The query has to be executed in all the nodes of the partition
    Result = run_kvs_query_at(PartitionNodes, Func, Args),
    {Result, KeyPartitionID}.


run_kvs_query_at(Nodes, get, [Key, CP]) ->
    %% A GET request is guaranteed to return the most up-to-date value
    %% present in the connected nodes of the partition.
    %% Thus, the first result received from one of the node of the partition
    %% can be returned to the client.
    %%
    %% TWO POSSIBLE SCENARIOS:
    %%   1. The current node belongs to the partition
    %%      -> no need to forward the request, return the local value
    %%   2. The current node does not belong to the partition
    %%      -> forward the request and return the first received result
    %%
    %% Return values
    %%  - {ok, Value, CausalPayload, Timestamp}
    %%  - keyerror
    %%  - all_disconnected
    %%
    case lists:member(node(), Nodes) of
        true  ->
            local_call(lab4kvs_kvstore, get, [Key, CP]);
        false ->
            forward_kvs_query_to(Nodes, get, [Key, CP])
    end;

run_kvs_query_at(Nodes, put, [Key, Value, CP]) ->
    %% A PUT request has to enforce the guarantee described above for the GET
    %% request. To achieve this goal, each node that receives a put request
    %% has to broadcast the request to all other nodes in the partition.
    %%
    %% The protocol is equivalent to the one used in 2PC.
    %% The client knows that everybody knows that everybody knows the value -- KcEE(value)
    %%
    %% TWO POSSIBLE SCENARIOS:
    %%   1. The current node belongs to the partition
    %%        forward the request to the other nodes in the partition
    %%        run the query locally
    %%        wait for all rpc calls to terminate
    %%        propagate the updated VC
    %%
    %%   2. The current node does not belong to the partition
    %%        forward the request to all nodes of the partition 
    %%        wait for all rpc calls to terminate
    %%        propagate the updated VC
    %%
    %% Return values
    %%  - {ok, CausalPayload, Timestamp}
    %%  - all_disconnected
    %%
    case lists:member(node(), Nodes) of
        true  ->
            ResLocal = local_call(lab4kvs_kvstore, put, [Key, Value, CP]),
            %% Forward put request to other nodes in the partition
            NewCP = lab4kvs_vcmanager:get_cp(), 
            ResForward  = forward_kvs_query_to(Nodes -- [node()], put, [Key, Value, NewCP]),
            case ResForward of
                all_disconnected ->
                    ResLocal;
                _ ->
                    ResForward
            end;
        false ->
            forward_kvs_query_to(Nodes, put, [Key, Value, CP])
    end;

run_kvs_query_at(Nodes, delete, [Key, CP]) ->
    %% To make sure a deleted key remains deleted after a partition
    %% is healed, a key is never deleted from the KVS.
    %% Instead, we perform a PUT <key_to_delete, 'deleted'>.
    %% This tombstone entry in the KVS has a causal payload associated
    %% with it, thus allowing to know when the deletion occurred. 
    %%
    %% During a GET operation, if the value is 'deleted'
    %% then a keyerror response is sent.
    %%
    %% Note: 'deleted' (atom) is not the same as 
    %%       "deleted" (string)
    %%    => a put request (key, "deleted") insert 
    %%       the new entry with no unexpected behavior
    %%
    %% Before deleting the key, make sure it's currently present in the KVS.
    %%
    case run_kvs_query_at(Nodes, get, [Key, CP]) of
        {ok, _, _, _} ->  %% Key present in the KVS, proceed with deletion
            run_kvs_query_at(Nodes, put, [Key, deleted, CP]);
        keyerror ->  %% Key is not present
            keyerror;
        all_disconnected ->  %% All node in the partition are disconnected
            all_disconnected
    end.




forward_kvs_query_to([], get, _) -> all_disconnected;
forward_kvs_query_to([Node|Nodes], get, [Key, CP]) -> 
    case rpc_call_with_timeout(Node, 
                               lab4kvs_kvstore, get, [Key, CP], 
                               ?FORWARD_TIMEOUT) of
        {ok, Res} ->  %% Res = {ok, Value, CausalPayload, Timestamp} | keyerror
            Res;
        {badrpc, _} ->
            forward_kvs_query_to(Nodes, get, [Key, CP])
    end;



forward_kvs_query_to([], put, _) -> all_disconnected;
forward_kvs_query_to(Nodes, put, [Key, Value, CP]) -> 
    Put = fun(Node) -> rpc_call_with_timeout(Node, 
                                             lab4kvs_kvstore, put, 
                                             [Key, Value, CP],
                                             ?FORWARD_TIMEOUT) end,
    Results = lists:map(Put, Nodes),
    io:format("Results ~p~n", [Results]),
    %% Results is a list of {ok, Result} | {badrpc, timeout} 
    UpNodeResultList = [{Node, Result} || 
                        {Node, {ok, Result}} <- lists:zip(Nodes, Results)],
    UpNodes = [Node || {Node, _} <- UpNodeResultList],

    {NewCP, Res} = case length(UpNodes) of
                    0 ->  %% All nodes are disconnected
                        {CP, all_disconnected};
                    _ ->  %% Each UpNodeResult is {node, {ok, CP, Timestamp}}
                        io:format("UpNodesResults ~p~n", [UpNodeResultList]),
                        VCs = [lab4kvs_vcmanager:cp_to_vc(ResCP) || 
                               {_, {ok, ResCP, _}} <- UpNodeResultList],
                        io:format("VCs = ~p~n", [VCs]),
                        MergedVC = lab4kvs_vcmanager:merge_vcs(VCs),
                        MergedCP = lab4kvs_vcmanager:vc_to_cp(MergedVC),
                        
                        io:format("MergedVC = ~p~n", [MergedVC]),
                        %% Broadcast MergedVC to all connected nodes
                        broadcast_vector_clock_to(UpNodes, Key, MergedVC),

                        R = {ok, MergedCP, lab4kvs_vcmanager:get_timestamp()},  
                        {MergedCP, R}
    end,
    %% All connected nodes have an up-to-date vector clock
    %% Async RPC call until success to all disconnected nodes
    DownNodes = Nodes -- UpNodes, 
    KVSValue  = lab4kvs_kvstore:prepare_kvsvalue(Key, Value, NewCP),
    AsyncPut  = fun(Node) ->
                   async_rpc_call_until_success(Node, lab4kvs_kvstore, put, [Key, KVSValue]) end,
    lists:map(AsyncPut, DownNodes),

    io:format("ASYNC PUT ~p to DOWN NODES ~p~n", [KVSValue, DownNodes]),
    Res.


broadcast_vector_clock_to(UpNodes, Key, VC) ->
    %% Update the vector clock associated with the specified key in the remote KVS
    %% Update the vector clock of the remote node 
    lab4kvs_debug:call({broadcast_vc_to, UpNodes, Key, VC}),
    UpdateVC = fun(Node) ->
                    %% rpc_call_with_timeout(Node,
                                          %% lab4kvs_kvstore,
                                          %% update_vc,
                                          %% [Key, VC],
                                          %% ?FORWARD_TIMEOUT),
                    rpc_call_with_timeout(Node, 
                                          lab4kvs_vcmanager,
                                          update_vc,
                                          [VC],
                                          ?FORWARD_TIMEOUT)
               end,
    lists:map(UpdateVC, UpNodes).


local_call(Module, Func, Args) ->
    %% Execute a local call to Module:Func(Args)
    apply(Module, Func, Args).


async_rpc_call_until_success(Node, Module, Func, Args) ->
    %% Execute a RPC call on a remote node
    %% This function spawns a new process that keeps doing
    %% RPC calls with timeout in the background until one succeeds.
    %%
    %% The return value of the RPC call is ignored, since
    %% this function returns immediately after having spawned the process.
    %%
    LoopRPC = fun Loop() ->
                  case rpc_call_with_timeout(Node, Module, Func, Args, ?FORWARD_TIMEOUT) of 
                      {ok, _Result} -> ok;
                      {badrpc, timeout} -> 
                          io:format("Timeout, trying again..~n"),
                          Loop()
                  end
              end,
    _Pid = spawn(LoopRPC).
    

rpc_call_with_timeout(Node, Module, Func, Args, Timeout) ->
    %% Execute a RPC call with timeout on a remote Node
    %% Return either {ok, Result} of the call or {badrpc, timeout}
    %% It's the same thing that the 5th argument to the rpc call would do,
    %% but for some reason it's not working..
    %%
    Pid = self(),
    SpawnPid = spawn(fun() -> 
                        Res = rpc:call(Node, Module, Func, Args, Timeout),
                        Pid ! {self(), ok, Res} end),
    receive
        {SpawnPid, ok, Res} -> 
            {ok, Res}
    after
        Timeout -> 
            %% Kill the rpc call
            exit(SpawnPid, kill), 
            {badrpc, timeout}
    end.
