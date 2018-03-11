%% lab4kvs_kvsquery
%%
%% This module runs the query in the distributed system.
%% exec(Function, Args) is the only exported function.

-module(lab4kvs_kvsquery).

-export([exec/2]).

-define(FORWARD_TIMEOUT, 200).

%% TODO refactor duplicate definition
-record(kvsvalue, {value, hash, vector_clock, timestamp}).

exec(Func, Args) ->
    %% Execute a query on the kvs
    lab4kvs_debug:call({exec, Func, Args}),

    Key = case Args of [K,_] -> K; [K] -> K end,
    
    KeyPartitionID = lab4kvs_viewmanager:get_key_owner_id(Key),
    PartitionNodes = lab4kvs_viewmanager:get_partition_members(KeyPartitionID),

    io:format("Key owned by partition ~p~n", [KeyPartitionID]),
    
    %% The query has to be executed in all the nodes of the partition
    Result = run_kvs_query_at(PartitionNodes, Func, Args),
    {Result, KeyPartitionID}.


run_kvs_query_at(Nodes, get, [Key]) ->
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
            local_call(lab4kvs_kvstore, get, [Key]);
        false ->
            forward_kvs_query_to(Nodes, get, [Key])
    end;

run_kvs_query_at(Nodes, put, [Key, Value, CausalPayload]) ->
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
    %%  - {ok, Value, CausalPayload, Timestamp}
    %%  - all_disconnected
    %%
    case lists:member(node(), Nodes) of
        true  ->
            ResLocal = local_call(lab4kvs_kvstore, put, [Key, Value, CausalPayload]),
            %% Forward put request to other nodes in the partition
            ResForward  = forward_kvs_query_to(Nodes -- [node()], put, [Key, Value, CausalPayload]),
            case ResForward of
                all_disconnected ->
                    ResLocal;
                _ ->
                    ResForward
            end;
        false ->
            forward_kvs_query_to(Nodes, put, [Key, Value, CausalPayload])
    end.


forward_kvs_query_to([], get, _) -> all_disconnected;
forward_kvs_query_to([Node|Nodes], get, [Key]) -> 
    case rpc_call_with_timeout(Node, lab4kvs_kvstore, get, [Key], ?FORWARD_TIMEOUT) of
        {ok, Res} ->  %% Res = {ok, Value, CausalPayload, Timestamp} | keyerror
            Res;
        {badrpc, timeout} ->
            forward_kvs_query_to(Nodes, get, [Key])
    end;



forward_kvs_query_to([Nodes], put, [Key, Value, CausalPayload]) -> 
    Put = fun(Node) -> rpc_call_with_timeout(Node, 
                                             lab4kvs_kvstore, 
                                             put, 
                                             [Key, Value, CausalPayload], 
                                             ?FORWARD_TIMEOUT) end,
    Results = lists:map(Put, Nodes),
    %% Results is a list of {ok, Result} | {badrpc, timeout} 
    Success = fun({_Node, Result}) ->
                      {ok, _} = Result end,
    UpNodeResultList = lists:filter(Success, [{Node, Result} || {Node, Result} <- lists:zip(Nodes, Results)]),
    case length(UpNodeResultList) of
        0 ->  %% All nodes are disconnected
            all_disconnected;
        _ ->  %% Each UpNodeResult is {node, {ok, CausalPayload, Timestamp}}
            VCs = [lab4kvs_vcmanager:cp_to_vc(CP) || 
                   {_, {ok, CP, _}} <- UpNodeResultList],
            MergedVC = lab4kvs_vcmanager:merge_vcs(VCs),
            MergedCP = lab4kvs_vcmanager:vc_to_cp(MergedVC),
            
            %% Broadcast MergedVC to all connected nodes
            UpNodes = [Node || {Node, _} <- UpNodeResultList],
            %% TODO pass also the key so that the vector clock of the can be updated
            lab4kvs_vcmanager:broadcast_vc_to(UpNodes),

            %% All connected nodes have an up-to-date vector clock
            %% Async RPC call until success to all disconnected nodes
            DownNodes = Nodes -- UpNodes, 
            KVSValue  = lab4kvs_kvstore:prepare_kvsvalue(Key, Value, CausalPayload),
            AsyncPut  = fun(Node) ->
                           async_rpc_call_until_success(Node, lab4kvs_kvstore, put, [Key, KVSValue]) end,
            lists:map(AsyncPut, DownNodes),
            {ok, MergedCP, KVSValue#kvsvalue.timestamp}
    end.


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
                  case rpc_call_with_timeout(Node, Module, Func, Args, 2) of 
                      {ok, _Result} -> ok;
                      {badrpc, timeout} -> Loop()
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
        {SpawnPid, ok, Res} -> {ok, Res}
    after
        Timeout -> {badrpc, timeout}
    end.
