%% lab4kvs_kvsquery
%%
%% This module runs the query in the distributed system.
%% exec(Function, Args) is the only exported function.

-module(lab4kvs_kvsquery).

-export([exec/2]).


exec(Func, Args) ->
    %% Execute a query on the kvs
    lab4kvs_debug:call({exec, Func, Args}),

    Key = case Args of [K,_] -> K; [K] -> K end,
    
    KeyPartitionID = lab4kvs_viewmanager:get_key_owner_id(Key),
    PartitionNodes = lab4kvs_viewmanager:get_partition_members(KeyPartitionID),

    io:format("Key owned by partition ~p~n", [KeyPartitionID]),
    
    %% The query has to be executed in all the nodes of the partition
    Result = run_kvs_query_at(PartitionNodes, Func, Args),
    %% TODO Result = ok
    {Result, KeyPartitionID}.


run_kvs_query_at(_Nodes, _Func, _Args) ->
    %% TODO
    ok.


local_call(Module, Func, Args) ->
    %% Execute a local call to Module:Func(Args)
    apply(Module, Func, Args).


rpc_call_until_success(Node, Module, Func, Args) ->
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
