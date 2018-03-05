%% lab4kvs_viewmanager
%% gen_server implementing the view manager

-module(lab4kvs_viewmanager).
-behaviour(gen_server).

%% API interface
-export([start_link/3]).
-export([get_partition_id/0]).
-export([get_partition_ids/0]).
-export([get_partition_members/1]).
-export([get_key_owners/1]).
-export([get_key_partition_id/1]).
-export([view_change/2]).

-export([dump/0]).

-export([test_initstate/0]).


%% Server Callbacks
-export([init/1, terminate/2, handle_info/2, handle_cast/2, handle_call/3, code_change/3]).

%% State of the view manager 
%% partition_id = Partition id which the current node belongs to
%% partitions   = Map in the format partition_id => {list of 'node@<ip>'} 
%% tokens       = List of tuples {<hash>, partition_id} representing the virtual partitions in the view 
%%                Each partition is split in multiple tokens.
%% tokens_per_partition = number of tokens that each partition will be split into
%% replicas_per_partition = max number of replicas for each partition
%%
-record(view, {partition_id,
               partitions,
               tokens,
               tokens_per_partition,
               replicas_per_partition
              }).

%%%%% Interface %%%%%

start_link(IPPortList, TokensPerPartition, ReplicasPerPartition) ->
    %% Initialize the view from a comma separated list of ip:port
    %%   e.g. "10.0.0.20:8080,10.0.0.21:8080"
    %%
    %% The module will be registered by the main app funtion 
    %% to allow messages to be received from other nodes.
    %%
    gen_server:start_link(?MODULE, [IPPortList, TokensPerPartition, ReplicasPerPartition], []).


get_partition_id() ->
    gen_server:call(whereis(view_manager), partition_id).


get_partition_ids() ->
    gen_server:call(whereis(view_manager), partition_ids).


get_partition_members(PartitionID) ->
    gen_server:call(whereis(view_manager), {partition_members, PartitionID}).


get_key_owners(Key) ->
    %% Return the nodes of the partition that owns the key
    gen_server:call(whereis(view_manager), {keyowners, Key}).


get_key_partition_id(Key) ->
    %% Return the partition_id of the partition that owns the key
    gen_server:call(whereis(view_manager), {keyowner_partition, Key}).


view_change(Type, IPPort) when Type =:= <<"add">> orelse Type =:= <<"remove">> ->
    %% Broadcast message and redistribute keys
    Node = lab4kvs_viewutils:get_node_name(binary_to_list(IPPort)),
    gen_server:call(whereis(view_manager), {view_change, binary_to_atom(Type, latin1), Node}).


dump() ->
    %% Dump the view state
    gen_server:call(whereis(view_manager), dump).



%%%%% Server Callbacks %%%%% 

init([IPPortList, TokensPerPartition, ReplicasPerPartition]) ->
    Partitions  = lab4kvs_viewutils:gen_partitions(IPPortList, ReplicasPerPartition),
    PartitionID = lab4kvs_viewutils:get_partition_id(node(), Partitions),
    Tokens      = lab4kvs_viewutils:gen_tokens(maps:size(Partitions), TokensPerPartition),
    {ok, #view{partition_id=PartitionID,
               partitions=Partitions,
               tokens=Tokens, 
               tokens_per_partition=TokensPerPartition,
               replicas_per_partition=ReplicasPerPartition}}.


handle_call(partition_id, _From, View = #view{partition_id=PartitionID}) ->
    {reply, PartitionID, View};


handle_call(partition_ids, _From, View = #view{partitions=Partitions}) ->
    {reply, maps:keys(Partitions), View};


handle_call({partition_members, ID}, _From, View = #view{partitions=Partitions}) ->
    {reply, maps:get(ID, Partitions), View};


handle_call({keyowners, Key}, _From, View = #view{partitions=Partitions,
                                                  tokens=Tokens}) ->
    {_Hash, PartitionID} = lab4kvs_viewutils:get_key_owner_token(Key, Tokens), 
    KeyOwners = maps:get(PartitionID, Partitions),
    {reply, KeyOwners, View};


handle_call({keyowner_partition, Key}, _From, View = #view{tokens=Tokens}) ->
    {_Hash, PartitionID} = lab4kvs_viewutils:get_key_owner_token(Key, Tokens), 
    {reply, PartitionID, View};


handle_call({view_change, Type, NodeChanged}, _From, View = #view{partitions=Partitions,
                                                                  replicas_per_partition=ReplicasPerPartition}) ->
    %% lab4kvs_debug:call({view_change, Type}),
    Ref = make_ref(),
    %% Update the partitions: 
    %%   - a new partition is created iff all partitions have already K nodes
    %%   - a partition is deleted iff the node being deleted is the last one of the partition 
    NodesToBroadcast = case Type of add -> get_all_nodes(Partitions) ++ [NodeChanged];
                                    remove -> get_all_nodes(Partitions) end,
    broadcast_view_change(Type, NodeChanged, Ref, NodesToBroadcast, View), %% TODO pass UpdatedView ?
    {Scenario, NewPartitions} = lab4kvs_viewutils:update_partitions(Partitions, Type, NodeChanged, ReplicasPerPartition),
    %% Scenario = {add|add_partition|remove|remove_partition, partition_id} 
    NewView = apply_view_change(Scenario, NodeChanged, View#view{partitions=NewPartitions}),
    wait_ack_view_change(Ref, NodesToBroadcast),
    %% lab4kvs_debug:return({view_change, Type}, NewView),
    {reply, view_changed, NewView};


handle_call(dump, _From, View = #view{partition_id=PartitionID,
                                      partitions=Partitions, 
                                      tokens=Tokens, 
                                      tokens_per_partition=TokensPerPartition,
                                      replicas_per_partition=ReplicasPerPartition}) ->
    FormatStr = "VIEW ~p~n"         ++ 
                "partition_id ~p~n" ++
                "partitions   ~p~n" ++ 
                "tokens       ~p~n" ++ 
                "tok_x_part   ~p~n" ++
                "rep_x_part   ~p~n",
    Terms = io_lib:format(FormatStr, [node(),
                                      PartitionID, 
                                      lists:sort(maps:to_list(Partitions)), 
                                      lists:sort(Tokens), 
                                      TokensPerPartition,
                                      ReplicasPerPartition]),
    Reply = list_to_binary(lists:flatten(Terms)),
    {reply, Reply, View}.


broadcast_view_change(Type, NodeChanged, Ref, NodesToBroadcast, View) ->
    %% Broadcast the view change to every other node in the view.
    %% A unique identifier Ref will be used for ACK the node who issued the broadcast.
    %% This message will be handled by the handle_info callback
    %%
    SendMsg = fun(Node) -> {view_manager, Node} ! 
                           {view_change, Type, NodeChanged, {node(), Ref}, View} end,
    [SendMsg(Node) || Node <- NodesToBroadcast, Node =/= node()].


wait_ack_view_change(Ref, BroadcastedNodes) -> 
    %% Wait for an ACK from every other node in the view
    %%
    io:format("Waiting for ACK from ~p~n", [BroadcastedNodes]),
    WaitACK = fun(_Node, R) -> 
                  receive 
                      {Sender, R} -> io:format("Received ACK from ~p~n", [Sender]),
                                   ok 
                  end end,
    [WaitACK(Node, Ref) || Node <- BroadcastedNodes, Node =/= node()].


handle_info({view_change, Type, NodeChanged, {NodeToACK, Ref}, BroadcastedView}, _View) ->
    %% The message sent in broadcast_view_change will trigger this callback.
    %% Execute the view change and send an ack back.
    %%
    io:format("Received broadcast view_change ~p ~p from ~p~n", [Type, NodeChanged, NodeToACK]),
    %% New partitions added to the system don't have the View set, use the one received
    %% as part of the broadcast message (BroadcastedView)
    Partitions = BroadcastedView#view.partitions,
    ReplicasPerPartition = BroadcastedView#view.replicas_per_partition,
    {Scenario, NewPartitions} = lab4kvs_viewutils:update_partitions(Partitions, Type, NodeChanged, ReplicasPerPartition),
    %% Scenario = {add|add_partition|remove|remove_partition, partition_id} 
    NewView = apply_view_change(Scenario, NodeChanged, BroadcastedView#view{partitions=NewPartitions}),
    {view_manager, NodeToACK} ! {node(), Ref},
    io:format("ACKed broadcast view_change ~p ~p from ~p~n", [Type, NodeChanged, NodeToACK]),
    {noreply, NewView};


handle_info({debug, Pid}, View) ->
    %% Hack to do some debug
    Pid ! View, 
    {noreply, View};


handle_info(Msg, View) ->
    io:format("Unknown message: ~p~n", [Msg]),
    {noreply, View}.


apply_view_change({add, AffectedPartitionID}, NodeToInsert, View = #view{partition_id=PartitionID,
                                                                         partitions=Partitions}) ->
    %% The node has been added to an existing partition
    %% No keys should be moved among partitions
    %% The key belonging to the affected partition should be replicated in the new node
    %%
    %% Note: Partitions map has already been updated with the new node
    %%
    AffectedPartition = maps:get(AffectedPartitionID, Partitions),
    SelectedNode = select_node_ignoring(AffectedPartition, NodeToInsert),
    case PartitionID =:= AffectedPartitionID andalso node() =:= SelectedNode of
        true -> %% I belong to the affected partition and I have been 
                %% selected to replicate my keys in the new node
            EntriesToReplicate = lab4kvs_kvstore:get_all_entries(),
            rpc:call(NodeToInsert, lab4kvs_kvstore, put, [EntriesToReplicate]);
        false -> %% Nothing to do
            io:format("Node ~p replicates its key to ~p~n", [SelectedNode, NodeToInsert])
    end,
    View; 


apply_view_change({add_partition, NewPartitionID}, NodeToInsert, View = #view{partition_id=PartitionID,
                                                                              partitions=Partitions,
                                                                              tokens=Tokens,
                                                                              tokens_per_partition=TokensPerPartition}) ->
    %% The node has been added to a new partition (yet to be tokenized)
    %% The other partitions should move the keys belonging to the new partition
    %%
    %% The foldl function works like a for loop. The token list is updated
    %% at each "iteration" using the variable AccTokens. At the end, the
    %% list with all the new tokens inserted is returned.
    %%
    %% Note: Partitions map has already been updated with the new partition
    %%
    %% TODO change name to gen_partition_tokens
    TokensToInsert = lab4kvs_viewutils:gen_tokens_partition(NewPartitionID, TokensPerPartition),
    Fun = fun(Token, AccTokens) -> move_keys(Token, AccTokens, NodeToInsert, PartitionID, Partitions) end,
    UpdatedTokens = lists:foldl(Fun, Tokens, TokensToInsert),
    View#view{tokens=UpdatedTokens};


apply_view_change({remove, _AffectedPartitionID}, _NodeToDelete, View) ->
    %% The node has been removed from a partition with more than one node
    %% No keys should be moved among partitions, nothing to replicate
    %%
    %% Note: Partitions map has already been updated with the new node
    %%
    View; 


apply_view_change({remove_partition, OldPartitionID}, NodeToRemove, View = #view{tokens=Tokens}) ->
    %% The node has been deleted from a partition with a single node 
    %% The partition has to be removed and its keys transferred to the other partitions
    %% Retrieve all partition's tokens and transfer the keys to the other partitions
    %%
    UpdatedTokens = [{Hash, Partition} || {Hash, Partition} <- Tokens, Partition =/= OldPartitionID],
    case NodeToRemove =:= node() of
        true -> %% I'm being deleted, move all my keys to other partitions
            KVSEntries = lab4kvs_kvstore:get_all_entries(),
            GetKeyOwner = fun({Key, _}) -> {_H, KOwn} = lab4kvs_viewutils:get_key_owner_token(Key, UpdatedTokens), 
                                           KOwn end,
            %% TODO optimize merging distinct RPC calls to the same destination node
            %%      build a map [DestNode => [Entries..]] and call move_entries for each pair
            io:format("Moving entries..~n"),
            [move_entry(KVSEntry, GetKeyOwner(KVSEntry)) || KVSEntry <- KVSEntries],
            io:format("DONE Moving entries~n");
        false -> %% I'm not the node being deleted, nothing to do
            io:format("I'm not the node being deleted, nothing to do ~p =/= ~p ~n", [NodeToRemove, node()])
    end,
    %% Note: partitions map has already been updated
    View#view{tokens=UpdatedTokens}.


move_keys(TokenToInsert = {Hash, DestPartition}, Tokens, DestNode, PartitionID, Partitions) ->
    %% Given a new token and the list of current tokens, move some of the keys
    %% to the new node (DestNode) in the new partition (DestPartition).
    %%
    lab4kvs_debug:call(move_keys_add),
    {PrevHash, _PrevPartition} = lab4kvs_viewutils:get_prev(TokenToInsert, Tokens),
    {_NextHash, NextPartition} = lab4kvs_viewutils:get_next(TokenToInsert, Tokens),
    SelectedNode = select_node(maps:get(PartitionID, Partitions)),
    case NextPartition =:= PartitionID andalso DestPartition =/= PartitionID andalso 
         SelectedNode  =:= node() of
        true  -> %% The keys to be moved to the new partition belong to my partition
                 %% I have been selected to move keys to the new virtual partition
            io:format("Moving keys in range ~p to ~p~n", [{PrevHash+1, Hash}, DestNode]),
            move_keyrange({PrevHash + 1, Hash}, DestNode);
        false -> %% Not my responsibility, nothing to do
            io:format("Keys are not mine or I have not been seleceted, continue")
    end,
    lab4kvs_debug:return(move_keys_add, Tokens ++ [TokenToInsert]),
    %% Update the token list to be used at the next iteration in foldl
    lab4kvs_viewutils:insert_token(TokenToInsert, Tokens).


move_keyrange({Start, End}, DestNode) ->
    %% Move the KVS entries in the range {Start, End} to DestNode, deleting the local copy
    %%
    lab4kvs_debug:call(move_keyrange),
    KVSRangeEntries = lab4kvs_kvstore:get_keyrange_entries(Start, End),
    io:format("KVSRange ~p~n", [KVSRangeEntries]),
    move_entries(KVSRangeEntries, DestNode),
    lab4kvs_debug:return(move_keyrange, nostate).


move_entries([], _DestNode) -> ok;
move_entries(KVSEntries, DestNode) when is_list(KVSEntries) ->
    %% RPC to destnode to insert the list of entries
    %% local call to delete the local copy of the entries
    %%
    io:format("Moving entries ~p to ~p~n", [KVSEntries, DestNode]),
    %% Move the entries to the new node
    rpc:call(DestNode, lab4kvs_kvstore, put, [KVSEntries]),
    %% Delete the local copy
    lab4kvs_kvstore:delete_list([Key || {Key, _} <- KVSEntries]),
    io:format("Moved  entries ~p to ~p~n", [KVSEntries, DestNode]).

move_entry({Key, {Val, Hash}}, DestNode) ->
    %% RPC to destnode to insert the key, local call to delete the key
    %%
    io:format("Moving entry ~p to ~p~n", [{Key, {Val, Hash}}, DestNode]),
    rpc:call(DestNode, lab4kvs_kvstore, put, [Key, Val, Hash]),
    lab4kvs_kvstore:delete(Key),
    io:format("Moved  entry ~p to ~p~n", [{Key, {Val, Hash}}, DestNode]).


get_all_nodes(Partitions) ->
    lists:append(maps:values(Partitions)).


select_node_ignoring(PartitionNodes, NodeToIgnore) when is_list(PartitionNodes) ->
    select_node(PartitionNodes -- [NodeToIgnore]).


select_node(PartitionNodes) when is_list(PartitionNodes) ->
    %% Return the node in the partition with the smaller IP address
    %% TODO handle case of nodes unavailable in the partition
    lists:min(PartitionNodes).


handle_cast(Msg, State) ->
    io:format("Unknown message: ~p~n", [Msg]),
    {noreply, State}.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


terminate(_Reason, _State) ->
    ok.


%%%%%%%% TESTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

dump(Data) ->
    io:format("~p~n", [Data]).

%% TODO update tests
test_initstate() ->
    %% net_kernel:start(['node@10.0.0.20', longnames]),
    TokensPerPartition = 3,
    K = 4,
    IPPortListStr = "10.0.0.20:8080,10.0.0.21:8080,10.0.0.22:8080," ++
                    "10.0.0.23:8080,10.0.0.24:8080,10.0.0.25:8080",
    {ok, Pid} = lab4kvs_viewmanager:start_link(IPPortListStr, TokensPerPartition, K),
    Pid ! {debug, self()},
    receive
        View -> dump(View)
    end.

