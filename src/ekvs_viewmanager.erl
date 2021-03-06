%% ekvs_viewmanager
%% gen_server implementing the view manager

-module(ekvs_viewmanager).
-behaviour(gen_server).

%% API interface
-export([start_link/3]).
-export([get_num_partitions/0]).
-export([get_partition_id/0]).
-export([get_partition_ids/0]).
-export([get_partition_members/1]).
-export([get_key_owner_id/1]).
-export([get_key_partition_id/1]).
-export([get_all_nodes/0]).
-export([view_change/2]).
-export([dump/0]).

%% Util functions
-export([get_all_nodes/1]).

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


get_num_partitions() ->
    gen_server:call(whereis(view_manager), num_partitions).


get_partition_id() ->
    gen_server:call(whereis(view_manager), partition_id).


get_partition_ids() ->
    gen_server:call(whereis(view_manager), partition_ids).


get_partition_members(PartitionID) ->
    gen_server:call(whereis(view_manager), {partition_members, PartitionID}).


get_key_owner_id(Key) ->
    %% Return the ID of the partition that owns the key
    gen_server:call(whereis(view_manager), {keyowner, Key}).


get_key_partition_id(Key) ->
    %% Return the partition_id of the partition that owns the key
    gen_server:call(whereis(view_manager), {keyowner_partition, Key}).

get_all_nodes() ->
    %% Return a list of all nodes in the partitions 
    gen_server:call(whereis(view_manager), all_nodes).

view_change(Type, IPPort) when Type =:= add orelse Type =:= remove ->
    %% Broadcast message and redistribute keys.
    %% On add, return the partition id of the new node
    Node = ekvs_viewutils:get_node_name(binary_to_list(IPPort)),
    gen_server:call(whereis(view_manager), {view_change, Type, Node}).


dump() ->
    %% Dump the view state
    gen_server:call(whereis(view_manager), dump).



%%%%% Server Callbacks %%%%% 

init([IPPortList, TokensPerPartition, ReplicasPerPartition]) ->
    Partitions  = ekvs_viewutils:gen_partitions(IPPortList, ReplicasPerPartition),
    PartitionID = ekvs_viewutils:get_partition_id(node(), Partitions),
    Tokens      = ekvs_viewutils:gen_tokens(maps:size(Partitions), TokensPerPartition),
    {ok, #view{partition_id=PartitionID,
               partitions=Partitions,
               tokens=Tokens, 
               tokens_per_partition=TokensPerPartition,
               replicas_per_partition=ReplicasPerPartition}}.


handle_call(num_partitions, _From, View = #view{partitions=Partitions}) ->
    {reply, maps:size(Partitions), View};


handle_call(partition_id, _From, View = #view{partition_id=PartitionID}) ->
    {reply, PartitionID, View};


handle_call(partition_ids, _From, View = #view{partitions=Partitions}) ->
    {reply, maps:keys(Partitions), View};


handle_call({partition_members, ID}, _From, View = #view{partitions=Partitions}) ->
    {reply, maps:get(ID, Partitions), View};


handle_call({keyowner, Key}, _From, View = #view{tokens=Tokens}) ->
    {_Hash, PartitionID} = ekvs_viewutils:get_key_owner_token(Key, Tokens), 
    {reply, PartitionID, View};


handle_call({keyowner_partition, Key}, _From, View = #view{tokens=Tokens}) ->
    {_Hash, PartitionID} = ekvs_viewutils:get_key_owner_token(Key, Tokens), 
    {reply, PartitionID, View};


handle_call(all_nodes, _From, View = #view{partitions=Partitions}) ->
    Nodes = get_all_nodes(Partitions), 
    {reply, Nodes, View};


handle_call({view_change, Type, NodeChanged}, _From, View) ->
    %% Update the partitions: 
    %%   - a new partition is created iff all partitions have already K nodes
    %%   - a partition is deleted iff the node being deleted is the last one of the partition 
    %%     or two partitions can be merged
    %%
    %% ekvs_debug:call({view_change, Type}),
    Ref = make_ref(),
    NodesToBroadcast = get_node_to_broadcast(Type, NodeChanged, View),

    broadcast_view_change(Type, NodeChanged, Ref, NodesToBroadcast, View),
    NewView = apply_view_change(Type, NodeChanged, View),
    wait_ack_view_change(Ref, NodesToBroadcast),

    Reply = case Type of
                add ->  %% On add, it should return the partition 
                        %% id of the new node
                    ekvs_viewutils:get_partition_id(NodeChanged, 
                                                       NewView#view.partitions);
                remove ->
                    ok
            end,
    %% ekvs_debug:return({view_change, Type}, NewView),
    {reply, Reply, NewView};


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


handle_info({view_change, Type, NodeChanged, {NodeToACK, Ref}, BroadcastedView}, _View) ->
    %% The message sent in broadcast_view_change will trigger this callback.
    %% Execute the view change and send an ack back.
    %% New partitions added to the system don't have the View set, use the one received
    %% as part of the broadcast message (BroadcastedView)
    %%
    io:format("Received broadcast view_change ~p ~p from ~p~n", [Type, NodeChanged, NodeToACK]),
    io:format("BroadcastedView~n~p~n", [BroadcastedView]),

    ID = ekvs_viewutils:get_partition_id(node(), BroadcastedView#view.partitions),
    View = BroadcastedView#view{partition_id=ID},
    NewView = apply_view_change(Type, NodeChanged, View),

    %% Send the ACK back to the node who broadcasted the view_change
    {view_manager, NodeToACK} ! {node(), Ref},
    io:format("ACKed broadcast view_change ~p ~p from ~p~n", [Type, NodeChanged, NodeToACK]),
    io:format("View after view_change ~p~n", [NewView]),
    {noreply, NewView};


handle_info({debug, Pid}, View) ->
    %% Hack to do some debug
    Pid ! View, 
    {noreply, View};


handle_info(Msg, View) ->
    io:format("Unknown message: ~p~n", [Msg]),
    {noreply, View}.


get_node_to_broadcast(Type, NodeChanged, #view{partitions=Partitions,
                                               replicas_per_partition=K}) ->
    AllNodes = get_all_nodes(Partitions),
    case Type of 
       add    -> AllNodes ++ [NodeChanged];
       remove -> 
           Ops = ekvs_viewutils:get_transformation_ops(Type, NodeChanged, Partitions, K),
           case Ops of
               [{remove_partition, _, _}] -> AllNodes;
                                         _ -> AllNodes -- [NodeChanged] 
           end
    end.


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
                  after
                      2000 -> ok
                  end end,
    [WaitACK(Node, Ref) || Node <- BroadcastedNodes, Node =/= node()].


apply_view_change(Type, NodeChanged, View) ->
    Partitions = View#view.partitions,
    ReplicasPerPartition = View#view.replicas_per_partition,
    Ops = ekvs_viewutils:get_transformation_ops(Type, NodeChanged, Partitions, ReplicasPerPartition),
    io:format("TRANSF OPS ~p~n", [Ops]),
    %% Apply all operations sequentially, each operation is in the format
    %% Op = {add|add_partition|remove|remove_partition, node, partition_id} 
    %% apply_view_change_op takes an Op and the current View and return
    %% the resulting view, used in the next foldl iteration
    NewView = lists:foldl(fun apply_view_change_op/2, View, Ops),
    %% Update the VC of the current node
    ekvs_vcmanager:view_change(get_all_nodes(NewView#view.partitions)),
    %% Make sure the partition ID is updated
    ID = ekvs_viewutils:get_partition_id(node(), NewView#view.partitions),
    NewView#view{partition_id=ID}.


apply_view_change_op(_Op={add, NodeToInsert, AffectedPartitionID}, View = #view{partition_id=PartitionID,
                                                                                partitions=Partitions}) ->
    %% The node has been added to an existing partition
    %% No keys should be moved among partitions
    %% The keys belonging to the affected partition should be replicated in the new node
    %%
    AffectedPartition = maps:get(AffectedPartitionID, Partitions),
    case PartitionID =:= AffectedPartitionID of %% andalso node() =:= SelectedNode of
        true -> %% I belong to the affected partition and I have been 
                %% selected to replicate my keys in the new node
            io:format("I belong to the affected partition and I have been selected to replicate my keys in the new node ~p~n", [NodeToInsert]),
            EntriesToReplicate = ekvs_kvstore:get_all_entries(),
            io:format("Node ~p replicates its entries ~p to ~p~n", [node(), EntriesToReplicate, NodeToInsert]),
            Res = rpc:call(NodeToInsert, ekvs_kvstore, put_list, [EntriesToReplicate]),
            io:format("RES ~p~n", [Res]),
            Replicated = rpc:call(NodeToInsert, ekvs_kvstore, get_all_entries, []),
            io:format("~p~n", [Replicated]);
        false -> %% Nothing to do
            io:format("Node ~p replicates its key to ~p~n", [PartitionID, NodeToInsert])
    end,
    NewPartitions = maps:put(AffectedPartitionID, AffectedPartition ++ [NodeToInsert], Partitions),
    View#view{partitions=NewPartitions}; 


apply_view_change_op(_Op={add_partition, NodeToInsert, NewPartitionID}, View = #view{partition_id=PartitionID,
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
    TokensToInsert = ekvs_viewutils:gen_tokens_partition(NewPartitionID, TokensPerPartition),
    Fun = fun(Token, AccTokens) -> move_keys(Token, AccTokens, NodeToInsert, PartitionID) end,
    NewPartitions = maps:put(NewPartitionID, [NodeToInsert], Partitions),
    UpdatedTokens = lists:foldl(Fun, Tokens, TokensToInsert),
    View#view{partitions=NewPartitions,tokens=UpdatedTokens};


apply_view_change_op(_Op={remove, NodeToRemove, AffectedPartitionID}, View = #view{partitions=Partitions,
                                                                                   tokens=Tokens}) ->
    %% The node has been removed from a partition with more than one node
    %% No keys should be moved among partitions, nothing to replicate
    %%
    OldNodes = maps:get(AffectedPartitionID, Partitions),
    NewNodes = OldNodes -- [NodeToRemove],
    case length(NewNodes) of 
        0 ->  %% Last node in the partition, remove the partition and all its tokens
            NewPartitions = maps:remove(AffectedPartitionID, Partitions),
            UpdatedTokens = [{Hash, Partition} || {Hash, Partition} <- Tokens, Partition =/= AffectedPartitionID],
            View#view{partitions=NewPartitions, tokens=UpdatedTokens};
        _ -> 
            NewPartitions = maps:put(AffectedPartitionID, NewNodes, Partitions),
            View#view{partitions=NewPartitions}
    end;

apply_view_change_op(_Op={remove_partition, NodeToRemove, OldPartitionID}, View = #view{partitions=Partitions,
                                                                                        tokens=Tokens}) ->
    %% The node has been deleted from a partition with a single node 
    %% The partition has to be removed and its keys transferred to the other partitions
    %% Retrieve all partition's tokens and transfer the keys to the other partitions
    %%
    UpdatedTokens = [{Hash, Partition} || {Hash, Partition} <- Tokens, Partition =/= OldPartitionID],
    case NodeToRemove =:= node() of
        true -> %% I'm being deleted, move all my keys to other partitions
            KVSEntries = ekvs_kvstore:get_all_entries(),
            GetPartID = fun({Key, _}) -> {_H, PartID} = ekvs_viewutils:get_key_owner_token(Key, UpdatedTokens), 
                                          PartID end,
            io:format("Moving ~p entries..~n", [length(KVSEntries)]),
            AssignEntry = fun(Entry, Map) ->
                                  ID = GetPartID(Entry),
                                  case maps:find(ID, Map) of
                                      {ok, Entries} ->
                                          maps:put(ID, Entries ++ [Entry], Map);
                                      error ->
                                          maps:put(ID, [Entry], Map)
                                  end
                          end,
            PartIDKVSEntries = lists:foldl(AssignEntry, maps:new(), KVSEntries),
            IDEntriesList = maps:to_list(PartIDKVSEntries),
            IDToNodes = fun({ID, Entries}) -> {maps:get(ID, Partitions), Entries} end,
            DestNodesEntries = lists:map(IDToNodes, IDEntriesList),
            [move_entries_to(Entries, DestNodes) || {DestNodes, Entries} <- DestNodesEntries],
            io:format("Moved  ~p entries..~n", [length(KVSEntries)]);
        false -> %% I'm not the node being deleted, nothing to do
            io:format("I'm not the node being deleted, nothing to do ~p =/= ~p ~n", [NodeToRemove, node()])
    end,
    NewPartitions = maps:remove(OldPartitionID, Partitions),
    View#view{partitions=NewPartitions, tokens=UpdatedTokens};


apply_view_change_op(_Op={move_keys_merged_partition, FromID, ToID}, View = #view{partition_id=PartitionID,
                                                                                    partitions=Partitions,
                                                                                    tokens=Tokens}) ->
    %% Under the assumption that a partition with a single node never dies,
    %% this can only happen before a merge.  If the current node belongs
    %% to the merged partition, it has to redistribute its own keys
    %% where they belong to in the new configuration.
    %%
    UpdatedTokens = [{Hash, Partition} || {Hash, Partition} <- Tokens, Partition =/= FromID],
    case PartitionID of
        FromID -> %% I'm being merged and my old partition deleted,
                  %% move all my keys to other partitions
            KVSEntries = ekvs_kvstore:get_all_entries(),
            GetKeyOwners = fun({Key, _}) -> {_H, PartID} = ekvs_viewutils:get_key_owner_token(Key, UpdatedTokens), 
                                            maps:get(PartID, Partitions) end,
            io:format("Moving entries..~n"),
            [move_entry_to(KVSEntry, GetKeyOwners(KVSEntry)) || KVSEntry <- KVSEntries],
            io:format("DONE Moving entries~n"),
            %% Send the message to all nodes in ToID partition 
            send_moved_keys_merged_to(maps:get(ToID, Partitions));
        ToID ->  %% Wait for the message sent by nodes in the FromID partition
            recv_moved_keys_merged_from(maps:get(FromID, Partitions));
        _    ->  %% I'm not the node being deleted, nothing to do
            io:format("I do not belong to the merged partition, nothing to do ~p~n", [node()])
    end,
    View.


send_moved_keys_merged_to(Nodes) ->
    io:format("Sending MOVED_KEYS_MERGED to ~p~n", [Nodes]),
    SendMsg = fun(Node) -> {view_manager, Node} ! {moved_keys_merged, node()} end,
    [SendMsg(Node) || Node <- Nodes, Node =/= node()].
    

recv_moved_keys_merged_from(Nodes) ->
    io:format("Waiting MOVED_KEYS_MERGED from ~p~n", [Nodes]),
    RecvMsg = fun(_Node) ->
                  receive 
                    {moved_keys_merged, Sender} -> 
                          io:format("Received moved_keys_merged from ~p~n", [Sender])
                  end end,
    [RecvMsg(Node) || Node <- Nodes, Node =/= node()].


move_keys(TokenToInsert = {Hash, DestPartition}, Tokens, DestNode, PartitionID) ->
    %% Given a new token and the list of current tokens, move some of the keys
    %% to the new node (DestNode) in the new partition (DestPartition).
    %%
    ekvs_debug:call(move_keys_add),
    {PrevHash, _PrevPartition} = ekvs_viewutils:get_prev(TokenToInsert, Tokens),
    {_NextHash, NextPartition} = ekvs_viewutils:get_next(TokenToInsert, Tokens),
    case NextPartition =:= PartitionID andalso DestPartition =/= PartitionID of
        true  -> %% The keys to be moved to the new partition belong to my partition
            io:format("Moving keys in range ~p to ~p~n", [{PrevHash+1, Hash}, DestNode]),
            move_keyrange({PrevHash + 1, Hash}, DestNode);
        false -> %% Not my responsibility, nothing to do
            io:format("Keys are not mine or I have not been seleceted, continue")
    end,
    ekvs_debug:return({move_keys_add, Tokens ++ [TokenToInsert]}),
    %% Update the token list to be used at the next iteration in foldl
    ekvs_viewutils:insert_token(TokenToInsert, Tokens).


move_keyrange({Start, End}, DestNode) ->
    %% Move the KVS entries in the range {Start, End} to DestNode, deleting the local copy
    %%
    ekvs_debug:call(move_keyrange),
    KVSRangeEntries = ekvs_kvstore:get_keyrange_entries(Start, End),
    io:format("KVSRange ~p~n", [KVSRangeEntries]),
    move_entries(KVSRangeEntries, DestNode),
    ekvs_debug:return({move_keyrange, nostate}).


move_entries_to([], _DestNodes) -> ok;
move_entries_to(KVSEntries, DestNodes) -> 
    MoveEntries = fun(Node) -> move_entries(KVSEntries, Node) end,
    lists:map(MoveEntries, DestNodes).


move_entries([], _DestNode) -> ok;
move_entries(KVSEntries, DestNode) when is_list(KVSEntries) ->
    %% RPC to destnode to insert the list of entries
    %% local call to delete the local copy of the entries
    %%
    io:format("Moving entries ~p to ~p~n", [KVSEntries, DestNode]),
    %% Move the entries to the new node
    rpc:call(DestNode, ekvs_kvstore, put_list, [KVSEntries]),
    %% Delete the local copy
    ekvs_kvstore:delete_list([Key || {Key, _} <- KVSEntries]),
    io:format("Moved  entries ~p to ~p~n", [KVSEntries, DestNode]).


move_entry_to({Key, KVSValue}, DestNodes) ->
    %% RPC to destnodes to insert the key, local call to delete the key
    %%
    io:format("Moving entry ~p to ~p~n", [{Key, KVSValue}, DestNodes]),
    Move = fun(DestNode) ->
            Res = rpc:call(DestNode, ekvs_kvstore, put, [Key, KVSValue]),
            io:format("~p~n", [Res]),
            {ok, _, _} = Res,
            ekvs_kvstore:delete(Key) end,
    lists:map(Move, DestNodes),
    io:format("Moved entry ~p to ~p~n", [{Key, KVSValue}, DestNodes]).


get_all_nodes(Partitions) ->
    lists:append(maps:values(Partitions)).


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

test_initstate() ->
    %% net_kernel:start(['node@10.0.0.20', longnames]),
    TokensPerPartition = 3,
    K = 4,
    IPPortListStr = "10.0.0.20:8080,10.0.0.21:8080,10.0.0.22:8080," ++
                    "10.0.0.23:8080,10.0.0.24:8080,10.0.0.25:8080",
    {ok, Pid} = ekvs_viewmanager:start_link(IPPortListStr, TokensPerPartition, K),
    Pid ! {debug, self()},
    receive
        View -> dump(View)
    end.

