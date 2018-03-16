%% lab4kvs_viewmanager
%% gen_server implementing the view manager

-module(lab4kvs_viewmanager).
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
    Node = lab4kvs_viewutils:get_node_name(binary_to_list(IPPort)),
    gen_server:call(whereis(view_manager), {view_change, Type, Node}).


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


handle_call(num_partitions, _From, View = #view{partitions=Partitions}) ->
    {reply, maps:size(Partitions), View};


handle_call(partition_id, _From, View = #view{partition_id=PartitionID}) ->
    {reply, PartitionID, View};


handle_call(partition_ids, _From, View = #view{partitions=Partitions}) ->
    {reply, maps:keys(Partitions), View};


handle_call({partition_members, ID}, _From, View = #view{partitions=Partitions}) ->
    {reply, maps:get(ID, Partitions), View};


handle_call({keyowner, Key}, _From, View = #view{tokens=Tokens}) ->
    {_Hash, PartitionID} = lab4kvs_viewutils:get_key_owner_token(Key, Tokens), 
    {reply, PartitionID, View};


handle_call({keyowner_partition, Key}, _From, View = #view{tokens=Tokens}) ->
    {_Hash, PartitionID} = lab4kvs_viewutils:get_key_owner_token(Key, Tokens), 
    {reply, PartitionID, View};


handle_call(all_nodes, _From, View = #view{partitions=Partitions}) ->
    Nodes = get_all_nodes(Partitions), 
    {reply, Nodes, View};


handle_call({view_change, Type, NodeChanged}, _From, View = #view{partitions=Partitions}) ->
    %% Update the partitions: 
    %%   - a new partition is created iff all partitions have already K nodes
    %%   - a partition is deleted iff the node being deleted is the last one of the partition 
    %%     or two partitions can be merged
    %%
    %% lab4kvs_debug:call({view_change, Type}),
    Ref = make_ref(),
    NodesToBroadcast = case Type of add -> get_all_nodes(Partitions) ++ [NodeChanged];
                                 remove -> get_all_nodes(Partitions) -- [NodeChanged] end,

    broadcast_view_change(Type, NodeChanged, Ref, NodesToBroadcast, View),
    NewView = apply_view_change(Type, NodeChanged, View),
    wait_ack_view_change(Ref, NodesToBroadcast),

    Reply = case Type of
                add ->  %% On add, it should return the partition 
                        %% id of the new node
                    lab4kvs_viewutils:get_partition_id(NodeChanged, 
                                                       NewView#view.partitions);
                remove ->
                    ok
            end,
    %% lab4kvs_debug:return({view_change, Type}, NewView),
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

    ID = lab4kvs_viewutils:get_partition_id(node(), BroadcastedView#view.partitions),
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
    %% TODO disconnected nodes won't reply, but at least one node
    %% per partition has to reply
    io:format("Waiting for ACK from ~p~n", [BroadcastedNodes]),
    WaitACK = fun(_Node, R) -> 
                  receive 
                      {Sender, R} -> io:format("Received ACK from ~p~n", [Sender]),
                                   ok 
                  end end,
    [WaitACK(Node, Ref) || Node <- BroadcastedNodes, Node =/= node()].


apply_view_change(Type, NodeChanged, View) ->
    Partitions = View#view.partitions,
    ReplicasPerPartition = View#view.replicas_per_partition,
    Ops = lab4kvs_viewutils:get_transformation_ops(Type, NodeChanged, Partitions, ReplicasPerPartition),
    io:format("TRANSF OPS ~p~n", [Ops]),
    %% Apply all operations sequentially, each operation is in the format
    %% Op = {add|add_partition|remove|remove_partition, node, partition_id} 
    %% apply_view_change_op takes an Op and the current View and return
    %% the resulting view, used in the next foldl iteration
    NewView = lists:foldl(fun apply_view_change_op/2, View, Ops),
    %% Make sure the partition ID is updated
    ID = lab4kvs_viewutils:get_partition_id(node(), NewView#view.partitions),
    NewView#view{partition_id=ID}.


apply_view_change_op(_Op={add, NodeToInsert, AffectedPartitionID}, View = #view{partition_id=PartitionID,
                                                                                partitions=Partitions}) ->
    %% The node has been added to an existing partition
    %% No keys should be moved among partitions
    %% The keys belonging to the affected partition should be replicated in the new node
    %%
    AffectedPartition = maps:get(AffectedPartitionID, Partitions),
    %% TODO remove
    SelectedNode = select_node(AffectedPartition),
    case PartitionID =:= AffectedPartitionID andalso node() =:= SelectedNode of
        true -> %% I belong to the affected partition and I have been 
                %% selected to replicate my keys in the new node
            EntriesToReplicate = lab4kvs_kvstore:get_all_entries(),
            rpc:call(NodeToInsert, lab4kvs_kvstore, put_list, [EntriesToReplicate]);
        false -> %% Nothing to do
            io:format("Node ~p replicates its key to ~p~n", [SelectedNode, NodeToInsert])
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
    TokensToInsert = lab4kvs_viewutils:gen_tokens_partition(NewPartitionID, TokensPerPartition),
    Fun = fun(Token, AccTokens) -> move_keys(Token, AccTokens, NodeToInsert, PartitionID, Partitions) end,
    NewPartitions = maps:put(NewPartitionID, [NodeToInsert], Partitions),
    UpdatedTokens = lists:foldl(Fun, Tokens, TokensToInsert),
    View#view{partitions=NewPartitions,tokens=UpdatedTokens};


apply_view_change_op(_Op={remove, NodeToRemove, AffectedPartitionID}, View = #view{partitions=Partitions}) ->
    %% The node has been removed from a partition with more than one node
    %% No keys should be moved among partitions, nothing to replicate
    %%
    OldNodes = maps:get(AffectedPartitionID, Partitions),
    NewNodes = OldNodes -- [NodeToRemove],
    NewPartitions = maps:put(AffectedPartitionID, NewNodes, Partitions),
    View#view{partitions=NewPartitions}; 


apply_view_change_op(_Op={remove_partition, NodeToRemove, OldPartitionID}, View = #view{partition_id=PartitionID,
                                                                                        partitions=Partitions,
                                                                                        tokens=Tokens}) ->
    %% Under the assumption that a partition with a single node never dies,
    %% this can only happen before a merge.  If the current node belongs
    %% to the merged partition, it has to redistribute its own keys
    %% where they belong to in the new configuration.
    %%
    UpdatedTokens = [{Hash, Partition} || {Hash, Partition} <- Tokens, Partition =/= OldPartitionID],
    case OldPartitionID =:= PartitionID of
        true -> %% I'm being merged and my old partition deleted,
                %% move all my keys to other partitions
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
    NewPartitions = maps:remove(OldPartitionID, Partitions),
    View#view{partitions=NewPartitions, tokens=UpdatedTokens}.


move_keys(TokenToInsert = {Hash, DestPartition}, Tokens, DestNode, PartitionID, Partitions) ->
    %% Given a new token and the list of current tokens, move some of the keys
    %% to the new node (DestNode) in the new partition (DestPartition).
    %%
    lab4kvs_debug:call(move_keys_add),
    {PrevHash, _PrevPartition} = lab4kvs_viewutils:get_prev(TokenToInsert, Tokens),
    {_NextHash, NextPartition} = lab4kvs_viewutils:get_next(TokenToInsert, Tokens),
    %% SourceNodes = maps:get(PartitionID, Partitions),
    %% SelectedNode = select_node(SourceNodes),
    case NextPartition =:= PartitionID andalso DestPartition =/= PartitionID of
        true  -> %% The keys to be moved to the new partition belong to my partition
            io:format("Moving keys in range ~p to ~p~n", [{PrevHash+1, Hash}, DestNode]),
            move_keyrange({PrevHash + 1, Hash}, DestNode);
        false -> %% Not my responsibility, nothing to do
            io:format("Keys are not mine or I have not been seleceted, continue")
    end,
    lab4kvs_debug:return({move_keys_add, Tokens ++ [TokenToInsert]}),
    %% Update the token list to be used at the next iteration in foldl
    lab4kvs_viewutils:insert_token(TokenToInsert, Tokens).


move_keyrange({Start, End}, DestNode) ->
    %% Move the KVS entries in the range {Start, End} to DestNode, deleting the local copy
    %%
    lab4kvs_debug:call(move_keyrange),
    KVSRangeEntries = lab4kvs_kvstore:get_keyrange_entries(Start, End),
    io:format("KVSRange ~p~n", [KVSRangeEntries]),
    move_entries(KVSRangeEntries, DestNode),
    lab4kvs_debug:return({move_keyrange, nostate}).


move_entries([], _DestNode) -> ok;
move_entries(KVSEntries, DestNode) when is_list(KVSEntries) ->
    %% RPC to destnode to insert the list of entries
    %% local call to delete the local copy of the entries
    %%
    io:format("Moving entries ~p to ~p~n", [KVSEntries, DestNode]),
    %% Move the entries to the new node
    rpc:call(DestNode, lab4kvs_kvstore, put_list, [KVSEntries]),
    %% Delete the local copy
    lab4kvs_kvstore:delete_list([Key || {Key, _} <- KVSEntries]),
    io:format("Moved  entries ~p to ~p~n", [KVSEntries, DestNode]).

move_entry({Key, KVSValue}, DestNode) ->
    %% RPC to destnode to insert the key, local call to delete the key
    %%
    io:format("Moving entry ~p to ~p~n", [{Key, KVSValue}, DestNode]),
    rpc:call(DestNode, lab4kvs_kvstore, put, [Key, KVSValue]),
    lab4kvs_kvstore:delete(Key),
    io:format("Moved entry ~p to ~p~n", [{Key, KVSValue}, DestNode]).


get_all_nodes(Partitions) ->
    lists:append(maps:values(Partitions)).


%% select_node_ignoring(PartitionNodes, NodeToIgnore) when is_list(PartitionNodes) ->
    %% select_node(PartitionNodes -- [NodeToIgnore]).


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

