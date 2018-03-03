%% lab4kvs_viewmanager
%% gen_server implementing the view manager

-module(lab4kvs_viewmanager).
-behaviour(gen_server).

%% API interface
-export([start_link/2]).
-export([get_key_owner/1]).
-export([view_change/2]).

-export([dump/0]).

-export([test_initstate/0]).


%% Server Callbacks
-export([init/1, terminate/2, handle_info/2, handle_cast/2, handle_call/3, code_change/3]).

%% State of the view manager 
%% nodes  => List of atoms  'node@<ip>' representing the nodes in the view 
%% tokens => List of tuples {<hash>, 'node@<ip>'} representing the tokens in the view 
%%           Each node is split in multiple tokens.
%% tokens_per_node => number of tokens that each node will be split into
-record(view, {nodes,
               tokens,
               tokens_per_node
              }).

%%%%% Interface %%%%%

start_link(IPPortList, TokensPerNode) ->
    %% Initialize the view from a comma separated list of ip:port
    %%   e.g. "10.0.0.20:8080,10.0.0.21:8080"
    %%
    %% The module will be registered by the main app funtion 
    %% to allow messages to be received from other nodes.
    %%
    %% gen_server:start_link({local, ?MODULE}, ?MODULE, [IPPortList, TokensPerNode], []).
    gen_server:start_link(?MODULE, [IPPortList, TokensPerNode], []).

get_key_owner(Key) ->
    %% Return the name@ip of the node that owns the key
    gen_server:call(whereis(view_manager), {keyowner, Key}).

view_change(Type, IPPort) when Type =:= <<"add">> orelse Type =:= <<"remove">> ->
    %% Broadcast message and redistribute keys
    Node = lab4kvs_viewutils:gen_node(binary_to_list(IPPort)),
    gen_server:call(whereis(view_manager), {view_change, binary_to_atom(Type, latin1), Node}).

dump() ->
    %% Dump the view state
    gen_server:call(whereis(view_manager), dump).



%%%%% Server Callbacks %%%%% 

init([IPPortList, TokensPerNode]) ->
    Nodes  = lab4kvs_viewutils:gen_nodes(IPPortList),
    Tokens = lab4kvs_viewutils:gen_tokens(IPPortList, TokensPerNode),
    {ok, #view{nodes=Nodes, 
               tokens=Tokens, 
               tokens_per_node=TokensPerNode}}.


handle_call({keyowner, Key}, _From, View = #view{tokens=Tokens}) ->
    {_Hash, KeyOwner} = lab4kvs_viewutils:get_key_owner(Key, Tokens), 
    {reply, KeyOwner, View};


handle_call({view_change, Type, NodeChanged}, _From, View = #view{nodes=Nodes}) ->
    lab4kvs_debug:call({view_change, Type}),
    Ref = make_ref(),
    %% In case we are adding a node to the view
    %% that node must be included in the broadcast
    NodesToBroadcast = case Type of add -> Nodes ++ [NodeChanged];
                                    remove -> Nodes end,
    broadcast_view_change(Type, NodeChanged, Ref, NodesToBroadcast, View),
    NewView = apply_view_change(Type, NodeChanged, View),  
    wait_ack_view_change(Ref, NodesToBroadcast),
    lab4kvs_debug:return({view_change, Type}, NewView),
    {reply, view_changed, NewView};


handle_call(dump, _From, View = #view{nodes=Nodes, 
                                      tokens=Tokens, 
                                      tokens_per_node=TokensPerNode}) ->
    FormatStr = "VIEW ~p~n" ++ 
                "nodes  ~p~n" ++ 
                "tokens ~p~n" ++ 
                "tpn    ~p~n",
    Terms = io_lib:format(FormatStr, [node(), lists:sort(Nodes), lists:sort(Tokens), TokensPerNode]),
    Reply = list_to_binary(lists:flatten(Terms)),
    {reply, Reply, View}.


broadcast_view_change(Type, NodeChanged, Ref, Nodes, View) ->
    %% Broadcast the view change to every other node in the view.
    %% A unique identifier Ref will be used for ACK the node who issued the broadcast.
    %% This message will be handled by the handle_info callback
    %%
    SendMsg = fun(Node) -> {view_manager, Node} ! 
                           {view_change, Type, NodeChanged, {node(), Ref}, View} end,
    [SendMsg(Node) || Node <- Nodes, Node =/= node()].


wait_ack_view_change(Ref, Nodes) -> 
    %% Wait for an ACK from every other node in the view
    %%
    io:format("Waiting for ACK from ~p~n", [Nodes]),
    WaitACK = fun(_Node, R) -> 
                  receive 
                      {Sender, R} -> io:format("Received ACK from ~p~n", [Sender]),
                                   ok 
                  end end,
    [WaitACK(Node, Ref) || Node <- Nodes, Node =/= node()].

handle_info({view_change, Type, Node, {NodeToACK, Ref}, BroadcastedView}, _View) ->
    %% The message sent in broadcast_view_change will trigger this callback.
    %% Execute the view change and send an ack back.
    %%
    io:format("Received broadcast view_change ~p ~p from ~p~n", [Type, Node, NodeToACK]),
    %% New nodes added to the system don't have the View set, use the one received
    %% as part of the broadcast message (BroadcastedView)
    NewView = apply_view_change(Type, Node, BroadcastedView),
    {view_manager, NodeToACK} ! {node(), Ref},
    io:format("ACKed broadcast view_change ~p ~p from ~p~n", [Type, Node, NodeToACK]),
    {noreply, NewView};

handle_info({debug, Pid}, View) ->
    %% Hack to do some debug
    Pid ! View, 
    {noreply, View};

handle_info(Msg, View) ->
    io:format("Unknown message: ~p~n", [Msg]),
    {noreply, View}.


apply_view_change(add, NodeToInsert, View = #view{nodes=Nodes, 
                                                  tokens=Tokens, 
                                                  tokens_per_node=TokensPerNode}) ->
    %% Given a new node, tokenize it and transfer the keys from other nodes
    %% The foldl function works like a for loop. The token list is updated
    %% at each "iteration" using the variable AccTokens. At the end, the 
    %% list with all the new tokens inserted is returned.
    %%
    TokensToInsert = lab4kvs_viewutils:gen_tokens_node(NodeToInsert, TokensPerNode),
    Fun = fun(Token, AccTokens) -> move_keys(Token, AccTokens) end,
    UpdatedTokens = lists:foldl(Fun, Tokens, TokensToInsert),
    View#view{nodes=Nodes++[NodeToInsert], tokens=UpdatedTokens};
    

apply_view_change(remove, NodeToDelete, View = #view{nodes=Nodes, 
                                                     tokens=Tokens}) ->
    %% Given a node to remove, retrieve all its tokens and transfer the keys to other nodes
    %%
    UpdatedTokens = [{Hash, Node} || {Hash, Node} <- Tokens, Node =/= NodeToDelete],
    case NodeToDelete =:= node() of
        true -> %% I'm being deleted, move all my keys to other nodes
            KVS = lab4kvs_kvstore:get_all(),
            KeyOwner = fun({Key, _}) -> {_H, KOwn} = lab4kvs_viewutils:get_key_owner(Key, UpdatedTokens), 
                                        KOwn end,
            io:format("Moving keys..~n"),
            [move_key(KVSEntry, KeyOwner(KVSEntry)) || KVSEntry <- KVS],
            io:format("DONE Moving keys~n");
        false -> %% I'm not the node being deleted, nothing to do
            io:format("I'm not the node being deleted, nothing to do ~p =/= ~p ~n", [NodeToDelete, node()])
    end,
    View#view{nodes=Nodes--[NodeToDelete], tokens=UpdatedTokens}.


move_keys(TokenToInsert = {Hash, DestNode}, Tokens) ->
    %% Given a new token and the list of current tokens, move some of the keys
    %% to the new node (DestNode).
    %%
    lab4kvs_debug:call(move_keys_add),
    {PrevHash, _PrevNode} = lab4kvs_viewutils:get_prev(TokenToInsert, Tokens),
    {_NextHash, NextNode} = lab4kvs_viewutils:get_next(TokenToInsert, Tokens),
    case NextNode =:= node() andalso DestNode =/= node() of
        true  -> %% The keys to be moved to the new node belong to me
            io:format("Moving keys in range ~p to ~p~n", [{PrevHash+1, Hash}, DestNode]),
            move_keyrange({PrevHash + 1, Hash}, DestNode);
        false -> %% Not my responsibility, nothing to do
            io:format("Keys are not mine, continue")
    end,
    lab4kvs_debug:return(move_keys_add, Tokens ++ [TokenToInsert]),
    %% Update the token list to be used at the next iteration in foldl
    lab4kvs_viewutils:insert_token(TokenToInsert, Tokens).


move_keyrange({Start, End}, DestNode) ->
    %% Move the KVS entries in the range to DestNode, deleting the local copy
    %%
    lab4kvs_debug:call(move_keyrange),
    KVSRange = lab4kvs_kvstore:get_keyrange(Start, End),
    io:format("KVSRange ~p~n", [KVSRange]),

    %% This might be optimized with a single RPC call for a group of keys
    %% for each destination node, but it still runs fast enough
    [move_key(KVSEntry, DestNode) || KVSEntry <- KVSRange],

    lab4kvs_debug:return(move_keyrange, nostate).

move_key({Key, {Val, Hash}}, DestNode) ->
    %% RPC to destnode to insert the key, local call to delete the key
    %%
    io:format("Moving key ~p to ~p~n", [{Key, {Val, Hash}}, DestNode]),
    rpc:call(DestNode, lab4kvs_kvstore, put, [Key, Val, Hash]),
    lab4kvs_kvstore:delete(Key),
    io:format("Moved key ~p to ~p~n", [{Key, {Val, Hash}}, DestNode]).

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
    TokensPerNode = 2,
    IPPortListStr = "10.0.0.20:8080,10.0.0.21:8080,10.0.0.22:8080",
    lab4kvs_viewmanager:start_link(IPPortListStr, TokensPerNode),
    ?MODULE ! {debug, self()},
    receive
        View -> dump(View)
    end.

