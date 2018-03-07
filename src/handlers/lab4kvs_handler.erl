%% lab4kvs_handler
%% Handler module kvs rest api

-module(lab4kvs_handler).

-export([init/2]).

%% RPC timeout in milliseconds
-define(RPC_TIMEOUT, 5000).

%% Response Header
-define(HEADER, #{<<"content-type">> => <<"application/json">>}).

%% Response Bodies
-define(BODY_GET(Value, Owner), 
    jsx:encode(#{
        <<"msg">> => <<"success">>, 
        <<"value">> => Value, 
        <<"partition_id">> => PartitionID, 
        <<"causal_payload">> => Payload, 
        <<"timestamp">> => Timestamp,
        <<"owner">> => Owner} % REMOVE owner 
    )).

-define(BODY_PUT(Replaced, Owner), 
    jsx:encode(#{
        <<"replaced">> => Replaced, %REMOVE replaced
        <<"msg">> => <<"success">>,
        <<"causal_payload">> => Payload, 
        <<"timestamp">> => Timestamp,
        <<"owner">> => Owner}
    )).

-define(BODY_DELETE, jsx:encode(#{<<"msg">> => <<"success">>})).

-define(BODY_KEYERROR, jsx:encode(#{<<"msg">> => <<"error">>, <<"error">> => <<"key does not exist">>})).

-define(BODY_ILLEGALKEY, jsx:encode(#{<<"msg">> => <<"error">>, <<"error">> => <<"illegal key">>})).

-define(BODY_ILLEGALREQ, jsx:encode(#{<<"msg">> => <<"error">>, <<"error">> => <<"illegal request">>})).

-define(BODY_NODEDOWN, jsx:encode(#{<<"msg">> => <<"error">>, <<"error">> => <<"service is not available">>})).



init(Req0=#{ method := <<"GET">> }, State) ->
    Req = try parse_body(get, Req0) of
            {true, Key, Payload} -> %% Legal key
                case kvs_query(get, [Key, Payload]) of
                    {{ok, Value}, Owner} ->
                        cowboy_req:reply(200, ?HEADER, ?BODY_GET(Value, Owner), Req0);
                    {error, _Owner} ->
                        cowboy_req:reply(404, ?HEADER, ?BODY_KEYERROR, Req0);
                    {{badrpc, Reason}, _Owner} ->
                        node_down_reply(Reason, Req0)
                end;
            {false, _} -> %% Illegal key
                cowboy_req:reply(404, ?HEADER, ?BODY_ILLEGALKEY, Req0)
        catch %% Missing key or value
            error:Error -> io:format("ERROR: ~p~n", [Error]),
            cowboy_req:reply(404, ?HEADER, ?BODY_ILLEGALREQ, Req0)
        end,
    {ok, Req, State};



init(Req0=#{ method := Method }, State)
        when Method =:= <<"PUT">> orelse Method =:= <<"POST">> ->
    Req = try parse_body(put, Req0) of
            {true, Key, Value} -> %% Legal key
                case kvs_query(put, [Key, Value]) of
                    {{replaced, true}, Owner} ->
                        cowboy_req:reply(200, ?HEADER, ?BODY_PUT(1, Owner), Req0);
                    {{replaced, false}, Owner} ->
                        cowboy_req:reply(201, ?HEADER, ?BODY_PUT(0, Owner), Req0);
                    {{badrpc, Reason}, _Owner} ->
                        node_down_reply(Reason, Req0)
                end;
            {false, _, _} -> %% Illegal key
                cowboy_req:reply(404, ?HEADER, ?BODY_ILLEGALKEY, Req0)
        catch %% Missing key or value
            error:Error -> io:format("ERROR: ~p~n", [Error]),
            cowboy_req:reply(404, ?HEADER, ?BODY_ILLEGALREQ, Req0)
        end,
    {ok, Req, State};


init(Req0=#{ method := <<"DELETE">> }, State) ->
    Req = try parse_body(get, Req0) of
            {true, Key} -> %% Legal key
                case kvs_query(delete, [Key]) of
                    {{deleted, true}, _Owner} ->
                        cowboy_req:reply(200, ?HEADER, ?BODY_DELETE, Req0);
                    {{deleted, false}, _Owner} ->
                        cowboy_req:reply(404, ?HEADER, ?BODY_KEYERROR, Req0);
                    {{badrpc, Reason}, _Owner} ->
                        node_down_reply(Reason, Req0)
                end;
            {false, _} -> %% Illegal key
                cowboy_req:reply(404, ?HEADER, ?BODY_ILLEGALKEY, Req0)
        catch %% Missing key or value
            error:Error -> io:format("ERROR: ~p~n", [Error]),
            cowboy_req:reply(404, ?HEADER, ?BODY_ILLEGALREQ, Req0)
        end,
    {ok, Req, State}.


%%%%%%%%%%%%%%%% Internal functions %%%%%%%%%%%%%%%%

call_with_timeout(Node, Module, Func, Args, Timeout) ->
    %% Execute a rpc call with timeout on a remote Node
    %% Return either the result of the call or {badrpc, timeout}
    %% It's the same thing that the 5th argument to the rpc call would do,
    %% but for some reason it's not working..
    Pid = self(),
    SpawnPid = spawn(fun() -> 
                        Res = rpc:call(Node, Module, Func, Args, Timeout),
                        Pid ! {self(), ok, Res} end),
    receive
        {SpawnPid, ok, Res} -> Res
    after
        Timeout -> {badrpc, timeout}
    end.
    

kvs_query(Func, Args) ->
    %% Execute a query on the kvs
    %% If the node is the main node, execute the query directly
    %% If the node is a forwarder node, execute the query remotely using a rpc
    lab4kvs_debug:call({kvs_query, Func}),
    Key = case Args of 
            [K,_] -> K;
            [K]   -> K
          end,
    %update view manager to take the causal payload
    Node = lab4kvs_viewmanager:get_key_owner(Key),
    io:format("Node ~p~n", [Node]),
    Owner = get_ip_port(Node),
    io:format("Got owner ~p~n", [Owner]),
    case Node =:= node() of
        true  ->
            io:format("I'm the owner~n"),
            {apply(lab4kvs_kvstore, Func, Args), Owner};
        false ->
            io:format("I'm NOT the owner~n"),
            %% If the main node is down the rpc call may take up to 8 seconds to return.
            %% For some reason the timeout passed as 5th parameter won't work.
            %% Thus, spawn a new process and set a timer
            {call_with_timeout(Node, lab4kvs_kvstore, Func, Args, ?RPC_TIMEOUT), Owner}
    end.

get_ip_port(Node) ->
    %% Given the pair "node@ip" of a node,
    %% return the ip:port pair of the node
    [_, IP] = string:split(atom_to_list(Node), "@", all), 
    list_to_binary(IP ++ ":8080").

node_down_reply(Reason, Req) ->
    %% Reply when the main node is down
    io:format("TIMEOUT ~p~n", [Reason]),
    cowboy_req:reply(404, ?HEADER, ?BODY_NODEDOWN, Req).

% deleting a key is bonus. Removing 'orelse Method == delete'
% from guard
parse_body(Method, Req) when Method == get  ->
    %% Extract query string values from the request body
    Data = cowboy_req:parse_qs(Req),
    {_, Key} = lists:keyfind(<<"key">>, 1, Data),
    {_, Payload} = lists:keyfind(<<"causal_payload">>,1, Data),
    {legal_key(Key), Key, Payload};

parse_body(put, Req) ->
    {ok, Data, _} = cowboy_req:read_urlencoded_body(Req),
    io:format("DATA=~p~n~n",[Data]),
    {_, Key}   = lists:keyfind(<<"key">>,   1, Data),
    {_, Value} = lists:keyfind(<<"value">>, 1, Data),
    
    % not sure if keyfind will return {_,Val} if empty string
    Payload = case lists:keyfind(<<"causal_payload">>, 1, Data) of
        {_, Payload} -> Payload; 
        false -> <<"">>,
    end,
    {legal_key(Key), Key, Value, Payload}.

legal_key(Key) -> 
    %% Return true if the key is legal, false otherwise
    case re:run(Key, "^[0-9A-Za-z_]+$") of 
        {match, _} -> string:length(Key) >= 1 andalso 
                      string:length(Key) =< 250;
        nomatch    -> false
    end.

