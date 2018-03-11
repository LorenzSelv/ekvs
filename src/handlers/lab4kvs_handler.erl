%% lab4kvs_handler
%% Handler module kvs rest api

-module(lab4kvs_handler).

-export([init/2]).

%% RPC timeout in milliseconds
-define(RPC_TIMEOUT, 5000).

%% Response Header
-define(HEADER, #{<<"content-type">> => <<"application/json">>}).

%% Response Bodies
-define(BODY_GET(Value, PartitionID, Payload, Timestamp), 
    jsx:encode(#{
        <<"msg">> => <<"success">>,
        <<"value">> => Value,
        <<"partition_id">> => PartitionID,
        <<"causal_payload">> => Payload,
        <<"timestamp">> => Timestamp
     })).

-define(BODY_PUT(PartitionID, Payload, Timestamp), 
    jsx:encode(#{
        <<"msg">> => <<"success">>,
        <<"partition_id">> => PartitionID, 
        <<"causal_payload">> => Payload, 
        <<"timestamp">> => Timestamp
     })).

-define(BODY_DELETE, jsx:encode(#{<<"msg">> => <<"success">>})).

-define(BODY_KEYERROR, jsx:encode(#{<<"msg">> => <<"error">>, <<"error">> => <<"key does not exist">>})).

-define(BODY_ILLEGALKEY, jsx:encode(#{<<"msg">> => <<"error">>, <<"error">> => <<"illegal key">>})).

-define(BODY_ILLEGALREQ, jsx:encode(#{<<"msg">> => <<"error">>, <<"error">> => <<"illegal request">>})).

-define(BODY_NODEDOWN, jsx:encode(#{<<"msg">> => <<"error">>, <<"error">> => <<"service is not available">>})).



init(Req0=#{ method := <<"GET">> }, State) ->
    Req = try parse_querystring(Req0) of
            {true, Key, Payload} -> %% Legal key
                get_kvs_query(Key, Payload, Req0);
            {false, _} -> %% Illegal key
                cowboy_req:reply(404, ?HEADER, ?BODY_ILLEGALKEY, Req0)
        catch %% Missing key or value
            error:Error -> io:format("ERROR: ~p~n", [Error]),
            cowboy_req:reply(404, ?HEADER, ?BODY_ILLEGALREQ, Req0)
        end,
    {ok, Req, State};



init(Req0=#{ method := Method }, State)
        when Method =:= <<"PUT">> orelse Method =:= <<"POST">> ->
    Req = try parse_body(Req0) of
            {true, Key, Value, Payload} -> %% Legal key
                put_kvs_query(Key, Value, Payload, Req0);
            {false, _, _} -> %% Illegal key
                cowboy_req:reply(404, ?HEADER, ?BODY_ILLEGALKEY, Req0)
        catch %% Missing key or value
            error:Error -> io:format("ERROR: ~p~n", [Error]),
            cowboy_req:reply(404, ?HEADER, ?BODY_ILLEGALREQ, Req0)
        end,
    {ok, Req, State};


init(Req0=#{ method := <<"DELETE">> }, State) ->
    Req = try parse_querystring(Req0) of
            {true, Key, Payload} -> %% Legal key
                delete_kvs_query(Key, Payload, Req0);
            {false, _} -> %% Illegal key
                cowboy_req:reply(404, ?HEADER, ?BODY_ILLEGALKEY, Req0)
        catch %% Missing key or value
            error:Error -> io:format("ERROR: ~p~n", [Error]),
            cowboy_req:reply(404, ?HEADER, ?BODY_ILLEGALREQ, Req0)
        end,
    {ok, Req, State}.


%%%%%%%%%%%%%%%% Internal functions %%%%%%%%%%%%%%%%

get_kvs_query(Key, Payload, Req0) ->
    case lab4kvs_kvsquery:exec(get, [Key, Payload]) of
        {{ok, Value, Payload, Timestamp}, PartitionID} ->
            Body = ?BODY_GET(Value, PartitionID, Payload, Timestamp),
            cowboy_req:reply(200, ?HEADER, Body, Req0);
        {keyerror, _PartitionID} -> %% TODO reason
            cowboy_req:reply(404, ?HEADER, ?BODY_KEYERROR, Req0);
        {all_disconnected, _PartitionID} -> %% TODO Service unavailable
            node_down_reply(all_disconnected, Req0)
    end.


put_kvs_query(Key, Value, Payload, Req0) ->
    case lab4kvs_kvsquery:exec(put, [Key, Value, Payload]) of
        {{ok, Payload, Timestamp}, PartitionID} ->
            Body = ?BODY_PUT(PartitionID, Payload, Timestamp),
            cowboy_req:reply(200, ?HEADER, Body, Req0); 
        {badrpc, _PartitionID} ->
            node_down_reply(badrpc, Req0)
    end.


%% TODO delete key
delete_kvs_query(Key, Payload, Req0) ->
    case klab4kvs_kvsquery:exec(delete, [Key, Payload]) of
        {deleted,  true} ->
            cowboy_req:reply(200, ?HEADER, ?BODY_DELETE, Req0);
        {deleted, false} ->
            cowboy_req:reply(404, ?HEADER, ?BODY_KEYERROR, Req0);
        {badrpc, Reason} ->
            node_down_reply(Reason, Req0)
    end.


node_down_reply(Reason, Req) ->
    %% Reply when the main node is down
    io:format("TIMEOUT ~p~n", [Reason]),
    cowboy_req:reply(404, ?HEADER, ?BODY_NODEDOWN, Req).


parse_querystring(Req) ->
    %% Extract query string values
    Data = cowboy_req:parse_qs(Req),
    {_, Key}     = lists:keyfind(<<"key">>, 1, Data),
    {_, Payload} = lists:keyfind(<<"causal_payload">>, 1, Data),
    {legal_key(Key), Key, Payload}.


parse_body(Req) ->
    %% PUT or POST requests
    {ok, Data, _} = cowboy_req:read_urlencoded_body(Req),
    {_, Key}     = lists:keyfind(<<"key">>,   1, Data),
    {_, Value}   = lists:keyfind(<<"value">>, 1, Data),
    {_, Payload} = lists:keyfind(<<"causal_payload">>, 1, Data),
    {legal_key(Key), Key, Value, Payload}.


legal_key(Key) -> 
    %% Return true if the key is legal, false otherwise
    case re:run(Key, "^[0-9A-Za-z_]+$") of 
        {match, _} -> string:length(Key) >= 1 andalso 
                      string:length(Key) =< 250;
        nomatch    -> false
    end.

