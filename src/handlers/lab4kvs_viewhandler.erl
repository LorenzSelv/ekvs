%% lab4kvs_viewhandler
%% Handler module implementing the endpoints for view changes 

-module(lab4kvs_viewhandler).

-export([init/2]).

%% Response Header
-define(HEADER, #{<<"content-type">> => <<"application/json">>}).

%% Response Bodies
-define(BODY_PUT, jsx:encode(#{<<"msg">> => <<"success">>})).

-define(BODY_ILLEGALREQ, jsx:encode(#{<<"msg">> => <<"error">>, <<"error">> => <<"illegal request">>})).


init(Req0=#{ method := <<"PUT">> }, State) ->
    
    Req = try parse_body(Req0) of
            {true, Type, IPPort} -> %% Legal type
                lab4kvs_viewmanager:view_change(Type, IPPort),
                cowboy_req:reply(200, ?HEADER, ?BODY_PUT, Req0);
            {false, _, _} -> %% Illegal type
                cowboy_req:reply(404, ?HEADER, ?BODY_ILLEGALREQ, Req0)
        catch %% Missing type or ip port
            error:Error -> io:format("ERROR: ~p~n", [Error]),
            cowboy_req:reply(404, ?HEADER, ?BODY_ILLEGALREQ, Req0)
        end,
    {ok, Req, State}.

%%%%%%%%%%%%%%%% Internal functions %%%%%%%%%%%%%%%%

parse_body(Req) ->
    {ok, Data, _} = cowboy_req:read_urlencoded_body(Req),
    io:format("DATA=~p~n~n",[Data]),
    {_, Type}   = lists:keyfind(<<"type">>, 1, Data),
    {_, IPPort} = lists:keyfind(<<"ip_port">>, 1, Data),
    {legal_type(Type), Type, IPPort}.

legal_type(Type) ->
    Type =:= <<"add">> orelse Type =:= <<"remove">>.

