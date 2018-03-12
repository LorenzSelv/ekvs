%% lab4kvs_viewhandler
%% Handler module implementing the endpoints for view changes 

-module(lab4kvs_viewhandler).

-export([init/2]).

%% Response Header
-define(HEADER, #{<<"content-type">> => <<"application/json">>}).

%% Response Bodies
-define(BODY_ADD(PartitionID, PartitionCount), 
    jsx:encode(#{
        <<"msg">> => <<"success">>,
        <<"partition_id">> => PartitionID,
        <<"number_of_partitions">> => PartitionCount
     })).

-define(BODY_REMOVE(PartitionCount), 
    jsx:encode(#{
        <<"msg">> => <<"success">>,
        <<"number_of_partitions">> => PartitionCount
     })).

-define(BODY_ILLEGALREQ, jsx:encode(#{<<"msg">> => <<"error">>, <<"error">> => <<"illegal request">>})).


init(Req0=#{ method := <<"PUT">> }, State) ->
    Req = try parse_body(Req0) of
            {true, Type, IPPort} ->  %% Legal type 
                  view_change(Type, IPPort, Req0);
            {false, _, _} -> %% Illegal type
                cowboy_req:reply(404, ?HEADER, ?BODY_ILLEGALREQ, Req0)
        catch  %% Missing type or ip port
            error:Error -> io:format("ERROR: ~p~n", [Error]),
            cowboy_req:reply(404, ?HEADER, ?BODY_ILLEGALREQ, Req0)
        end,
    {ok, Req, State}.


%%%%%%%%%%%%%%%% Internal functions %%%%%%%%%%%%%%%%

view_change(<<"add">>, IPPort, Req0) ->
    PartitionID = lab4kvs_viewmanager:view_change(add, IPPort),
    PartitionCount = lab4kvs_viewmanager:get_num_partitions(),
    Body = ?BODY_ADD(PartitionID, PartitionCount), 
    cowboy_req:reply(200, ?HEADER, Body, Req0);


view_change(<<"remove">>, IPPort, Req0) ->
    lab4kvs_viewmanager:view_change(remove, IPPort),
    PartitionCount = lab4kvs_viewmanager:get_num_partitions(),
    Body = ?BODY_REMOVE(PartitionCount),
    cowboy_req:reply(200, ?HEADER, Body, Req0).


parse_body(Req) ->
    {ok, Data, _} = cowboy_req:read_urlencoded_body(Req),
    io:format("DATA=~p~n~n",[Data]),
    {_, Type}   = lists:keyfind(<<"type">>, 1, Data),
    {_, IPPort} = lists:keyfind(<<"ip_port">>, 1, Data),
    {legal_type(Type), Type, IPPort}.

legal_type(Type) ->
    Type =:= <<"add">> orelse Type =:= <<"remove">>.

