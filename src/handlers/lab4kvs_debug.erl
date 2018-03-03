%% lab4kvs_debug
%% Debug module for taking snapshots of the view
%% A GET request made at kvs/debug collects the state from each node and returns it

-module(lab4kvs_debug).

-export([init/2]).
-export([call/1]).
-export([return/2]).

%% Response Header
-define(HEADER, #{<<"content-type">> => <<"application/json">>}).

%% Response Body
-define(BODY_GET(KVS, View), jsx:encode(#{<<"kvs">> => KVS, <<"view">> => View})).


init(Req0=#{ method := <<"GET">> }, State) ->
    KVS  = lab4kvs_kvstore:dump(),
    View = lab4kvs_viewmanager:dump(), 
    Req = cowboy_req:reply(200, ?HEADER, ?BODY_GET(KVS, View), Req0),
    {ok, Req, State}.


%%%%%%%% DEBUG %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

call(Function) ->
    io:format("~n===================~n"),
    io:format("Calling function \"~p\"~n", [Function]),
    io:format("~n===================~n").

return(Function, State) ->
    io:format("~n===================~n"),
    io:format("Returning from function \"~p\"~n~n", [Function]),
    io:format("STATE~n"),
    io:format("~p~n", [State]),
    io:format("~n===================~n").

