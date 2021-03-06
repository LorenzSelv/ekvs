%% ekvs_keynumhandler
%% Handler module implementing the endpoint for getting the number of keys of the node 

-module(ekvs_keynumhandler).

-export([init/2]).

%% Response Header
-define(HEADER, #{<<"content-type">> => <<"application/json">>}).

%% Response Bodies
-define(BODY_GET(Count), jsx:encode(#{<<"count">> => Count})).


init(Req0=#{ method := <<"GET">> }, State) ->
    NumKeys = ekvs_kvstore:get_numkeys(),
    Req = cowboy_req:reply(200, ?HEADER, ?BODY_GET(NumKeys), Req0),
    {ok, Req, State}.

