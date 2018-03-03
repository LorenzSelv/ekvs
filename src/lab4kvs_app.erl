%% lab4kvs_app
%% Main Application

-module(lab4kvs_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

-define(DEFAULT_TOKENSPERNODE, "50").

start(_StartType, _StartArgs) ->
    io:format("node() = ~p~n", [node()]),
    %% Start the kvs storage server
    {ok, _} = lab4kvs_kvstore:start_link(),
    %% Start the view manager server
    View = os:getenv("VIEW", none),  %% New nodes do not have VIEW set
    TokensPerNode = list_to_integer(os:getenv("TOKENSPERNODE", ?DEFAULT_TOKENSPERNODE)),
    {ok, PidVM} = lab4kvs_viewmanager:start_link(View, TokensPerNode),
    true = register(view_manager, PidVM),
    %% Define mapping routes - handlers
    Dispatch = cowboy_router:compile([
            {'_', [
                    {"/kvs", lab4kvs_handler, []},
                    {"/kvs/view_update", lab4kvs_viewhandler, []},
                    {"/kvs/get_number_of_keys", lab4kvs_keynumhandler, []},
                    {"/kvs/debug", lab4kvs_debug, []}
                ]}
        ]),
    {ok, _} = cowboy:start_clear(restserv, [{port, 8080}], #{env => #{dispatch => Dispatch}}).

stop(_State) ->
    ok.
