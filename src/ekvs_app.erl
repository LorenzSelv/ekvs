%% ekvs_app
%% Main Application

-module(ekvs_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

-define(DEFAULT_TOKENSPERPARTITION, "50").

start(_StartType, _StartArgs) ->
    io:format("node() = ~p~n", [node()]),

    %% Start the kvs storage server
    {ok, _} = ekvs_kvstore:start_link(),

    %% Start the view manager server
    View = os:getenv("VIEW", none),  %% New nodes do not have VIEW set
    ReplicasPerPartition = list_to_integer(os:getenv("K")),
    TokensPerPartition   = list_to_integer(os:getenv("TOKENSPERPARTITION", ?DEFAULT_TOKENSPERPARTITION)),
    {ok, PidVM} = ekvs_viewmanager:start_link(View, TokensPerPartition, ReplicasPerPartition),
    %% Register the view_manager so it can receive messages from other nodes
    true = register(view_manager, PidVM),

    %% Start the vector clock manager
    {ok, _} = ekvs_vcmanager:start_link(),

    %% Define mapping routes - handlers
    Dispatch = cowboy_router:compile([
            {'_', [
                    {"/kvs", ekvs_handler, []},
                    {"/kvs/view_update", ekvs_viewhandler, []},
                    {"/kvs/get_number_of_keys", ekvs_keynumhandler, []},

                    {"/kvs/debug", ekvs_debug, []},

                    %% Partition informations handlers
                    {"/kvs/:requested_info", ekvs_partitionsinfohandler, []}

                ]}
        ]),
    {ok, _} = cowboy:start_clear(restserv, [{port, 8080}], #{env => #{dispatch => Dispatch}}).


stop(_State) ->
    ok.
