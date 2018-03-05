%% lab4kvs_app
%% Main Application

-module(lab4kvs_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

-define(DEFAULT_TOKENSPERPARTITION, "50").

start(_StartType, _StartArgs) ->
    io:format("node() = ~p~n", [node()]),

    %% Start the kvs storage server
    {ok, _} = lab4kvs_kvstore:start_link(),

    %% Start the view manager server
    View = os:getenv("VIEW", none),  %% New nodes do not have VIEW set
    ReplicasPerPartition = list_to_integer(os:getenv("K")),
    TokensPerPartition   = list_to_integer(os:getenv("TOKENSPERPARTITION", ?DEFAULT_TOKENSPERPARTITION)),
    {ok, PidVM} = lab4kvs_viewmanager:start_link(View, TokensPerPartition, ReplicasPerPartition),
    %% Register the view_manager so it can receive messages from other nodes
    true = register(view_manager, PidVM),

    %% Define mapping routes - handlers
    Dispatch = cowboy_router:compile([
            {'_', [
                    {"/kvs", lab4kvs_handler, []},
                    {"/kvs/view_update", lab4kvs_viewhandler, []},
                    {"/kvs/get_number_of_keys", lab4kvs_keynumhandler, []},

                    {"/kvs/debug", lab4kvs_debug, []},

                    %% Partition informations handlers
                    {"/kvs/:requested_info", lab4kvs_partitionsinfohandler, []}

                ]}
        ]),
    {ok, _} = cowboy:start_clear(restserv, [{port, 8080}], #{env => #{dispatch => Dispatch}}).


stop(_State) ->
    ok.
