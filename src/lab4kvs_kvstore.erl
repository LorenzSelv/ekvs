%% lab4kvs_kvstore
%% gen_server implementing kvs storage

-module(lab4kvs_kvstore).
-behaviour(gen_server).

%% API interface
-export([start_link/0]).
-export([get/1]).
-export([put/2]).
-export([put/3]).
-export([put_list/1]).
-export([delete/1]).
-export([delete_list/1]).
-export([get_all_entries/0]).
-export([get_keyrange_entries/2]).
-export([get_numkeys/0]).

-export([dump/0]).

%% Server Callbacks
-export([init/1, terminate/2, handle_info/2, handle_cast/2, handle_call/3, code_change/3]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get(Key) ->
    gen_server:call(?MODULE, {get, Key}).

put(Key, Value) ->
    gen_server:call(?MODULE, {put, Key, Value}).

put(Key, Value, Hash) ->
    gen_server:call(?MODULE, {put, Key, Value, Hash}).

put_list(KeyValueHashList) when is_list(KeyValueHashList) ->
    gen_server:call(?MODULE, {put_list, KeyValueHashList}).

delete(Key) ->
    gen_server:call(?MODULE, {delete, Key}).

delete_list(Keys) ->
    gen_server:call(?MODULE, {delete_list, Keys}).

get_all_entries() ->
    %% Return a list of {Key, {Value, Hash}}
    gen_server:call(?MODULE, all).

get_keyrange_entries(Start, End) ->
    %% Return a list of {Key, {Value, Hash}} such that Start <= Hash <= End
    gen_server:call(?MODULE, {keyrange, Start, End}).

get_numkeys() ->
    %% Return the number of keys
    gen_server:call(?MODULE, numkeys).

dump() ->
    %% Dump the KVS
    gen_server:call(?MODULE, dump).

%%%%% Server Callbacks %%%%%

init(_Args) ->
    %% KVS map is in the format Key: {Value, Hash}
    {ok, maps:new()}.

handle_call({get, Key}, _From, KVS) when is_map(KVS) ->
    Reply = case maps:find(Key, KVS) of
                {ok, {Value, _Hash}} -> {ok, Value};
                error -> error
            end, %the val returned from maps:find should be a tuple containing the value, timestamp, and causal payload
    {reply, Reply, KVS};

handle_call({put, Key, Value}, _From, KVS) when is_map(KVS) ->
    Hash  =  lab4kvs_viewutils:hash(Key),
    Reply = {replaced, maps:is_key(Key, KVS)},
    {reply, Reply, maps:put(Key, {Value, Hash}, KVS)};%put value, timestamp, and causal payload under key

handle_call({put, Key, Value, Hash}, _From, KVS) when is_map(KVS) ->
    Reply = {replaced, maps:is_key(Key, KVS)},
    {reply, Reply, maps:put(Key, {Value, Hash}, KVS)};

handle_call({delete, Key}, _From, KVS) when is_map(KVS) ->
    Reply = {deleted, maps:is_key(Key, KVS)},
    {reply, Reply, maps:remove(Key, KVS)};

handle_call({put_list, KVSEntries}, _From, KVS) when is_map(KVS) ->
    Put    = fun (Key, Map) -> maps:put(Key, Map) end,
    NewKVS = lists:foldl(Put, KVS, KVSEntries),
    {reply, ok, NewKVS};

handle_call({delete_list, KeysToDelete}, _From, KVS) when is_map(KVS) ->
    Remove = fun (Key, Map) -> maps:remove(Key, Map) end,
    NewKVS = lists:foldl(Remove, KVS, KeysToDelete),
    {reply, ok, NewKVS};

handle_call(all, _From, KVS) when is_map(KVS) ->
    KVSList = maps:to_list(KVS),
    {reply, KVSList, KVS};

handle_call({keyrange, Start, End}, _From, KVS) when is_map(KVS) ->
    InRange = fun(_Key, {_Val, Hash}) -> Start =< Hash andalso Hash =< End end,
    KVSRange = maps:to_list(maps:filter(InRange, KVS)),
    {reply, KVSRange, KVS};

handle_call(numkeys, _From, KVS) when is_map(KVS) ->
    {reply, maps:size(KVS), KVS};

handle_call(dump, _From, KVS) ->
    FormatStr = "KVS~n" ++ "~p~n",
    Terms = io_lib:format(FormatStr, [KVS]),
    Reply = list_to_binary(lists:flatten(Terms)),
    {reply, Reply, KVS}.

handle_cast(Msg, State) ->
    io:format("Unknown message: ~p~n", [Msg]),
    {noreply, State}.

handle_info(Msg, State) ->
    io:format("Unknown message: ~p~n", [Msg]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.
