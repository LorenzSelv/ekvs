%% lab4kvs_kvstore
%% gen_server implementing kvs storage

-module(lab4kvs_kvstore).
-behaviour(gen_server).

%% API interface
-export([start_link/0]).
-export([get/1]).
-export([put/3]).
-export([put/2]).
-export([put_list/1]).
-export([delete/1]).
-export([delete_list/1]).
-export([get_all_entries/0]).
-export([get_keyrange_entries/2]).
-export([get_numkeys/0]).

-export([dump/0]).

%% Server Callbacks
-export([init/1, terminate/2, handle_info/2, handle_cast/2, handle_call/3, code_change/3]).

%% A KVS Entry has the following format
%% Key => kvsvalue#{value, hash, vector_clock, timestamp}
-record(kvsvalue, {value, hash, vector_clock, timestamp}).


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


get(Key) ->
    gen_server:call(?MODULE, {get, Key}).


put(Key, Value, CausalPayload) ->
    %% put returns {ok, CausalPayload, Timestamp}
    gen_server:call(?MODULE, {put, Key, Value, CausalPayload}).


put(Key, KVSValue) when is_record(KVSValue, kvsvalue) ->
    %% put returns {ok, CausalPayload, Timestamp}
    gen_server:call(?MODULE, {put_kvsvalue, Key, KVSValue}).


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


handle_call({get, Key}, _From, KVS) ->
    Reply = case maps:find(Key, KVS) of
                %% TODO handle deleted keys
                %% {ok, #kvsvalue{value=deleted}} -> error;
                {ok, #kvsvalue{value=Value,
                               vector_clock=VC,
                               timestamp=Timestamp}} ->
                    CausalPayload = lab4kvs_kvsutils:vector_clock_to_causal_payload(VC),
                    {ok, Value, CausalPayload, Timestamp};
                error -> error
            end,
    {reply, Reply, KVS};


handle_call({put, Key, Value, CausalPayload}, _From, KVS) ->
    Timestamp = lab4kvs_kvsutils:get_timestamp(),
    Hash = lab4kvs_viewutils:hash(Key),
    VC   = lab4kvs_kvsutils:causal_payload_to_vector_clock(CausalPayload),
    KVSValue = #kvsvalue{value=Value,
                         hash=Hash,
                         vector_clock=VC,
                         timestamp=Timestamp},
    CausalPayload = lab4kvs_kvsutils:vector_clock_to_causal_payload(VC),
    Reply = {ok, CausalPayload, Timestamp},
    {reply, Reply, maps:put(Key, KVSValue, KVS)};


handle_call({put_kvsvalue, Key, KVSValue}, _From, KVS) when is_record(KVSValue, kvsvalue) ->
    %% Resove the value against what might already be present in the KVS
    ResolvedKVSValue = resolve_put(Key, KVSValue, KVS),
    CausalPayload = lab4kvs_kvsutils:vector_clock_to_causal_payload(KVSValue#kvsvalue.vector_clock),
    Timestamp = KVSValue#kvsvalue.timestamp,
    Reply = {ok, CausalPayload, Timestamp},
    {reply, Reply, maps:put(Key, ResolvedKVSValue, KVS)};


%% TODO delete is not a delete, is a put deleted
handle_call({delete, Key}, _From, KVS) ->
    Reply = {deleted, maps:is_key(Key, KVS)},
    {reply, Reply, maps:remove(Key, KVS)};


handle_call({put_list, KVSEntries}, _From, KVS) ->
    Put = fun ({Key, KVSValue}, Map) ->
            ResolvedKVSValue = resolve_put(Key, KVSValue, Map),
            maps:put(Key, ResolvedKVSValue, Map) end,
    NewKVS = lists:foldl(Put, KVS, KVSEntries),
    {reply, ok, NewKVS};


%% TODO delete is not a delete, is a put deleted
handle_call({delete_list, KeysToDelete}, _From, KVS) ->
    Remove = fun (Key, Map) -> maps:remove(Key, Map) end,
    NewKVS = lists:foldl(Remove, KVS, KeysToDelete),
    {reply, ok, NewKVS};


handle_call(all, _From, KVS) ->
    KVSList = maps:to_list(KVS),
    {reply, KVSList, KVS};


handle_call({keyrange, Start, End}, _From, KVS) ->
    InRange = fun(_Key, #kvsvalue{hash=Hash}) ->
                      Start =< Hash andalso Hash =< End end,
    KVSRange = maps:to_list(maps:filter(InRange, KVS)),
    {reply, KVSRange, KVS};


handle_call(numkeys, _From, KVS) ->
    {reply, maps:size(KVS), KVS};


handle_call(dump, _From, KVS) ->
    Terms = io_lib:format("KVS~n~p~n", [KVS]),
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


%%%%%%%%%%%%%%% INTERNAL FUNCTIONS %%%%%%%%%%%%%%%


resolve_put(Key, NewKVSValue, KVS) ->
    %% Given a Key and KVSValue to put in the KVS
    %% Return the ResolvedValue that should be inserted in the KVS
    %% The causal order is determined comparing
    %% VC, Timestamp, node@<ip> in decreasing order of priority
    %%
    case maps:find(Key, KVS) of
        {ok, KVSValue} ->
            latest_kvsvalue(KVSValue, NewKVSValue);
        error ->  %% Key is not present in the KVS, no conflicts
            NewKVSValue
    end.


latest_kvsvalue(Va = #kvsvalue{vector_clock=VCa, timestamp=TSa},
                Vb = #kvsvalue{vector_clock=VCb, timestamp=TSb}) ->
    case lab4kvs_kvsutils:happens_before(VCa, VCb) of 
        true  -> Va;
        false ->
            case lab4kvs_kvsutils:happens_before(VCb, VCa) of
                true  -> Vb;
                false ->
                    %% concurrent
                    if 
                        TSa > TSb -> Va;
                        TSb > TSa -> Vb;
                        %% same timestamp, pick the first one
                        true -> Va
                    end
            end
    end.
