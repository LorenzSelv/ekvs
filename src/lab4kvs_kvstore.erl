%% lab4kvs_kvstore
%% gen_server implementing kvs storage

-module(lab4kvs_kvstore).
-behaviour(gen_server).

%% API interface
-export([start_link/0]).
-export([get/2]).
-export([put/3]).
-export([put/2]).
-export([put_list/1]).
-export([update_vc/2]).
-export([delete/1]).
-export([delete_list/1]).
-export([get_all_entries/0]).
-export([get_keyrange_entries/2]).
-export([get_numkeys/0]).
-export([prepare_kvsvalue/3]).


-export([dump/0]).

%% Server Callbacks
-export([init/1, terminate/2, handle_info/2, handle_cast/2, handle_call/3, code_change/3]).

%% A KVS Entry has the following format
%% Key => kvsvalue#{value, hash, vector_clock, timestamp}
-record(kvsvalue, {value, hash, vector_clock, timestamp}).


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


get(Key, CausalPayload) ->
    %% Return {ok, Value, CausalPayload, Timestamp};
    %%     or keyerror
    gen_server:call(?MODULE, {get, Key, CausalPayload}).


put(Key, Value, CausalPayload) ->
    %% put returns {ok, CausalPayload, Timestamp}
    gen_server:call(?MODULE, {put, Key, Value, CausalPayload}).


put(Key, KVSValue) when is_record(KVSValue, kvsvalue) ->
    %% put returns {ok, CausalPayload, Timestamp}
    gen_server:call(?MODULE, {put_kvsvalue, Key, KVSValue}).


put_list(KVSEntries) when is_list(KVSEntries) ->
    gen_server:call(?MODULE, {put_list, KVSEntries}).


update_vc(Key, VC) ->
    gen_server:call(?MODULE, {update_vc, Key, VC}).


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


handle_call({get, Key, RequestCP}, _From, KVS) ->
    Reply = case maps:find(Key, KVS) of
                %% TODO handle deleted keys
                %% {ok, #kvsvalue{value=deleted}} -> keyerror;
                {ok, #kvsvalue{value=Value,
                               vector_clock=VC,
                               timestamp=Timestamp}} ->
                    %% TODO do not ignore request CP
                    CausalPayload = lab4kvs_vcmanager:vc_to_cp(VC),
                    {ok, Value, CausalPayload, Timestamp};
                error -> keyerror
            end,
    {reply, Reply, KVS};


handle_call({put, Key, Value, CausalPayload}, _From, KVS) ->
    lab4kvs_debug:call({put, Key, Value, CausalPayload, KVS}),
    %% Update node vector clock and return it
    NewVC = lab4kvs_vcmanager:new_event(CausalPayload),
    KVSValue = prepare_kvsvalue(Key, Value, NewVC),
    %% TODO no need to resolve
    %% ResKVSValue = resolve_put(Key, KVSValue, KVS),
    %% ResVC = ResKVSValue#kvsvalue.vector_clock,
    %% ResCausalPayload = lab4kvs_vcmanager:vc_to_cp(ResVC),
    %% ResTimestamp = ResKVSValue#kvsvalue.timestamp,
    %% Reply = {ok, ResCausalPayload, ResTimestamp},
    NewVC = KVSValue#kvsvalue.vector_clock, %% Assert equal
    NewCausalPayload = lab4kvs_vcmanager:vc_to_cp(NewVC),
    Timestamp = KVSValue#kvsvalue.timestamp,
    Reply = {ok, NewCausalPayload, Timestamp},
    lab4kvs_debug:return({put, Reply, maps:put(Key, KVSValue, KVS)}),
    %% {reply, Reply, maps:put(Key, ResKVSValue, KVS)};
    {reply, Reply, maps:put(Key, KVSValue, KVS)};


handle_call({put_kvsvalue, Key, KVSValue}, _From, KVS) when is_record(KVSValue, kvsvalue) ->
    %% Resove the value against what might already be present in the KVS
    %% This function is called only for internal key transfer, 
    %% thus no new event happened
    lab4kvs_debug:call({put_kvsvalue, Key, KVSValue}),
    ResKVSValue = resolve_put(Key, KVSValue, KVS),
    ResVC = ResKVSValue#kvsvalue.vector_clock,
    ResCausalPayload = lab4kvs_vcmanager:vc_to_cp(ResVC),
    ResTimestamp = ResKVSValue#kvsvalue.timestamp,
    Reply = {ok, ResCausalPayload, ResTimestamp},
    lab4kvs_debug:return({put_kvsvalue, Reply}),
    {reply, Reply, maps:put(Key, ResKVSValue, KVS)};


handle_call({put_list, []}, _From, KVS) -> {reply, ok, KVS};

handle_call({put_list, KVSEntries}, _From, KVS) ->
    Put = fun ({Key, KVSValue}, Map) ->
            ResolvedKVSValue = resolve_put(Key, KVSValue, Map),
            maps:put(Key, ResolvedKVSValue, Map) end,
    NewKVS = lists:foldl(Put, KVS, KVSEntries),
    {reply, ok, NewKVS};


handle_call({update_vc, Key, VC}, _From, KVS) ->
    lab4kvs_debug:call({update_vc, Key, VC}),
    KVSValue = maps:get(Key, KVS),
    true = lab4kvs_vcmanager:happens_before_or_equal(KVSValue#kvsvalue.vector_clock, VC),
    NewKVSValue = KVSValue#kvsvalue{vector_clock=VC},
    {reply, ok, maps:put(Key, NewKVSValue, KVS)};


%% TODO delete is not a delete, is a put 'deleted'
%% TODO delete is also used by move_entries, that one should not change
handle_call({delete, Key}, _From, KVS) ->
    Reply = {deleted, maps:is_key(Key, KVS)},
    {reply, Reply, maps:remove(Key, KVS)};

handle_call({delete_list, []}, _From, KVS) -> {reply, ok, KVS};

%% TODO delete is not a delete, is a put deleted
handle_call({delete_list, KeysToDelete}, _From, KVS) ->
    Remove = fun (Key, Map) -> maps:remove(Key, Map) end,
    NewKVS = lists:foldl(Remove, KVS, KeysToDelete),
    {reply, ok, NewKVS};


handle_call(all, _From, KVS) ->
    KVSList = maps:to_list(KVS),
    {reply, KVSList, KVS};


handle_call({keyrange, Start, End}, _From, KVS) ->
    InRange = case Start < End of
                true ->
                      fun(_Key, KVSValue) -> 
                              Hash = KVSValue#kvsvalue.hash,
                              Start =< Hash andalso Hash =< End end;
                false ->
                      fun(_Key, KVSValue) -> 
                              Hash = KVSValue#kvsvalue.hash,
                              Start =< Hash orelse  Hash =< End end
              end,
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
    lab4kvs_debug:call({resolve_put, NewKVSValue, KVS}),
    case maps:find(Key, KVS) of
        {ok, KVSValue} ->
            latest_kvsvalue(KVSValue, NewKVSValue);
        error ->  %% Key is not present in the KVS, no conflicts
            NewKVSValue
    end.


latest_kvsvalue(Va = #kvsvalue{vector_clock=VCa, timestamp=TSa},
                Vb = #kvsvalue{vector_clock=VCb, timestamp=TSb}) ->
    case lab4kvs_vcmanager:happens_before(VCa, VCb) of 
        true  -> Vb;
        false ->
            case lab4kvs_vcmanager:happens_before(VCb, VCa) of
                true  -> Va;
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


prepare_kvsvalue(Key, Value, CP) when is_binary(CP)->
    VC = lab4kvs_vcmanager:cp_to_vc(CP),
    prepare_kvsvalue(Key, Value, VC);

prepare_kvsvalue(Key, Value, VC) when is_map(VC)->
    Hash  = lab4kvs_viewutils:hash(Key),
    Timestamp = lab4kvs_vcmanager:get_timestamp(),
    #kvsvalue{value=Value,
              hash=Hash,
              vector_clock=VC,
              timestamp=Timestamp}.

