%% lab4kvs_kvsutils
%% Implements util functions for the kvstore

-module(lab4kvs_kvsutils).

-export([get_timestamp/0]).
-export([causal_payload_to_vector_clock/1]).
-export([vector_clock_to_causal_payload/1]).

-export([test_CP_VC/0]).
-export([test_happens_before/0]).


get_timestamp() ->
    %% Return a timestamp as a binary string in the format
    %% 2018-3-6 2:30:13.143818
    %% so that it can be compared lexicographically using the
    %% min function
    %% https://stackoverflow.com/questions/37190769/how-to-compare-two-timestamps-with-erlang
    TS = {_,_,MS} = os:timestamp(),
    {{Y,Mon,D},{H,Min,S}} = calendar:now_to_local_time(TS),
    TimestampStr = io_lib:format("~w-~w-~w ~w:~w:~w.~w", [Y,Mon,D,H,Min,S,MS]),
    list_to_binary(TimestampStr).

%% CausalPayload is a binary string in the format 
%% <<"node@<ip1>:Clock1,node@<ip2>:Clock2">>
%% 
%% VC is the map representation of that string
%% 'node@<ip1>' => Clock1
%% 'node@<ip2>' => Clock2
%%

causal_payload_to_vector_clock(CausalPayload) ->
    NodeClockStrList = string:split(CausalPayload, ",", all),
    StrToPair = fun(NodeClockStr) ->
                    [Node, Clock] = string:split(NodeClockStr, ":", all),
                    {binary_to_atom(Node, latin1), binary_to_integer(Clock)}
                end,
    NodeClockList = lists:map(StrToPair, NodeClockStrList),
    maps:from_list(NodeClockList).


vector_clock_to_causal_payload(VC) ->
    NodeClockList = maps:to_list(VC),
    PairToStr = fun({Node,Clock}) ->
                    L = [atom_to_list(Node), integer_to_list(Clock)],
                    string:join(L, ":") end,
    NodeClockStrList = lists:map(PairToStr, NodeClockList),
    NodeClockStr = string:join(NodeClockStrList, ","),
    list_to_binary(NodeClockStr).


happens_before(VCa, VCb) when is_map(VCa) andalso is_map(VCb) ->
    VCaKeys = maps:keys(VCa),
    VCbKeys = maps:keys(VCb),
    %% First make sure the nodes are the same (Keys)
    VCaKeys = VCbKeys,
    %% Then get the clock values
    VCaClocks = [maps:get(Node, VCa) || Node <- VCaKeys],
    VCbClocks = [maps:get(Node, VCb) || Node <- VCbKeys],
    Clocks = lists:zip(VCaClocks, VCbClocks),
    %% All have to be =<, at least 
    LessOrEqual = lists:all(fun({Ca, Cb}) -> Ca =< Cb end, Clocks),
    Less        = lists:any(fun({Ca, Cb}) -> Ca  < Cb end, Clocks),
    LessOrEqual andalso Less.

    

%%%%%%%% TESTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

test_CP_VC() ->
    CP = <<"node@10.0.0.0:1,node@10.0.0.2:4,node@10.0.0.3:3">>,
    VC = causal_payload_to_vector_clock(CP),
    io:format("~p~n", [VC]),
    CP = vector_clock_to_causal_payload(VC).

test_happens_before(1) ->
    %% a -> b
    VCa = #{n0 => 0, n1 => 2, n2 => 3},
    VCb = #{n0 => 1, n1 => 3, n2 => 4},
    true  = happens_before(VCa, VCb),
    false = happens_before(VCb, VCa),
    ok;

test_happens_before(2) ->
    %% a -> b
    VCa = #{n0 => 1, n1 => 3, n2 => 3},
    VCb = #{n0 => 1, n1 => 3, n2 => 4},
    true  = happens_before(VCa, VCb),
    false = happens_before(VCb, VCa),
    ok;

test_happens_before(3) ->
    %% b -> a
    VCa = #{n0 => 1, n1 => 3, n2 => 4},
    VCb = #{n0 => 0, n1 => 3, n2 => 4},
    false = happens_before(VCa, VCb),
    true  = happens_before(VCb, VCa),
    ok;

test_happens_before(4) ->
    %% concurrent
    VCa = #{n0 => 1, n1 => 3, n2 => 4},
    VCb = #{n0 => 1, n1 => 3, n2 => 4},
    false = happens_before(VCa, VCb),
    false = happens_before(VCb, VCa),
    ok;

test_happens_before(5) ->
    %% concurrent
    VCa = #{n0 => 1, n1 => 4, n2 => 4},
    VCb = #{n0 => 2, n1 => 3, n2 => 4},
    false = happens_before(VCa, VCb),
    false = happens_before(VCb, VCa),
    ok.

test_happens_before() ->
    [test_happens_before(N) || N <- lists:seq(1,5)].




















