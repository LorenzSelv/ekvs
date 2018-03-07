-module(lab4kvs_kvsutils_tests).


-include_lib("eunit/include/eunit.hrl").

%%%%%%%% TESTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

test_CP_VC() ->
    CP = <<"node@10.0.0.0:1,node@10.0.0.2:4,node@10.0.0.3:3">>,
    VC = lab4kvs_kvsutils:causal_payload_to_vector_clock(CP),
    io:format("~p~n", [VC]),
    CP = lab4kvs_kvsutils:vector_clock_to_causal_payload(VC).

test_happens_before(1) ->
    %% a -> b
    VCa = #{n0 => 0, n1 => 2, n2 => 3},
    VCb = #{n0 => 1, n1 => 3, n2 => 4},
    true  = lab4kvs_kvsutils:happens_before(VCa, VCb),
    false = lab4kvs_kvsutils:happens_before(VCb, VCa),
    ok;

test_happens_before(2) ->
    %% a -> b
    VCa = #{n0 => 1, n1 => 3, n2 => 3},
    VCb = #{n0 => 1, n1 => 3, n2 => 4},
    true  = lab4kvs_kvsutils:happens_before(VCa, VCb),
    false = lab4kvs_kvsutils:happens_before(VCb, VCa),
    ok;

test_happens_before(3) ->
    %% b -> a
    VCa = #{n0 => 1, n1 => 3, n2 => 4},
    VCb = #{n0 => 0, n1 => 3, n2 => 4},
    false = lab4kvs_kvsutils:happens_before(VCa, VCb),
    true  = lab4kvs_kvsutils:happens_before(VCb, VCa),
    ok;

test_happens_before(4) ->
    %% concurrent
    VCa = #{n0 => 1, n1 => 3, n2 => 4},
    VCb = #{n0 => 1, n1 => 3, n2 => 4},
    false = lab4kvs_kvsutils:happens_before(VCa, VCb),
    false = lab4kvs_kvsutils:happens_before(VCb, VCa),
    ok;

test_happens_before(5) ->
    %% concurrent
    VCa = #{n0 => 1, n1 => 4, n2 => 4},
    VCb = #{n0 => 2, n1 => 3, n2 => 4},
    false = lab4kvs_kvsutils:happens_before(VCa, VCb),
    false = lab4kvs_kvsutils:happens_before(VCb, VCa),
    ok.

happens_before_test() ->
    [test_happens_before(N) || N <- lists:seq(1,5)].

