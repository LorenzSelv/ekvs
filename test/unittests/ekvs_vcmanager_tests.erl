-module(ekvs_vcmanager_tests).


-include_lib("eunit/include/eunit.hrl").

%%%%%%%% TESTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

test_CP_VC() ->
    CP = <<"node@10.0.0.0:1,node@10.0.0.2:4,node@10.0.0.3:3">>,
    VC = ekvs_vcmanager:cp_to_vc(CP),
    io:format("~p~n", [VC]),
    CP = ekvs_vcmanager:vc_to_cp(VC).

happens_before_test(1) ->
    %% a -> b
    VCa = #{n0 => 0, n1 => 2, n2 => 3},
    VCb = #{n0 => 1, n1 => 3, n2 => 4},
    true  = ekvs_vcmanager:happens_before(VCa, VCb),
    false = ekvs_vcmanager:happens_before(VCb, VCa),
    ok;

happens_before_test(2) ->
    %% a -> b
    VCa = #{n0 => 1, n1 => 3, n2 => 3},
    VCb = #{n0 => 1, n1 => 3, n2 => 4},
    true  = ekvs_vcmanager:happens_before(VCa, VCb),
    false = ekvs_vcmanager:happens_before(VCb, VCa),
    ok;

happens_before_test(3) ->
    %% b -> a
    VCa = #{n0 => 1, n1 => 3, n2 => 4},
    VCb = #{n0 => 0, n1 => 3, n2 => 4},
    false = ekvs_vcmanager:happens_before(VCa, VCb),
    true  = ekvs_vcmanager:happens_before(VCb, VCa),
    ok;

happens_before_test(4) ->
    %% concurrent
    VCa = #{n0 => 1, n1 => 3, n2 => 4},
    VCb = #{n0 => 1, n1 => 3, n2 => 4},
    false = ekvs_vcmanager:happens_before(VCa, VCb),
    false = ekvs_vcmanager:happens_before(VCb, VCa),
    ok;

happens_before_test(5) ->
    %% concurrent
    VCa = #{n0 => 1, n1 => 4, n2 => 4},
    VCb = #{n0 => 2, n1 => 3, n2 => 4},
    false = ekvs_vcmanager:happens_before(VCa, VCb),
    false = ekvs_vcmanager:happens_before(VCb, VCa),
    ok.

happens_before_test() ->
    [happens_before_test(N) || N <- lists:seq(1,5)].

