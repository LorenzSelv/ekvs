-module(ekvs_viewutils_tests).

-include_lib("eunit/include/eunit.hrl").

%%%%%%%% TESTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

dump(Data) ->
    io:format(user, "~p~n", [Data]).

test_common() ->
    TokensPerPartition = 3,
    K = 4,
    IPPortListStr = "10.0.0.20:8080,10.0.0.21:8080,10.0.0.22:8080," ++
                    "10.0.0.23:8080,10.0.0.24:8080,10.0.0.25:8080",
    Partitions = ekvs_viewutils:gen_partitions(IPPortListStr, K),
    dump(Partitions),
    Tokens = ekvs_viewutils:gen_tokens(maps:size(Partitions), TokensPerPartition),
    dump(lists:sort(Tokens)),
    Tokens.

test_keyowner() ->
    Tokens = test_common(),
    Keys = ["One", "Two", "Three", "Four", "Five", "Six"],
    KeyHashes = [ekvs_viewutils:hash(Key) || Key <- Keys],
    [dump({H, K}) || {H, K} <- lists:zip(KeyHashes, Keys)],
    KeyOwners = [ekvs_viewutils:get_key_owner_token(Key, Tokens) || Key <- Keys],
    [dump({H, O}) || {H, O} <- lists:zip(KeyHashes, KeyOwners)].

test_prev() ->
    Tokens = test_common(),
    Keys = ["One", "Two", "Three", "Four", "Five", "Six"],
    KeyHashes = [ekvs_viewutils:hash(Key) || Key <- Keys],
    [dump({H, K}) || {H, K} <- lists:zip(KeyHashes, Keys)],
    Prevs = [ekvs_viewutils:get_prev({Hash, null}, Tokens) || Hash <- KeyHashes],
    [dump({P, H}) || {H, P} <- lists:zip(KeyHashes, Prevs)].

test_wraparound() ->
    Tokens = [{10, node1}, {20, node2}, {30, node3}],
    Hashes = [5, 15, 25, 35],
    Prevs = [ekvs_viewutils:get_prev({Hash, null}, Tokens) || Hash <- Hashes],
    Nexts = [ekvs_viewutils:get_next({Hash, null}, Tokens) || Hash <- Hashes],
    [{30, node3}, {10, node1}, {20, node2}, {30, node3}] = Prevs, 
    [{10, node1}, {20, node2}, {30, node3}, {10, node1}] = Nexts, 
    [dump({P,H,N}) || {P,H,N} <- lists:zip3(Prevs, Hashes, Nexts)].

%%% can_merge_partitions %%%

both_directions(From, To, A, B) ->
    case {From, To} =:= {A, B} orelse 
         {From, To} =:= {B, A} of
        true -> ok;
        false -> error
    end.

can_merge_partition_test(1) ->
    K = 2,
    Partitions = #{p0 => [n0], p1 => [n1]},
    {true, FromID, ToID} = ekvs_viewutils:can_merge_partitions(Partitions, K),
    both_directions(FromID, ToID, p0, p1);

can_merge_partition_test(2) ->
    K = 2,
    Partitions = #{p0 => [n0], p1 => [n1], p3 => [n2, n3]},
    {true, FromID, ToID} = ekvs_viewutils:can_merge_partitions(Partitions, K),
    both_directions(FromID, ToID, p0, p1);

can_merge_partition_test(3) ->
    K = 2,
    Partitions = #{p0 => [n0, n1], p1 => [n2]},
    false = ekvs_viewutils:can_merge_partitions(Partitions, K),
    ok;

can_merge_partition_test(4) ->
    K = 2,
    Partitions = #{p0 => [n0]},
    false = ekvs_viewutils:can_merge_partitions(Partitions, K),
    ok;

can_merge_partition_test(5) ->
    K = 2,
    Partitions = #{p0 => [n0, n1], p1 => [n2, n3]},
    false = ekvs_viewutils:can_merge_partitions(Partitions, K),
    ok;

can_merge_partition_test(6) ->
    K = 3,
    Partitions = #{p0 => [n0, n1], p1 => [n1]},
    {true, p1, p0} = ekvs_viewutils:can_merge_partitions(Partitions, K),
    ok;

can_merge_partition_test(7) ->
    K = 3,
    Partitions = #{p0 => [n0, n1], p1 => [n2, n3]},
    false = ekvs_viewutils:can_merge_partitions(Partitions, K),
    ok;

can_merge_partition_test(8) ->
    K = 4,
    Partitions = #{p0 => [n0, n1], p1 => [n2, n3]},
    {true, FromID, ToID} = ekvs_viewutils:can_merge_partitions(Partitions, K),
    both_directions(FromID, ToID, p0, p1);

can_merge_partition_test(9) ->
    K = 4,
    Partitions = #{p0 => [n0, n1], p1 => [n2, n3], p2 => [n4, n5, n6, n7]},
    {true, FromID, ToID} = ekvs_viewutils:can_merge_partitions(Partitions, K),
    both_directions(FromID, ToID, p0, p1).

can_merge_partition_test() ->
    [can_merge_partition_test(N) || N <- lists:seq(1, 9)].


%%% get_transformation_ops %%%

get_transformation_ops_test(1) ->
    K = 2,
    Partitions = #{0 => [n0, n1], 1 => [n2]},
    Ops = ekvs_viewutils:get_transformation_ops(add, n3, Partitions, K),
    ?assertMatch([{add, n3, 1}], Ops);

get_transformation_ops_test(2) ->
    K = 2,
    Partitions = #{0 => [n0, n1], 1 => [n2, n3]},
    Ops = ekvs_viewutils:get_transformation_ops(add, n4, Partitions, K),
    ?assertMatch([{add_partition, n4, 2}], Ops);

get_transformation_ops_test(3) ->
    K = 3,
    Partitions = #{0 => [n0, n1, n2], 1 => [n3]},
    Ops = ekvs_viewutils:get_transformation_ops(add, n4, Partitions, K),
    ?assertMatch([{add, n4, 1}], Ops);

%% Never happens
%% get_transformation_ops_test(4) ->
    %% K = 3,
    %% Partitions = #{0 => [n0, n1, n2], 1 => [n3]},
    %% Ops = ekvs_viewutils:get_transformation_ops(remove, n3, Partitions, K),
    %% ?assertMatch([{remove_partition, n3, 1}], Ops);

get_transformation_ops_test(4) ->
    K = 3,
    Partitions = #{0 => [n0, n1, n2], 1 => [n3, n4]},
    Ops = ekvs_viewutils:get_transformation_ops(remove, n3, Partitions, K),
    ?assertMatch([{remove, n3, 1}], Ops);

get_transformation_ops_test(5) ->
    K = 2,
    Partitions = #{0 => [n0, n1], 1 => [n2]},
    Ops = ekvs_viewutils:get_transformation_ops(remove, n1, Partitions, K),
    ?assertMatch([{remove, n1, 0},
                  {move_keys_merged_partition, 1, 0},
                  {remove, n2, 1},
                  {add, n2, 0}], Ops);

get_transformation_ops_test(6) ->
    K = 3,
    Partitions = #{0 => [n0, n1, n2], 1 => [n3]},
    Ops = ekvs_viewutils:get_transformation_ops(remove, n1, Partitions, K),
    ?assertMatch([{remove, n1, 0},
                  {move_keys_merged_partition, 1, 0},
                  {remove, n3, 1},
                  {add,    n3, 0}], Ops).


get_transformation_ops_test() ->
    [get_transformation_ops_test(N) || N <- lists:seq(1, 6)].


