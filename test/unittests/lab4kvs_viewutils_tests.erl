-module(lab4kvs_viewutils_tests).

-include_lib("eunit/include/eunit.hrl").

%%%%%%%% TESTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

dump(Data) ->
    io:format(user, "~p~n", [Data]).

test_common() ->
    TokensPerPartition = 3,
    K = 4,
    IPPortListStr = "10.0.0.20:8080,10.0.0.21:8080,10.0.0.22:8080," ++
                    "10.0.0.23:8080,10.0.0.24:8080,10.0.0.25:8080",
    Partitions = lab4kvs_viewutils:gen_partitions(IPPortListStr, K),
    dump(Partitions),
    Tokens = lab4kvs_viewutils:gen_tokens(maps:size(Partitions), TokensPerPartition),
    dump(lists:sort(Tokens)),
    Tokens.

test_keyowner() ->
    Tokens = test_common(),
    Keys = ["One", "Two", "Three", "Four", "Five", "Six"],
    KeyHashes = [lab4kvs_viewutils:hash(Key) || Key <- Keys],
    [dump({H, K}) || {H, K} <- lists:zip(KeyHashes, Keys)],
    KeyOwners = [lab4kvs_viewutils:get_key_owner_token(Key, Tokens) || Key <- Keys],
    [dump({H, O}) || {H, O} <- lists:zip(KeyHashes, KeyOwners)].

test_prev() ->
    Tokens = test_common(),
    Keys = ["One", "Two", "Three", "Four", "Five", "Six"],
    KeyHashes = [lab4kvs_viewutils:hash(Key) || Key <- Keys],
    [dump({H, K}) || {H, K} <- lists:zip(KeyHashes, Keys)],
    Prevs = [lab4kvs_viewutils:get_prev({Hash, null}, Tokens) || Hash <- KeyHashes],
    [dump({P, H}) || {H, P} <- lists:zip(KeyHashes, Prevs)].

test_wraparound() ->
    Tokens = [{10, node1}, {20, node2}, {30, node3}],
    Hashes = [5, 15, 25, 35],
    Prevs = [lab4kvs_viewutils:get_prev({Hash, null}, Tokens) || Hash <- Hashes],
    Nexts = [lab4kvs_viewutils:get_next({Hash, null}, Tokens) || Hash <- Hashes],
    [{30, node3}, {10, node1}, {20, node2}, {30, node3}] = Prevs, 
    [{10, node1}, {20, node2}, {30, node3}, {10, node1}] = Nexts, 
    [dump({P,H,N}) || {P,H,N} <- lists:zip3(Prevs, Hashes, Nexts)].


both_directions(From, To, A, B) ->
    case {From, To} =:= {A, B} orelse 
         {From, To} =:= {B, A} of
        true -> ok;
        false -> error
    end.

can_merge_partition_test(1) ->
    K = 2,
    Partitions = #{p0 => [n0], p1 => [n1]},
    {true, FromID, ToID} = lab4kvs_viewutils:can_merge_partitions(Partitions, K),
    both_directions(FromID, ToID, p0, p1);

can_merge_partition_test(2) ->
    K = 2,
    Partitions = #{p0 => [n0], p1 => [n1], p3 => [n2, n3]},
    {true, FromID, ToID} = lab4kvs_viewutils:can_merge_partitions(Partitions, K),
    both_directions(FromID, ToID, p0, p1);

can_merge_partition_test(3) ->
    K = 2,
    Partitions = #{p0 => [n0, n1], p1 => [n2]},
    false = lab4kvs_viewutils:can_merge_partitions(Partitions, K),
    ok;

can_merge_partition_test(4) ->
    K = 2,
    Partitions = #{p0 => [n0]},
    false = lab4kvs_viewutils:can_merge_partitions(Partitions, K),
    ok;

can_merge_partition_test(5) ->
    K = 2,
    Partitions = #{p0 => [n0, n1], p1 => [n2, n3]},
    false = lab4kvs_viewutils:can_merge_partitions(Partitions, K),
    ok;

can_merge_partition_test(6) ->
    K = 3,
    Partitions = #{p0 => [n0, n1], p1 => [n1]},
    {true, p1, p0} = lab4kvs_viewutils:can_merge_partitions(Partitions, K),
    ok;

can_merge_partition_test(7) ->
    K = 3,
    Partitions = #{p0 => [n0, n1], p1 => [n2, n3]},
    false = lab4kvs_viewutils:can_merge_partitions(Partitions, K),
    ok;

can_merge_partition_test(8) ->
    K = 4,
    Partitions = #{p0 => [n0, n1], p1 => [n2, n3]},
    {true, FromID, ToID} = lab4kvs_viewutils:can_merge_partitions(Partitions, K),
    both_directions(FromID, ToID, p0, p1);

can_merge_partition_test(9) ->
    K = 4,
    Partitions = #{p0 => [n0, n1], p1 => [n2, n3], p2 => [n4, n5, n6, n7]},
    {true, FromID, ToID} = lab4kvs_viewutils:can_merge_partitions(Partitions, K),
    both_directions(FromID, ToID, p0, p1).

can_merge_partition_test() ->
    [can_merge_partition_test(N) || N <- lists:seq(1, 9)].


get_transformation_ops_test() ->
    pass.
