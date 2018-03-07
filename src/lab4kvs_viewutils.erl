%% lab4kvs_viewutils
%% Implements util functions for the view manager 

-module(lab4kvs_viewutils).

-export([hash/1]).
-export([get_node_name/1]).
-export([get_partition_id/2]).
-export([gen_partitions/2]).
-export([gen_tokens/2]).
-export([gen_tokens_partition/2]).
-export([get_transformation_ops/4]).
-export([get_key_owner_token/2]).
-export([get_prev/2]).
-export([get_next/2]).
-export([insert_token/2]).
-export([delete_token/2]).

-ifdef(TEST).
-compile(export_all).
-endif.


-define(HASHALGO, md5).
-define(HASHMOD,  277).


hash(String) ->
    BinHash = crypto:hash(?HASHALGO, String),
    binary:decode_unsigned(BinHash) rem ?HASHMOD.


get_node_name(IPPort) ->
    %% Given a string "<ip>:<port>" return an atom 'node@<ip>'
    [IP, _Port] = string:split(IPPort, ":", all),
    list_to_atom("node@" ++ IP).


get_partition_id(_Node, []) -> -1;

get_partition_id(Node, [{ID, Nodes}|Partitions]) -> 
    case lists:member(Node, Nodes) of 
        true  -> ID; 
        false -> get_partition_id(Node, Partitions)
    end;

get_partition_id(Node, Partitions) ->
    get_partition_id(Node, maps:to_list(Partitions)).


gen_partitions(none, _) -> maps:new();
gen_partitions(IPPortListStr, K) ->
    %% Given a comma separated list of ip:port and the number of replicas per partition (K)
    %%   e.g. "10.0.0.20:8080,10.0.0.21:8080", K=2
    %% Return the map of partitions in the format {partition_id => list of nodes}
    %%   e.g. {0 => ['node@10.0.0.20', 'node@10.0.0.21']}
    %%
    %% Notes: node is given as default name, the single quotes represent an atom
    %% 
    IPPortList = string:split(IPPortListStr, ",", all),
    NodeList = lists:map(fun get_node_name/1, IPPortList), 
    N = length(NodeList), 
    GenPartition = fun(Start, Partitions) ->
                           PartID = (Start - 1) div K,
                           maps:put(PartID, lists:sublist(NodeList, Start, K), Partitions) end, 
    lists:foldl(GenPartition, maps:new(), lists:seq(1, N, K)).


gen_tokens(none, _) -> [];
gen_tokens(NumOfPartitions, TokensPerPartition) ->
    %% Given the number of partitions 
    %% Return the list of tokens in the format {<hash>, partition_id}
    %% For each Partition `TokensPerPartitions` tokens are generated.
    %%
    ListOfListOfTokens = [gen_tokens_partition(ID, TokensPerPartition) || 
                          ID <- lists:seq(0, NumOfPartitions-1)],
    lists:append(ListOfListOfTokens).


gen_tokens_partition(ID, TokensPerPartition) ->
    IDStr = integer_to_list(ID),
    TokenNums = lists:map(fun integer_to_list/1, lists:seq(1, TokensPerPartition)),
    %% Concatenate the node name with a number to perturbate the hash
    [{hash(IDStr++TokenNum), ID} || TokenNum <- TokenNums].


%% get_transformation_ops
%%
%% Return a list of one or more operations to sequentially
%% perform to the view in order to apply the view_change
%% An operation (Op) is in the format {OpType, Node, PartitionID}.
%%
%% There are 4 types of Ops:
%%   - {remove, NodeToRemove, AffectedPartitionID} ->
%%          remove the node from the partition
%%   - {remove_partition, NodeToRemove, RemovedPartitionID} --> 
%%          the removed node is the last of the partition, 
%%          remove the node and the partition 
%%   - {add, NewNode, AffectedPartitionID} ->
%%          add the node to the partition
%%   - {add_partition, NewNode, NewPartitionID} --> 
%%          all partitions have already K replicas,
%%          create a new partition and add the node to it
%%
%% The only case when the list is longer than 1 is during a remove
%% which results in 2 nodes having less than K replicas that can be
%% merged in a single one. 
%% For example for K=2 and Partitions
%% #{p0 => [n0, n1], p1 => [n2, n3], p2 => [n3]}
%% removing n2 results in
%% #{p0 => [n0, n1], p1 => [n3], p2 => [n3]}
%% p1 and p2 should be merged
%% #{p0 => [n0, n1], p1 => [n3, n3]}
%%
%% The returned value is the following list of Ops:
%% [{remove, n2, p1},           %% remove n2 from p1
%%  {remove_partition, n3, p2}, %% remove n3 and p2
%%  {add, n3, p1}]              %% add    n3 to p1
%%
%% TODO handle the case of more than one node for the partitions to merge
%%
get_transformation_ops(add, NewNode, Partitions, K) ->
    NonFullPartition = fun(_K, V) -> length(V) < K end, 
    NonFullPartitions = maps:filter(NonFullPartition, Partitions), 
    case maps:size(NonFullPartitions) of
        0 ->  %% All partitions are full, create a new one
            PartitionID = get_new_partition_id(Partitions),
            [{add_partition, NewNode, PartitionID}];
        _ ->  %% At least one partition has less than K replicas
              %% Add the node to the first non-full one
            PartitionID = hd(maps:keys(NonFullPartitions)),
            [{add, NewNode, PartitionID}]
    end;

get_transformation_ops(remove, NodeToRemove, Partitions, K) ->
    PartitionID = get_partition_id(NodeToRemove, Partitions),
    Nodes = maps:get(PartitionID, Partitions),
    case length(Nodes) of
        1 ->  %% The node is the only one in the partition, 
              %% remove the node and the partition 
            [{remove_partition, NodeToRemove, PartitionID}];
        _ ->  %% The node is not the only one in the partition, remove the node
              %% and check if there is a possibility to merge the affected partition
              %% with another non-full one
            FirstOp = {remove, NodeToRemove, PartitionID},
            NewPartitions = maps:put(PartitionID, Nodes -- [NodeToRemove], Partitions),
            case can_merge_partitions(NewPartitions, K) of
                {true, FromID, ToID} ->
                    %% Move all the nodes from the From partition to
                    %% the To partition, then remove the From partition
                    NodesToMove = maps:get(FromID, Partitions),
                    AddOps    = [{add,    Node, ToID}   || Node <- NodesToMove],
                    RemoveOps = [{remove, Node, FromID} || Node <- tl(NodesToMove)],
                    LastOp = {remove_partition, hd(NodeToRemove), FromID}, 
                    [FirstOp] ++ AddOps ++ RemoveOps ++ [LastOp];
                false ->
                    [FirstOp]
            end
    end.



get_new_partition_id(Partitions) ->
    %% Pick the lowest free partition_id starting from 0
    IDs = maps:keys(Partitions),
    get_new_partition_id(0, IDs).

get_new_partition_id(ID, []) -> ID;
get_new_partition_id(ID, IDs) ->
    case lists:member(ID, IDs) of
        false -> ID;
        true  -> get_new_partition_id(ID+1, IDs)
    end.


can_merge_partitions(Partitions, K) ->
    %% If two partitions can be merged (sum of both nodes <= K)
    %% then return {true, FromID, ToID}, false otherwise
    GetSize = fun(_ID, Nodes) -> length(Nodes) end,
    PartitionSizes = maps:map(GetSize, Partitions),
    IsNonFull = fun(_ID, Size) -> Size < K end,
    NonFullSizes = maps:filter(IsNonFull, PartitionSizes),
    SubtractFromK = fun({ID, Size}) -> {ID, K-Size} end,
    TargetSizes = lists:map(SubtractFromK, maps:to_list(NonFullSizes)),
    FindMatch = fun({ID, Target}, AccMatches) ->
                        find_match(ID, Target, AccMatches, NonFullSizes, K) end,
    Matches = lists:foldl(FindMatch, [], TargetSizes),
    case Matches of
        [] -> false;
        [{FromID, ToID}|_]-> {true, FromID, ToID}
    end.


find_match(ID, Target, AccMatches, NonFullSizes, K) ->
    IsPossibleMatch = fun(OtherID, Size) ->
                          ID =/= OtherID andalso
                          Target =:= Size end,
    PossibleMatches = maps:filter(IsPossibleMatch, NonFullSizes),
    case maps:size(PossibleMatches) of
        0 -> AccMatches;
        _ -> 
            OtherID = hd(maps:keys(PossibleMatches)),
            OtherSize = Target,
            MySize = K - Target,
            NewMatch = case OtherSize > MySize of
                           true  -> {ID, OtherID};
                           false -> {OtherID, ID}
                        end,
            AccMatches ++ [NewMatch]
    end.


get_key_owner_token(Key, Tokens) ->
    %% Given a Key and the Tokens list, return the owner 
    %% (next token in the ring)
    %%
    FakeToken = {hash(Key), none},
    get_next(FakeToken, Tokens).


%% TODO change name from Node to Partition

%% Implement the tokens as a unordered list
%% O(1) insertion of new a token 
%% O(N*TokensPerNode) lookup (prev, next, deletion)
%% 
%% Since the N and TokensPerNode are relatively small
%% there is not point to keep the a more complex data structure 
%% such as gb_trees (Balanced Binary Search Tree).

get_prev(_Token, []) -> error; %% empty view? never

get_prev(Token, Tokens) ->
    %% Prev = {PHash, PNode} 
    %% Prev is the element in Tokens such that PHash
    %% is the highest hash < THash
    %%
    %% If there is no such token, i.e. THash is smaller than
    %% all the hashes in the token list, then return the token
    %% with the highest hash (the ring wraps around).
    
    {THash, _TNode} = Token,

    Fun = fun({Hash, Node}, {AccHash, _AccNode}) 
                when Hash > AccHash andalso Hash =< THash -> {Hash, Node};
             (_, Acc) -> Acc end,
    %% io:format("MIN ~p~n", [lists:min(Tokens)]),
    {MinHash, _} = Min = lists:min(Tokens),
    case THash < MinHash of
        true -> %% Return last element (wraps around)
            lists:max(Tokens);
        false ->
            lists:foldl(Fun, Min, Tokens)
    end.


get_next(_Token, []) -> error; %% empty view? never

get_next(Token, Tokens) ->
    %% Token = {THash, TNode}
    %% Next  = {NHash, NNode} 
    %% Next is the element in Tokens such that NHash
    %% is the smallest hash > THash
    %%
    %% If there is no such token, i.e. THash is bigger than
    %% all the hashes in the token list, then return the token
    %% with the smallest hash (the ring wraps around).
    
    {THash, _TNode} = Token,

    Fun = fun({Hash, Node}, {AccHash, _AccNode}) 
                when Hash < AccHash andalso Hash >= THash -> {Hash, Node};
             (_, Acc) -> Acc end,

    {MaxHash, _} = Max = lists:max(Tokens),
    case THash > MaxHash of
        true -> %% Return first element (wraps around)
            lists:min(Tokens);
        false ->
            lists:foldl(Fun, Max, Tokens)
    end.


insert_token(Token, Tokens) ->
    %% You got it
    Tokens++[Token].

delete_token(Token, Tokens) ->
    %% You got it
    Tokens--[Token].


