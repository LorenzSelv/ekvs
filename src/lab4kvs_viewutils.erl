%% lab4kvs_handler
%% Handler module kvs rest api

-module(lab4kvs_viewutils).

-export([hash/1]).
-export([gen_nodes/1]).
-export([gen_node/1]).
-export([gen_tokens/2]).
-export([gen_tokens_node/2]).
-export([get_key_owner/2]).
-export([get_prev/2]).
-export([get_next/2]).
-export([insert_token/2]).
-export([delete_token/2]).

%% unitest functions
-export([test_keyowner/0]).
-export([test_prev/0]).
-export([test_wraparound/0]).


-define(HASHALGO, md5).
-define(HASHMOD,  277).


hash(String) ->
    BinHash = crypto:hash(?HASHALGO, String),
    binary:decode_unsigned(BinHash) rem ?HASHMOD.

gen_nodes(none) -> [];
gen_nodes(IPPortListStr) ->
    %% Given a comma separated list of ip:port
    %%   e.g. "10.0.0.20:8080,10.0.0.21:8080"
    %% Return the list of node names in the format node@<ip>
    %%   e.g. ['node@10.0.0.20', 'node@10.0.0.21'] 
    %%
    %% Notes: node is given as default name
    %%       the single quotes represent an atom
    %% 
    IPPortList = string:split(IPPortListStr, ",", all), 
    lists:map(fun gen_node/1, IPPortList).

gen_node(IPPort) ->
    %% Same as above, but just for one ip:port
    [IP, _Port] = string:split(IPPort, ":", all),
    list_to_atom("node@" ++ IP).

gen_tokens(none, _) -> [];
gen_tokens(IPPortListStr, TokensPerNode) ->
    %% Given a comma separated list of ip:port
    %%   e.g. "10.0.0.20:8080,10.0.0.21:8080"
    %% Return the list of tokens in the format {<hash>, node@<ip>}
    %% For each Node `TokensPerNode` tokens are generated.
    %%
    Nodes = gen_nodes(IPPortListStr),
    ListOfListOfTokens = [gen_tokens_node(Node, TokensPerNode) || Node <- Nodes],
    lists:append(ListOfListOfTokens).     
        

gen_tokens_node(Node, TokensPerNode) ->
    NodeStr = atom_to_list(Node),
    TokenNums = lists:map(fun integer_to_list/1, lists:seq(1, TokensPerNode)),
    %% Concatenate the node name with a number to perturbate the hash
    [{hash(NodeStr++TokenNum), Node} || TokenNum <- TokenNums].


get_key_owner(Key, Tokens) ->
    %% Given a Key and the Tokens list, return the owner 
    %% (next token in the ring)
    %%
    FakeToken = {hash(Key), none},
    get_next(FakeToken, Tokens).


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

%%%%%%%% TESTS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

dump(Data) ->
    io:format("~p~n", [Data]).

test_common() ->
    TokensPerNode = 2,
    IPPortListStr = "10.0.0.20:8080,10.0.0.21:8080,10.0.0.22:8080",
    Nodes = gen_nodes(IPPortListStr),
    dump(Nodes),
    Tokens = gen_tokens(IPPortListStr, TokensPerNode),
    dump(lists:sort(Tokens)),
    dump(Tokens),
    Tokens.

test_keyowner() ->
    Tokens = test_common(),
    Keys = ["One", "Two", "Three", "Four", "Five", "Six"],
    KeyHashes = [hash(Key) || Key <- Keys],
    [dump({H, K}) || {H, K} <- lists:zip(KeyHashes, Keys)],
    KeyOwners = [get_key_owner(Key, Tokens) || Key <- Keys],
    [dump({H, O}) || {H, O} <- lists:zip(KeyHashes, KeyOwners)].

test_prev() ->
    Tokens = test_common(),
    Keys = ["One", "Two", "Three", "Four", "Five", "Six"],
    KeyHashes = [hash(Key) || Key <- Keys],
    [dump({H, K}) || {H, K} <- lists:zip(KeyHashes, Keys)],
    Prevs = [get_prev({Hash, null}, Tokens) || Hash <- KeyHashes],
    [dump({P, H}) || {H, P} <- lists:zip(KeyHashes, Prevs)].

test_wraparound() ->
    Tokens = [{10, node1}, {20, node2}, {30, node3}],
    Hashes = [5, 15, 25, 35],
    Prevs = [get_prev({Hash, null}, Tokens) || Hash <- Hashes],
    Nexts = [get_next({Hash, null}, Tokens) || Hash <- Hashes],
    [{30, node3}, {10, node1}, {20, node2}, {30, node3}] = Prevs, 
    [{10, node1}, {20, node2}, {30, node3}, {10, node1}] = Nexts, 
    [dump({P,H,N}) || {P,H,N} <- lists:zip3(Prevs, Hashes, Nexts)].

