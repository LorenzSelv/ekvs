%% lab4kvs_vcmanager
%% Implements functionalities for handling vector clocks
%% Store the vector clock of the current node

-module(lab4kvs_vcmanager).
-behaviour(gen_server).

%% Server functions

-export([start_link/0]).
-export([view_change/1]).
-export([merge_vcs/1]).
-export([new_event/1]).
-export([broadcast_vc_to/1]).

-export([get_timestamp/0]).
-export([cp_to_vc/1]).
-export([vc_to_cp/1]).
-export([happens_before/2]).

%% Server Callbacks
-export([init/1, terminate/2, handle_info/2, handle_cast/2, handle_call/3, code_change/3]).

-ifdef(TEST).
-compile(export_all).
-endif.

-record(state, {vector_clock}).


%%%%% Interface %%%%% 

start_link() ->
    %% TODO register server globally
    gen_server:start_link({local, ?MODULE}, [], []).


view_change(NewPartitions) ->
    %% TODO how to compare VC after view changes
    gen_server:call(?MODULE, {update_vc, NewPartitions}).


merge_vcs(VCs) ->
    %% Given a list of vector clocks, merge them with the
    %% vector clock of the current node and return the result.
    %% The result is also used to update the vector clock
    %% of the current node
    gen_server:call(?MODULE, {merge_vcs, VCs}).


new_event(CausalPayload) ->
    gen_server:call(?MODULE, {new_event, CausalPayload}).


broadcast_vc_to(Nodes) ->
    %% Broadcast the vector clock of the current node to all
    %% the specified nodes
    gen_server:call(?MODULE, {broadcasted_vc, Nodes}).


%%%%% Server Callbacks %%%%% 

init([]) ->
    Nodes = lab4kvs_viewmanager:get_all_nodes(),
    %% All nodes are initialized with clock 0
    VCList = lists:map(fun(Node) -> {Node, 0} end, Nodes),
    VC = maps:from_list(VCList),
    {ok, #state{vector_clock=VC}}.


handle_call({view_change, NewPartitions}, _From, #state{vector_clock=VC}) ->
    %% TODO
    {reply, ok, todo};


handle_call({new_event, CausalPayload}, _From, #state{vector_clock=VC}) ->
    RequestVC = cp_to_vc(CausalPayload),
    MergedVC  = get_merged_vcs([RequestVC, VC]),
    Clock = maps:get(node(), MergedVC),
    NewVC = maps:put(node(), Clock+1, MergedVC),
    {reply, NewVC, #state{vector_clock=NewVC}};


handle_call({merge_vcs, VCs}, _From, #state{vector_clock=VC}) ->
    MergedVC = get_merged_vcs(VCs ++ [VC]),
    {reply, MergedVC, #state{vector_clock=MergedVC}};


handle_call({broadcast_vc_to, _Nodes}, _From, State=#state{vector_clock=VC}) ->
    %% TODO
    {reply, ok, State}.

%% TODO handle receive broadcasted_vc

handle_info({debug, Pid}, View) ->
    %% Hack to do some debug
    Pid ! View, 
    {noreply, View};


handle_info(Msg, View) ->
    io:format("Unknown message: ~p~n", [Msg]),
    {noreply, View}.


handle_cast(Msg, State) ->
    io:format("Unknown message: ~p~n", [Msg]),
    {noreply, State}.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


terminate(_Reason, _State) ->
    ok.


%%%%% Util functions %%%%%

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

cp_to_vc(CausalPayload) ->
    NodeClockStrList = string:split(CausalPayload, ",", all),
    StrToPair = fun(NodeClockStr) ->
                    [Node, Clock] = string:split(NodeClockStr, ":", all),
                    {binary_to_atom(Node, latin1), binary_to_integer(Clock)}
                end,
    NodeClockList = lists:map(StrToPair, NodeClockStrList),
    maps:from_list(NodeClockList).


vc_to_cp(VC) ->
    NodeClockList = maps:to_list(VC),
    PairToStr = fun({Node,Clock}) ->
                    L = [atom_to_list(Node), integer_to_list(Clock)],
                    string:join(L, ":") end,
    NodeClockStrList = lists:map(PairToStr, NodeClockList),
    NodeClockStr = string:join(NodeClockStrList, ","),
    list_to_binary(NodeClockStr).


happens_before(VCa, VCb) when is_map(VCa) andalso is_map(VCb) ->
    %% Standard happens_before relationship for vector clocks
    %%
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


comparable_vcs(VCs) ->
    %% A list of vector clocks is comparable iff all of them refer
    %% to the same set of nodes
    io:format("Comparing VCs ~n~p~n", [VCs]),
    VCNodesList = [maps:keys(VC) || VC <- VCs],
    VCNodes = lists:sort(hd(VCNodesList)),
    Comparable = fun(OtherVCNodes) ->
                    VCNodes =:= lists:sort(OtherVCNodes) end,
    %% Return true if they are all comparable
    lists:all(Comparable, tl(VCNodesList)).
         
    
get_merged_vcs(VCs) ->
    true  = comparable_vcs(VCs),
    Nodes = maps:keys(hd(VCs)),
    GetNodeClockPair = fun(Node) ->
                           Clock = lists:max([maps:get(Node, VC) || VC <- VCs]),
                           {Node, Clock} end,
    MergedVCList = lists:map(GetNodeClockPair, Nodes),
    maps:from_list(MergedVCList).









