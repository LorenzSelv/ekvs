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
-export([update_vc/1]).
-export([get_vc/0]).
-export([get_cp/0]).

%% -export([broadcast_vc_to/1]).

-export([get_timestamp/0]).
-export([cp_to_vc/1]).
-export([vc_to_cp/1]).
-export([happens_before/2]).
-export([happens_before_or_equal/2]).

%% Server Callbacks
-export([init/1, terminate/2, handle_info/2, handle_cast/2, handle_call/3, code_change/3]).

-ifdef(TEST).
-compile(export_all).
-endif.

-record(state, {vector_clock}).


%%%%% Interface %%%%% 

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


view_change(AllNodes) ->
    %% Update the VC of the current node after a view change
    gen_server:call(?MODULE, {view_change, AllNodes}).


merge_vcs(VCs) ->
    %% Given a list of vector clocks, merge them with the
    %% vector clock of the current node and return the result.
    %% The result is also used to update the vector clock
    %% of the current node
    gen_server:call(?MODULE, {merge_vcs, VCs}).


new_event(CausalPayload) ->
    gen_server:call(?MODULE, {new_event, CausalPayload}).


update_vc(VC) ->
    gen_server:call(?MODULE, {update_vc, VC}).


get_vc() ->
    gen_server:call(?MODULE, get_vc).


get_cp() ->
    gen_server:call(?MODULE, get_cp).


%% broadcast_vc_to(Nodes) ->
    %% Broadcast the vector clock of the current node to all
    %% the specified nodes
    %% gen_server:call(?MODULE, {broadcasted_vc, Nodes}).


%%%%% Server Callbacks %%%%% 

init([]) ->
    %% All nodes are initialized with clock 0
    VC = get_default_vc(),
    {ok, #state{vector_clock=VC}}.


handle_call({view_change, AllNodes}, _From, #state{vector_clock=VC}) ->
    %% Increment the number of view changes
    ViewChanges = {view_changes, maps:get(view_changes, VC) + 1},
    NewVCList = [{Node, maps:get(Node, VC, 0)} || Node <- AllNodes],
    NewVC = maps:from_list(NewVCList ++ [ViewChanges]),
    {reply, ok, #state{vector_clock=NewVC}};


handle_call({new_event, CausalPayload}, _From, #state{vector_clock=VC}) ->
    lab4kvs_debug:call({new_event,CausalPayload, VC}),
    RequestVC = cp_to_vc(CausalPayload),
    MergedVC  = get_merged_vcs([RequestVC, VC]),
    Clock = maps:get(node(), MergedVC),
    NewVC = maps:put(node(), Clock+1, MergedVC),
    lab4kvs_debug:return({new_event,NewVC}),
    {reply, NewVC, #state{vector_clock=NewVC}};


handle_call({merge_vcs, VCs}, _From, #state{vector_clock=VC}) ->
    MergedVC = get_merged_vcs(VCs ++ [VC]),
    {reply, MergedVC, #state{vector_clock=MergedVC}};


handle_call({update_vc, NewVC}, _From, #state{vector_clock=VC}) ->
    MergedVC = get_merged_vcs([VC, NewVC]),
    {reply, ok, #state{vector_clock=MergedVC}};


handle_call(get_vc, _From, S=#state{vector_clock=VC}) ->
    {reply, VC, S};


handle_call(get_cp, _From, S=#state{vector_clock=VC}) ->
    {reply, vc_to_cp(VC), S}.


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


get_default_vc() ->
    Nodes = lab4kvs_viewmanager:get_all_nodes(),
    %% All nodes are initialized with clock 0
    VCList = lists:map(fun(Node) -> {Node, 0} end, Nodes),
    %% Number of view_changes is initialized to 0
    maps:from_list(VCList ++ [{view_changes, 0}]).


%% CausalPayload is a binary string in the format 
%% <<"node@<ip1>:Clock1,...,node@<ipN>:ClockN,view_changes=NumViewChanges">>
%% 
%% VC is the map representation of that string
%% 'node@<ip1>'   => Clock1
%%   ...
%% 'node@<ipN>'   => ClockN
%% 'view_changes' => NumViewChanges
%%

cp_to_vc(<<"">>) -> get_default_vc();
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
    lab4kvs_debug:call({happens_before, VCa, VCb}),
    %% Standard happens_before relationship for vector clocks
    %% This function should be called only if the VCa and VCb
    %% are comparable
    true = comparable_vcs([VCa, VCb]),
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


happens_before_or_equal(VCa, VCb) when is_map(VCa) andalso is_map(VCb) ->
    happens_before(VCa, VCb) orelse VCa =:= VCb.


comparable_vcs([_VC]) -> true;
comparable_vcs(VCs) ->
    %% A list of vector clocks is comparable iff all of them refer
    %% to the same set of nodes
    io:format("comparable_vcs ? VCs ~n~p~n", [VCs]),
    VCNodesList = [maps:keys(VC) || VC <- VCs],
    VCNodes = lists:sort(hd(VCNodesList)),
    Comparable = fun(OtherVCNodes) ->
                    VCNodes =:= lists:sort(OtherVCNodes) end,
    %% Return true if they are all comparable
    lists:all(Comparable, tl(VCNodesList)).
         
    
get_merged_vcs(VCs) ->
    %% Vector Clocks might be non-comparable because of view_changes
    %% Perform the merge only on most recent ones 
    lab4kvs_debug:call({get_merged_vcs, VCs}),
    ViewChanges = [maps:get(view_changes, VC) || VC <- VCs],
    Min = lists:min(ViewChanges),
    Max = lists:max(ViewChanges),
    CompVCs = case Min =:= Max of
                false ->  %% Take only the most recent vector clocks
                    IsRecent = fun(VC) -> maps:get(view_changes, VC) =:= Max end,
                    lists:filter(IsRecent, VCs);
                true  -> %% All vector clocks have the same number of view_changes
                    VCs
              end,
    io:format("CompsVCs ~p~n", [CompVCs]),
    %% Make sure the vector clocks are now comparable
    true  = comparable_vcs(CompVCs),
    Nodes = maps:keys(hd(CompVCs)),
    GetNodeClockPair = fun(Node) ->
                           Clock = lists:max([maps:get(Node, VC) || VC <- CompVCs]),
                           {Node, Clock} end,
    MergedVCList = lists:map(GetNodeClockPair, Nodes),
    MergedVC = maps:from_list(MergedVCList),
    lab4kvs_debug:return({get_merged_vcs, MergedVC}),
    MergedVC.



