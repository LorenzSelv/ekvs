%% lab4kvs_partitionsinfohandler
%% Handler module implementing the endpoint for getting informations about partitions 

-module(lab4kvs_partitionsinfohandler).

-export([init/2]).

%% Response Header
-define(HEADER, #{<<"content-type">> => <<"application/json">>}).

%% Response Bodies
-define(BODY_GET_ID(ID), jsx:encode(#{<<"message">> => <<"success">>, <<"partition_id">> => ID})).

-define(BODY_GET_IDS(IDs), jsx:encode(#{<<"message">> => <<"success">>, <<"partition_id_list">> => IDs})).

-define(BODY_GET_MEMBERS(Members), jsx:encode(#{<<"message">> => <<"success">>, <<"partition_members">> => Members})).


init(Req0, State) ->
    RequestedInfoStr = cowboy_req:binding(requested_info, Req0),
    RequestedInfo = binary_to_atom(RequestedInfoStr, latin1),
    Req = handle_requested_info(RequestedInfo, Req0),
    {ok, Req, State}.


handle_requested_info(get_partition_id, Req0=#{ method := <<"GET">> }) ->
    ID = lab4kvs_viewmanager:get_partition_id(),
    cowboy_req:reply(200, ?HEADER, ?BODY_GET_ID(ID), Req0);


handle_requested_info(get_all_partition_ids, Req0=#{ method := <<"GET">> }) ->
    IDs = lab4kvs_viewmanager:get_partition_ids(),
    cowboy_req:reply(200, ?HEADER, ?BODY_GET_IDS(IDs), Req0);


handle_requested_info(get_partition_members, Req0=#{ method := <<"GET">> }) ->
    Data = cowboy_req:parse_qs(Req0),
    {_, PartitionIDStr} = lists:keyfind(<<"partition_id">>, 1, Data),
    PartitionID = binary_to_integer(PartitionIDStr),
    Members = lab4kvs_viewmanager:get_partition_members(PartitionID),
    cowboy_req:reply(200, ?HEADER, ?BODY_GET_MEMBERS(Members), Req0).

