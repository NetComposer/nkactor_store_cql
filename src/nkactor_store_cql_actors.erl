%% -------------------------------------------------------------------
%%
%% Copyright (c) 2019 Carlos Gonzalez Florido.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(nkactor_store_cql_actors).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([find/3, read/3, create/3, update/3, delete/3, quote/1]).
-export([get_time_day/0, get_time_day/1]).
-import(nkactor_store_cql, [query/3]).
-import(nklib_util, [bjoin/1]).

-define(LLOG(Type, Txt, Args), lager:Type("NkACTOR CASSANDRA "++Txt, Args)).

-define(ACTOR_COLUMNS, "namespace,group,resource,name,uid,data,metadata").
-define(UUID_COLUMNS, "part,uid,namespace,group,resource,name").
-define(INDEX_COLUMNS, "class,key,value,uid,namespace,group,resource,name").
-define(UID_PARTITIONS, 1024).

-include_lib("nkactor/include/nkactor.hrl").
-include("nkactor_store_cql.hrl").


%% ===================================================================
%% Types
%% ===================================================================


-record(fields, {
    uid,
    actor,
    add_indices,
    delete_indices
}).



%% ===================================================================
%% API
%% ===================================================================

%% @doc
find(SrvId, #actor_id{group=Group, resource=Res, name=Name, namespace=Namespace}=ActorId, Opts)
        when is_binary(Group), is_binary(Res), is_binary(Name), is_binary(Namespace) ->
    Query = list_to_binary([
        <<"SELECT uid FROM actors">>,
        <<" WHERE namespace=">>, quote(Namespace),
        <<" AND \"group\"=">>, quote(Group),
        <<" AND resource=">>, quote(Res),
        <<" AND name=">>, quote(Name), <<";">>
    ]),
    case query(SrvId, Query, Opts) of
        {ok, {_, _, [[UID2]]}} ->
            {ok, ActorId#actor_id{uid=UID2, pid=undefined}, #{}};
        {ok, {_, _, []}} ->
            {error, actor_not_found};
        {error, Error} ->
            {error, Error}
    end;

find(SrvId, #actor_id{uid=UID}, _Opts) when is_binary(UID) ->
    QUID = quote(UID),
    QPart = quote(erlang:phash2(UID) rem ?UID_PARTITIONS),
    Query = <<
        "SELECT namespace,group,resource,name FROM actors_uid",
        " WHERE part=", QPart/binary, " AND uid=", QUID/binary, ";"
    >>,
    case query(SrvId, Query, #{span_op=><<"ActorFind">>}) of
        {ok, {_, _, [[Namespace, Group, Res, Name]]}} ->
            ActorId2 = #actor_id{
                uid = UID,
                group = Group,
                resource = Res,
                name = Name,
                namespace = Namespace
            },
            {ok, ActorId2, #{}};
        {ok, {_, _, []}} ->
            {error, actor_not_found};
        {error, Error} ->
            {error, Error}
    end.


%% @doc
read(SrvId, #actor_id{namespace=Namespace, group=Group, resource=Res, name=Name}, _Opts)
        when is_binary(Group), is_binary(Res), is_binary(Name), is_binary(Namespace) ->
    Query = list_to_binary([
            <<"SELECT uid,metadata,data FROM actors ">>,
            <<" WHERE namespace=">>, quote(Namespace),
            <<" AND \"group\"=">>, quote(Group),
            <<" AND resource=">>, quote(Res),
            <<" AND name=">>, quote(Name), <<";">>
        ]),
        case query(SrvId, Query, #{span_op=><<"ActorRead">>}) of
            {ok, {_, _, [[UID, Meta, Data]]}} ->
                Actor = #{
                    group => Group,
                    resource => Res,
                    name => Name,
                    namespace => Namespace,
                    uid => UID,
                    data => nklib_json:decode(Data),
                    metadata => nklib_json:decode(Meta)
                },
                {ok, Actor, #{}};
            {ok, {_, _, [[]]}} ->
                {error, actor_not_found};
            {error, Error} ->
                {error, Error}
        end;

read(SrvId, #actor_id{uid=UID}, Opts) when is_binary(UID) ->
    case find(SrvId, #actor_id{uid=UID}, Opts) of
        {ok, ActorId2} ->
            read(SrvId, ActorId2, Opts);
        {error, Error} ->
            {error, Error}
    end.


%% @doc
%% no_unique_check: do not check uniqueness
%% no_indices: do not generate entries in index table

create(SrvId, Actor, #{check_unique:=false}=Opts) ->
    Fields = make_fields(Actor, Opts),
    #fields{
        actor = ActorFields,
        uid = UIDFields,
        add_indices = IndicesFields
    } = Fields,
    IndexQueries = list_to_binary([
        <<" INSERT INTO actors_index (", ?INDEX_COLUMNS, ")"
        " VALUES (", (bjoin(Values))/binary, ");">>
        || Values <- IndicesFields
    ]),
    ActorFields2 = bjoin(ActorFields),
    UIDFields2 = bjoin(UIDFields),
    ActorQuery = <<
        "BEGIN BATCH ",
        "INSERT INTO actors (", ?ACTOR_COLUMNS, ") VALUES (", ActorFields2/binary, "); ",
        "INSERT INTO actors_uid (", ?UUID_COLUMNS, ") VALUES (", UIDFields2/binary, "); ",
        IndexQueries/binary,
        " APPLY BATCH;"
    >>,
    QueryOpts = #{
        span_op => <<"ActorCreate">>,
        span_tags => #{<<"actor.uid">> => maps:get(uid, Actor)}
    },
    case query(SrvId, ActorQuery, QueryOpts) of
        ok ->
            save_time(SrvId, Actor, create, Opts),
            {ok, #{}};
        {error, Error} ->
            {error, Error}
    end;

create(SrvId, Actor, Opts) ->
    Fields = make_fields(Actor, Opts),
    #fields{
        actor = ActorFields,
        uid = UIDFields,
        add_indices = IndicesFields

    } = Fields,
    ActorFields2 = bjoin(ActorFields),
    Query1 = <<
        "INSERT INTO actors (", ?ACTOR_COLUMNS, ") VALUES (", ActorFields2/binary, ") IF NOT EXISTS; "
    >>,
    QueryOpts = #{
        span_op => <<"ActorCreate (1/2)">>,
        span_tags => #{<<"actor.uid">> => maps:get(uid, Actor)}
    },
    case query(SrvId, Query1, QueryOpts) of
        {ok, {_, _, [[false|_]]}} ->
            {error, uniqueness_violation};
        {ok, {_, _, [[true|_]]}} ->
            IndexQueries = list_to_binary([
                <<" INSERT INTO actors_index (", ?INDEX_COLUMNS, ")"
                " VALUES (", (bjoin(Values))/binary, ");">>
                || Values <- IndicesFields
            ]),
            UIDFields2 = bjoin(UIDFields),
            Query2 = <<
                "BEGIN BATCH ",
                "INSERT INTO actors_uid (", ?UUID_COLUMNS, ") VALUES (", UIDFields2/binary, "); ",
                IndexQueries/binary,
                " APPLY BATCH;"
            >>,
            QueryOpts2 = QueryOpts#{span_op => <<"ActorCreate (2/2)">>},
            case query(SrvId, Query2, QueryOpts2) of
                ok ->
                    save_time(SrvId, Actor, create, Opts),
                    {ok, #{}};
                {error, Error} ->
                    {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    end.


%% @doc
%% Opts removed_indices
update(SrvId, Actor, #{last_metadata:=_}=Opts) ->
    Fields = make_fields(Actor, Opts),
    #fields{
        actor = ActorFields,
        add_indices = AddIndices,
        delete_indices = DeleteIndices
    } = Fields,
    %lager:error("NKLOG DELETE INDICES ~p", [DeleteIndices]),
    DeleteQuery = [
        [
            <<"DELETE FROM actors_index WHERE ">>,
            <<"class=">>, QClass, <<" AND ">>,
            <<"key=">>, QKey, <<" AND ">>,
            <<"value=">>, QValue,<<" AND ">>,
            <<"uid=">>, QUID, <<";">>
        ]
        ||
        [QClass, QKey, QValue, QUID] <- DeleteIndices
    ],
    %lager:error("NKLOG DELETE QUERY ~p", [DeleteQuery]),
    ActorQuery = <<
        "INSERT INTO actors (", ?ACTOR_COLUMNS, ") "
        "VALUES (", (bjoin(ActorFields))/binary, ");"
    >>,
    IndexQuery = [
        <<
            "INSERT INTO actors_index (", ?INDEX_COLUMNS, ") "
            "VALUES (", (bjoin(Values))/binary, ");"
        >>
        || Values <- AddIndices
    ],
    Query = list_to_binary([
        <<"BEGIN BATCH ">>,
        DeleteQuery,
        ActorQuery,
        IndexQuery,
        <<" APPLY BATCH;">>
    ]),
    QueryOpts = #{
        span_op => <<"ActorUpdate">>,
        span_tags => #{<<"actor.uid">> => maps:get(uid, Actor)}
    },
    case query(SrvId, Query, QueryOpts) of
        ok ->
            save_time(SrvId, Actor, update, Opts),
            {ok, #{}};
        {error, Error} ->
            {error, Error}
    end.



%%%% @doc
%%add_label(SrvId, {Key, Value, ActorId}, _Opts) ->
%%    #actor_id{group=Group, resource=Res, name=Name, namespace=Namespace, uid=UID} = ActorId,
%%    Values = nklib_util:bjoin([
%%        <<"'label'">>,
%%        quote(Key),
%%        quote(to_bin(Value)),
%%        quote(Namespace),
%%        quote(Group),
%%        quote(Res),
%%        quote(Name),
%%        quote(UID),
%%    ]),
%%    Columns = <<"class,key,value,namespace,\"group\",resource,name,uid,last_update">>,
%%    Query = <<"INSERT INTO actors_index (", Columns/binary, ") VALUES (", Values/binary, ");">>,
%%    case nkcassandra:query(SrvId, Query, quorum) of
%%        ok ->
%%            ok;
%%        {error, Error} ->
%%            {error, Error}
%%    end.




%% @doc
%% Option 'cascade' to delete all linked
delete(SrvId, UID, Opts) when is_binary(UID) ->
    %nkactor_store_cql_time_srv:save_time(SrvId, UID, delete),
    delete(SrvId, [UID], Opts);

delete(_SrvId, _UIDs, _Opts) ->
    ok.


%% ===================================================================
%% Internal
%% ===================================================================


%% @private
make_fields(Actor, Opts) ->
    #{
        uid := UID,
        namespace := Namespace,
        group := Group,
        resource := Res,
        name := Name,
        data := Data,
        metadata := Meta
    } = Actor,
    true = is_binary(UID) andalso UID /= <<>>,
    QUID = quote(UID),
    QPart = quote(erlang:phash2(UID) rem 1024),
    QNamespace = quote(Namespace),
    QGroup = quote(Group),
    QRes = quote(Res),
    QName = quote(Name),
    QFullName = [QNamespace, QGroup, QRes, QName],
    OldMeta = maps:get(last_metadata, Opts, #{}),
    {Old2, New2} = make_is_active([], [], OldMeta, Meta, QUID, QFullName),
    {Old3, New3} = make_expires(Old2, New2, OldMeta, Meta, QUID, QFullName),
    {Old4, New4} = make_labels(Old3, New3, OldMeta, Meta, QUID, QFullName),
    {Old5, New5} = make_links(Old4, New4, OldMeta, Meta, QUID, QFullName),
    {Old6, New6} = {Old5, New5},
    #fields{
        actor = QFullName ++ [QUID, quote(Data), quote(Meta)],
        uid = [QPart, QUID | QFullName],
        delete_indices = Old6,
        add_indices = New6
    }.


make_is_active(Old, New, OldMeta, Meta, QUID, QFullName) ->
    NewIsActive = maps:get(is_active, Meta, false),
    OldIsActive = maps:get(is_active, OldMeta, false),
    case {OldIsActive, NewIsActive} of
        {false, false} ->
            {Old, New};
        {false, true} ->
            {
                Old,
                [[<<"'db'">>, <<"'active'">>, <<"'T'">>, QUID | QFullName]|New]
            };
        {true, false} ->
            {
                [[<<"'db'">>, <<"'active'">>, <<"'T'">>, QUID]|Old],
                New
            };
        {true, true} ->
            {Old, New}
    end.


make_expires(Old, New, OldMeta, Meta, QUID, QFullName) ->
    NewExpires = maps:get(expires_time, Meta, <<>>),
    OldExpires = maps:get(expires_time, OldMeta, <<>>),
    case {OldExpires, NewExpires} of
        {Same, Same} ->
            {Old, New};
        {<<>>, Time} ->
            {
                Old,
                [[<<"'db'">>, <<"'expires'">>, quote(Time), QUID | QFullName]|New]
            };
        {OldTime, <<>>} ->
            {
                % Since there is no expires, we remove all
                [[<<"'db'">>, <<"'expires'">>, quote(OldTime), QUID]|Old],
                New
            };
        {OldTime, NewTime} ->
            {
                % We cannot remove all, since it can remove also the new
                [[<<"'db'">>, <<"'expires'">>, quote(OldTime), QUID]|Old],
                [[<<"'db'">>, <<"'expires'">>, quote(NewTime), QUID | QFullName]|New]
            }
    end.


make_labels(Old, New, OldMeta, Meta, QUID, QFullName) ->
    NewLabels = maps:get(labels, Meta, #{}),
    OldLabels = maps:get(labels, OldMeta, #{}),
    %lager:error("NKLOG OLD LABELS ~p", [OldLabels]),
    %lager:error("NKLOG NEW LABELS ~p", [NewLabels]),
    Old2 = lists:foldl(
        fun({Key, OldValue}, AccOld) ->
            case maps:is_key(Key, NewLabels) of
                true ->
                    AccOld;
                false ->
                    [[<<"'label'">>, quote(Key), quote(OldValue), QUID]|AccOld]
            end
        end,
        Old,
        maps:to_list(OldLabels)
    ),
    {Old3, New3} = lists:foldl(
        fun({Key, Value}, {AccOld, AccNew}) ->
            case maps:find(Key, OldLabels) of
                {ok, Value} ->
                    {AccOld, AccNew};
                {ok, OldValue} ->
                    {
                        [[<<"'label'">>, quote(Key), quote(OldValue), QUID]|AccOld],
                        [[<<"'label'">>, quote(Key), quote(Value), QUID | QFullName]|AccNew]
                    };
                error ->
                    {
                        AccOld,
                        [[<<"'label'">>, quote(Key), quote(Value), QUID | QFullName]|AccNew]
                    }
            end
        end,
        {Old2, New},
        maps:to_list(NewLabels)),
    %lager:error("NKLOG NEw3 ~p", [New3]),
    {Old3, New3}.


make_links(Old, New, OldMeta, Meta, QUID, QFullName) ->
    NewLinks = maps:get(links, Meta, #{}),
    OldLinks = maps:get(links, OldMeta, #{}),
    Old2 = lists:foldl(
        fun({Key, OldValue}, AccOld) ->
            case maps:is_key(Key, NewLinks) of
                true ->
                    AccOld;
                false ->
                    [[<<"'link'">>, quote(Key), quote(OldValue), QUID]|AccOld]
            end
        end,
        Old,
        maps:to_list(OldLinks)
    ),
    {Old3, New3} = lists:foldl(
        fun({Key, Value}, {AccOld, AccNew}) ->
            case maps:find(Key, OldLinks) of
                {ok, Value} ->
                    {AccOld, AccNew};
                {ok, OldValue} ->
                    {
                        [[<<"'link'">>, quote(Key), quote(OldValue), QUID]|AccOld],
                        [[<<"'link'">>, quote(Key), quote(Value), QUID | QFullName]|AccNew]
                    };
                error ->
                    {
                        AccOld,
                        [[<<"'links'">>, quote(Key), quote(Value), QUID | QFullName]|AccNew]
                    }
            end
        end,
        {Old2, New},
        maps:to_list(NewLinks)),
    {Old3, New3}.



%% @doc
get_time_day() ->
    get_time_day(nklib_date:epoch(usecs)).


%% @doc
get_time_day(Time) ->
    Time div (24*60*60*1000*1000).


%% @private
save_time(SrvId, Actor, Op, #{save_times:=true}) ->
    #{
        namespace := Namespace,
        group := Group,
        resource := Res,
        name := Name,
        uid := UID
    } = Actor,
    Time = nklib_date:epoch(usecs),
    Query = list_to_binary([
        <<"INSERT INTO actors_time (day,time,namespace,group,resource,name,uid,op) VALUES (">>,
        integer_to_binary(get_time_day(Time)), $,,
        integer_to_binary(Time), $,,
        quote(Namespace), $,,
        quote(Group), $,,
        quote(Res), $,,
        quote(Name), $,,
        quote(UID), $,,
        quote(Op), <<");">>
    ]),
    case nkcassandra:query(SrvId, Query) of
        ok ->
            ok;
        {error, Error} ->
            lager:warning("CQL TIME (~p) could not save: ~p", [SrvId, Error])
    end;

save_time(_SrvId, _Op, _Actor, _) ->
    ok.




%% @private
quote(Field) when is_binary(Field) -> <<$', (to_field(Field))/binary, $'>>;
quote(Field) when is_list(Field) -> <<$', (to_field(Field))/binary, $'>>;
quote(Field) when is_integer(Field); is_float(Field) -> to_bin(Field);
quote(true) -> <<"TRUE">>;
quote(false) -> <<"FALSE">>;
quote(null) -> <<"NULL">>;
quote(Field) when is_atom(Field) -> quote(atom_to_binary(Field, utf8));
quote(Field) when is_map(Field) ->
    case nklib_json:encode(Field) of
        error ->
            lager:error("Error enconding JSON: ~p", [Field]),
            error(json_encode_error);
        Json when is_binary(Json)->
            quote(Json)
    end.


%% @private
to_field(Field) ->
    Field2 = to_bin(Field),
    case binary:match(Field2, <<$'>>) of
        nomatch ->
            Field2;
        _ ->
            re:replace(Field2, <<$'>>, <<$',$'>>, [global, {return, binary}])
    end.


%% @private
to_bin(Term) when is_binary(Term) -> Term;
to_bin(Term) -> nklib_util:to_binary(Term).

