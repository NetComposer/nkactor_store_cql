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
-export([find/3, read/3, create/3, update/3, delete/3, add_label/3, batch/3]).
-import(nkactor_store_cql, [query/2]).

-define(LLOG(Type, Txt, Args), lager:Type("NkACTOR CASSANDRA "++Txt, Args)).


-include_lib("nkactor/include/nkactor.hrl").

%% ===================================================================
%% Types
%% ===================================================================


%% ===================================================================
%% API
%% ===================================================================

%% @doc
find(SrvId, #actor_id{group=Group, resource=Res, name=Name, namespace=Namespace}=ActorId, _Opts)
        when is_binary(Group), is_binary(Res), is_binary(Name), is_binary(Namespace) ->
    Query = list_to_binary([
        <<"SELECT uid FROM actors">>,
        <<" WHERE namespace=">>, quote(Namespace),
        <<" AND \"group\"=">>, quote(Group),
        <<" AND resource=">>, quote(Res),
        <<" AND name=">>, quote(Name), <<";">>
    ]),
    case query(SrvId, Query) of
        {ok, {_, _, [[UID2]]}} ->
            {ok, ActorId#actor_id{uid=UID2, pid=undefined}, #{}};
        {ok, {_, _, []}} ->
            {error, actor_not_found};
        {error, Error} ->
            {error, Error}
    end;

find(SrvId, #actor_id{uid=UID}, _Opts) when is_binary(UID) ->
    Query = list_to_binary([
        <<"SELECT namespace,\"group\",resource,name FROM actors_uid">>,
        <<" WHERE uid=">>, quote(UID), <<";">>
    ]),
    case query(SrvId, Query) of
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
        case query(SrvId, Query) of
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


-record(save_fields, {
    uids = [],
    actors = [],
    indices = [],
    uuid_columns,
    actor_columns,
    index_columns
}).


%% @doc
%% no_unique_check: do not check uniqueness
%% no_indices: do not generate entries in index table

create(SrvId, Actor, #{no_unique_check:=true}=Opts) ->
    Fields = populate_fields([Actor], #save_fields{}, Opts),
    #save_fields{
        actors = [ActorFields],
        uids = [UIDFields],
        indices = IndicesFields,
        actor_columns = ActorColumns,
        uuid_columns = UIDColumns,
        index_columns = IndexColumns
    } = Fields,
    IndexQueries = list_to_binary([
        <<" INSERT INTO actors_index (", IndexColumns/binary, ")"
        " VALUES (", Values/binary, ");">>
        || Values <- IndicesFields
    ]),
    ActorQuery = <<
        "BEGIN BATCH ",
        "INSERT INTO actors (", ActorColumns/binary, ") VALUES (", ActorFields/binary, "); ",
        "INSERT INTO actors_uid (", UIDColumns/binary, ") VALUES (", UIDFields/binary, "); ",
        IndexQueries/binary,
        " APPLY BATCH;"
    >>,
    case query(SrvId, ActorQuery) of
        ok ->
            {ok, #{}};
        {error, Error} ->
            {error, Error}
    end;

create(SrvId, Actor, Opts) ->
    Fields = populate_fields([Actor], #save_fields{}, Opts),
    #save_fields{
        actors = [ActorFields],
        uids = [UIDFields],
        indices = IndicesFields,
        actor_columns = ActorColumns,
        uuid_columns = UIDColumns,
        index_columns = IndexColumns
    } = Fields,
    Query1 = <<
        "INSERT INTO actors (", ActorColumns/binary, ") VALUES (",
        ActorFields/binary, ") IF NOT EXISTS; "
    >>,
    case query(SrvId, Query1) of
        {ok, {_, _, [[false|_]]}} ->
            {error, uniqueness_violation};
        {ok, {_, _, [[true|_]]}} ->
            IndexQueries = list_to_binary([
                <<" INSERT INTO actors_index (", IndexColumns/binary, ")"
                " VALUES (", Values/binary, ");">>
                || Values <- IndicesFields
            ]),
            Query2 = <<
                "BEGIN BATCH ",
                "INSERT INTO actors_uid (", UIDColumns/binary, ") VALUES (", UIDFields/binary, "); ",
                IndexQueries/binary,
                " APPLY BATCH;"
            >>,
            case query(SrvId, Query2) of
                ok ->
                    {ok, #{}};
                {error, Error} ->
                    {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    end.


%%%% @private
%%create_indices(_SrvId, []) ->
%%    {ok, #{}};
%%
%%create_indices(SrvId, [Single]) ->
%%    case query(SrvId, Single) of
%%        ok ->
%%            {ok, #{}};
%%        {error, Error} ->
%%            {error, Error}
%%    end;
%%
%%create_indices(SrvId, Indices) ->
%%    Query = list_to_binary([
%%        <<"BEGIN BATCH ">>,
%%        Indices,
%%        <<" APPLY BATCH;">>
%%    ]),
%%    case query(SrvId,Query) of
%%        ok ->
%%            {ok, #{}};
%%        {error, Error} ->
%%            {error, Error}
%%    end.


%% @doc
%% Opts removed_indices
update(_SrvId, Actor, Opts) ->
    Fields = populate_fields([Actor], #save_fields{}, Opts),
    #save_fields{
        actors = [ActorFields],
        indices = IndicesFields,
        actor_columns = ActorColumns,
        index_columns = IndexColumns
    } = Fields,
    ActorQuery =
        <<"INSERT INTO actors (", ActorColumns/binary, ") VALUES (",ActorFields/binary, ");">>,
    DeleteQuery = [
        [
            <<"DELETE FROM actors_index WHERE ">>,
            <<"class=">>, quote(Class), <<" AND ">>,
            <<"key=">>, quote(Key), <<" AND ">>,
            <<"value=">>, quote(Value), <<" AND ">>,
            <<"uid=">>, quote(UID), <<";">>
        ]
        ||
        {Class, Key, Value, UID} <- maps:get(removed_indices, Opts, [])
    ],
    IndexQuery = [
        <<"INSERT INTO actors_index (", IndexColumns/binary, ") VALUES (",Values/binary, ");">>
        || Values <- IndicesFields
    ],
    _Query = case DeleteQuery==[] andalso IndexQuery==[] of
        true ->
            ActorQuery;
        false ->
            list_to_binary([
                <<"BEGIN BATCH ">>,
                ActorQuery,
                DeleteQuery,
                IndexQuery,
                <<" APPLY BATCH;">>
            ])
    end,
    {ok, #{}}.
%%    case query(SrvId, Query) of
%%        ok ->
%%            {ok, #{}};
%%        {error, Error} ->
%%            {error, Error}
%%    end.


%% @doc
add_label(SrvId, {Key, Value, ActorId}, _Opts) ->
    #actor_id{group=Group, resource=Res, name=Name, namespace=Namespace, uid=UID} = ActorId,
    Values = nklib_util:bjoin([
        <<"'label'">>,
        quote(Key),
        quote(to_bin(Value)),
        quote(Namespace),
        quote(Group),
        quote(Res),
        quote(Name),
        quote(UID),
        quote(nklib_date:now_3339(usecs))
    ]),
    Columns = <<"class,key,value,namespace,\"group\",resource,name,uid,last_update">>,
    Query = <<"INSERT INTO actors_index (", Columns/binary, ") VALUES (", Values/binary, ");">>,
    case nkcassandra:query(SrvId, Query, quorum) of
        ok ->
            ok;
        {error, Error} ->
            {error, Error}
    end.



% Create in batch, mode all actors and indices at the same time,
% no uniqueness check
batch(SrvId, Actors, Opts) ->
    Fields = populate_fields(Actors, #save_fields{}, Opts),
    #save_fields{
        uids = _UIDs,
        actors = ActorsFields,
        indices = IndicesFields,
        actor_columns = ActorColumns,
        index_columns = IndexColumns
    } = Fields,
    ActorsQueries = [
        <<" INSERT INTO actors (", ActorColumns/binary, ") VALUES (",
            Values/binary, ");">>
        || Values <- ActorsFields
    ],
    IndexQueries = [
        <<" INSERT INTO actors_index (", IndexColumns/binary, ")"
        " VALUES (", Values/binary, ");">>
        || Values <- IndicesFields
    ],
    Query = list_to_binary([
        <<"BEGIN BATCH ">>,
        ActorsQueries,
        IndexQueries,
        <<" APPLY BATCH;">>
    ]),
    case query(SrvId, Query) of
        ok ->
            {ok, #{}};
        {error, Error} ->
            {error, Error}
    end.



%% @private
populate_fields([], #save_fields{indices=Indices}=SaveFields, _Opts) ->
    SaveFields#save_fields{
        indices = lists:flatten(Indices),
        uuid_columns = <<"uid,namespace,\"group\",resource,name">>,
        actor_columns = <<"namespace,\"group\",resource,name,uid,data,metadata,last_update,is_active,expires">>,
        index_columns = <<"class,key,value,namespace,\"group\",resource,name,uid,last_update">>
    };

populate_fields([Actor|Rest], SaveFields, Opts) ->
    #save_fields{
        uids = UIDs,
        actors = Actors,
        indices = Indices
    } = SaveFields,
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
    Updated = maps:get(update_time, Meta),
    IsActive = maps:get(is_active, Meta, false),
    Expires = case maps:get(expires_time, Meta, <<>>) of
        <<>> ->
            <<>>;
        Exp1 ->
            {ok, Exp2} = nklib_date:to_epoch(Exp1, secs),
            Exp2
    end,
    FtsWords1 = maps:fold(
        fun(Key, Text, Acc) ->
            Acc#{Key => nkactor_lib:fts_normalize_multi(Text)}
        end,
        #{},
        maps:get(fts, Meta, #{})),
    QNamespace = quote(Namespace),
    QGroup = quote(Group),
    QRes = quote(Res),
    QName = quote(Name),
    QUpdated = quote(Updated),
    UID2 = nklib_util:bjoin([QUID, QNamespace, QGroup, QRes, QName]),
    Actor2 = nklib_util:bjoin([
        QNamespace, QGroup, QRes, QName, QUID,
        quote(Data), quote(Meta), QUpdated, quote(IsActive), quote(Expires)
    ]),
    SaveFields2 = SaveFields#save_fields{
        uids = [UID2|UIDs],
        actors = [Actor2|Actors]
    },
    case maps:get(no_indices, Opts, false) of
        true ->
            populate_fields(Rest, SaveFields2, Opts);
        false ->
            IndexTail = [QNamespace, QGroup, QRes, QName, QUID, QUpdated],
            Indices2 = maps:fold(
                fun(Key, Val, Acc) ->
                    [
                        nklib_util:bjoin([<<"'label'">>, quote(Key), quote(to_bin(Val)) | IndexTail])
                        | Acc
                    ]
                end,
                Indices,
                maps:get(labels, Meta, #{})),
            Indices3 = maps:fold(
                fun(U2, LinkType, Acc) ->
                    [
                        nklib_util:bjoin([<<"'link'">>, quote(LinkType), quote(U2) | IndexTail])
                        | Acc
                    ]
                end,
                Indices2,
                maps:get(links, Meta, #{})),
            Indices4 = maps:fold(
                fun(Field, WordList, Acc) ->
                    [
                        [
                            nklib_util:bjoin([<<"'fts'">>, quote(Field), quote(Word) | IndexTail])
                            | Acc
                        ] || Word <- WordList
                    ]
                end,
                Indices3,
                FtsWords1),
            SaveFields3 = SaveFields2#save_fields{indices = Indices4},
            populate_fields(Rest, SaveFields3, Opts)
    end.


%% @doc
%% Option 'cascade' to delete all linked
delete(SrvId, UID, Opts) when is_binary(UID) ->
    delete(SrvId, [UID], Opts);

delete(_SrvId, _UIDs, _Opts) ->
    ok.


quote(Term) ->
    list_to_binary([nkactor_sql:quote(Term)]).



%% @private
to_bin(Term) when is_binary(Term) -> Term;
to_bin(Term) -> nklib_util:to_binary(Term).
