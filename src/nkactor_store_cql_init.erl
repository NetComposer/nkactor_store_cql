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

-module(nkactor_store_cql_init).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([init/1, init/2, drop/1, truncate/1]).
-import(nkactor_store_cql, [query/2]).
-define(LLOG(Type, Txt, Args), lager:Type("NkACTOR CASSANDRA "++Txt, Args)).


%% ===================================================================
%% API
%% ===================================================================

%% @private
init(SrvId) ->
    init(SrvId, 10).


%% @private
init(SrvId, Tries) when Tries > 0 ->
    case query(SrvId, <<"SELECT id,version FROM versions">>) of
        {ok, {_, _, Rows}} ->
            Tables = lists:foldl(
                fun([Id, Vsn], Acc) -> Acc#{Id => Vsn} end,
                #{},
                Rows),
            case Tables of
                #{
                    <<"actors">> := <<"1">>,
                    <<"actors_uid">> := <<"1">>
                } ->
                    ?LLOG(notice, "detected database at last version", []),
                    ok;
                _ ->
                    ?LLOG(warning, "detected database at wrong version: ~p", [Tables]),
                    {error, database_not_recognizez}
            end;
        {error, {cql_error, 8704, _}} ->
            ?LLOG(warning, "database not found: Creating it.", []),
            create(SrvId),
            ok;
        {error, Error} ->
            ?LLOG(notice, "could notcreate database: ~p (~p tries left)", [Error, Tries]),
            timer:sleep(1000),
            init(SrvId, Tries-1)
    end;

init(_SrvId, _Tries) ->
    {error, database_not_available}.



%% @private
create(SrvId) ->
    create(SrvId, [versions, actors, actors_uid, actors_index]).


create(_SrvId, []) ->
    ok;

create(SrvId, [versions|Rest]) ->
    Query = <<"
        CREATE TABLE versions (
            id text PRIMARY KEY,
            version text
        );
    ">>,
    {ok, _} = query(SrvId, Query),
    create(SrvId, Rest);

create(SrvId, [actors|Rest]) ->
    Query1 = <<"
        CREATE TABLE actors (
            namespace text,
            \"group\" text,
            resource text,
            name text,
            uid text,
            data text,
            metadata text,
            PRIMARY KEY ((namespace,\"group\",resource), name)
        );
    ">>,
    {ok, _} = query(SrvId, Query1),
    Query3 = <<"
        INSERT INTO versions (id, version) VALUES ('actors', '1');
    ">>,
    ok = query(SrvId, Query3),
    create(SrvId, Rest);

create(SrvId, [actors_uid|Rest]) ->
    Query = <<"
        CREATE TABLE actors_uid (
            uid text PRIMARY KEY,
            namespace text,
            \"group\" text,
            resource text,
            name text
        );
    ">>,
    {ok, _} = query(SrvId, Query),
    Query2 = <<"
        INSERT INTO versions (id, version) VALUES ('actors_uid', '1');
    ">>,
    ok = query(SrvId, Query2),
    create(SrvId, Rest);

create(SrvId, [actors_index|Rest]) ->
    Query1 = <<"
        CREATE TABLE actors_index (
            class text,
            key text,
            value text,
            uid text,
            namespace text,
            \"group\" text,
            resource text,
            name text,
            PRIMARY KEY ((class, key), value, uid)
        );
    ">>,
    {ok, _} = query(SrvId, Query1),
    Query2 = <<"
        INSERT INTO versions (id, version) VALUES ('actors_index', '1');
    ">>,
    ok = query(SrvId, Query2),
    create(SrvId, Rest).



%% @private
drop(SrvId) ->
    Tables = [actors_uid, actors, actors_index, versions],
    drop(SrvId, Tables).


drop(_SrvId, []) ->
    ok;

drop(SrvId, [actors_uid|Rest]) ->
    Query = <<"DROP TABLE IF EXISTS actors_uid">>,
    _ = query(SrvId, Query),
    drop(SrvId, Rest);

drop(SrvId, [actors|Rest]) ->
    Query = <<"DROP TABLE IF EXISTS actors">>,
    _ = query(SrvId, Query),
    drop(SrvId, Rest);

drop(SrvId, [actors_index|Rest]) ->
    Query = <<"DROP TABLE IF EXISTS actors_index">>,
    _ = query(SrvId, Query),
    drop(SrvId, Rest);

drop(SrvId, [versions|Rest]) ->
    Query = <<"DROP TABLE IF EXISTS versions">>,
    _ = query(SrvId, Query),
    drop(SrvId, Rest).



%% @private
truncate(SrvId) ->
    Query1 = <<"TRUNCATE actors">>,
    ok = query(SrvId, Query1),
    Query2 = <<"TRUNCATE actors_uid">>,
    ok = query(SrvId, Query2),
    Query3 = <<"TRUNCATE actors_index">>,
    ok = query(SrvId, Query3),
    ok.


