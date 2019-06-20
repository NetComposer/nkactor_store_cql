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

%% @doc Default plugin callbacks
-module(nkactor_store_cql_callbacks).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').


-export([actor_store_cql_parse/4, actor_store_cql_unparse/4]).
-export([actor_db_init/1,
         actor_db_find/3, actor_db_read/3, actor_db_create/3, actor_db_update/3,
         actor_db_delete/3, actor_db_search/3, actor_db_aggregate/3,
         actor_db_truncate/2]).


-include("nkactor_store_cql.hrl").
-include_lib("nkserver/include/nkserver.hrl").


%% ===================================================================
%% Offered callbacks
%% ===================================================================


%% @doc Called after reading the actor, to process further de-serializations
-spec actor_store_cql_parse(nkserver:id(), nkactor:actor(), map(), db_opts()) ->
    {ok, nkactor:actor(), map()} | {error, term()}.

actor_store_cql_parse(_SrvId, Actor, Meta, _Opts) ->
    {ok, Actor, Meta}.


%% @doc Called before saving the actor, to process further serializations
-spec actor_store_cql_unparse(nkserver:id(), nkactor:actor(), create|updated, db_opts()) ->
    {ok, nkactor:actor()} | {error, term()}.

actor_store_cql_unparse(_SrvId, _Op, Actor, _Opts) ->
    {ok, Actor}.



%% ===================================================================
%% Persistence callbacks
%% ===================================================================

-type id() :: nkserver:id().
-type actor_id() :: nkactor:actor_id().
-type actor() :: nkactor:actor().

-type continue() :: nkserver_callbacks:continue().

-type db_opts() :: nkactor_callbacks:db_opts().


%% @doc Called after the core has initialized the database
-spec actor_db_init(nkserver:id()) ->
    ok | {error, term()} | continue().

actor_db_init(_SrvId) ->
    ok.


%% @doc Must find an actor on disk by UID (if available) or name, and return
%% full actor_id data
-spec actor_db_find(id(), actor_id(), db_opts()) ->
    {ok, actor_id(), Meta::map()} | {error, actor_not_found|term()} | continue().

actor_db_find(SrvId, ActorId, Opts) ->
    call(SrvId, find, ActorId, Opts).


%% @doc Must find and read a full actor on disk by UID (if available) or name
-spec actor_db_read(id(), actor_id(), db_opts()) ->
    {ok, nkactor:actor(), Meta::map()} | {error, actor_not_found|term()} | continue().

actor_db_read(SrvId, ActorId, Opts) ->
    case call(SrvId, read, [ActorId], Opts) of
        {ok, RawActor, Meta} ->
            case nkactor_syntax:parse_actor(RawActor) of
                {ok, Actor} ->
                    ?CALL_SRV(SrvId, actor_store_cql_parse, [SrvId, Actor, Opts, Meta]);
                {error, Error} ->
                    {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    end.


%% @doc Must create a new actor on disk. Should fail if already present
-spec actor_db_create(id(), actor(), db_opts()) ->
    {ok, Meta::map()} | {error, uniqueness_violation|term()} | continue().

actor_db_create(SrvId, Actor, Opts) ->
    case ?CALL_SRV(SrvId, actor_store_cql_unparse, [SrvId, create, Actor, Opts]) of
        {ok, Actor2} ->
            call(SrvId, create, Actor2, Opts);
        {error, Error} ->
            {error, Error}
    end.


%% @doc Must update a new actor on disk.
-spec actor_db_update(id(), actor(), db_opts()) ->
    {ok, Meta::map()} | {error, term()} | continue().

actor_db_update(SrvId, Actor, Opts) ->
    case ?CALL_SRV(SrvId, actor_store_cql_unparse, [SrvId, update, Actor, Opts]) of
        {ok, Actor2} ->
            call(SrvId, update, Actor2, Opts);
        {error, Error} ->
            {error, Error}
    end.


%% @doc
-spec actor_db_delete(id(), actor_id(), db_opts()) ->
    {ok, [actor_id()], Meta::map()} | {error, term()} | continue().

actor_db_delete(SrvId, ActorId, Opts) ->
    call(SrvId, delete, ActorId, Opts).


%% @doc
-spec actor_db_search(id(), nkactor_backend:search_type(), db_opts()) ->
    {ok, [actor_id()], Meta::map()} | {error, term()} | continue().

actor_db_search(SrvId, Type, Opts) ->
    case nkactor_store_cql:get_cassandra_srv(SrvId) of
        undefined ->
            continue;
        CassSrvId ->
            start_span(CassSrvId, <<"search">>, Opts),
            Result = case nkactor_store_cql_search:search(Type, Opts) of
                {ok, Actors, Meta} ->
                    {ok, Actors, Meta};
                {query, Query, Fun} ->
                    case nkactor_store_cql:query(CassSrvId, Query, #{}) of
                        {ok, {_, _, Fields}} ->
                            Fun(Fields, Opts);
                        {error, Error} ->
                            {error, Error}
                    end;
                {error, Error} ->
                    {error, Error}
            end,
            stop_span(),
            Result
    end.


%% @doc
-spec actor_db_aggregate(id(), nkactor_backend:agg_type(), db_opts()) ->
    {ok, [actor_id()], Meta::map()} | {error, term()} | continue().

actor_db_aggregate(SrvId, Type, Opts) ->
    case nkactor_store_cql:get_cassandra_srv(SrvId) of
        undefined ->
            continue;
        CassSrvId ->
            start_span(CassSrvId, <<"aggregate">>, Opts),
            Result = case nkactor_store_cql_aggregation:aggregation(Type, Opts) of
                {query, Query, Fun} ->
                    case nkactor_store_cql:query(CassSrvId, Query, #{}) of
                        {ok, {_, _, Fields}} ->
                            Fun(Fields, Opts);
                        {error, Error} ->
                            {error, Error}
                    end;
                {error, Error} ->
                    {error, Error}
            end,
            stop_span(),
            Result
    end.


%% @doc
-spec actor_db_truncate(id(), db_opts()) ->
    {ok, Meta::map()} | {error, term()} | continue().

actor_db_truncate(SrvId, _Opts) ->
    case nkactor_store_cql:get_cassandra_srv(SrvId) of
        undefined ->
            continue;
        CassSrvId ->
            nkactor_store_cql_init:truncate(CassSrvId)
    end.




%% ===================================================================
%% Internal
%% ===================================================================

%% @private
call(SrvId, Op, Arg, Opts) ->
    case nkactor_store_cql:get_cassandra_srv(SrvId) of
        undefined ->
            continue;
        CassSrvId ->
            start_span(CassSrvId, Op, Opts),
            Opts2 = case Op==create orelse Op==update orelse Op==delete of
                true ->
                    SaveTimes = nkserver:get_cached_config(SrvId, nkactor_store_cql, save_times),
                    Opts#{save_times=>SaveTimes};
                false ->
                    Opts
            end,
            Result = nkactor_store_cql_actors:Op(CassSrvId, Arg, Opts2),
            stop_span(),
            Result
    end.


%% @private
start_span(SrvId, Op, Opts) ->
    ParentSpan = maps:get(ot_span_id, Opts, undefined),
    SpanName = <<"CASSANDRA::", (nklib_util:to_binary(Op))/binary>>,
    nkserver_ot:new(?CQL_SPAN, SrvId, SpanName, ParentSpan).


%% @private
stop_span() ->
    nkserver_ot:finish(?CQL_SPAN).
