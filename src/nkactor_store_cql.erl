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

-module(nkactor_store_cql).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([get_cassandra_srv/1]).
-export([query/2]).
-export([truncate/1]).

-define(LLOG(Type, Txt, Args), lager:Type("NkACTOR CASSANDRA "++Txt, Args)).


%% ===================================================================
%% Types
%% ===================================================================



%% ===================================================================
%% API
%% ===================================================================


%% @doc
get_cassandra_srv(ActorSrvId) ->
    nkserver:get_cached_config(ActorSrvId, nkactor_store_cql, cassandra_service).



%% @doc Performs a query. Must use the Cassandra service
-spec query(nkserver:id(), string()|binary()) ->
    {ok, nkcassandra:result()} | {error, term()}.

query(CassSrvId, Query) ->
    nkserver_ot:tag(actor_store_cassandra, sql, Query),
    nkcassandra:query(CassSrvId, Query).


truncate(ActorSrvId) ->
    nkactor_store_cql_init:truncate(get_cassandra_srv(ActorSrvId)).
