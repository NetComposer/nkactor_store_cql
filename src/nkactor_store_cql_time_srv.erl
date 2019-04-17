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

-module(nkactor_store_cql_time_srv).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-behaviour(gen_server).

-export([start_link/2, save_time/3]).
-export([init/1, terminate/2, code_change/3, handle_call/3,  handle_cast/2, handle_info/2]).


%% ===================================================================
%% Types
%% ===================================================================




%% ===================================================================
%% Public
%% ===================================================================


%% @doc
save_time(CassSrvId, UID, Op) ->
    Time = nklib_date:epoch(usecs),
    case nklib_proc:values({?MODULE, CassSrvId}) of
        [{_, Pid}] ->
            gen_server:cast(Pid, {save_time, Time, Op, UID});
        [] ->
            {error, service_not_started}
    end.


%% @private
start_link(ActorSrvId, CassSrvId) ->
    gen_server:start_link(?MODULE, [ActorSrvId, CassSrvId], []).



% ===================================================================
%% gen_server behaviour
%% ===================================================================

-record(state, {
    actor_srv,
    cass_srv
}).


%% @private
init([ActorSrvId, CassSrvId]) ->
    true = nklib_proc:reg({?MODULE, CassSrvId}),
    {ok, #state{actor_srv=ActorSrvId, cass_srv=CassSrvId}}.


%% @private
handle_call(Msg, _From, State) ->
    {stop, {unexpected_handle_call, Msg}, State}.


%% @private
handle_cast({save_time, Time, Op, UID}, #state{cass_srv = SrvId}=State) ->
    Day = Time div (24*60*60*1000*1000),
    Query = <<
        "INSERT INTO actors_time (day,time,uid,op) VALUES (",
        (integer_to_binary(Day))/binary, ",",
        (integer_to_binary(Time))/binary, ",",
        "'", UID/binary, "',",
        "'", (nklib_util:to_binary(Op))/binary, "');"
    >>,
    case nkcassandra:query(SrvId, Query) of
        ok ->
            ok;
        {error, Error} ->
            lager:warning("CQL TIME (~p) could not save: ~p", [SrvId, Error])
    end,
    {noreply, State}.


handle_info(Msg, State) ->
    lager:warning("Module ~p received unexpected info: ~p", [?MODULE, Msg]),
    {noreply, State}.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


terminate(_Reason, _State) ->
    ok.
