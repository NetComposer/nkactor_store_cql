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

-module(nkactor_store_cql_search).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([search/2]).
-export([cassandra_actors/2, cassandra_delete/2, cassandra_any/2]).
-import(nkactor_sql, [quote/1, filter_path/2]).
-import(nkactor_store_cql, [query/2, query/3]).

-define(LLOG(Type, Txt, Args), lager:Type("NkACTOR CASSANDRA "++Txt, Args)).

-include_lib("nkactor/include/nkactor.hrl").

%% ===================================================================
%% Search Types
%% ===================================================================


search(actors_search_linked, Params) ->
    UID = maps:get(uid, Params),
    LinkType = maps:get(link_type, Params, any),
    Namespace = maps:get(namespace, Params, <<>>),
    Deep = maps:get(deep, Params, false),
    From = maps:get(from, Params, 0),
    Limit = maps:get(size, Params, 100),
    Query = [
        <<"SELECT uid,link_type FROM links">>,
        <<" WHERE link_target=">>, quote(to_bin(UID)),
        case LinkType of
            any ->
                <<>>;
            _ ->
                [<<" AND link_type=">>, quote(LinkType)]
        end,
        <<" AND ">>, filter_path(Namespace, Deep),
        <<" OFFSET ">>, to_bin(From), <<" LIMIT ">>, to_bin(Limit),
        <<";">>
    ],
    ResultFun = fun(Ops, Meta) ->
        case Ops of
            [{{select, _}, [], _OpMeta}] ->
                {ok, [], Meta};
            [{{select, Size}, Rows, _OpMeta}] ->
                {ok, Rows, Meta#{size=>Size}}
        end
    end,
    {query, Query, ResultFun};


search(actors_search_fts, Params) ->
    Word = maps:get(word, Params),
    Field = maps:get(field, Params, any),
    Namespace = maps:get(namespace, Params, <<>>),
    Deep = maps:get(deep, Params, false),
    From = maps:get(from, Params, 0),
    Limit = maps:get(size, Params, 100),
    Word2 = nklib_parse:normalize(Word, #{unrecognized=>keep}),
    Last = byte_size(Word2)-1,
    Filter = case Word2 of
        <<Word3:Last/binary, $*>> ->
            [<<"fts_word LIKE ">>, quote(<<Word3/binary, $%>>)];
        _ ->
            [<<"fts_word=">>, quote(Word2)]
    end,
    Query = [
        <<"SELECT uid FROM fts">>,
        <<" WHERE ">>, Filter, <<" AND ">>, filter_path(Namespace, Deep),
        case Field of
            any ->
                [];
            _ ->
                [<<" AND fts_field = ">>, quote(Field)]
        end,
        <<" ORDER BY fts_word" >>,
        <<" OFFSET ">>, to_bin(From), <<" LIMIT ">>, to_bin(Limit),
        <<";">>
    ],
    ResultFun = fun([{{select, _}, List, _OpMeta}],Meta) ->
        List2 = [UID || {UID} <-List],
        {ok, List2, Meta}
    end,
    {query, Query, ResultFun};

search(actors_search, Params) ->
    case analyze(Params) of
        only_labels ->
            search(actors_search_labels, Params);
        generic ->
            search(actors_search_generic, Params)
    end;

search(actors_search_generic, Params) ->
    From = maps:get(from, Params, 0),
    Size = maps:get(size, Params, 10),
    Totals = maps:get(totals, Params, false),
    SQLFilters = nkactor_sql:filters(Params, actors),
    SQLSort = nkactor_sql:sort(Params, actors),

    % We could use SELECT COUNT(*) OVER(),src,uid... but it doesn't work if no
    % rows are returned

    Query = [
        case Totals of
            true ->
                [
                    <<"SELECT COUNT(*) FROM actors">>,
                    SQLFilters,
                    <<";">>
                ];
            false ->
                []
        end,
        nkactor_sql:select(Params, actors),
        SQLFilters,
        SQLSort,
        <<" OFFSET ">>, to_bin(From), <<" LIMIT ">>, to_bin(Size),
        <<";">>
    ],
    {query, Query, fun ?MODULE:cassandra_actors/2};

search(actors_search_indices, #{only_uid:=true}=Params) ->
    From = maps:get(from, Params, 0),
    Size = maps:get(size, Params, 10),
    Totals = maps:get(totals, Params, false),
    SQLFilters = nkactor_sql:filters(Params, labels),
    SQLSort = nkactor_sql:sort(Params, labels),

    Query = [
        case Totals of
            true ->
                [
                    <<"SELECT COUNT(*) FROM labels">>,
                    SQLFilters,
                    <<";">>
                ];
            false ->
                []
        end,
        nkactor_sql:select(Params, labels),
        SQLFilters,
        SQLSort,
        <<" OFFSET ">>, to_bin(From), <<" LIMIT ">>, to_bin(Size),
        <<";">>
    ],
    {query, Query, fun ?MODULE:cassandra_actors/2};

search(actors_delete, Params) ->
    DoDelete = maps:get(do_delete, Params, false),
    SQLFilters = nkactor_sql:filters(Params, actors),
    Query = [
        case DoDelete of
            false ->
                <<"SELECT COUNT(*) FROM actors">>;
            true ->
                <<"DELETE FROM actors">>
        end,
        SQLFilters,
        <<";">>
    ],
    {query, Query, fun cassandra_delete/2};

search(actors_delete_old, Params) ->
    Group = maps:get(group, Params),
    Res = maps:get(resource, Params),
    Epoch = maps:get(epoch, params),
    Namespace = maps:get(namespace, Params, <<>>),
    Deep = maps:get(deep, Params, false),
    Query = [
        <<"DELETE FROM actors">>,
        <<" WHERE \"group\"=">>, quote(Group), <<" AND resource=">>, quote(Res),
        <<" AND last_update<">>, quote(Epoch),
        <<" AND ">>, filter_path(Namespace, Deep),
        <<";">>
    ],
    {query, Query, fun cassandra_delete/2};


search(actors_active, Params) ->
    LastUID = maps:get(last_cursor, Params, <<>>),
    Size = maps:get(size, Params, 100),
    Query = [
        <<"SELECT uid,namespace,\"group\",resource,name FROM actors_index">>,
        <<" WHERE class='db' AND key='active' AND value='T' AND ">>,
        <<" uid>">>, quote(LastUID),
        <<" LIMIT ">>, to_bin(Size),
        <<";">>
    ],
    {query, Query, fun cassandra_active/2};

search(actors_expired, Params) ->
    LastDate = case maps:find(last_cursor, Params) of
        {ok, Date0} ->
            Date0;
        error ->
            nklib_date:now_3339(usecs)
    end,
    Size = maps:get(size, Params, 100),
    Query = [
        <<"SELECT uid,value,namespace,\"group\",resource,name FROM actors_index">>,
        <<" WHERE class='db' AND key='expires' AND value<">>, quote(LastDate),
        <<" LIMIT ">>, to_bin(Size),
        <<";">>
    ],
    {query, Query, fun cassandra_expires/2};


search(actors_truncate, _) ->
    Query = [<<"TRUNCATE TABLE actors CASCADE;">>],
    {query, Query, fun cassandra_any/2};

search(SearchType, _Params) ->
    {error, {search_not_implemented, SearchType}}.



%% ===================================================================
%% Analyze
%% ===================================================================

%% @private
analyze(#{filter_fields:=Filter, sort_fields:=Sort, only_uid:=true}) ->
    case analyze_filter_labels(Filter, false) of
        true ->
            case analyze_filter_sort(Sort) of
                true ->
                    only_labels;
                false ->
                    generic
            end;
        false ->
            generic
    end;

analyze(_) ->
    generic.


%% @private
analyze_filter_labels([], Res) ->
    Res;

analyze_filter_labels([{<<"metadata.labels.", _/binary>>, _Op}|Rest], _Res) ->
    analyze_filter_labels(Rest, true);

analyze_filter_labels(_, _Res) ->
    false.


%% @private
analyze_filter_sort([]) ->
    true;

analyze_filter_sort([<<"metadata.labels", _/binary>>|Rest]) ->
    analyze_filter_sort(Rest);

analyze_filter_sort(_) ->
    false.




%% ===================================================================
%% Result funs
%% ===================================================================


%% @private
cassandra_actors(Result, Meta) ->
    % lager:error("NKLOG META ~p", [_Meta]),
    #{nkactor_params:=Params, cassandra:=#{time:=Time}} = Meta,
    {Rows, Meta2} = case Result of
        [{{select, Size}, Rows0, _OpMeta}] ->
            {Rows0, #{size=>Size, time=>Time}};
        [{{select, 1}, [{Total}], _}, {{select, Size}, Rows0, _OpMeta}] ->
            {Rows0, #{size=>Size, total=>Total, time=>Time}}
    end,
    GetData = maps:get(get_data, Params, false),
    GetMeta = maps:get(get_metadata, Params, false),
    Actors = lists:map(
        fun
            ({UID}) ->
                #{uid => UID};
            (Row) ->
                Actor1 = #{
                    uid => element(1, Row),
                    namespace => element(2, Row),
                    group => element(3, Row),
                    resource => element(4, Row),
                    name => element(5, Row)
                },
                Actor2 = case GetMeta of
                    true ->
                        {jsonb, MetaData} = element(6, Row),
                        Actor1#{metadata => nklib_json:decode(MetaData)};
                    false ->
                        Actor1
                end,
                Actor3 = case GetData of
                    true when GetMeta ->
                        {jsonb, Data} = element(7, Row),
                        Actor2#{data => nklib_json:decode(Data)};
                    true ->
                        {jsonb, Data} = element(6, Row),
                        Actor2#{data => nklib_json:decode(Data)};
                    false ->
                        Actor2
                end,
                Actor3
        end,
        Rows),
    {ok, Actors, Meta2}.


%% @private
cassandra_delete([{{delete, Total}, [], _}], Meta) ->
    {ok, Total, Meta};


cassandra_delete([{{select, _}, [{Total}], _}], Meta) ->
    {ok, Total, Meta}.


%% @private
cassandra_active([], _Opts) ->
    {ok, [], #{}};

cassandra_active(Fields, _Opts) ->
    ActorIds = [
        #actor_id{
            uid = UID,
            namespace = Namespace,
            group = Group,
            resource = Resource,
            name = Name
        }
        || [UID, Namespace, Group, Resource, Name] <- Fields],
    {ok, ActorIds, #{last_cursor=>hd(hd(lists:reverse(Fields)))}}.


%% @private
cassandra_expires([], _Opts) ->
    {ok, [], #{}};

cassandra_expires(Fields, _Opts) ->
    ActorIds = [
        #actor_id{
            uid = UID,
            namespace = Namespace,
            group = Group,
            resource = Resource,
            name = Name
        }
        || [UID, _Value, Namespace, Group, Resource, Name] <- Fields],
    [[_, Last|_]|_] = lists:reverse(Fields),
    {ok, ActorIds, #{last_cursor=>Last}}.


%% @private
cassandra_any(List, Meta) ->
    {ok, List, #{meta=>Meta}}.


%% ===================================================================
%% Internal
%% ===================================================================


%% @private
to_bin(Term) when is_binary(Term) -> Term;
to_bin(Term) -> nklib_util:to_binary(Term).
