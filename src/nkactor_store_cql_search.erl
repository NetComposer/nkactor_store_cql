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
-import(nkactor_store_cql, [query/2, query/3]).

-define(LLOG(Type, Txt, Args), lager:Type("NkACTOR CASSANDRA "++Txt, Args)).

-include_lib("nkactor/include/nkactor.hrl").

%% ===================================================================
%% Search Types
%% ===================================================================


%%search(actors_search_linked, Params) ->
%%    UID = maps:get(uid, Params),
%%    LinkType = maps:get(link_type, Params, any),
%%    Namespace = maps:get(namespace, Params, <<>>),
%%    Deep = maps:get(deep, Params, false),
%%    From = maps:get(from, Params, 0),
%%    Limit = maps:get(size, Params, 100),
%%    Query = [
%%        <<"SELECT uid,link_type FROM links">>,
%%        <<" WHERE link_target=">>, quote(to_bin(UID)),
%%        case LinkType of
%%            any ->
%%                <<>>;
%%            _ ->
%%                [<<" AND link_type=">>, quote(LinkType)]
%%        end,
%%        <<" AND ">>, filter_path(Namespace, Deep),
%%        <<" OFFSET ">>, to_bin(From), <<" LIMIT ">>, to_bin(Limit),
%%        <<";">>
%%    ],
%%    ResultFun = fun(Ops, Meta) ->
%%        case Ops of
%%            [{{select, _}, [], _OpMeta}] ->
%%                {ok, [], Meta};
%%            [{{select, Size}, Rows, _OpMeta}] ->
%%                {ok, Rows, Meta#{size=>Size}}
%%        end
%%    end,
%%    {query, Query, ResultFun};
%%
%%
%%search(actors_search_fts, Params) ->
%%    Word = maps:get(word, Params),
%%    Field = maps:get(field, Params, any),
%%    Namespace = maps:get(namespace, Params, <<>>),
%%    Deep = maps:get(deep, Params, false),
%%    From = maps:get(from, Params, 0),
%%    Limit = maps:get(size, Params, 100),
%%    Word2 = nklib_parse:normalize(Word, #{unrecognized=>keep}),
%%    Last = byte_size(Word2)-1,
%%    Filter = case Word2 of
%%        <<Word3:Last/binary, $*>> ->
%%            [<<"fts_word LIKE ">>, quote(<<Word3/binary, $%>>)];
%%        _ ->
%%            [<<"fts_word=">>, quote(Word2)]
%%    end,
%%    Query = [
%%        <<"SELECT uid FROM fts">>,
%%        <<" WHERE ">>, Filter, <<" AND ">>, filter_path(Namespace, Deep),
%%        case Field of
%%            any ->
%%                [];
%%            _ ->
%%                [<<" AND fts_field = ">>, quote(Field)]
%%        end,
%%        <<" ORDER BY fts_word" >>,
%%        <<" OFFSET ">>, to_bin(From), <<" LIMIT ">>, to_bin(Limit),
%%        <<";">>
%%    ],
%%    ResultFun = fun([{{select, _}, List, _OpMeta}],Meta) ->
%%        List2 = [UID || {UID} <-List],
%%        {ok, List2, Meta}
%%    end,
%%    {query, Query, ResultFun};
%%
%%search(actors_search, Params) ->
%%    case analyze(Params) of
%%        only_labels ->
%%            search(actors_search_labels, Params);
%%        generic ->
%%            search(actors_search_generic, Params)
%%    end;
%%
%%search(actors_search_generic, Params) ->
%%    From = maps:get(from, Params, 0),
%%    Size = maps:get(size, Params, 10),
%%    Totals = maps:get(totals, Params, false),
%%    SQLFilters = nkactor_store_pgsql_sql:filters(Params, actors),
%%    SQLSort = nkactor_store_pgsql_sql:sort(Params, actors),
%%
%%    % We could use SELECT COUNT(*) OVER(),src,uid... but it doesn't work if no
%%    % rows are returned
%%
%%    Query = [
%%        case Totals of
%%            true ->
%%                [
%%                    <<"SELECT COUNT(*) FROM actors">>,
%%                    SQLFilters,
%%                    <<";">>
%%                ];
%%            false ->
%%                []
%%        end,
%%        nkactor_store_pgsql_sql:select(Params, actors),
%%        SQLFilters,
%%        SQLSort,
%%        <<" OFFSET ">>, to_bin(From), <<" LIMIT ">>, to_bin(Size),
%%        <<";">>
%%    ],
%%    {query, Query, fun ?MODULE:cassandra_actors/2};

search(actors_search_labels, Params) ->
    Size = to_bin(maps:get(size, Params, 10)),
    case check_label(Params) of
        {ok, Filter, Sort} ->
            Query = <<
                "SELECT value,uid,namespace,group,resource,name FROM actors_index",
                " WHERE class='label' AND ", Filter/binary,
                Sort/binary,
                " LIMIT ", Size/binary, ";"
            >>,
            {query, Query, fun cassandra_index/2};
        {error, Error} ->
            {error, Error}
    end;

%%search(actors_delete, Params) ->
%%    DoDelete = maps:get(do_delete, Params, false),
%%    SQLFilters = nkactor_store_pgsql_sql:filters(Params, actors),
%%    Query = [
%%        case DoDelete of
%%            false ->
%%                <<"SELECT COUNT(*) FROM actors">>;
%%            true ->
%%                <<"DELETE FROM actors">>
%%        end,
%%        SQLFilters,
%%        <<";">>
%%    ],
%%    {query, Query, fun cassandra_delete/2};
%%
%%search(actors_delete_old, Params) ->
%%    Group = maps:get(group, Params),
%%    Res = maps:get(resource, Params),
%%    Epoch = maps:get(epoch, params),
%%    Namespace = maps:get(namespace, Params, <<>>),
%%    Deep = maps:get(deep, Params, false),
%%    Query = [
%%        <<"DELETE FROM actors">>,
%%        <<" WHERE group=">>, quote(Group), <<" AND resource=">>, quote(Res),
%%        <<" AND last_update<">>, quote(Epoch),
%%        <<" AND ">>, filter_path(Namespace, Deep),
%%        <<";">>
%%    ],
%%    {query, Query, fun cassandra_delete/2};
%%
%%
search(actors_active, Params) ->
    LastUID = maps:get(last_cursor, Params, <<>>),
    Size = maps:get(size, Params, 100),
    Query = [
        <<"SELECT uid,namespace,group,resource,name FROM actors_index">>,
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
        <<"SELECT uid,value,namespace,group,resource,name FROM actors_index">>,
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
check_label(#{filter:=#{'and':=[#{field:=<<"label:", Label/binary>>}=Filter]}}=Params) ->
    Filter2 = make_filter(Label, Filter),
    check_label_sort(Params, Label, Filter2);

check_label(_) ->
    {error, query_invalid}.


%% @private
check_label_sort(#{sort:=[#{field:=<<"label:", Label/binary>>}=Sort]}, Label, Filter) ->
    Sort2 = case maps:get(order, Sort, asc) of
        asc -> <<" ORDER BY value ASC ">>;
        desc -> <<" ORDER BY value DESC ">>
    end,
    {ok, Filter, Sort2};

check_label_sort(#{sort:=[]}, _Label, Filter) ->
    {ok, Filter, <<>>};

check_label_sort(#{sort:=_}, _Label, _Filter) ->
    {error, query_invalid};

check_label_sort(_, _Label, Filter) ->
    {ok, Filter, <<>>}.



make_filter(Label, Filter) ->
    QLabel = quote(Label),
    Op = maps:get(op, Filter, eq),
    Value = maps:get(value, Filter, <<>>),
    case {Op, Value} of
        {exists, false} ->
            <<"key <> ", QLabel/binary>>;
        {exists, _} ->
            <<"key = ", QLabel/binary>>;
        {eq, _} ->
            <<"key = ", QLabel/binary, " AND value = ", (quote(Value))/binary>>;
        {ne, _} ->
            <<"key = ", QLabel/binary, " AND value <> ", (quote(Value))/binary>>;
        {gt, _} ->
            <<"key = ", QLabel/binary, " AND value > ", (quote(Value))/binary>>;
        {gte, _} ->
            <<"key = ", QLabel/binary, " AND value >= ", (quote(Value))/binary>>;
        {lt, _} ->
            <<"key = ", QLabel/binary, " AND value < ", (quote(Value))/binary>>;
        {lte, _} ->
            <<"key = ", QLabel/binary, " AND value <= ", (quote(Value))/binary>>;
        {values, List} ->
            List2 = nklib_util:bjoin([quote(Val) || Val <-List]),
            <<"key = ", QLabel/binary, " AND value IN (", List2/binary, ")">>
    end.



%% ===================================================================
%% Result funs
%% ===================================================================


%% @private
cassandra_index([], _Opts) ->
    {ok, [], #{}};

cassandra_index(Fields, _Opts) ->
    ActorIds = [
        {
            Value,
            #actor_id{
                uid = UID,
                namespace = Namespace,
                group = Group,
                resource = Resource,
                name = Name
            }
        }
        || [Value, UID, Namespace, Group, Resource, Name] <- Fields
    ],
    {ok, ActorIds, #{last_cursor=>hd(hd(lists:reverse(Fields)))}}.


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

