-module(etest_sample).

-export([start/1, go/1]).

start(Count) ->
    {ok, spawn_link(?MODULE, go, [Count])}.

go(0) -> ok;
go(Count) ->
    error_logger:info_msg("Did something from ~p~n", [node()]),
    go(Count - 1).
