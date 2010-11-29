-module(etest_node_sup).

-export([start_children/4]).

-define(SERVER, ?MODULE).

start_children(Mod, Func, Args, Count) ->
    lists:map(fun(_N) ->
                      spawn_link(Mod, Func, Args)
              end, lists:seq(1, Count)).
