-module(etest_dist).

-export([start_slaves/4, start_slaves/5]).

%
% Installs and starts code on every known node.
% Returns a list of started PIDs (one per node).
%
start_slaves(Modules, StartMod, StartFunc, StartArgs) ->
    _World = (catch net_adm:world()),
    start_slaves(Modules, StartMod, StartFunc, StartArgs,
                 nodes([this, visible])).

%
% Installs and starts code on each listed node.
% Returns a tuple of lists of results from each node and list
% of bad nodes.
%
% See rpc:multicall/4
%
start_slaves(Modules, StartMod, StartFunc, StartArgs, Nodes)
  when is_list(Modules), is_atom(StartMod),
       is_list(StartArgs), is_list(Nodes) ->
    % List all the nodes that aren't the current node
	OtherNodes = lists:filter(fun(N) -> N =/= node() end, Nodes),
    % Install all the required code on them.
	error_logger:info_msg("Loading code on:  ~p~n", [OtherNodes]),
    lists:foreach(fun(Mod) -> rload_module(Mod, Nodes) end, Modules),

	% Required code is loaded, start 'em up
    rpc:multicall(Nodes, StartMod, StartFunc, StartArgs).

rload_module(Mod, Nodes) ->
    {Mod, Bin, File} = code:get_object_code(Mod),
	{BinaryReplies, _} = rpc:multicall(Nodes, code, load_binary,
                                       [Mod, File, Bin]),

	% Validate the multicall
	lists:foreach(fun({module, Mod2}) -> Mod2 = Mod end, BinaryReplies),
	error_logger:info_msg("Loaded code for ~p:  ~p~n", [Mod, BinaryReplies]).
