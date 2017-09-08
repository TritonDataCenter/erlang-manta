%% -*- mode: erlang; tab-width: 4; indent-tabs-mode: 1; st-rulers: [70] -*-
%% vim: ts=4 sw=4 ft=erlang noet
%%%-------------------------------------------------------------------
%%% @author Andrew Bennett <andrew@pixid.com>
%%% @copyright 2014-2015, Andrew Bennett
%%% @doc
%%%
%%% @end
%%% Created :  20 Nov 2014 by Andrew Bennett <andrew@pixid.com>
%%%-------------------------------------------------------------------
-module(manta_snaplink).

-include("manta.hrl").

%% API exports
-export([put/2,
		 put/3,
		 put/4]).

-ignore_xref({put,2}).

%%====================================================================
%% API functions
%%====================================================================

put(Pathname, Location) ->
	?MODULE:put(Pathname, Location, []).

put(Pathname, Location, Options) ->
	?MODULE:put(manta:default_config(), Pathname, Location, Options).

put(Config=#manta_config{}, Pathname, Location, Opts0) ->
	{Headers0, Opts1} = manta:take_value(headers, Opts0, []),
	{Timeout, Opts2} = manta:take_value(timeout, Opts1, Config#manta_config.timeout),
	Path = manta_httpc:make_path(Pathname, []),
	Link = manta_object:path(Config, Location, []),
	Headers1 = [
		{<<"Content-Type">>, <<"application/json; type=link">>},
		{<<"Location">>, Link}
		| Headers0
	],
	manta_httpc:request(Config, Path, put, Headers1, [], Timeout, Opts2).

%%%-------------------------------------------------------------------
%%% Internal functions
%%%-------------------------------------------------------------------
