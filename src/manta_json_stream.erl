%% -*- mode: erlang; tab-width: 4; indent-tabs-mode: 1; st-rulers: [70] -*-
%% vim: ts=4 sw=4 ft=erlang noet
%%%-------------------------------------------------------------------
%%% @author Andrew Bennett <andrew@pixid.com>
%%% @copyright 2014, Andrew Bennett
%%% @doc
%%%
%%% @end
%%% Created :  20 Nov 2014 by Andrew Bennett <andrew@pixid.com>
%%%-------------------------------------------------------------------
-module(manta_json_stream).

-include("manta.hrl").

%% API exports
-export([decode/1,
		 decode/2]).

-ignore_xref({decode,1}).

%% Internal
-export([error_handler/3,
		 incomplete_handler/3]).

%%====================================================================
%% API functions
%%====================================================================

decode(Source) ->
	decode(Source, []).

decode(Source, Config) ->
	NewConfig = [
		stream,
		{error_handler, fun manta_json_stream:error_handler/3},
		{incomplete_handler, fun manta_json_stream:incomplete_handler/3}
		| Config
	],
	jsx:decode(Source, NewConfig).

%%%-------------------------------------------------------------------
%%% Internal functions
%%%-------------------------------------------------------------------

%% @private
error_handler(Rest, {decoder, done, {jsx_to_term, Term}, _Acc, _Stack}, _Config) ->
	{true, Term, Rest}.

%% @private
incomplete_handler(Rest, {decoder, done, {jsx_to_term, Term}, _Acc, _Stack}, _Config) ->
	{true, Term, Rest};
incomplete_handler(_Rest, {decoder, _State, _Handler, _Acc, _Stack}, _Config) ->
	false.
