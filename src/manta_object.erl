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
-module(manta_object).

-include("manta.hrl").

%% API exports
-export([delete/1]).
-export([delete/2]).
-export([delete/3]).
-export([get/1]).
-export([get/2]).
-export([get/3]).
-export([head/1]).
-export([head/2]).
-export([head/3]).
-export([put/2]).
-export([put/3]).
-export([put/4]).
-export([put_metadata/2]).
-export([put_metadata/3]).
-export([put_metadata/4]).

%%====================================================================
%% API functions
%%====================================================================

delete(Pathname) ->
	?MODULE:delete(Pathname, []).

delete(Pathname, Options) ->
	?MODULE:delete(manta:default_config(), Pathname, Options).

delete(Config=#manta_config{}, Pathname, Opts0) ->
	{Headers, Opts1} = manta:take_value(headers, Opts0, []),
	{Timeout, Opts2} = manta:take_value(timeout, Opts1, ?DEFAULT_TIMEOUT),
	Path = manta_httpc:make_path(Pathname, []),
	manta_httpc:request(Config, Path, delete, Headers, [], Timeout, Opts2).

get(Pathname) ->
	?MODULE:get(Pathname, []).

get(Pathname, Options) ->
	?MODULE:get(manta:default_config(), Pathname, Options).

get(Config=#manta_config{}, Pathname, Opts0) ->
	{Headers, Opts1} = manta:take_value(headers, Opts0, []),
	{Timeout, Opts2} = manta:take_value(timeout, Opts1, ?DEFAULT_TIMEOUT),
	Path = manta_httpc:make_path(Pathname, []),
	manta_httpc:request(Config, Path, get, Headers, [], Timeout, Opts2).

head(Pathname) ->
	?MODULE:head(Pathname, []).

head(Pathname, Options) ->
	?MODULE:head(manta:default_config(), Pathname, Options).

head(Config=#manta_config{}, Pathname, Opts0) ->
	{Headers, Opts1} = manta:take_value(headers, Opts0, []),
	{Timeout, Opts2} = manta:take_value(timeout, Opts1, ?DEFAULT_TIMEOUT),
	Path = manta_httpc:make_path(Pathname, []),
	manta_httpc:request(Config, Path, head, Headers, [], Timeout, Opts2).

put(Pathname, Body) ->
	?MODULE:put(Pathname, Body, []).

put(Pathname, Body, Options) ->
	?MODULE:put(manta:default_config(), Pathname, Body, Options).

put(Config=#manta_config{}, Pathname, Body, Opts0) ->
	{Headers, Opts1} = manta:take_value(headers, Opts0, []),
	{Timeout, Opts2} = manta:take_value(timeout, Opts1, ?DEFAULT_TIMEOUT),
	Path = manta_httpc:make_path(Pathname, []),
	manta_httpc:request(Config, Path, put, Headers, Body, Timeout, Opts2).

put_metadata(Pathname, Metadata) ->
	?MODULE:put_metadata(Pathname, Metadata, []).

put_metadata(Pathname, Metadata, Options) ->
	?MODULE:put_metadata(manta:default_config(), Pathname, Metadata, Options).

put_metadata(Config=#manta_config{}, Pathname, Metadata, Opts0) ->
	{Headers0, Opts1} = manta:take_value(headers, Opts0, []),
	{Timeout, Opts2} = manta:take_value(timeout, Opts1, ?DEFAULT_TIMEOUT),
	Path = manta_httpc:make_path(Pathname, [{<<"metadata">>, <<"true">>}]),
	Headers1 = [
		Metadata
		| Headers0
	],
	manta_httpc:request(Config, Path, put, Headers1, [], Timeout, Opts2).

%%%-------------------------------------------------------------------
%%% Internal functions
%%%-------------------------------------------------------------------
