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
-module(manta_directory).
-behaviour(manta_handler).

-include("manta.hrl").

%% manta_handler callbacks
-export([init/3]).
-export([handle_response/2]).
-export([handle_download/2]).
-export([terminate/1]).

%% API exports
-export([delete/1]).
-export([delete/2]).
-export([delete/3]).
-export([head/0]).
-export([head/1]).
-export([head/2]).
-export([head/3]).
-export([list/0]).
-export([list/1]).
-export([list/2]).
-export([list/3]).
-export([put/1]).
-export([put/2]).
-export([put/3]).

-define(MAX_LIMIT, 1000).

-record(state, {
	buffer    = <<>>      :: binary(),
	default   = undefined :: undefined | any(),
	format    = undefined :: undefined | list,
	pid       = undefined :: undefined | pid(),
	client_id = undefined :: undefined | any(),
	stream_to = undefined :: undefined | pid()
}).

%%====================================================================
%% manta_handler callbacks
%%====================================================================

init(ClientId, StreamTo, Format) ->
	Default = manta_handler:init(ClientId, StreamTo, Format),
	State = #state{default=Default, format=Format, pid=self(),
		client_id=ClientId, stream_to=StreamTo},
	State.

handle_response(Response={ok, {Status={200, _}, Headers, Result}},
		State=#state{buffer=Buffer, format=list, pid=Pid,
		client_id=ClientId, stream_to=StreamTo}) ->
	case dlhttpc_lib:header_value("content-type", Headers) of
		"application/x-json-stream; type=directory" ->
			case Result of
				_ when is_binary(Result) ->
					List = parse_response(<< Buffer/binary, Result/binary >>, []),
					StreamTo ! {response, ClientId, Pid, {ok, {Status, Headers, List}}},
					State;
				_ ->
					StreamTo ! {response, ClientId, Pid, Response},
					State
			end;
		_ ->
			StreamTo ! {response, ClientId, Pid, Response},
			State
	end;
handle_response(Response, State=#state{default=Default}) ->
	NewDefault = manta_handler:handle_response(Response, Default),
	State#state{default=NewDefault, format=undefined}.

handle_download({body_part, Pid, Data}, State=#state{buffer=Buffer, format=list, pid=Pid}) ->
	parse_body_part(<< Buffer/binary, Data/binary >>, State);
handle_download(Download, State=#state{default=Default}) ->
	NewDefault = manta_handler:handle_download(Download, Default),
	State#state{default=NewDefault}.

terminate(#state{default=Default}) ->
	manta_handler:terminate(Default).

%%====================================================================
%% API functions
%%====================================================================

delete(Pathname) ->
	?MODULE:delete(Pathname, []).

delete(Pathname, Options) ->
	?MODULE:delete(manta:default_config(), Pathname, Options).

delete(Config=#manta_config{}, Pathname, Opts0) ->
	{Headers, Opts1} = manta:take_value(headers, Opts0, []),
	{Timeout, Opts2} = manta:take_value(timeout, Opts1, Config#manta_config.timeout),
	Path = manta_httpc:make_path(Pathname, []),
	manta_httpc:request(Config, Path, delete, Headers, [], Timeout, Opts2).

head() ->
	?MODULE:head(<<>>).

head(Pathname) ->
	?MODULE:head(Pathname, []).

head(Pathname, Options) ->
	?MODULE:head(manta:default_config(), Pathname, Options).

head(Config=#manta_config{}, Pathname, Opts0) ->
	{Limit, Opts1} = manta:take_value(limit, Opts0, ?MAX_LIMIT),
	{Marker, Opts2} = manta:take_value(marker, Opts1, undefined),
	{Headers, Opts3} = manta:take_value(headers, Opts2, []),
	{Timeout, Opts4} = manta:take_value(timeout, Opts3, Config#manta_config.timeout),
	Path = manta_httpc:make_path(Pathname, [{limit, Limit}, {marker, Marker}]),
	manta_httpc:request(Config, Path, head, Headers, [], Timeout, Opts4).

list() ->
	?MODULE:list(<<>>).

list(Pathname) ->
	?MODULE:list(Pathname, []).

list(Pathname, Options) ->
	?MODULE:list(manta:default_config(), Pathname, Options).

list(Config=#manta_config{}, Pathname, Opts0) ->
	{Limit, Opts1} = manta:take_value(limit, Opts0, ?MAX_LIMIT),
	{Marker, Opts2} = manta:take_value(marker, Opts1, undefined),
	{Headers, Opts3} = manta:take_value(headers, Opts2, []),
	{Timeout, Opts4} = manta:take_value(timeout, Opts3, Config#manta_config.timeout),
	Path = manta_httpc:make_path(Pathname, [{limit, Limit}, {marker, Marker}]),
	manta_httpc:request(Config, Path, get, Headers, [], Timeout, Opts4).

put(Pathname) ->
	?MODULE:put(Pathname, []).

put(Pathname, Options) ->
	?MODULE:put(manta:default_config(), Pathname, Options).

put(Config=#manta_config{}, Pathname, Opts0) ->
	{Headers0, Opts1} = manta:take_value(headers, Opts0, []),
	{Timeout, Opts2} = manta:take_value(timeout, Opts1, Config#manta_config.timeout),
	Path = manta_httpc:make_path(Pathname, []),
	Headers1 = [
		{<<"Content-Type">>, <<"application/json; type=directory">>}
		| Headers0
	],
	manta_httpc:request(Config, Path, put, Headers1, [], Timeout, Opts2).

%%%-------------------------------------------------------------------
%%% Internal functions
%%%-------------------------------------------------------------------

%% @private
parse_response(Body, Acc) ->
	case manta_json_stream:decode(Body) of
		{true, Term, Rest} ->
			parse_response(Rest, [Term | Acc]);
		false ->
			lists:reverse(Acc)
	end.

%% @private
parse_body_part(Body, State) ->
	case manta_json_stream:decode(Body) of
		{true, Term, Rest} ->
			State#state.stream_to ! {body_part, State#state.pid, Term},
			parse_body_part(Rest, State);
		false ->
			State#state{buffer=Body}
	end.
