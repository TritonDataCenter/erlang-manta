%% -*- mode: erlang; tab-width: 4; indent-tabs-mode: 1; st-rulers: [70] -*-
%% vim: ts=4 sw=4 ft=erlang noet
%%%-------------------------------------------------------------------
%%% @author Andrew Bennett <andrew@pixid.com>
%%% @copyright 2014-2015, Andrew Bennett
%%% @doc
%%%
%%% @end
%%% Created :  15 Jul 2015 by Andrew Bennett <andrew@pixid.com>
%%%-------------------------------------------------------------------
-module(manta_job).
-behaviour(manta_handler).

-include("manta.hrl").

%% manta_handler callbacks
-export([init/3]).
-export([handle_response/2]).
-export([handle_download/2]).
-export([terminate/1]).

%% API exports
-export([add_inputs/2]).
-export([add_inputs/3]).
-export([add_inputs/4]).
-export([cancel/1]).
-export([cancel/2]).
-export([cancel/3]).
-export([create/1]).
-export([create/2]).
-export([create/3]).
-export([end_input/1]).
-export([end_input/2]).
-export([end_input/3]).
-export([get/1]).
-export([get/2]).
-export([get/3]).
-export([get_errors/1]).
-export([get_errors/2]).
-export([get_errors/3]).
-export([get_failures/1]).
-export([get_failures/2]).
-export([get_failures/3]).
-export([get_input/1]).
-export([get_input/2]).
-export([get_input/3]).
-export([get_output/1]).
-export([get_output/2]).
-export([get_output/3]).
-export([list/0]).
-export([list/1]).
-export([list/2]).
-export([path/1]).

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
		State=#state{buffer=Buffer, format=list_jobs, pid=Pid,
		client_id=ClientId, stream_to=StreamTo}) ->
	case dlhttpc_lib:header_value("content-type", Headers) of
		"application/x-json-stream; " ++ Type
				when Type == "type=job"
				orelse Type == "type=directory" ->
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
handle_response(Response={ok, {Status={200, _}, Headers, Result}},
		State=#state{buffer=Buffer, format=get_job, pid=Pid,
		client_id=ClientId, stream_to=StreamTo}) ->
	case dlhttpc_lib:header_value("content-type", Headers) of
		"application/json" ->
			case Result of
				_ when is_binary(Result) ->
					Term = parse_response_single(<< Buffer/binary, Result/binary >>),
					StreamTo ! {response, ClientId, Pid, {ok, {Status, Headers, Term}}},
					State;
				_ ->
					StreamTo ! {response, ClientId, Pid, Response},
					State
			end;
		_ ->
			StreamTo ! {response, ClientId, Pid, Response},
			State
	end;
handle_response(Response={ok, {Status={200, _}, Headers, Result}},
		State=#state{buffer=Buffer, format=get_job_lines, pid=Pid,
		client_id=ClientId, stream_to=StreamTo}) ->
	case dlhttpc_lib:header_value("content-type", Headers) of
		"text/plain" ->
			case Result of
				_ when is_binary(Result) ->
					Term = parse_response_lines(<< Buffer/binary, Result/binary >>, []),
					StreamTo ! {response, ClientId, Pid, {ok, {Status, Headers, Term}}},
					State;
				_ ->
					StreamTo ! {response, ClientId, Pid, Response},
					State
			end;
		_ ->
			StreamTo ! {response, ClientId, Pid, Response},
			State
	end;
handle_response(Response={ok, {Status={200, _}, Headers, Result}},
		State=#state{buffer=Buffer, format=get_job_errors, pid=Pid,
		client_id=ClientId, stream_to=StreamTo}) ->
	case dlhttpc_lib:header_value("content-type", Headers) of
		"application/x-json-stream; " ++ Type
				when Type == "type=job-error"
				orelse Type == "type=directory" ->
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

handle_download({body_part, Pid, Data}, State=#state{buffer=Buffer, format=Format, pid=Pid})
		when Format =:= get_job
		orelse Format =:= list_jobs
		orelse Format =:= get_job_lines
		orelse Format =:= get_job_errors ->
	parse_body_part(<< Buffer/binary, Data/binary >>, State);
handle_download(Download, State=#state{default=Default}) ->
	NewDefault = manta_handler:handle_download(Download, Default),
	State#state{default=NewDefault}.

terminate(#state{default=Default}) ->
	manta_handler:terminate(Default).

%%====================================================================
%% API functions
%%====================================================================

add_inputs(JobPath, Inputs) ->
	?MODULE:add_inputs(JobPath, Inputs, []).

add_inputs(JobPath, Inputs, Options) ->
	?MODULE:add_inputs(manta:default_config(), JobPath, Inputs, Options).

add_inputs(Config=#manta_config{}, JobPath, Inputs, Opts0) ->
	{Headers0, Opts1} = manta:take_value(headers, Opts0, []),
	{Timeout, Opts2} = manta:take_value(timeout, Opts1, Config#manta_config.timeout),
	Path = << (path(JobPath))/binary, "/live/in" >>,
	Body = [[manta:object_path(Input), $\n] || Input <- Inputs],
	Headers1 = [
		{<<"Content-Type">>, <<"text/plain">>}
		| Headers0
	],
	manta_httpc:request(Config, Path, post, Headers1, Body, Timeout, Opts2).

cancel(JobPath) ->
	?MODULE:cancel(JobPath, []).

cancel(JobPath, Options) ->
	?MODULE:cancel(manta:default_config(), JobPath, Options).

cancel(Config=#manta_config{}, JobPath, Opts0) ->
	{Headers, Opts1} = manta:take_value(headers, Opts0, []),
	{Timeout, Opts2} = manta:take_value(timeout, Opts1, Config#manta_config.timeout),
	Path = << (path(JobPath))/binary, "/live/cancel" >>,
	manta_httpc:request(Config, Path, post, Headers, [], Timeout, Opts2).

create(Job) ->
	?MODULE:create(Job, []).

create(Job, Options) ->
	?MODULE:create(manta:default_config(), Job, Options).

create(Config=#manta_config{}, Job, Opts0) ->
	{Headers0, Opts1} = manta:take_value(headers, Opts0, []),
	{Timeout, Opts2} = manta:take_value(timeout, Opts1, Config#manta_config.timeout),
	Path = <<"~~/jobs">>,
	Body = jsx:encode(Job),
	Headers1 = [
		{<<"Content-Type">>, <<"application/json; type=job">>}
		| Headers0
	],
	manta_httpc:request(Config, Path, post, Headers1, Body, Timeout, Opts2).

end_input(JobPath) ->
	?MODULE:end_input(JobPath, []).

end_input(JobPath, Options) ->
	?MODULE:end_input(manta:default_config(), JobPath, Options).

end_input(Config=#manta_config{}, JobPath, Opts0) ->
	{Headers, Opts1} = manta:take_value(headers, Opts0, []),
	{Timeout, Opts2} = manta:take_value(timeout, Opts1, Config#manta_config.timeout),
	Path = << (path(JobPath))/binary, "/live/in/end" >>,
	manta_httpc:request(Config, Path, post, Headers, [], Timeout, Opts2).

get(JobPath) ->
	?MODULE:get(JobPath, []).

get(JobPath, Options) ->
	?MODULE:get(manta:default_config(), JobPath, Options).

get(Config=#manta_config{}, JobPath, Opts0) ->
	{Headers, Opts1} = manta:take_value(headers, Opts0, []),
	{Timeout, Opts2} = manta:take_value(timeout, Opts1, Config#manta_config.timeout),
	Path = << (path(JobPath))/binary, "/live/status" >>,
	manta_httpc:request(Config, Path, get, Headers, [], Timeout, Opts2).

get_errors(JobPath) ->
	?MODULE:get_errors(JobPath, []).

get_errors(JobPath, Options) ->
	?MODULE:get_errors(manta:default_config(), JobPath, Options).

get_errors(Config=#manta_config{}, JobPath, Opts0) ->
	{Headers, Opts1} = manta:take_value(headers, Opts0, []),
	{Timeout, Opts2} = manta:take_value(timeout, Opts1, Config#manta_config.timeout),
	Path = << (path(JobPath))/binary, "/live/err" >>,
	manta_httpc:request(Config, Path, get, Headers, [], Timeout, Opts2).

get_failures(JobPath) ->
	?MODULE:get_failures(JobPath, []).

get_failures(JobPath, Options) ->
	?MODULE:get_failures(manta:default_config(), JobPath, Options).

get_failures(Config=#manta_config{}, JobPath, Opts0) ->
	{Headers, Opts1} = manta:take_value(headers, Opts0, []),
	{Timeout, Opts2} = manta:take_value(timeout, Opts1, Config#manta_config.timeout),
	Path = << (path(JobPath))/binary, "/live/fail" >>,
	manta_httpc:request(Config, Path, get, Headers, [], Timeout, Opts2).

get_input(JobPath) ->
	?MODULE:get_input(JobPath, []).

get_input(JobPath, Options) ->
	?MODULE:get_input(manta:default_config(), JobPath, Options).

get_input(Config=#manta_config{}, JobPath, Opts0) ->
	{Headers, Opts1} = manta:take_value(headers, Opts0, []),
	{Timeout, Opts2} = manta:take_value(timeout, Opts1, Config#manta_config.timeout),
	Path = << (path(JobPath))/binary, "/live/in" >>,
	manta_httpc:request(Config, Path, get, Headers, [], Timeout, Opts2).

get_output(JobPath) ->
	?MODULE:get_output(JobPath, []).

get_output(JobPath, Options) ->
	?MODULE:get_output(manta:default_config(), JobPath, Options).

get_output(Config=#manta_config{}, JobPath, Opts0) ->
	{Headers, Opts1} = manta:take_value(headers, Opts0, []),
	{Timeout, Opts2} = manta:take_value(timeout, Opts1, Config#manta_config.timeout),
	Path = << (path(JobPath))/binary, "/live/out" >>,
	manta_httpc:request(Config, Path, get, Headers, [], Timeout, Opts2).

list() ->
	?MODULE:list([]).

list(Options) ->
	?MODULE:list(manta:default_config(), Options).

list(Config=#manta_config{}, Opts0) ->
	{Limit, Opts1} = manta:take_value(limit, Opts0, ?MAX_LIMIT),
	{Marker, Opts2} = manta:take_value(marker, Opts1, undefined),
	{State, Opts3} = manta:take_value(state, Opts2, undefined),
	{Headers, Opts4} = manta:take_value(headers, Opts3, []),
	{Timeout, Opts5} = manta:take_value(timeout, Opts4, Config#manta_config.timeout),
	Path = manta_httpc:make_path(<<"~~/jobs">>, [
		{limit, Limit},
		{marker, Marker},
		{state, State}
	]),
	manta_httpc:request(Config, Path, get, Headers, [], Timeout, Opts5).

path(JobPath = << $/, _/binary >>) ->
	JobPath;
path(JobPath = << $~, $~, $/, _/binary >>) ->
	JobPath;
path(JobId) ->
	<< "~~/jobs/", JobId/binary >>.

%%%-------------------------------------------------------------------
%%% Internal functions
%%%-------------------------------------------------------------------

%% @private
parse_response(Body, Acc) ->
	case manta_json_stream:decode(Body, [return_maps]) of
		{true, Term, Rest} ->
			parse_response(Rest, [Term | Acc]);
		false ->
			lists:reverse(Acc)
	end.

%% @private
parse_response_lines(Body, Acc) ->
	case binary:split(Body, << $\n >>) of
		[Term, Rest] ->
			parse_response_lines(Rest, [Term | Acc]);
		_ ->
			lists:reverse(Acc)
	end.

%% @private
parse_response_single(Body) ->
	case manta_json_stream:decode(Body, [return_maps]) of
		{true, Term, _Rest} ->
			Term;
		false ->
			Body
	end.

%% @private
parse_body_part(Body, State=#state{format=get_job_lines}) ->
	case binary:split(Body, << $\n >>) of
		[Term, Rest] ->
			State#state.stream_to ! {body_part, State#state.pid, Term},
			parse_body_part(Rest, State);
		_ ->
			State#state{buffer=Body}
	end;
parse_body_part(Body, State) ->
	case manta_json_stream:decode(Body, [return_maps]) of
		{true, Term, Rest} ->
			State#state.stream_to ! {body_part, State#state.pid, Term},
			parse_body_part(Rest, State);
		false ->
			State#state{buffer=Body}
	end.
