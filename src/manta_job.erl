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
-export([init/3,
		 handle_response/2,
		 handle_download/2,
		 terminate/1]).

%% API exports
-export([add_inputs/4,
		 cancel/3,
		 create/3,
		 end_input/3,
		 get/3,
		 get_errors/3,
		 get_failures/3,
		 get_input/3,
		 get_output/3,
		 list/2,
		 path/1]).

-ignore_xref({path,1}).

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

cancel(Config=#manta_config{}, JobPath, Opts0) ->
	{Headers, Opts1} = manta:take_value(headers, Opts0, []),
	{Timeout, Opts2} = manta:take_value(timeout, Opts1, Config#manta_config.timeout),
	Path = << (path(JobPath))/binary, "/live/cancel" >>,
	manta_httpc:request(Config, Path, post, Headers, [], Timeout, Opts2).

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

end_input(Config=#manta_config{}, JobPath, Opts0) ->
	{Headers, Opts1} = manta:take_value(headers, Opts0, []),
	{Timeout, Opts2} = manta:take_value(timeout, Opts1, Config#manta_config.timeout),
	Path = << (path(JobPath))/binary, "/live/in/end" >>,
	manta_httpc:request(Config, Path, post, Headers, [], Timeout, Opts2).

get(Config=#manta_config{}, JobPath, Opts0) ->
	{Headers, Opts1} = manta:take_value(headers, Opts0, []),
	{Timeout, Opts2} = manta:take_value(timeout, Opts1, Config#manta_config.timeout),
	Path = << (path(JobPath))/binary, "/live/status" >>,
	manta_httpc:request(Config, Path, get, Headers, [], Timeout, Opts2).

get_errors(Config=#manta_config{}, JobPath, Opts0) ->
	{Headers, Opts1} = manta:take_value(headers, Opts0, []),
	{Timeout, Opts2} = manta:take_value(timeout, Opts1, Config#manta_config.timeout),
	Path = << (path(JobPath))/binary, "/live/err" >>,
	manta_httpc:request(Config, Path, get, Headers, [], Timeout, Opts2).

get_failures(Config=#manta_config{}, JobPath, Opts0) ->
	{Headers, Opts1} = manta:take_value(headers, Opts0, []),
	{Timeout, Opts2} = manta:take_value(timeout, Opts1, Config#manta_config.timeout),
	Path = << (path(JobPath))/binary, "/live/fail" >>,
	manta_httpc:request(Config, Path, get, Headers, [], Timeout, Opts2).

get_input(Config=#manta_config{}, JobPath, Opts0) ->
	{Headers, Opts1} = manta:take_value(headers, Opts0, []),
	{Timeout, Opts2} = manta:take_value(timeout, Opts1, Config#manta_config.timeout),
	Path = << (path(JobPath))/binary, "/live/in" >>,
	manta_httpc:request(Config, Path, get, Headers, [], Timeout, Opts2).

get_output(Config=#manta_config{}, JobPath, Opts0) ->
	{Headers, Opts1} = manta:take_value(headers, Opts0, []),
	{Timeout, Opts2} = manta:take_value(timeout, Opts1, Config#manta_config.timeout),
	Path = << (path(JobPath))/binary, "/live/out" >>,
	manta_httpc:request(Config, Path, get, Headers, [], Timeout, Opts2).

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
