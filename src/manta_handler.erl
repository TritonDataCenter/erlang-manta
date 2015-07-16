%% -*- mode: erlang; tab-width: 4; indent-tabs-mode: 1; st-rulers: [70] -*-
%% vim: ts=4 sw=4 ft=erlang noet
%%%-------------------------------------------------------------------
%%% @author Andrew Bennett <andrew@pixid.com>
%%% @copyright 2014-2015, Andrew Bennett
%%% @doc
%%%
%%% @end
%%% Created :  21 Nov 2014 by Andrew Bennett <andrew@pixid.com>
%%%-------------------------------------------------------------------
-module(manta_handler).

-include("manta.hrl").

-callback init(ClientId, StreamTo, InitialState) -> State
	when
		ClientId     :: any(),
		StreamTo     :: pid(),
		InitialState :: any(),
		State        :: any().
-callback handle_response(Response, State) -> NewState
	when
		Response :: any(),
		State    :: any(),
		NewState :: any().
-callback handle_download(Download, State) -> NewState
	when
		Download :: {body_part, Pid, iodata()} | {http_eob, Pid, iodata()},
		Pid      :: pid(),
		State    :: any(),
		NewState :: any().
-callback terminate(State) -> term()
	when
		State :: any().

%% manta_handler callbacks
-export([init/3]).
-export([handle_response/2]).
-export([handle_download/2]).
-export([terminate/1]).

-record(state, {
	buffer    = <<>>      :: binary(),
	error     = false     :: boolean(),
	pid       = undefined :: undefined | pid(),
	client_id = undefined :: undefined | any(),
	stream_to = undefined :: undefined | pid()
}).

%%====================================================================
%% manta_handler callbacks
%%====================================================================

init(ClientId, StreamTo, _InitialState) ->
	State = #state{pid=self(), client_id=ClientId, stream_to=StreamTo},
	State.

handle_response(Response={ok, {Status={Code, _}, Headers, Result}},
		State=#state{buffer=Buffer, pid=Pid, client_id=ClientId,
		stream_to=StreamTo})
			when Code >= 400 ->
	case dlhttpc_lib:header_value("content-type", Headers) of
		"application/json" ->
			case Result of
				_ when is_binary(Result) ->
					Term = parse_response_single(<< Buffer/binary, Result/binary >>),
					StreamTo ! {response, ClientId, Pid, {ok, {Status, Headers, Term}}},
					State#state{error=true};
				_ ->
					StreamTo ! {response, ClientId, Pid, Response},
					State#state{error=true}
			end;
		"application/x-json-stream" ++ _ ->
			case Result of
				_ when is_binary(Result) ->
					List = parse_response(<< Buffer/binary, Result/binary >>, []),
					StreamTo ! {response, ClientId, Pid, {ok, {Status, Headers, List}}},
					State#state{error=true};
				_ ->
					StreamTo ! {response, ClientId, Pid, Response},
					State#state{error=true}
			end;
		_ ->
			StreamTo ! {response, ClientId, Pid, Response},
			State#state{error=true}
	end;
handle_response(Response, State=#state{stream_to=StreamTo, pid=Pid,
		client_id=ClientId}) ->
	StreamTo ! {response, ClientId, Pid, Response},
	State.

handle_download({body_part, Pid, Data}, State=#state{buffer=Buffer, error=true, pid=Pid}) ->
	parse_body_part(<< Buffer/binary, Data/binary >>, State);
handle_download(Download, State=#state{stream_to=StreamTo}) ->
	StreamTo ! Download,
	State.

terminate(_State) ->
	ok.

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
parse_response_single(Body) ->
	case manta_json_stream:decode(Body, [return_maps]) of
		{true, Term, _Rest} ->
			Term;
		false ->
			Body
	end.

%% @private
parse_body_part(Body, State) ->
	case manta_json_stream:decode(Body, [return_maps]) of
		{true, Term, Rest} ->
			State#state.stream_to ! {body_part, State#state.pid, Term},
			parse_body_part(Rest, State);
		false ->
			State#state{buffer=Body}
	end.
