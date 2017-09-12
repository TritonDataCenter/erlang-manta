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
-module(manta_client).

-include("manta.hrl").

%% API exports
-export([request/6]).
-export([init/7]).

-ignore_xref({init,7}).

-record(request, {
	attempts   = undefined :: undefined | pos_integer(),
	a_timeout  = undefined :: undefined | timeout(),
	connect    = undefined :: undefined | function(),
	client_id  = undefined :: undefined | any(),
	handler    = undefined :: undefined | module(),
	state      = undefined :: undefined | any(),
	stream_to  = undefined :: undefined | pid(),
	stream_mon = undefined :: undefined | reference(),
	req_id     = undefined :: undefined | any(),
	req_pid    = undefined :: undefined | pid()
}).

%%====================================================================
%% API functions
%%====================================================================

request(URL, Method, Headers, Body, Timeout, Opts) ->
	case proplists:is_defined(stream_to, Opts) of
		true ->
			proc_lib:start(?MODULE, init, [self(), URL, Method, Headers, Body, Timeout, Opts]);
		false ->
			NewOpts = lists:keystore(stream_to, 1, Opts, {stream_to, self()}),
			proc_lib:start_link(?MODULE, init, [self(), URL, Method, Headers, Body, Timeout, NewOpts])
	end.

%% @private
init(Parent, URL, Method, Headers, Body, Timeout, Opts0) ->
	{Handler, Opts1} = manta:take_value(handler, Opts0, undefined),
	{State, Opts2} = manta:take_value(handler_state, Opts1, undefined),
	{Attempts, Opts3} = manta:take_value(attempts, Opts2, ?DEFAULT_ATTEMPTS),
	{AttemptTimeout, Opts4} = manta:take_value(attempt_timeout, Opts3, ?DEFAULT_ATTEMPT_TIMEOUT),
	StreamTo = proplists:get_value(stream_to, Opts4),
	Opts5 = lists:keystore(stream_to, 1, Opts4, {stream_to, self()}),
	Connect = fun() ->
		dlhttpc:request(URL, Method, Headers, Body, Timeout, Opts5)
	end,
	ClientId = make_client_id(),
	proc_lib:init_ack(Parent, {ClientId, self()}),
	StreamMon = erlang:monitor(process, StreamTo),
	Request = #request{
		attempts   = Attempts,
		a_timeout  = AttemptTimeout,
		connect    = Connect,
		client_id  = ClientId,
		handler    = Handler,
		state      = State,
		stream_to  = StreamTo,
		stream_mon = StreamMon
	},
	connect(handler_init(Request)).

%% @private
connect(R=#request{connect=Connect}) ->
	{ReqId, ReqPid} = Connect(),
	loop(R#request{req_id=ReqId, req_pid=ReqPid}).

%% @private
loop(R=#request{req_id=ReqId, req_pid=ReqPid, stream_to=StreamTo,
		stream_mon=StreamMon}) ->
	receive
		{response, ReqId, ReqPid, Error={error, _}} ->
			maybe_connect(R, Error);
		{response, ReqId, ReqPid, {ok, {Status, Headers, ReqPid}}} ->
			Response = {ok, {Status, Headers, self()}},
			download(handler_response(R#request{attempts=1}, Response));
		{response, ReqId, ReqPid, {ok, {ReqPid, UploadWindow}}} ->
			Response = {ok, {self(), UploadWindow}},
			upload(handler_response(R#request{attempts=1}, Response));
		{response, ReqId, ReqPid, Response} ->
			terminate(handler_response(R, Response));
		{exit, ReqId, ReqPid, Reason} ->
			Error = {exit, R#request.client_id, self(), Reason},
			maybe_connect(R, Error);
		{'DOWN', StreamMon, process, StreamTo, Reason} ->
			exit(Reason)
	end.

%% @private
maybe_connect(R=#request{attempts=1}, Response) ->
	terminate(handler_response(R, Response));
maybe_connect(R=#request{attempts=A, a_timeout=ATimeout,
		stream_to=StreamTo, stream_mon=StreamMon}, _Response) ->
	receive
		{'DOWN', StreamMon, process, StreamTo, Reason} ->
			exit(Reason)
	after
		ATimeout ->
			connect(R#request{attempts=A-1})
	end.

%% @private
download(R=#request{req_pid=ReqPid, stream_to=StreamTo,
		stream_mon=StreamMon}) ->
	receive
		{body_part, ReqPid, Bin} ->
			Download = {body_part, self(), Bin},
			download(handler_download(R, Download));
		{http_eob, ReqPid, Trailers} ->
			Download = {http_eob, self(), Trailers},
			terminate(handler_download(R, Download));
		{'DOWN', StreamMon, process, StreamTo, Reason} ->
			exit(Reason)
	end.

%% @private
upload(R=#request{req_pid=ReqPid, stream_to=StreamTo,
		stream_mon=StreamMon}) ->
	receive
		{trailers, StreamTo, Trailers} ->
			ReqPid ! {trailers, self(), Trailers},
			loop(R);
		{body_part, StreamTo, http_eob} ->
			ReqPid ! {body_part, self(), http_eob},
			loop(R);
		{body_part, StreamTo, Data} ->
			ReqPid ! {body_part, self(), Data},
			upload_ack(R);
		{'DOWN', StreamMon, process, StreamTo, Reason} ->
			exit(Reason)
	end.

%% @private
upload_ack(R=#request{req_pid=ReqPid, stream_to=StreamTo,
		stream_mon=StreamMon}) ->
	receive
		{ack, ReqPid} ->
			StreamTo ! {ack, self()},
			upload(R);
		{'DOWN', StreamMon, process, StreamTo, Reason} ->
			exit(Reason)
	end.

%% @private
terminate(R=#request{stream_mon=StreamMon, req_pid=undefined}) ->
	true = erlang:demonitor(StreamMon, [flush]),
	_ = handler_terminate(R),
	exit(normal);
terminate(R=#request{req_pid=ReqPid}) ->
	ReqMon = erlang:monitor(process, ReqPid),
	receive
		{'DOWN', ReqMon, process, ReqPid, _Reason} ->
			terminate(R#request{req_pid=undefined})
	end.

%%%-------------------------------------------------------------------
%%% Internal Handler functions
%%%-------------------------------------------------------------------

%% @private
handler_init(R=#request{handler=undefined}) ->
	R;
handler_init(R=#request{handler=Handler, state=State,
		client_id=ClientId, stream_to=StreamTo}) ->
	NewState = Handler:init(ClientId, StreamTo, State),
	R#request{state=NewState}.

%% @private
handler_response(R=#request{handler=undefined, stream_to=StreamTo,
		client_id=ClientId}, Response) ->
	StreamTo ! {response, ClientId, self(), Response},
	R;
handler_response(R=#request{handler=Handler, state=State}, Response) ->
	NewState = Handler:handle_response(Response, State),
	R#request{state=NewState}.

%% @private
handler_download(R=#request{handler=undefined, stream_to=StreamTo}, Download) ->
	StreamTo ! Download,
	R;
handler_download(R=#request{handler=Handler, state=State}, Download) ->
	NewState = Handler:handle_download(Download, State),
	R#request{state=NewState}.

%% @private
handler_terminate(#request{handler=undefined}) ->
	ok;
handler_terminate(#request{handler=Handler, state=State}) ->
	catch Handler:terminate(State).

%%%-------------------------------------------------------------------
%%% Internal functions
%%%-------------------------------------------------------------------

%% @private
-ifdef(time_correction).
make_client_id() ->
	erlang:timestamp().
-else.
make_client_id() ->
	erlang:now().
-endif.
