%% -*- mode: erlang; tab-width: 4; indent-tabs-mode: 1; st-rulers: [70] -*-
%% vim: ts=4 sw=4 ft=erlang noet
%%%-------------------------------------------------------------------
%%% @author Andrew Bennett <andrew@pixid.com>
%%% @copyright 2014, Andrew Bennett
%%% @doc
%%%
%%% @end
%%% Created :  21 Nov 2014 by Andrew Bennett <andrew@pixid.com>
%%%-------------------------------------------------------------------
-module(manta_client).

-include("manta.hrl").

%% Proxy API exports
-export([proxy/5]).

%%====================================================================
%% Proxy API functions
%%====================================================================

proxy(Parent, ReqRef, Handler, InitialState, StreamTo) ->
	Monitor = erlang:monitor(process, Parent),
	receive
		{ReqRef, {ReqId, ReqPid}} ->
			true = erlang:demonitor(Monitor, [flush]),
			StreamMon = erlang:monitor(process, StreamTo),
			State = handler_init(Handler, InitialState, ReqId, StreamTo),
			proxy_response(ReqId, ReqPid, Handler, State, StreamTo, StreamMon);
		{'DOWN', Monitor, process, Parent, _Reason} ->
			exit(normal)
	end.

%%%-------------------------------------------------------------------
%%% Internal Proxy functions
%%%-------------------------------------------------------------------

%% @private
proxy_response(ReqId, Pid, Handler, State, StreamTo, StreamMon) ->
	receive
		{response, ReqId, Pid, {ok, {Status, Headers, Pid}}} ->
			Response = {response, ReqId, self(), {ok, {Status, Headers, self()}}},
			NewState = handler_response(Handler, State, StreamTo, Response),
			proxy_download(Pid, Handler, NewState, StreamTo, StreamMon);
		{response, ReqId, Pid, {ok, {Status, Headers, undefined}}} ->
			Response = {response, ReqId, self(), {ok, {Status, Headers, undefined}}},
			NewState = handler_response(Handler, State, StreamTo, Response),
			proxy_terminate(Pid, Handler, NewState, StreamMon);
		{response, ReqId, Pid, {ok, {Status, Headers, Body}}} when is_binary(Body) ->
			Response = {response, ReqId, self(), {ok, {Status, Headers, Body}}},
			NewState = handler_response(Handler, State, StreamTo, Response),
			proxy_terminate(Pid, Handler, NewState, StreamMon);
		{response, ReqId, Pid, {ok, {Pid, UploadWindow}}} ->
			Response = {response, ReqId, self(), {ok, {self(), UploadWindow}}},
			NewState = handler_response(Handler, State, StreamTo, Response),
			proxy_upload(ReqId, Pid, Handler, NewState, StreamTo, StreamMon);
		{response, ReqId, Pid, Result} ->
			Response = {response, ReqId, self(), Result},
			NewState = handler_response(Handler, State, StreamTo, Response),
			proxy_terminate(Pid, Handler, NewState, StreamMon);
		{'DOWN', StreamMon, process, StreamTo, Reason} ->
			exit(Reason)
	end.

%% @private
proxy_download(Pid, Handler, State, StreamTo, StreamMon) ->
	receive
		{body_part, Pid, Bin} ->
			Download = {body_part, self(), Bin},
			NewState = handler_download(Handler, State, StreamTo, Download),
			proxy_download(Pid, Handler, NewState, StreamTo, StreamMon);
		{http_eob, Pid, Trailers} ->
			Download = {http_eob, self(), Trailers},
			NewState = handler_download(Handler, State, StreamTo, Download),
			proxy_terminate(Pid, Handler, NewState, StreamMon);
		{'DOWN', StreamMon, process, StreamTo, Reason} ->
			exit(Reason)
	end.

%% @private
proxy_upload(ReqId, Pid, Handler, State, StreamTo, StreamMon) ->
	receive
		{trailers, StreamTo, Trailers} ->
			Pid ! {trailers, self(), Trailers},
			proxy_response(ReqId, Pid, Handler, State, StreamTo, StreamMon);
		{body_part, StreamTo, http_eob} ->
			Pid ! {body_part, self(), http_eob},
			proxy_response(ReqId, Pid, Handler, State, StreamTo, StreamMon);
		{body_part, StreamTo, Data} ->
			Pid ! {body_part, self(), Data},
			proxy_upload_ack(ReqId, Pid, Handler, State, StreamTo, StreamMon)
	end.

%% @private
proxy_upload_ack(ReqId, Pid, Handler, State, StreamTo, StreamMon) ->
	receive
		{ack, Pid} ->
			StreamTo ! {ack, self()},
			proxy_upload(ReqId, Pid, Handler, State, StreamTo, StreamMon)
	end.

%% @private
proxy_terminate(Pid, Handler, State, StreamMon) ->
	true = erlang:demonitor(StreamMon, [flush]),
	_ = handler_terminate(Handler, State),
	erlang:monitor(process, Pid),
	receive
		{'DOWN', _, process, Pid, _Reason} ->
			exit(normal)
	end.

%%%-------------------------------------------------------------------
%%% Internal Handler functions
%%%-------------------------------------------------------------------

%% @private
handler_init(undefined, _, _, _) ->
	undefined;
handler_init(Handler, InitialState, ReqId, StreamTo) ->
	Handler:init(ReqId, StreamTo, InitialState).

%% @private
handler_response(undefined, State, StreamTo, Response) ->
	StreamTo ! Response,
	State;
handler_response(Handler, State, _StreamTo, Response) ->
	Handler:handle_response(Response, State).

%% @private
handler_download(undefined, State, StreamTo, Download) ->
	StreamTo ! Download,
	State;
handler_download(Handler, State, _StreamTo, Download) ->
	Handler:handle_download(Download, State).

%% @private
handler_terminate(undefined, _) ->
	ok;
handler_terminate(Handler, State) ->
	catch Handler:terminate(State).
