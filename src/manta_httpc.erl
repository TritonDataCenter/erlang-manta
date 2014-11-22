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
-module(manta_httpc).

-include("manta.hrl").

%% HTTP API exports
-export([request/6]).
-export([request/7]).
-export([send_body_part/2]).
-export([send_body_part/3]).
-export([send_trailers/2]).
-export([send_trailers/3]).

%% API exports
-export([account_headers/1]).
-export([account_headers/2]).
-export([account_path/1]).
-export([account_path/2]).
-export([make_path/2]).

%%====================================================================
%% HTTP API functions
%%====================================================================

request(Path, Method, Headers, Body, Timeout, Options) ->
	request(manta:default_config(), Path, Method, Headers, Body, Timeout, Options).

request(Config=#manta_config{url=BaseURL}, Path, Method, BaseHeaders, Body, Timeout, Opts0) ->
	URL = << BaseURL/binary, (account_path(Config, Path))/binary >>,
	Headers = account_headers(Config, BaseHeaders),
	StrURL = binary_to_list(URL),
	StrHeaders = [{binary_to_list(K), binary_to_list(V)} || {K, V} <- Headers],
	{Handler, Opts1} = manta:take_value(handler, Opts0, undefined),
	{State, Opts2} = manta:take_value(state, Opts1, undefined),
	ReqRef = make_ref(),
	case proplists:is_defined(stream_to, Opts2) of
		true ->
			StreamTo = proplists:get_value(stream_to, Opts2),
			Pid = spawn(manta_client, proxy, [self(), ReqRef, Handler, State, StreamTo]),
			Opts3 = lists:keystore(stream_to, 1, Opts2, {stream_to, Pid}),
			case dlhttpc:request(StrURL, Method, StrHeaders, Body, Timeout, Opts3) of
				{ReqId, ReqPid} ->
					Pid ! {ReqRef, {ReqId, ReqPid}},
					{ReqId, Pid}
			end;
		false ->
			Pid = spawn_link(manta_client, proxy, [self(), ReqRef, Handler, State, self()]),
			Opts3 = lists:keystore(stream_to, 1, Opts2, {stream_to, Pid}),
			case dlhttpc:request(StrURL, Method, StrHeaders, Body, Timeout, Opts3) of
				{ReqId, ReqPid} ->
					Pid ! {ReqRef, {ReqId, ReqPid}},
					receive
						{response, ReqId, Pid, R} ->
							R;
						{exit, ReqId, Pid, Reason} ->
							exit(Reason);
						{'EXIT', Pid, Reason} ->
							exit(Reason)
					after
						Timeout ->
							Monitor = erlang:monitor(process, Pid),
							unlink(Pid),
							exit(Pid, timeout),
							receive
								{response, _ReqId, Pid, R} ->
									erlang:demonitor(Monitor, [flush]),
									R;
								{'DOWN', Monitor, process, Pid, timeout} ->
									{error, timeout};
								{'DOWN', Monitor, process, Pid, Reason} ->
									erlang:error(Reason)
							end
					end
			end
	end.

send_body_part({Pid, Window}, IoList) ->
	send_body_part({Pid, Window}, IoList, infinity).

send_body_part({Pid, Window}, IoList, Timeout) ->
	dlhttpc:send_body_part({Pid, Window}, IoList, Timeout).

send_trailers({Pid, Window}, Trailers) ->
	send_trailers({Pid, Window}, Trailers, infinity).

send_trailers({Pid, Window}, Trailers, Timeout) ->
	dlhttpc:send_trailers({Pid, Window}, Trailers, Timeout).

%%====================================================================
%% API functions
%%====================================================================

account_headers(Headers) ->
	account_headers(manta:default_config(), Headers).

account_headers(Config=#manta_config{agent=UserAgentVal, key=Key, key_id=KeyID}, Headers) ->
	DateVal = iolist_to_binary(httpd_util:rfc1123_date()),
	AuthorizationVal = <<
		"Signature keyId=\"",
		(account_path(Config, << "keys/", KeyID/binary >>))/binary,
		"\",algorithm=\"",
		(manta_private_key:sign_algorithm(Key))/binary,
		"\",signature=\"",
		(base64:encode(manta_private_key:sign(<< "date: ", DateVal/binary >>, Key)))/binary,
		"\""
	>>,
	[
		{<<"Date">>, DateVal},
		{<<"Authorization">>, AuthorizationVal},
		{<<"User-Agent">>, UserAgentVal}
		| Headers
	].

account_path(Path) ->
	account_path(manta:default_config(), Path).

account_path(#manta_config{subuser=undefined, user=User}, Path) ->
	<< $/, User/binary, $/, Path/binary >>;
account_path(#manta_config{subuser=Subuser, user=User}, Path) ->
	<< $/, User/binary, $/, Subuser/binary, $/, Path/binary >>.

make_path(Path0, QS0) ->
	QS1 = [begin
		K1 = case K0 of
			_ when is_atom(K0) ->
				atom_to_binary(K0, utf8);
			_ when is_binary(K0) ->
				K0;
			_ when is_float(K0) ->
				iolist_to_binary(io_lib:format("~p", [K0]));
			_ when is_integer(K0) ->
				integer_to_binary(K0);
			_ when is_list(K0) ->
				iolist_to_binary(K0)
		end,
		V1 = case V0 of
			_ when is_atom(V0) ->
				atom_to_binary(V0, utf8);
			_ when is_binary(V0) ->
				V0;
			_ when is_float(V0) ->
				iolist_to_binary(io_lib:format("~p", [V0]));
			_ when is_integer(V0) ->
				integer_to_binary(V0);
			_ when is_list(V0) ->
				iolist_to_binary(V0)
		end,
		{K1, V1}
	end || {K0, V0} <- QS0, V0 =/= undefined],
	{Path1, QS2} = case cow_http:parse_fullpath(Path0) of
		{P1, <<>>} ->
			{P1, QS1};
		{P1, Q1} ->
			{P1, QS1 ++ cow_qs:parse_qs(Q1)}
	end,
	QS3 = orddict:from_list(QS2),
	case cow_qs:qs(QS3) of
		<<>> ->
			Path1;
		QS4 ->
			<< Path1/binary, $?, QS4/binary >>
	end.

%%%-------------------------------------------------------------------
%%% Internal functions
%%%-------------------------------------------------------------------
