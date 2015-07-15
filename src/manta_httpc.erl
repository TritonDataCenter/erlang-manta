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
-export([make_path/2]).
-export([prepend_account_path/1]).
-export([prepend_account_path/2]).
-export([role_path/1]).
-export([role_path/2]).
-export([role_path/3]).
-export([urlencode_path/1]).
-export([user_path/1]).
-export([user_path/2]).
-export([user_path/3]).

%%====================================================================
%% HTTP API functions
%%====================================================================

request(Path, Method, Headers, Body, Timeout, Options) ->
	request(manta:default_config(), Path, Method, Headers, Body, Timeout, Options).

request(Config=#manta_config{url=BaseURL}, Path, Method, BaseHeaders, Body, Timeout, Opts) ->
	URL = << BaseURL/binary, (role_path(Config, Path))/binary >>,
	Headers = account_headers(Config, BaseHeaders),
	StrURL = binary_to_list(URL),
	StrHeaders = [{binary_to_list(K), binary_to_list(V)} || {K, V} <- Headers],
	Result = manta_client:request(StrURL, Method, StrHeaders, Body, Timeout, default_options(Config, Opts)),
	case proplists:is_defined(stream_to, Opts) of
		true ->
			Result;
		false ->
			case Result of
				{ReqId, ReqPid} ->
					receive
						{response, ReqId, ReqPid, R} ->
							R;
						{exit, ReqId, ReqPid, Reason} ->
							exit(Reason);
						{'EXIT', ReqPid, Reason} ->
							exit(Reason)
					after
						Timeout ->
							Monitor = erlang:monitor(process, ReqPid),
							unlink(ReqPid),
							exit(ReqPid, timeout),
							receive
								{response, _ReqId, ReqPid, R} ->
									erlang:demonitor(Monitor, [flush]),
									R;
								{'DOWN', Monitor, process, ReqPid, timeout} ->
									{error, timeout};
								{'DOWN', Monitor, process, ReqPid, Reason} ->
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
		(prepend_account_path(Config, << "keys/", KeyID/binary >>))/binary,
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

prepend_account_path(Path) ->
	prepend_account_path(manta:default_config(), Path).

prepend_account_path(#manta_config{subuser=undefined, user=User}, Path) ->
	urlencode_path(<< $/, User/binary, $/, Path/binary >>);
prepend_account_path(#manta_config{subuser=Subuser, user=User}, Path) ->
	urlencode_path(<< $/, User/binary, $/, Subuser/binary, $/, Path/binary >>).

role_path(Path) ->
	role_path(manta:default_config(), Path).

role_path(C=#manta_config{role=undefined}, Path) ->
	user_path(C, Path);
role_path(C=#manta_config{role=Role}, Path) ->
	user_path(C, Path, [{<<"role">>, Role}]).

role_path(C=#manta_config{}, Path, []) ->
	role_path(C, Path);
role_path(C=#manta_config{}, Path, QS) ->
	role_path(C, user_path(C, Path, QS)).

urlencode_path(Path) ->
	{P0, Q0} = cow_http:parse_fullpath(Path),
	Parts = binary:split(P0, << $/ >>, [global, trim]),
	P1 = join_path([iolist_to_binary(http_uri:encode(binary_to_list(Part))) || Part <- Parts], <<>>),
	case Q0 of
		<<>> ->
			P1;
		_ ->
			<< P1/binary, $?, Q0/binary >>
	end.

user_path(Path) ->
	user_path(manta:default_config(), Path).

user_path(#manta_config{user=User}, << $~, $~, $/, Path/binary >>) ->
	urlencode_path(<< $/, User/binary, $/, Path/binary >>);
user_path(#manta_config{}, Path) ->
	urlencode_path(Path).

user_path(C=#manta_config{}, Path, []) ->
	user_path(C, Path);
user_path(C=#manta_config{}, Path, QS) ->
	user_path(C, make_path(Path, QS)).

%%%-------------------------------------------------------------------
%%% Internal functions
%%%-------------------------------------------------------------------

%% @private
default_options(Config, Opts) ->
	default_options([attempts, attempt_timeout, connect_timeout], Config, Opts).

%% @private
default_options([Key | Keys], Config, Opts) ->
	case lists:keymember(Key, 1, Opts) of
		false ->
			default_options(Keys, Config, default_option(Key, Config, Opts));
		true ->
			default_options(Keys, Config, Opts)
	end;
default_options([], _Config, Opts) ->
	Opts.

%% @private
default_option(attempts, #manta_config{attempts=Attempts}, Opts) ->
	[{attempts, Attempts} | Opts];
default_option(attempt_timeout, #manta_config{attempt_timeout=AttemptTimeout}, Opts) ->
	[{attempt_timeout, AttemptTimeout} | Opts];
default_option(connect_timeout, #manta_config{connect_timeout=ConnectTimeout}, Opts) ->
	[{connect_timeout, ConnectTimeout} | Opts].

%% @private
join_path([<<>> | Parts], Acc) ->
	join_path(Parts, Acc);
join_path([Part], Acc) ->
	<< Acc/binary, $/, Part/binary >>;
join_path([Part | Parts], Acc) ->
	join_path(Parts, << Acc/binary, $/, Part/binary >>);
join_path([], Acc) ->
	Acc.
