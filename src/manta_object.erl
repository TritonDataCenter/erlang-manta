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
-module(manta_object).

-include("manta.hrl").

%% API exports
-export([delete/3,
		 get/3,
		 head/3,
		 path/3,
		 put/4,
		 put_metadata/4,
		 url/3]).

%%====================================================================
%% API functions
%%====================================================================

delete(Config=#manta_config{}, Pathname, Opts0) ->
	{Headers, Opts1} = manta:take_value(headers, Opts0, []),
	{Timeout, Opts2} = manta:take_value(timeout, Opts1, Config#manta_config.timeout),
	Path = manta_httpc:make_path(Pathname, []),
	manta_httpc:request(Config, Path, delete, Headers, [], Timeout, Opts2).

get(Config=#manta_config{}, Pathname, Opts0) ->
	{Headers, Opts1} = manta:take_value(headers, Opts0, []),
	{Timeout, Opts2} = manta:take_value(timeout, Opts1, Config#manta_config.timeout),
	Path = manta_httpc:make_path(Pathname, []),
	manta_httpc:request(Config, Path, get, Headers, [], Timeout, Opts2).

head(Config=#manta_config{}, Pathname, Opts0) ->
	{Headers, Opts1} = manta:take_value(headers, Opts0, []),
	{Timeout, Opts2} = manta:take_value(timeout, Opts1, Config#manta_config.timeout),
	Path = manta_httpc:make_path(Pathname, []),
	manta_httpc:request(Config, Path, head, Headers, [], Timeout, Opts2).

path(Config=#manta_config{}, Pathname, QueryString) ->
	manta_httpc:user_path(Config, Pathname, QueryString).

put(Config=#manta_config{}, Pathname, Body, Opts0) ->
	{Headers, Opts1} = manta:take_value(headers, Opts0, []),
	{Timeout, Opts2} = manta:take_value(timeout, Opts1, Config#manta_config.timeout),
	Path = manta_httpc:make_path(Pathname, []),
	manta_httpc:request(Config, Path, put, Headers, Body, Timeout, Opts2).

put_metadata(Config=#manta_config{}, Pathname, Metadata, Opts0) ->
	{Headers0, Opts1} = manta:take_value(headers, Opts0, []),
	{Timeout, Opts2} = manta:take_value(timeout, Opts1, Config#manta_config.timeout),
	Path = manta_httpc:make_path(Pathname, [{<<"metadata">>, <<"true">>}]),
	Headers1 = Metadata ++ Headers0,
	case proplists:is_defined(partial_upload, Opts2) of
		true ->
			manta_httpc:request(Config, Path, put, Headers1, [], Timeout, Opts2);
		false ->
			Opts3 = [{partial_upload, infinity} | Opts2],
			case manta_httpc:request(Config, Path, put, Headers1, [], Timeout, Opts3) of
				{ok, Upload={Pid, _Window}}  ->
					UploadMonitor = erlang:monitor(process, Pid),
					Response = manta_httpc:send_body_part(Upload, http_eob),
					erlang:demonitor(UploadMonitor, [flush]),
					Response;
				RequestError ->
					RequestError
			end
	end.

url(Config=#manta_config{url=BaseURL}, Pathname, QueryString) ->
	<< BaseURL/binary, (?MODULE:path(Config, Pathname, QueryString))/binary >>.

%%%-------------------------------------------------------------------
%%% Internal functions
%%%-------------------------------------------------------------------
