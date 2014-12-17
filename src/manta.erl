%% -*- mode: erlang; tab-width: 4; indent-tabs-mode: 1; st-rulers: [70] -*-
%% vim: ts=4 sw=4 ft=erlang noet
%%%-------------------------------------------------------------------
%%% @author Andrew Bennett <andrew@pixid.com>
%%% @copyright 2014, Andrew Bennett
%%% @doc
%%%
%%% @end
%%% Created :  18 Nov 2014 by Andrew Bennett <andrew@pixid.com>
%%%-------------------------------------------------------------------
-module(manta).

-include("manta.hrl").
-include_lib("public_key/include/public_key.hrl").

%% API exports
%%% Directories
-export([delete_directory/1]).
-export([delete_directory/2]).
-export([delete_directory/3]).
-export([list_directory/0]).
-export([list_directory/1]).
-export([list_directory/2]).
-export([list_directory/3]).
-export([put_directory/1]).
-export([put_directory/2]).
-export([put_directory/3]).
%%% Objects
-export([delete_object/1]).
-export([delete_object/2]).
-export([delete_object/3]).
-export([get_object/1]).
-export([get_object/2]).
-export([get_object/3]).
-export([put_metadata/2]).
-export([put_metadata/3]).
-export([put_metadata/4]).
-export([put_object/2]).
-export([put_object/3]).
-export([put_object/4]).
%%% SnapLinks
-export([put_snap_link/2]).
-export([put_snap_link/3]).
-export([put_snap_link/4]).

%% Config API exports
-export([configure/1]).
-export([default_config/0]).
-export([new/1]).
-export([update/1]).
-export([user_agent/0]).

%% Utility API
-export([require/1]).
-export([start/0]).
-export([take_value/3]).

%% Internal API
-export([get_value/3]).

-define(DEFAULT_URL, "https://us-east.manta.joyent.com").

%%====================================================================
%% API functions
%%====================================================================

%%% Directories

delete_directory(Pathname) ->
	delete_directory(Pathname, []).

delete_directory(Pathname, Options) ->
	delete_directory(default_config(), Pathname, Options).

delete_directory(Config=#manta_config{}, Pathname, Opts0) ->
	Opts1 = lists:keystore(handler, 1, Opts0, {handler, manta_handler}),
	manta_directory:delete(Config, Pathname, Opts1).

list_directory() ->
	list_directory(<<>>).

list_directory(Pathname) ->
	list_directory(Pathname, []).

list_directory(Pathname, Options) ->
	list_directory(default_config(), Pathname, Options).

list_directory(Config=#manta_config{}, Pathname, Opts0) ->
	Opts1 = lists:keystore(handler, 1, Opts0, {handler, manta_directory}),
	Opts2 = lists:keystore(state, 1, Opts1, {state, list}),
	manta_directory:list(Config, Pathname, Opts2).

put_directory(Pathname) ->
	put_directory(Pathname, []).

put_directory(Pathname, Options) ->
	put_directory(default_config(), Pathname, Options).

put_directory(Config=#manta_config{}, Pathname, Opts0) ->
	Opts1 = lists:keystore(handler, 1, Opts0, {handler, manta_handler}),
	manta_directory:put(Config, Pathname, Opts1).

%%% Objects

delete_object(Pathname) ->
	delete_object(Pathname, []).

delete_object(Pathname, Options) ->
	delete_object(default_config(), Pathname, Options).

delete_object(Config=#manta_config{}, Pathname, Opts0) ->
	Opts1 = lists:keystore(handler, 1, Opts0, {handler, manta_handler}),
	manta_object:delete(Config, Pathname, Opts1).

get_object(Pathname) ->
	get_object(Pathname, []).

get_object(Pathname, Options) ->
	get_object(default_config(), Pathname, Options).

get_object(Config=#manta_config{}, Pathname, Opts0) ->
	Opts1 = lists:keystore(handler, 1, Opts0, {handler, manta_handler}),
	manta_object:get(Config, Pathname, Opts1).

put_metadata(Pathname, Metadata) ->
	put_metadata(Pathname, Metadata, []).

put_metadata(Pathname, Metadata, Options) ->
	put_metadata(default_config(), Pathname, Metadata, Options).

put_metadata(Config=#manta_config{}, Pathname, Metadata, Options) ->
	manta_object:put_metadata(Config, Pathname, Metadata, Options).

put_object(Pathname, Body) ->
	put_object(Pathname, Body, []).

put_object(Pathname, Body, Options) ->
	put_object(default_config(), Pathname, Body, Options).

put_object(Config=#manta_config{}, Pathname, Body, Opts0) ->
	Opts1 = lists:keystore(handler, 1, Opts0, {handler, manta_handler}),
	manta_object:put(Config, Pathname, Body, Opts1).

%%% SnapLinks

put_snap_link(Pathname, Location) ->
	put_snap_link(Pathname, Location, []).

put_snap_link(Pathname, Location, Options) ->
	put_snap_link(default_config(), Pathname, Location, Options).

put_snap_link(Config=#manta_config{}, Pathname, Location, Opts0) ->
	Opts1 = lists:keystore(handler, 1, Opts0, {handler, manta_handler}),
	manta_snap_link:put(Config, Pathname, Location, Opts1).

%%====================================================================
%% Config API functions
%%====================================================================

-spec configure(Options) -> ok | {error, Reason}
	when
		Options :: [{atom(), term()}],
		Reason  :: term().
configure(Options) when is_list(Options) ->
	case new(Options) of
		{ok, Config=#manta_config{}} ->
			update(Config);
		Error ->
			Error
	end.

-spec default_config() -> #manta_config{}.
default_config() ->
	case get(manta_config) of
		undefined ->
			Options = [{K, V} || {K, V} <- [
				{key_file, os:getenv("MANTA_KEY_FILE")},
				{role, os:getenv("MANTA_ROLE")},
				{subuser, os:getenv("MANTA_SUBUSER")},
				{url, os:getenv("MANTA_URL")},
				{user, os:getenv("MANTA_USER")}
			], V =/= false],
			case new(Options) of
				{ok, MantaConfig=#manta_config{}} ->
					MantaConfig;
				Error ->
					Error
			end;
		MantaConfig ->
			MantaConfig
	end.

-spec new(Options) -> {ok, Config} | {error, Reason}
	when
		Options :: [{atom(), term()}],
		Config  :: #manta_config{},
		Reason  :: term().
new(Options) when is_list(Options) ->
	ok = verify_options(Options, []),
	new(Options, #manta_config{}).

-spec update(Config) -> ok
	when
		Config :: #manta_config{}.
update(Config=#manta_config{}) ->
	_ = put(manta_config, Config),
	ok.

-spec user_agent() -> binary().
user_agent() ->
	_ = application:load(?MODULE),
	{ok, Version} = application:get_key(?MODULE, vsn),
	SystemArchitecture = erlang:system_info(system_architecture),
	OTPRelease = erlang:system_info(otp_release),
	SystemVersion = erlang:system_info(version),
	[{_, _, OpenSSL} | _] = crypto:info_lib(),
	iolist_to_binary(io_lib:format(
		"erlang-manta/~s "
		"(~s; ~s) "
		"erlang/~s erts/~s",
		[Version, SystemArchitecture, OpenSSL, OTPRelease, SystemVersion])).

%%====================================================================
%% Utility API functions
%%====================================================================

require([]) ->
	ok;
require([App | Apps]) ->
	case application:ensure_started(App) of
		ok ->
			require(Apps);
		StartError ->
			StartError
	end.

start() ->
	_ = application:load(?MODULE),
	{ok, Apps} = application:get_key(?MODULE, applications),
	case require(Apps) of
		ok ->
			application:ensure_started(?MODULE);
		StartError ->
			StartError
	end.

take_value(Key, Opts0, Default) ->
	case lists:keytake(Key, 1, Opts0) of
		{value, {Key, Value}, Opts1} ->
			{Value, Opts1};
		false ->
			{Default, Opts0}
	end.

%%%-------------------------------------------------------------------
%%% Internal functions
%%%-------------------------------------------------------------------

%% @private
-spec bad_options([term()]) -> no_return().
bad_options(Errors) ->
	erlang:error({bad_options, Errors}).

%% Faster alternative to proplists:get_value/3.
%% @private
get_value(Key, Opts, Default) ->
	case lists:keyfind(Key, 1, Opts) of
		{_, Value} -> Value;
		_ -> Default
	end.

%% @private
new([{key, Key} | Opts], Config) ->
	case manta_private_key:fingerprint(Key) of
		{ok, KeyID} ->
			new(Opts, Config#manta_config{key=Key, key_id=KeyID});
		KeyError ->
			KeyError
	end;
new([{key_file, KeyFile} | Opts], Config) ->
	case lists:keytake(key_pass, 1, Opts) of
		false ->
			case manta_private_key:decode_file(KeyFile) of
				{ok, Key} ->
					new([{key, Key} | Opts], Config#manta_config{key_file=KeyFile});
				DecodeError ->
					DecodeError
			end;
		{value, {key_pass, KeyPass}, NewOpts} ->
			case manta_private_key:decode_file(KeyFile, KeyPass) of
				{ok, Key} ->
					new([{key, Key} | NewOpts], Config#manta_config{key_file=KeyFile});
				DecodeError ->
					DecodeError
			end
	end;
new([{role, Role} | Opts], Config) ->
	Roles = [role_strip(R) || R <- role_split(Role)],
	new(Opts, Config#manta_config{role=role_join(Roles)});
new([{subuser, Subuser} | Opts], Config) ->
	new(Opts, Config#manta_config{subuser=iolist_to_binary(Subuser)});
new([{url, URL} | Opts], Config) ->
	new(Opts, Config#manta_config{url=iolist_to_binary(URL)});
new([{user, User} | Opts], Config) ->
	new(Opts, Config#manta_config{user=iolist_to_binary(User)});
new([], #manta_config{key=undefined}) ->
	{error, key_required};
new([], Config=#manta_config{url=undefined}) ->
	new([], Config#manta_config{url=iolist_to_binary(?DEFAULT_URL)});
new([], #manta_config{user=undefined}) ->
	{error, user_required};
new([], Config) ->
	{ok, Config#manta_config{agent=manta:user_agent()}}.

%% @private
role_join(Roles) ->
	<< $,, Role/binary >> = << << $,, R/binary >> || R <- Roles, R =/= <<>> >>,
	Role.

%% @private
role_split(Role) ->
	binary:split(iolist_to_binary(Role), <<",">>, [global, trim]).

%% @private
role_strip(Role) ->
	strip_whitespace(Role, <<>>).

%% @private
strip_whitespace(<< $\n, Rest/binary >>, Acc) ->
	strip_whitespace(Rest, Acc);
strip_whitespace(<< $\r, Rest/binary >>, Acc) ->
	strip_whitespace(Rest, Acc);
strip_whitespace(<< $\s, Rest/binary >>, Acc) ->
	strip_whitespace(Rest, Acc);
strip_whitespace(<< $\t, Rest/binary >>, Acc) ->
	strip_whitespace(Rest, Acc);
strip_whitespace(<< C, Rest/binary >>, Acc) ->
	strip_whitespace(Rest, << Acc/binary, C >>);
strip_whitespace(<<>>, Acc) ->
	Acc.

%% @private
verify_options([{key, Key} | Options], Errors)
		when is_record(Key, 'DSAPrivateKey')
		orelse is_record(Key, 'ECPrivateKey')
		orelse is_record(Key, 'RSAPrivateKey') ->
	verify_options(Options, Errors);
verify_options([{key_file, KeyFile} | Options], Errors)
		when is_binary(KeyFile)
		orelse is_list(KeyFile) ->
	verify_options(Options, Errors);
verify_options([{key_pass, KeyPass} | Options], Errors)
		when is_binary(KeyPass)
		orelse is_list(KeyPass) ->
	verify_options(Options, Errors);
verify_options([{role, Role} | Options], Errors)
		when is_binary(Role)
		orelse is_list(Role) ->
	verify_options(Options, Errors);
verify_options([{subuser, Subuser} | Options], Errors)
		when is_binary(Subuser)
		orelse is_list(Subuser) ->
	verify_options(Options, Errors);
verify_options([{url, URL} | Options], Errors)
		when is_binary(URL)
		orelse is_list(URL) ->
	verify_options(Options, Errors);
verify_options([{user, User} | Options], Errors)
		when is_binary(User)
		orelse is_list(User) ->
	verify_options(Options, Errors);
verify_options([Option | Options], Errors) ->
	verify_options(Options, [Option | Errors]);
verify_options([], []) ->
	ok;
verify_options([], Errors) ->
	bad_options(Errors).
