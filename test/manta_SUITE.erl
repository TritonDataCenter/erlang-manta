%% -*- mode: erlang; tab-width: 4; indent-tabs-mode: 1; st-rulers: [70] -*-
%% vim: ts=4 sw=4 ft=erlang noet
-module(manta_SUITE).

-include_lib("common_test/include/ct.hrl").

-define(BASE64URL_ENCODE(Bin), <<
	<< (
		case C of
			$/ -> $_;
			$+ -> $-;
			C  -> C
		end
	) >> || << C >> <= base64:encode(Bin), C =/= $=
>>).

-define(MANTA_TEST_DIR, <<"~~/stor/test">>).
-define(MANTA_SUITE_DIR, filename:join([?MANTA_TEST_DIR, <<"erlang-manta">>])).
-define(MANTA_CASE_DIR, filename:join([?MANTA_SUITE_DIR, ?BASE64URL_ENCODE(crypto:strong_rand_bytes(4))])).

%% ct.
-export([all/0]).
-export([groups/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).
-export([init_per_group/2]).
-export([end_per_group/2]).

%% Tests.
-export([manta_directories/1]).
-export([manta_objects/1]).
-export([manta_snaplinks/1]).

all() ->
	[
		{group, storage}
	].

groups() ->
	[
		{storage, [parallel], [
			manta_directories,
			manta_objects,
			manta_snaplinks
		]}
	].

init_per_suite(Config) ->
	ok = manta:start(),
	MantaOptions = [
		{key_file, os:getenv("MANTA_KEY_FILE", filename:join([priv_dir(), "test.pem"]))},
		{subuser, os:getenv("MANTA_SUBUSER")},
		{user, os:getenv("MANTA_USER")}
	],
	ok = manta:configure(MantaOptions),
	{ok, {{204, _}, _, _}} = manta:put_directory(?MANTA_TEST_DIR),
	{ok, {{204, _}, _, _}} = manta:put_directory(?MANTA_SUITE_DIR),
	[
		{manta_options, MantaOptions}
		| Config
	].

end_per_suite(Config) ->
	% Clean up old temporary files
	manta:configure(?config(manta_options, Config)),
	{ok, {{200, _}, _, Folders}} = manta:list_directory(?MANTA_SUITE_DIR),
	OldTime = jsx:decode(jsx:encode(calendar:gregorian_seconds_to_datetime(calendar:datetime_to_gregorian_seconds(calendar:universal_time()) - (1 * 60 * 60)))),
	Deletes = [begin
		proplists:get_value(<<"name">>, Folder)
	end || Folder <- Folders, proplists:get_value(<<"mtime">>, Folder) < OldTime],
	_ = [begin
		rm_rf(filename:join([?MANTA_SUITE_DIR, Delete]))
	end || Delete <- Deletes],
	application:stop(manta),
	ok.

init_per_group(storage, Config) ->
	Config.

end_per_group(_Group, _Config) ->
	ok.

%%====================================================================
%% Tests
%%====================================================================

manta_directories(Config) ->
	manta:configure(?config(manta_options, Config)),
	MantaCaseDir = ?MANTA_CASE_DIR,
	{ok, {{204, _}, _, _}} = manta:put_directory(MantaCaseDir),
	{ok, {{204, _}, _, _}} = manta:put_directory(filename:join([MantaCaseDir, <<"a">>])),
	{ok, {{204, _}, _, _}} = manta:put_directory(filename:join([MantaCaseDir, <<"b">>])),
	{ok, {{200, _}, _, L0}} = manta:list_directory(MantaCaseDir),
	[<<"a">>, <<"b">>] = [Name || [{<<"name">>, Name}, {<<"type">>, <<"directory">>} | _] <- L0],
	{ok, {{204, _}, _, _}} = manta:delete_directory(filename:join([MantaCaseDir, <<"b">>])),
	{ok, {{200, _}, _, L1}} = manta:list_directory(MantaCaseDir),
	[<<"a">>] = [Name || [{<<"name">>, Name}, {<<"type">>, <<"directory">>} | _] <- L1],
	{ok, {{204, _}, _, _}} = manta:delete_directory(filename:join([MantaCaseDir, <<"a">>])),
	{ok, {{204, _}, _, _}} = manta:delete_directory(MantaCaseDir),
	ok.

manta_objects(Config) ->
	manta:configure(?config(manta_options, Config)),
	MantaCaseDir = ?MANTA_CASE_DIR,
	Body = crypto:strong_rand_bytes(1024),
	File = filename:join([MantaCaseDir, <<"x">>]),
	{ok, {{204, _}, _, _}} = manta:put_directory(MantaCaseDir),
	{ok, {{204, _}, _, _}} = manta:put_object(File, Body),
	{ok, {{200, _}, _, Body}} = manta:get_object(File),
	{ok, {{200, _}, H0, _}} = manta:get_metadata(File),
	"application/octet-stream" = ?config("Content-Type", H0),
	{ok, {{204, _}, _, _}} = manta:put_metadata(File, [
		{<<"Content-Type">>, <<"application/binary">>},
		{<<"M-Test">>, <<"abc">>}
	]),
	{ok, {{200, _}, H1, _}} = manta:get_metadata(File),
	"application/binary" = ?config("Content-Type", H1),
	"abc" = ?config("M-Test", H1),
	{ok, {{204, _}, _, _}} = manta:delete_object(File),
	{ok, {{204, _}, _, _}} = manta:delete_directory(MantaCaseDir),
	ok.

manta_snaplinks(Config) ->
	manta:configure(?config(manta_options, Config)),
	MantaCaseDir = ?MANTA_CASE_DIR,
	Body0 = crypto:strong_rand_bytes(1024),
	Body1 = crypto:strong_rand_bytes(1024),
	Body2 = crypto:strong_rand_bytes(1024),
	File = filename:join([MantaCaseDir, <<"y">>]),
	Link = filename:join([MantaCaseDir, <<"z">>]),
	{ok, {{204, _}, _, _}} = manta:put_directory(MantaCaseDir),
	{ok, {{204, _}, _, _}} = manta:put_object(File, Body0),
	{ok, {{204, _}, _, _}} = manta:put_snaplink(Link, File),
	{ok, {{200, _}, _, Body0}} = manta:get_object(File),
	{ok, {{200, _}, _, Body0}} = manta:get_object(Link),
	{ok, {{204, _}, _, _}} = manta:put_object(File, Body1),
	{ok, {{200, _}, _, Body1}} = manta:get_object(File),
	{ok, {{200, _}, _, Body0}} = manta:get_object(Link),
	{ok, {{204, _}, _, _}} = manta:put_object(Link, Body2),
	{ok, {{200, _}, _, Body1}} = manta:get_object(File),
	{ok, {{200, _}, _, Body2}} = manta:get_object(Link),
	{ok, {{204, _}, _, _}} = manta:delete_object(File),
	{ok, {{200, _}, _, Body2}} = manta:get_object(Link),
	{ok, {{204, _}, _, _}} = manta:delete_object(Link),
	{ok, {{204, _}, _, _}} = manta:delete_directory(MantaCaseDir),
	ok.

%%%-------------------------------------------------------------------
%%% Internal functions
%%%-------------------------------------------------------------------

%% @private
ltree(Path) ->
	lists:sort(fun({DepthA, _}, {DepthB, _}) ->
		DepthA >= DepthB
	end, ltree(0, {d, Path}, [])).

%% @private
ltree(Depth, {d, Path}, Acc) ->
	case manta:list_directory(Path) of
		{ok, {{200, _}, _, List}} ->
			ltree_add(Depth + 1, Path, List, [{Depth, {d, Path}} | Acc]);
		_ ->
			[{Depth, {d, Path}} | Acc]
	end.

%% @private
ltree_add(Depth, Path, [Item | List], Acc) ->
	Name = proplists:get_value(<<"name">>, Item),
	Type = proplists:get_value(<<"type">>, Item),
	case Type of
		<<"directory">> ->
			ltree_add(Depth, Path, List, ltree(Depth, {d, filename:join([Path, Name])}, Acc));
		_ ->
			ltree_add(Depth, Path, List, [{Depth, {o, filename:join([Path, Name])}} | Acc])
	end;
ltree_add(_Depth, _, [], Acc) ->
	Acc.

%% @private
rm_rf(Directory) ->
	_ = [begin
		case Type of
			d ->
				manta:delete_directory(Path);
			o ->
				manta:delete_object(Path)
		end
	end || {_, {Type, Path}} <- ltree(Directory)],
	ok.

%% @private
priv_dir() ->
	case code:priv_dir(manta) of
		{error, bad_name} ->
			case code:which(?MODULE) of
				Filename when is_list(Filename) ->
					filename:join([filename:dirname(Filename), "../priv"]);
				_ ->
					"../priv"
			end;
		Dir ->
			Dir
	end.
