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
-export([manta_jobs/1]).

all() ->
	[
		{group, storage}
	].

groups() ->
	[
		{storage, [parallel], [
			manta_directories,
			manta_objects,
			manta_snaplinks,
			manta_jobs
		]}
	].

init_per_suite(Config) ->
	ok = manta:start(),
	MantaOptions = [
		{key_file, os:getenv("MANTA_KEY_FILE")},
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
	Deletes = [Name || #{ <<"name">> := Name, <<"mtime">> := MTime } <- Folders, MTime < OldTime],
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
	[<<"a">>, <<"b">>] = [Name || #{ <<"name">> := Name, <<"type">> := <<"directory">> } <- L0],
	{ok, {{204, _}, _, _}} = manta:delete_directory(filename:join([MantaCaseDir, <<"b">>])),
	{ok, {{200, _}, _, L1}} = manta:list_directory(MantaCaseDir),
	[<<"a">>] = [Name || #{ <<"name">> := Name, <<"type">> := <<"directory">> } <- L1],
	{ok, {{204, _}, _, _}} = manta:delete_directory(filename:join([MantaCaseDir, <<"a">>])),
	{ok, {{204, _}, _, _}} = manta:delete_directory(MantaCaseDir),
	ok.

manta_objects(Config) ->
	manta:configure(?config(manta_options, Config)),
	MantaCaseDir = ?MANTA_CASE_DIR,
	Body = crypto:strong_rand_bytes(16),
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
	Body0 = crypto:strong_rand_bytes(16),
	Body1 = crypto:strong_rand_bytes(16),
	Body2 = crypto:strong_rand_bytes(16),
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

manta_jobs(Config) ->
	manta:configure(?config(manta_options, Config)),
	MantaCaseDir = ?MANTA_CASE_DIR,
	Body0 = ?BASE64URL_ENCODE(crypto:strong_rand_bytes(random:uniform(64))),
	Body1 = ?BASE64URL_ENCODE(crypto:strong_rand_bytes(random:uniform(64))),
	File0 = filename:join([MantaCaseDir, <<"j">>]),
	File1 = filename:join([MantaCaseDir, <<"k">>]),
	Size0 = byte_size(Body0),
	Size1 = byte_size(Body1),
	{ok, {{204, _}, _, _}} = manta:put_directory(MantaCaseDir),
	{ok, {{204, _}, _, _}} = manta:put_object(File0, Body0),
	{ok, {{204, _}, _, _}} = manta:put_object(File1, Body1),
	{ok, {{201, _}, H, _}} = manta:create_job(#{
		name => manta:user_agent(),
		phases => [
			#{
				type => <<"map">>,
				exec => <<"awk '{print length}'">>
			}
		]
	}),
	JobPath = list_to_binary(dlhttpc_lib:header_value("Location", H)),
	[<<>>, << $/, JobId/binary >>] = binary:split(JobPath, manta:object_path(<<"~~/jobs">>)),
	{ok, {{204, _}, _, _}} = manta:add_job_inputs(JobPath, [File0, File1]),
	{ok, {{200, _}, _, L}} = manta:list_jobs([{state, <<"running">>}]),
	true = lists:any(fun(#{ <<"name">> := JobName }) ->
		JobName =:= JobId
	end, L),
	{ok, {{202, _}, _, _}} = manta:end_job_input(JobPath),
	In = lists:usort([manta:object_path(File0), manta:object_path(File1)]),
	{ok, {{200, _}, _, In0}} = manta:get_job_input(JobPath),
	In = lists:usort(In0),
	{ok, {{200, _}, _, _}} = manta:get_job_failures(JobPath),
	{ok, {{200, _}, _, _}} = manta:get_job_errors(JobPath),
	_Job = watch_job(JobPath),
	Out = lists:usort([Size0, Size1]),
	{ok, {{200, _}, _, Out0}} = manta:get_job_output(JobPath),
	Out1 = [begin
		{ok, {{200, _}, _, Data}} = manta:get_object(Obj),
		[Bin] = binary:split(Data, <<"\n">>, [trim]),
		binary_to_integer(Bin)
	end || Obj <- Out0],
	Out = lists:usort(Out1),
	{ok, {{204, _}, _, _}} = manta:delete_object(File0),
	{ok, {{204, _}, _, _}} = manta:delete_object(File1),
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
ltree_add(Depth, Path, [#{ <<"name">> := Name, <<"type">> := <<"directory">> } | List], Acc) ->
	ltree_add(Depth, Path, List, ltree(Depth, {d, filename:join([Path, Name])}, Acc));
ltree_add(Depth, Path, [#{ <<"name">> := Name } | List], Acc) ->
	ltree_add(Depth, Path, List, [{Depth, {o, filename:join([Path, Name])}} | Acc]);
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
watch_job(JobPath) ->
	case manta:get_job(JobPath) of
		{ok, {{200, _}, _, Job = #{ <<"state">> := <<"done">> }}} ->
			Job;
		_ ->
			timer:sleep(timer:seconds(1)),
			watch_job(JobPath)
	end.
