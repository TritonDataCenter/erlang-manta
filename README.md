manta
=====

[![Build Status](https://travis-ci.org/joyent/erlang-manta.png?branch=master)](https://travis-ci.org/joyent/erlang-manta)

Erlang Manta Client

Build
-----

	$ make

Usage
-----

```erlang
manta:configure([
	{key_file, "/path/to/key.pem"}, % or {key, #'DSAPrivateKey'{} | #'RSAPrivateKey'{}}
	% {subuser, <<"foo">>},
	{url, <<"https://us-east.manta.joyent.com">>},
	{user, <<"bar">>},
	% Client Option Defaults
	{attempts, 3},
	{attempt_timeout, 2000},
	{connect_timeout, 5000},
	{timeout, 60000}
]).

DirPath = <<"~~/stor/erlang-manta-example">>.

{ok, {{204, _StatusText}, _Headers, _Body}} = manta:put_directory(DirPath).

Body = <<"hello">>,
File = filename:join([DirPath, <<"test.txt">>]),
{ok, {{204, _}, _, _}} = manta:put_object(File, Body, [{
	{headers, [
		{<<"Content-Type">>, <<"text/plain">>}
	]}
}]).

{ok, {{200, _}, _, Body}} = manta:get_object(File).

% This example job runs the wc UNIX command on every object for the
% map phase, then uses awk during reduce to sum up the three numbers each wc
% returned.
JobDetails = #{
	name => <<"total word count">>,
	phases => [
		#{
			exec => <<"wc">>
		},
		#{
			type => <<"reduce">>,
			exec => <<"awk '{ l += $1; w += $2; c += $3 } END { print l, w, c }'">>
		}
	]
}.

% Create the job, then add the objects the job should operate on.
{ok, {{201, _}, Headers, _}} = manta:create_job(JobDetails),
JobPath = list_to_binary(dlhttpc_lib:header_value("Location", H)).

{ok, {{200, _}, _, Entries}} = manta:list_directory(DirPath),
ObjPaths = [begin
	filename:join([DirPath, Name])
end || #{ <<"name">> := Name, <<"type">> := <<"object">> } <- Entries].

manta:add_job_inputs(JobPath, ObjPaths).

% Tell Manta we're done adding objects to the job. Manta doesn't need this
% to start running a job -- you can see map results without it, for
% example -- but reduce phases in particular depend on all mapping
% finishing.
manta:end_job_input(JobPath).

% Poll until Manta finishes the job.
WatchJob = fun WatchJob(JP) ->
	case manta:get_job(JP) of
		{ok, {{200, _}, _, J = #{ <<"state">> := <<"done">> }}} ->
			J;
		_ ->
			timer:sleep(timer:seconds(1)),
			WatchJob(JP)
	end
end,
Job = WatchJob(JobPath).

% We know in this case there will be only one result. Fetch it and
% display it.
{ok, {{200, _}, _, [Result]}} = manta:get_job_output(JobPath),
{ok, {{200, _}, _, Data}} = manta:get_object(Result),
io:format("Output: ~p~n", [Data]).

% Clean up; remove objects and directory.
[begin
	manta:delete_object(ObjPath)
end || ObjPath <- ObjPaths].

manta:delete_directory(DirPath).
```
