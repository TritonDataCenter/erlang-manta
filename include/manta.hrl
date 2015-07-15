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

-ifndef(MANTA_HRL).

-define(DEFAULT_ATTEMPTS,            3).
-define(DEFAULT_ATTEMPT_TIMEOUT,  2000). % timer:seconds(2)
-define(DEFAULT_CONNECT_TIMEOUT,  5000). % timer:seconds(5)
-define(DEFAULT_TIMEOUT,         60000). % timer:minutes(1)

-record(manta_config, {
	agent    = undefined :: undefined | binary(),
	key      = undefined :: undefined | public_key:private_key(),
	key_file = undefined :: undefined | file:filename(),
	key_id   = undefined :: undefined | binary(),
	role     = undefined :: undefined | binary(),
	subuser  = undefined :: undefined | binary(),
	url      = undefined :: undefined | http:url(),
	user     = undefined :: undefined | binary(),
	% Client
	attempts        = ?DEFAULT_ATTEMPTS        :: pos_integer(),
	attempt_timeout = ?DEFAULT_ATTEMPT_TIMEOUT :: timeout(),
	connect_timeout = ?DEFAULT_CONNECT_TIMEOUT :: timeout() | infinity,
	timeout         = ?DEFAULT_TIMEOUT         :: timeout() | infinity
}).

-define(MANTA_HRL, 1).

-endif.
