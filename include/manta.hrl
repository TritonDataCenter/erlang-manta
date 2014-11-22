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

-ifndef(MANTA_HRL).

-record(manta_config, {
	agent    = undefined :: undefined | binary(),
	key      = undefined :: undefined | public_key:private_key(),
	key_file = undefined :: undefined | file:filename(),
	key_id   = undefined :: undefined | binary(),
	subuser  = undefined :: undefined | string(),
	url      = undefined :: undefined | http:url(),
	user     = undefined :: undefined | string()
}).

-define(DEFAULT_TIMEOUT, 60000). % timer:minutes(1)

-define(MANTA_HRL, 1).

-endif.
