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
-module(manta_public_key).

-include_lib("public_key/include/public_key.hrl").

%% API exports
-export([decode/1]).
-export([decode_file/1]).
-export([encode/1]).
-export([fingerprint/1]).

-define(INLINE_INT2HEX(Int),
	case Int of
		0 -> $0;
		1 -> $1;
		2 -> $2;
		3 -> $3;
		4 -> $4;
		5 -> $5;
		6 -> $6;
		7 -> $7;
		8 -> $8;
		9 -> $9;
		10 -> $a;
		11 -> $b;
		12 -> $c;
		13 -> $d;
		14 -> $e;
		15 -> $f
	end).

-define(INLINE_BIN2HEX_BC(Bin),
	<< <<
		?INLINE_INT2HEX(I div 16),
		?INLINE_INT2HEX(I rem 16)
	>> || << I >> <= Bin >>).

-define(INLINE_FINGERPRINT_BC(Bin),
	<< <<
		$:,
		?INLINE_INT2HEX(I div 16),
		?INLINE_INT2HEX(I rem 16)
	>> || << I >> <= Bin >>).

%%====================================================================
%% API functions
%%====================================================================

decode(FileBin) ->
	case public_key:ssh_decode(FileBin, auth_keys) of
		[PublicKey | _] ->
			{ok, PublicKey};
		BadResult ->
			{error, BadResult}
	end.

decode_file(Filename) ->
	case file:read_file(Filename) of
		{ok, FileBin} ->
			decode(FileBin);
		ReadError ->
			ReadError
	end.

encode(PublicKey={{_, #'Dss-Parms'{}}, _}) ->
	public_key:ssh_encode([PublicKey], auth_keys);
encode(PublicKey={#'RSAPublicKey'{}, _}) ->
	public_key:ssh_encode([PublicKey], auth_keys);
encode({{KeyType, PublicKey}, Attributes})
		when KeyType =:= <<"ecdsa-sha2-nistp256">>
		orelse KeyType =:= <<"ecdsa-sha2-nistp384">>
		orelse KeyType =:= <<"ecdsa-sha2-nistp521">> ->
	Comment = proplists:get_value(comment, Attributes, ""),
	<<
		KeyType/binary, $\s,
		(base64:encode(PublicKey))/binary,
		(line_end(Comment))/binary
	>>.

fingerprint(PublicKey) ->
	Base64Key = extract_base64_key(encode(PublicKey)),
	<< $:, Fingerprint/binary >> = ?INLINE_FINGERPRINT_BC(crypto:hash(md5, base64:decode(Base64Key))),
	{ok, Fingerprint}.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private
extract_base64_key(<< $\s, Rest/binary >>) ->
	extract_base64_key(Rest, <<>>);
extract_base64_key(<< _, Rest/binary >>) ->
	extract_base64_key(Rest).

%% @private
extract_base64_key(<< $\n, _/binary >>, Key) ->
	Key;
extract_base64_key(<< $\s, _/binary >>, Key) ->
	Key;
extract_base64_key(<< C, Rest/binary >>, Key) ->
	extract_base64_key(Rest, << Key/binary, C >>).

%% @private
line_end("") ->
	<< $\n >>;
line_end(Comment) ->
	<< $\s, (iolist_to_binary(Comment))/binary, $\n >>.
