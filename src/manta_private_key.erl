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
-module(manta_private_key).

-include_lib("public_key/include/public_key.hrl").

%% API exports
-export([decode/1]).
-export([decode/2]).
-export([decode_file/1]).
-export([decode_file/2]).
-export([encode/1]).
-export([encode/2]).
-export([fingerprint/1]).
-export([public/1]).
-export([sign/2]).
-export([sign_algorithm/1]).

%%====================================================================
%% API functions
%%====================================================================

decode(FileBin) ->
        case public_key:pem_decode(FileBin) of
                [PemEntry] ->
                        {ok, public_key:pem_entry_decode(PemEntry)};
                BadResult ->
                        {error, BadResult}
        end.

decode(FileBin, Password) ->
        case public_key:pem_decode(FileBin) of
                [PemEntry] ->
                        {ok, public_key:pem_entry_decode(PemEntry, Password)};
                BadResult ->
                        {error, BadResult}
        end.

decode_file(Filename) ->
        case read_file(Filename) of
                {ok, PemEntry} ->
                        {ok, public_key:pem_entry_decode(PemEntry)};
                OpenError ->
                        OpenError
        end.

decode_file(Filename, Password) ->
        case read_file(Filename) of
                {ok, PemEntry} ->
                        {ok, public_key:pem_entry_decode(PemEntry, Password)};
                OpenError ->
                        OpenError
        end.

encode(DSAPrivateKey=#'DSAPrivateKey'{}) ->
        PemEntry = public_key:pem_entry_encode('DSAPrivateKey', DSAPrivateKey),
        public_key:pem_encode([PemEntry]);
encode(ECPrivateKey=#'ECPrivateKey'{}) ->
        PemEntry = public_key:pem_entry_encode('ECPrivateKey', ECPrivateKey),
        public_key:pem_encode([PemEntry]);
encode(RSAPrivateKey=#'RSAPrivateKey'{}) ->
        PemEntry = public_key:pem_entry_encode('RSAPrivateKey', RSAPrivateKey),
        public_key:pem_encode([PemEntry]).

encode(DSAPrivateKey=#'DSAPrivateKey'{}, Password) ->
        PemEntry = public_key:pem_entry_encode('DSAPrivateKey', DSAPrivateKey, {{"DES-EDE3-CBC", crypto:strong_rand_bytes(8)}, Password}),
        public_key:pem_encode([PemEntry]);
encode(ECPrivateKey=#'ECPrivateKey'{}, Password) ->
        PemEntry = public_key:pem_entry_encode('ECPrivateKey', ECPrivateKey, {{"DES-EDE3-CBC", crypto:strong_rand_bytes(8)}, Password}),
        public_key:pem_encode([PemEntry]);
encode(RSAPrivateKey=#'RSAPrivateKey'{}, Password) ->
        PemEntry = public_key:pem_entry_encode('RSAPrivateKey', RSAPrivateKey, {{"DES-EDE3-CBC", crypto:strong_rand_bytes(8)}, Password}),
        public_key:pem_encode([PemEntry]).

fingerprint(PrivateKey) ->
        case public(PrivateKey) of
                {ok, PublicKey} ->
                        manta_public_key:fingerprint(PublicKey);
                PublicError ->
                        PublicError
        end.

public(#'DSAPrivateKey'{y=Y, p=P, q=Q, g=G}) ->
        DSAPublicKey = {Y, #'Dss-Parms'{p=P, q=Q, g=G}},
        {ok, {DSAPublicKey, []}};
public(#'ECPrivateKey'{parameters={namedCurve, Parameters}, publicKey={_, PublicKey}}) ->
        {SignatureAlgorithm, DomainParameters} = case pubkey_cert_records:namedCurves(Parameters) of
                secp256r1 ->
                        {<<"ecdsa-sha2-nistp256">>, <<"nistp256">>};
                secp384r1 ->
                        {<<"ecdsa-sha2-nistp384">>, <<"nistp384">>};
                secp521r1 ->
                        {<<"ecdsa-sha2-nistp521">>, <<"nistp521">>}
        end,
        SignatureAlgorithmSize = byte_size(SignatureAlgorithm),
        DomainParametersSize = byte_size(DomainParameters),
        PublicKeySize = byte_size(PublicKey),
        ECPublicKey = {SignatureAlgorithm, <<
                SignatureAlgorithmSize:32/big-unsigned-integer,
                SignatureAlgorithm:SignatureAlgorithmSize/binary,
                DomainParametersSize:32/big-unsigned-integer,
                DomainParameters:DomainParametersSize/binary,
                PublicKeySize:32/big-unsigned-integer,
                PublicKey:PublicKeySize/binary
        >>},
        {ok, {ECPublicKey, []}};
public(#'RSAPrivateKey'{modulus=Modulus, publicExponent=PublicExponent}) ->
        RSAPublicKey = #'RSAPublicKey'{modulus=Modulus, publicExponent=PublicExponent},
        {ok, {RSAPublicKey, []}}.

sign(Message, DSAPrivateKey=#'DSAPrivateKey'{}) ->
        public_key:sign(Message, sha, DSAPrivateKey);
sign(Message, ECPrivateKey=#'ECPrivateKey'{}) ->
        public_key:sign(Message, sha256, ECPrivateKey);
sign(Message, RSAPrivateKey=#'RSAPrivateKey'{}) ->
        public_key:sign(Message, sha256, RSAPrivateKey).

sign_algorithm(#'DSAPrivateKey'{}) ->
        <<"dsa-sha">>;
sign_algorithm(#'ECPrivateKey'{}) ->
        <<"ecdsa-sha256">>;
sign_algorithm(#'RSAPrivateKey'{}) ->
        <<"rsa-sha256">>.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private
read_file(Filename) ->
        case file:read_file(Filename) of
                {ok, FileBin} ->
                        case public_key:pem_decode(FileBin) of
                                [PemEntry] ->
                                        {ok, PemEntry};
                                BadResult ->
                                        {error, BadResult}
                        end;
                ReadError ->
                        ReadError
        end.
