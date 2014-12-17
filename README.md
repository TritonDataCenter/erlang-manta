manta
=====

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
	{user, <<"bar">>}
]).
```
