PROJECT = manta
COMPILE_FIRST = manta_handler
DEPS = cowlib dlhttpc jsx
include erlang.mk

otp_release = $(shell erl -noshell -eval 'io:fwrite("~s\n", [erlang:system_info(otp_release)]).' -s erlang halt)
otp_ge_17 = $(shell echo $(otp_release) | grep -q -E "^[[:digit:]]+$$" && echo true)
ifeq ($(otp_ge_17),true)
	otp_ge_18 = $(shell [ $(otp_release) -ge "18" ] && echo true)
endif

ifeq ($(otp_ge_18),true)
	ERLC_OPTS += -Dtime_correction=1
	TEST_ERLC_OPTS += -Dtime_correction=1
endif
