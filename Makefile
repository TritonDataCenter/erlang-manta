.PHONY: deps compile rel

REBAR3_URL=https://s3.amazonaws.com/rebar3/rebar3

# Default to rebar on PATH
REBAR3 ?= $(shell which rebar3)

# If no rebar3 on PATH, then try the current directory
ifeq ($(REBAR3),)
ifeq ($(wildcard rebar3),rebar3)
REBAR3 = $(CURDIR)/rebar3
endif
endif

DIALYZER_APPS = kernel stdlib erts sasl eunit syntax_tools compiler crypto
DEP_DIR = "_build/lib"
SHORTSHA = `git rev-parse --short HEAD`
PKGOS ?= $(shell uname -s)

ifeq ($(PKGOS),SunOS)
  PKGOS=sunos
  PKG_NAME_VER := $(SHORTSHA)-$(PKGOS)
else
  PKG_NAME_VER := $(SHORTSHA)
endif

all: compile

cover: test
	${REBAR3} cover

compile: deps
	${REBAR3} compile

clean:
	${REBAR3} clean

rel: compile
	${REBAR3} release

stage: compile
	${REBAR3} release -d

test: ct

ct:
	${REBAR3} ct

docs:
	${REBAR3} doc skip_deps=true

xref: compile
	${REBAR3} xref skip_deps=true

dialyzer: compile
	${REBAR3} dialyzer

package: compile
	@rm -rf _build/prod/rel
	${REBAR3} as prod release
	@cd _build/prod/rel && mv erlang-manta erlang-manta-${SHORTSHA} && tar -czf erlang-manta-${PKG_NAME_VER}.tgz erlang-manta-${SHORTSHA}
	@cp _build/prod/rel/erlang-manta-${PKG_NAME_VER}.tgz .
	@echo "package is erlang-manta-${PKG_NAME_VER}.tgz"

package-clean:
	@rm -f erlang-manta-*.tgz
