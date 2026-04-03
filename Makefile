.PHONY: deps compile test test.all test.integration lint format dialyzer docs coverage proto clean

deps:
	mix deps.get

compile: deps
	mix compile

test: compile
	mix test

test.all: compile
	mix test --include integration

test.integration: compile
	mix test --only integration

lint: compile
	mix format --check-formatted
	mix credo --strict
	mix dialyzer

format:
	mix format

dialyzer: compile
	mix dialyzer

docs: compile
	mix docs

coverage: compile
	mix coveralls

proto:
	protoc \
		--elixir_out=plugins=grpc,one_file_per_module=true:lib/kubemq/proto \
		--proto_path=../../ \
		../../kubemq.proto

clean:
	rm -rf _build deps doc cover
