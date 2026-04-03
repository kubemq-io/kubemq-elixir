FROM elixir:1.17-otp-27-slim AS build

WORKDIR /app

RUN mix local.hex --force && mix local.rebar --force

COPY ../mix.exs ../mix.lock /app/sdk/
COPY ../lib /app/sdk/lib

COPY mix.exs /app/burnin/
WORKDIR /app/burnin

ENV MIX_ENV=prod
RUN sed -i 's|path: "../"|path: "/app/sdk"|' mix.exs

RUN mix deps.get --only prod
RUN mix compile

FROM elixir:1.17-otp-27-slim AS runtime

WORKDIR /app

RUN mix local.hex --force

COPY --from=build /app/burnin/_build/prod /app/_build/prod
COPY --from=build /app/burnin/deps /app/deps
COPY --from=build /app/burnin/mix.exs /app/
COPY --from=build /app/burnin/lib /app/lib
COPY --from=build /app/burnin/config /app/config
COPY --from=build /app/sdk /app/sdk

ENV MIX_ENV=prod
ENV BURNIN_PORT=8888
ENV KUBEMQ_BROKER_ADDRESS=localhost:50000

EXPOSE 8888

CMD ["mix", "run", "--no-halt"]
