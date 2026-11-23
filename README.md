# Middleware Pub/Sub — Cenário C

Implementação em Go de um middleware distribuído Publish/Subscribe sobre UDP, voltado para interação ao vivo. A v1 usa semântica at-most-once, eventos efêmeros, filas limitadas por inscrito, topologia estática e réplica assíncrona do estado de controle.

## Requisitos

- Go 1.24 ou superior.

## Compilar e testar

```sh
go test ./...
go build ./...
```

## Executar o broker

```sh
go run ./cmd/broker -address 127.0.0.1:9000
```

Parâmetros úteis:

- `-address`: endereço UDP do broker.
- `-queue-messages`: máximo de mensagens por fila de inscrito.
- `-queue-bytes`: máximo de bytes por fila de inscrito.

## Executar o exemplo cliente/broker

Em um terminal:

```sh
go run ./cmd/broker -address 127.0.0.1:9000
```

Em outro terminal:

```sh
go run ./cmd/demo -broker 127.0.0.1:9000 -client demo-client
```

O demo cria o tópico `demo`, inscreve a sessão e publica `hello`. Os callbacks mostram entrega, GAP, tópico fechado, throttling e erros.

## Cenários de desempenho

```sh
go run ./cmd/bench -list
go run ./cmd/bench -scenario steady-fanout -json results/steady-fanout.json -csv results/steady-fanout.csv
go run ./cmd/bench -scenario burst-publisher -json results/burst-publisher.json -csv results/burst-publisher.csv
go run ./cmd/bench -scenario slow-subscriber -json results/slow-subscriber.json -csv results/slow-subscriber.csv
go run ./cmd/bench -scenario primary-failover -json results/primary-failover.json -csv results/primary-failover.csv
```

Os cenários são experimentos reproduzíveis em loopback/simulação. Os parâmetros e resultados versionados ficam em `results/`.

## Limites conhecidos da v1

- Transporte UDP com datagrama máximo de 1200 bytes.
- PUBLISH e DELIVER não usam retransmissão.
- Controle usa RequestID, timeout e reenvio limitado.
- Réplicas replicam tópicos, inscrições e sessões mínimas; eventos em trânsito podem ser perdidos.
- Sem anel físico de replay: RESUME informa GAP para sequências ausentes.
- Topologia e membership são estáticos.
- Não há autenticação, autorização ou criptografia na v1.

## Estrutura

- `api`: contrato público.
- `client`: SDK.
- `internal/wire`: protocolo binário v1.
- `internal/broker`: orquestração do broker.
- `internal/topic`, `session`, `queue`, `admission`, `degrade`, `topology`, `replication`, `transport`: implementação interna.
- `metrics`, `scenarios`: observabilidade e benchmarks.
- `cmd/broker`, `cmd/bench`, `cmd/demo`: executáveis.
