# Contexto Atual e Backlog Ordenado de Execucao

Este documento complementa os arquivos de planejamento ja existentes em `docs/Guia_Desenvolvimento_Middleware_PubSub_Go.docx` e `docs/Estrutura_Projeto_e_Plano_Implementacao_PubSub_Go.docx`.

Objetivo: registrar o estado real do projeto no ponto atual e definir uma trilha unica, ordenada e verificavel para concluir o desenvolvimento do middleware Pub/Sub.

---

## 1. Contexto Atual do Projeto

### 1.1 Escopo do produto (v1)

- Middleware Pub/Sub distribuido para cenario de interacao ao vivo.
- Transporte UDP com datagramas pequenos.
- Semantica de dados: at-most-once.
- Mensagens efemeras, sem persistencia historica.
- Perdas observaveis via GAP.
- Isolamento de cliente lento por fila dedicada com limites.

### 1.2 O que ja esta implementado

- `api`:
  - Tipos publicos basicos (`Topic`, `ClientID`, `Message`, `PublishResult`, `Gap`).
  - Contrato de callbacks e opcoes do cliente (`DeliverCallback`, `GapCallback`, `Options`).
  - Codigos de erro iniciais (`ErrorCode`) e tipo `Error`.
- `internal/wire`:
  - Header binario v1, validacoes e encode/decode de datagrama.
  - Enumeracao de tipos de mensagem e codec completo dos corpos v1.
  - Inclui CONTROL_ACK para confirmacao idempotente de controle.
- `internal/queue`:
  - Fila bounded por inscrito (mensagens/bytes), `drop-oldest`, coalescencia de GAP.
- `internal/topic`:
  - Registry de topicos e inscricoes com semantica idempotente.
- `internal/transport`:
  - Wrapper UDP para send/receive, validacao de budget e close seguro.
- `internal/session`:
  - Gerenciamento de sessao em memoria e resolucao por endereco UDP.
- `internal/broker`:
  - Broker single-node minimo com fanout, PUB_RESULT, DELIVER/GAP e CONTROL_ACK.
  - Cache de respostas por endereco, tipo de mensagem e RequestID para replay seguro.
- `client`:
  - SDK público com Connect, Publish, Subscribe, Unsubscribe, CreateTopic, DeleteTopic e Close.
  - Receive loop separado, callbacks assíncronos, controle com retry e deduplicação de sequência por tópico.
- `cmd/broker`:
  - Processo UDP mínimo que conecta transport, broker e socket real.
- `internal/admission`:
  - Token bucket por publicador e DRR de ingresso.
- `internal/degrade`:
  - Estados normal, pressao e lento persistente por fila de inscricao.

### 1.3 Validacao do baseline

- Testes atuais passando para os modulos implementados:
  - `api`
  - `internal/wire`
  - `internal/queue`
  - `internal/topic`
  - `internal/transport`
  - `internal/broker`
  - `internal/session`
  - `client`
  - `cmd/broker`

### 1.4 Lacunas principais no ponto atual

1. Falta topologia particionada, redirect e failover.
2. Falta suite de benchmarks e evidencias finais.

---

## 2. Principios de Execucao para Concluir o Projeto

- Implementar em ordem de dependencia tecnica, evitando retrabalho.
- Cada fase precisa de criterio claro de entrada e saida.
- Nao avancar sem testes da fase anterior no verde.
- Preservar desacoplamento:
  - `api` e contrato publico estavel.
  - `internal/*` sem vazamento de detalhes para aplicacao.
- Metricas e observabilidade entram cedo o suficiente para guiar tuning.

---

## 3. Backlog Ordenado de Desenvolvimento (Fim a Fim)

## Fase A - Fechar Protocolo Wire (Corpos)

### Objetivo

Completar encode/decode dos corpos das mensagens v1 para destravar broker e cliente.

### Dependencias

- Header wire pronto.
- Tipos de mensagem definidos.

### Tarefas

1. Definir structs de corpo por tipo de mensagem (HELLO, WELCOME, CREATE_TOPIC, DELETE_TOPIC, SUBSCRIBE, UNSUBSCRIBE, PUBLISH, PUB_RESULT, DELIVER, GAP, RESUME, THROTTLE, REDIRECT, HEARTBEAT, TOPIC_CLOSED, ERROR).
2. Implementar validacao de campos obrigatorios e limites de tamanho.
3. Implementar encode/decode deterministico por mensagem.
4. Definir mapeamento consistente entre erros wire e `api.ErrorCode`.
5. Cobrir testes de roundtrip e rejeicao de payload invalido.

### Criterio de saida

- Todo tipo de mensagem com codec coberto por teste de roundtrip.
- Testes de erro para: versao invalida, tipo invalido, corpo malformado, tamanho excedido.

---

## Fase B - Consolidar Contrato Publico da API

### Objetivo

Fechar API publica para uso real da aplicacao, sem depender de detalhes internos.

### Dependencias

- Fase A concluida.

### Tarefas

1. Definir tipos publicos faltantes para operacoes de controle:
   - Connect/Resume
   - CreateTopic/DeleteTopic
   - Subscribe/Unsubscribe
   - Publish
2. Definir eventos de callback do cliente:
   - Deliver
   - Gap
   - TopicClosed
   - Throttle
   - Error
3. Definir opcoes publicas de timeout/retry para plano de controle.
4. Revisar nomenclatura e semantica para manter estabilidade da API.
5. Adicionar testes de contrato em `api`.

### Criterio de saida

- API publica com contratos minimamente completos para broker+client.
- Sem mudancas breaking planejadas para fases seguintes.

---

## Fase C - Broker Single-Node Minimo Funcional

### Objetivo

Subir broker local com fluxo completo de controle e dados para um no.

### Dependencias

- Fases A e B concluidas.
- Queue/topic/transport reutilizados.

### Tarefas

1. Criar modulo de sessao (`internal/session`):
   - abertura de sessao (HELLO/WELCOME),
   - identidade de cliente,
   - role.
2. Criar orquestrador do broker (`internal/broker`):
   - receive datagram,
   - decode wire,
   - dispatch por tipo.
3. Implementar fluxo de topicos e inscricoes:
   - CREATE_TOPIC, DELETE_TOPIC,
   - SUBSCRIBE, UNSUBSCRIBE.
4. Implementar fluxo de publicacao:
   - validacao,
   - sequencia monotona por topico,
   - fanout para filas dos inscritos,
   - resposta PUB_RESULT.
5. Implementar entrega:
   - worker de envio por inscrito,
   - DELIVER e GAP seguindo ordem da queue.

### Criterio de saida

- Dois clientes conseguem receber evento do mesmo topico.
- Publicacao em topico inexistente e em topico sem inscritos retornam resultados distintos.
- Cliente lento nao bloqueia outros (isolation por fila).

---

## Fase D - Idempotencia e Confiabilidade de Controle

### Objetivo

Tornar operacoes de controle seguras para retry sem duplicacao de efeito.

### Dependencias

- Broker single-node operacional.

### Tarefas

1. Introduzir `RequestID` no plano de controle.
2. Cache de deduplicacao por sessao para requests de controle.
3. Timeout e retry limitado no cliente para mensagens de controle.
4. Garantir que data plane (PUBLISH/DELIVER) nao use retransmissao automatica.

### Criterio de saida

- Reenvio de controle nao gera efeitos duplicados.
- Sem regressao da semantica at-most-once no data plane.

### Status da Fase D

**Concluida no estado atual:** controle com CONTROL_ACK, cache de replay por RequestID, e helper UDP com timeout/reenvio limitado. O PUBLISH continua at-most-once e nao usa retransmissao.

---

## Fase E - Biblioteca Cliente (SDK) v1

### Objetivo

Entregar API de uso da aplicacao com ciclo de vida completo.

### Dependencias

- Fases C e D concluidas.

### Tarefas

1. Implementar API publica:
   - Connect
   - Publish
   - Subscribe
   - Unsubscribe
   - CreateTopic
   - DeleteTopic
   - Close
2. Encapsular topologia conhecida sem expor complexidade para aplicacao.
3. Executar callbacks em goroutine separada do receive loop.
4. Implementar deduplicacao e deteccao de lacuna por sequencia no cliente.
5. Garantir shutdown limpo e deterministico.

### Criterio de saida

- Aplicacao de exemplo publica e consome sem conhecer detalhes do broker.
- Callback lento da aplicacao nao trava recepcao de rede.

### Status da Fase E

**Concluida no estado atual:** API pública de SDK, receive loop dedicado, callbacks assíncronos, operações de controle com RequestID, publicação at-most-once com PUB_RESULT, deduplicação/deteção de lacunas e integração loopback com o broker. Falta apenas ampliar o ciclo de vida com reconexão/RESUME, que pertence às fases futuras de disponibilidade.

---

## Fase F - Admission Control e Degradacao

### Objetivo

Proteger o sistema sob rajada e assinantes lentos mantendo previsibilidade.

### Dependencias

- Fluxo basico broker+cliente funcionando.

### Tarefas

1. Implementar token bucket por publicador.
2. Implementar DRR (ou politica equivalente definida) para justica entre publicadores.
3. Emitir THROTTLE para excesso de taxa.
4. Introduzir nivel de degradacao por sessao (normal, pressao, lento persistente).
5. Definir politica de corte/desconexao de sessao lenta persistente.
6. Expor metricas de fila, descarte e throttling.

### Criterio de saida

- Publicador em rajada e limitado sem colapsar o broker.
- Inscrito lento degrada apenas a propria sessao.

### Status da Fase F

**Concluida no estado atual:** token bucket por publicador, DRR de ingresso, resposta THROTTLE, monitor normal/pressao/persistente para filas, desconexao logica de inscricao persistente lenta e snapshot basico de metricas de admissao e perda.

---

## Fase G - Particoes e Topologia Estatica

### Objetivo

Evoluir de no unico para roteamento por particao.

### Dependencias

- Fases anteriores estaveis.

### Tarefas

1. Implementar modulo de topologia estatica (particao, primario, replica, epoch).
2. Hash consistente para resolucao de topico -> particao.
3. Suporte a REDIRECT quando cliente envia para broker errado.
4. Cliente atualiza rota sem quebrar API de aplicacao.

### Criterio de saida

- Topicos distintos roteados ao primario correto.
- Redirect transparente para a aplicacao.

---

## Fase H - Replica de Controle e Failover

### Objetivo

Garantir continuidade sob falha de primario com perda observavel.

### Dependencias

- Topologia/particoes concluida.

### Tarefas

1. Heartbeat entre primario e replica.
2. Promocao com incremento de epoch.
3. Replica assincrona do estado de controle:
   - topicos,
   - inscricoes,
   - sessoes minimas necessarias.
4. Implementar RESUME no cliente com ultimas sequencias vistas.
5. Tratar intervalo ausente com GAP quando nao houver dados para retomada.

### Criterio de saida

- Queda de primario com retomada de sessao no novo primario.
- Sem duplicacao indevida apos failover.
- Perda eventual sinalizada via GAP.

---

## Fase I - Observabilidade, Bench e Evidencias Finais

### Objetivo

Produzir evidencia tecnica de comportamento no cenario C.

### Dependencias

- Fluxo funcional completo (incluindo failover).

### Tarefas

1. Consolidar metricas:
   - latencia p50/p99/p99.9,
   - vazao,
   - perda por causa,
   - filas,
   - throttling,
   - tempo de recuperacao,
   - indice de Jain.
2. Criar cenarios de teste de sistema:
   - steady fanout,
   - burst publisher,
   - slow subscriber,
   - primary failover.
3. Implementar gerador de carga e execucao repetivel.
4. Gerar resultados em formato versionavel (CSV/JSON/relatorio).

### Criterio de saida

- Evidencias reproduziveis do comportamento esperado sob carga e falha.
- Parametros criticos calibrados e documentados.

---

## Fase J - Fechamento de Release v1

### Objetivo

Finalizar o desenvolvimento com criterio de pronto de produto.

### Tarefas

1. Revisar compatibilidade e estabilidade da API publica.
2. Revisar backward compatibility do protocolo v1.
3. Padronizar logs e erros para operacao.
4. Atualizar documentacao de uso:
   - guia rapido,
   - exemplo cliente-broker,
   - configuracao de cluster estatico,
   - limites conhecidos.
5. Executar regressao final (unitarios + integracao + cenarios).

### Criterio de saida (Definition of Done da v1)

- Funcionalidades essenciais entregues:
  - broker distribuido com continuidade basica,
  - API cliente utilizavel,
  - isolamento de lentidao,
  - perda observavel.
- Suite de testes e cenarios no verde.
- Evidencias de desempenho e recuperacao publicadas.
- Documentacao suficiente para operar e evoluir a versao.

---

## 4. Ordem Recomendada de Execucao Curta (Checklist)

Use este checklist como trilha rapida de acompanhamento:

1. Wire bodies completos e testados.
2. Contrato da API publica fechado.
3. Broker single-node funcional.
4. Idempotencia de controle com retry seguro.
5. SDK cliente funcional e estavel.
6. Admission + degradacao.
7. Particoes + redirect.
8. Replica de controle + failover + resume.
9. Benchmarks, metricas e evidencias.
10. Fechamento de release v1.

---

## 5. Riscos e Mitigacoes para as Proximas Fases

- Risco: quebrar API publica durante evolucao interna.
  - Mitigacao: congelar contratos da Fase B e proteger com testes de contrato.
- Risco: retrabalho no broker por codec incompleto.
  - Mitigacao: concluir Fase A antes de expandir orquestracao.
- Risco: acoplamento entre modulos internos.
  - Mitigacao: interfaces pequenas definidas no consumidor.
- Risco: tuning sem visibilidade.
  - Mitigacao: instrumentar metricas antes de calibrar limites.

---

## 6. Proxima Acao Imediata

Iniciar pela Fase G (Particoes e Topologia Estatica):

- implementar topologia estatica de particoes,
- definir primario, replica e epoch por particao,
- calcular particao por hash consistente de topico,
- implementar REDIRECT,
- atualizar rota no cliente.
