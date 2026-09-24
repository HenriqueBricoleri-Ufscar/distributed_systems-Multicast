# Exclusão Mútua Distribuída — Ricart & Agrawala

Incremento acadêmico de **Sistemas Distribuídos** que implementa o **algoritmo
de exclusão mútua distribuída de Ricart & Agrawala**, tomando como referência
a especificação apresentada por Andrew S. Tanenbaum e Maarten van Steen em
*Distributed Systems* (seção sobre exclusão mútua baseada em multicast e
relógios lógicos).

Esta etapa **reaproveita, por importação**, a infraestrutura já construída no
incremento anterior (multicast totalmente ordenado): o canal UDP com ordem
FIFO (`fifo.py`), os tipos de mensagem (`Message.py`), as constantes de rede
e as funções de (de)serialização (`process.py`), e a topologia Mininet
(`mininet_topology.py`). Nada disso foi duplicado — apenas estendido.

---

## 1. Objetivo

Implementar uma versão minimalista do algoritmo de Ricart & Agrawala entre
`p1`, `p2` e `p3`, com as seguintes premissas do enunciado:

- **toda requisição de acesso à seção crítica é sempre respondida**,
  concedendo o acesso imediatamente ou adiando a resposta — nunca ignorada;
- **não há falhas de processo nem de canal** (sem timeout, retransmissão ou
  detecção de falha);
- o cenário funciona para **3 processos**;
- existem **diferentes situações de teste** que provam o funcionamento do
  algoritmo (concorrência, desempate, ausência de starvation).

O servidor central apenas injeta comandos (`request`/`release`) em um
processo específico, do mesmo jeito que no projeto de multicast ele injetava
uma mensagem para iniciar um multicast. O servidor não participa da máquina
de estados do algoritmo.

---

## 2. Como o algoritmo funciona

Cada processo mantém três coisas, exatamente como no livro:

| Campo | Significado |
|---|---|
| `clock` | relógio lógico de Lamport (reaproveita `INITIAL_CLOCKS` do `process.py`: `p1=0`, `p2=5`, `p3=10`) |
| `state` | `RELEASED`, `WANTED` ou `HELD` |
| `request_timestamp` | timestamp do **meu** pedido corrente (só existe enquanto `WANTED`/`HELD`) |
| `pending_replies` | de quem eu ainda espero `REPLY` para poder entrar |
| `deferred` | lista de processos cujo `REQUEST` eu **adiei** e ainda preciso responder |

### 2.1. Pedindo a seção crítica (`request_cs`)

1. Incrementa o relógio e usa esse valor como `request_timestamp`.
2. Muda para `WANTED`.
3. Envia `RequestMessage(timestamp, meu_nome)` em broadcast para os outros
   dois processos (nunca para si mesmo — diferente do multicast, aqui não
   faz sentido pedir permissão a si próprio).
4. Bloqueia até receber `REPLY` dos dois outros.
5. Ao receber o último `REPLY` esperado, muda para `HELD` e entra na CS.

### 2.2. Recebendo um `REQUEST` de outro processo

Ao receber `RequestMessage(ts, origem)`, o relógio local é atualizado
(`max(clock, ts) + 1`) e a decisão de responder na hora ou adiar segue
**exatamente os três casos do livro**:

```text
se eu estou em HELD:
    adio a resposta (guardo "origem" em deferred)

senão, se eu estou em WANTED
   e (meu_request_timestamp, meu_nome) < (ts, origem):
    # meu pedido tem prioridade -> o pedido dele espera
    adio a resposta

senão (estou RELEASED, ou estou WANTED mas o pedido dele tem prioridade):
    respondo imediatamente com ReplyMessage
```

O desempate `(timestamp, nome)` garante uma ordem total mesmo quando dois
processos pedem com o **mesmo** timestamp lógico: quem tem o identificador
menor (`p1` < `p2` < `p3`) tem prioridade.

### 2.3. Liberando a seção crítica (`release_cs`)

1. Muda de `HELD` para `RELEASED`.
2. Envia um `ReplyMessage` para **cada** processo que ficou na lista
   `deferred`, na ordem em que foram adiados, e limpa a lista.

### 2.4. "Toda requisição é sempre respondida"

Esse é o requisito mais fácil de implementar errado, então vale destacar:
**nenhum `REQUEST` é descartado**. Ele gera exatamente um `REPLY`:

- imediatamente, se o receptor não tem prioridade sobre ele; ou
- mais tarde, quando o receptor libera a CS (passo 2.3).

Não existe um "não" explícito no protocolo (não há `DenyMessage`) — a forma
de "não conceder agora" é justamente adiar o `REPLY`, e o `REPLY` adiado
**sempre** sai depois, porque `release_cs` percorre `deferred` de forma
incondicional. Como o enunciado garante ausência de falhas de canal, essa
entrega garantida é suficiente — não é preciso timeout nem reenvio.

---

## 3. Mensagens (`Message.py`, na raiz do repositório)

Os três tipos abaixo foram **adicionados** ao `Message.py` já existente
(que continua com `DataMessage`/`AckMessage` do multicast, inalterados):

```python
@dataclass(frozen=True)
class RequestMessage:
    timestamp: int
    sender: str

@dataclass(frozen=True)
class ReplyMessage:
    timestamp: int
    sender: str

@dataclass(frozen=True)
class TriggerMessage:
    action: str          # "REQUEST" ou "RELEASE"
    timestamp: int
    sender: str = "server"
```

`TriggerMessage` não faz parte do algoritmo de Ricart & Agrawala em si — é
só o mecanismo (igual ao usado no multicast) para o servidor provocar, de
fora, o início de um pedido ou de uma liberação em um processo específico.

`process.py` da raiz também foi estendido: `serialize_packet` e
`deserialize_packet` agora reconhecem esses três tipos novos além de
`DataMessage`/`AckMessage`. O `Process` do multicast nunca produz os tipos
novos, então esse incremento não muda o comportamento do multicast.

---

## 4. Estrutura de arquivos

```text
distributed_systems-Multicast/
├── Message.py                 # (raiz) estendido com Request/Reply/Trigger
├── fifo.py                    # (raiz) reaproveitado sem nenhuma alteração
├── process.py                 # (raiz) estendido: serialize/deserialize reconhecem os novos tipos
├── mininet_topology.py        # (raiz) build_network() ganhou parâmetros de script opcionais
├── command.py                 # (raiz) reaproveitado sem nenhuma alteração
└── exclusao/
    ├── README.md               # este arquivo
    ├── process.py              # MutexProcess: máquina de estados de Ricart & Agrawala
    ├── server.py                # injeta TriggerMessage (request/release) via canal de controle
    ├── topology.py               # chama build_network() apontando para os scripts acima
    └── tests/
        └── test_mutex.py          # 5 cenários determinísticos (ver seção 6)
```

Nenhum arquivo da pasta `exclusao/` reimplementa `FifoChannel`, os tipos de
mensagem já existentes ou a topologia Mininet — todos são importados por
caminho explícito (ver seção 5.1) para evitar duplicação.

### 4.1. Descrição de cada arquivo novo

**`exclusao/process.py` — `MutexProcess`**
Classe análoga ao `Process` do multicast, mas com a máquina de estados de
Ricart & Agrawala em vez da fila de entrega ordenada. Reaproveita:
- `FifoChannel` (de `fifo.py`, sem alterações);
- `PORT`, `PEERS`, `INITIAL_CLOCKS`, `serialize_packet`, `deserialize_packet`
  (de `process.py` da raiz).

Também aceita um parâmetro opcional `transport`, usado só nos testes (ver
seção 6) para trocar o socket UDP real por uma função em memória, sem mudar
uma linha da lógica do algoritmo.

**`exclusao/server.py`**
Reaproveita `SERVER_IP`, `SERVER_PORT`, `CONTROL_IP`, `CONTROL_PORT` e a
tabela `processes` do `server.py` da raiz. Adiciona só a lógica de
interpretar dois comandos no canal de controle (`127.0.0.1:9000`):

```text
request <processo>
release <processo>
```

Cada comando vira um `TriggerMessage` enviado ao processo indicado, que ao
recebê-lo dispara `request_cs()` ou `release_cs()` em uma thread separada
(sem bloquear o laço principal de recepção de pacotes do processo).

**`exclusao/topology.py`**
Não recria a topologia: importa `build_network()` de `mininet_topology.py`
da raiz e só passa os caminhos dos scripts novos:

```python
build_network(server_script="exclusao/server.py", process_script="exclusao/process.py")
```

---

## 5. Como executar

### 5.1. Pré-requisitos

Os mesmos do projeto de multicast (ver README da raiz, seção 2): Linux com
suporte a network namespaces/veth, Mininet, Open vSwitch e Python 3.12+.
Nenhuma dependência nova foi introduzida.

### 5.2. Execução real na mini-rede (Mininet)

A partir da **raiz do repositório** (importante: os caminhos dos scripts em
`exclusao/topology.py` são relativos à raiz):

```bash
sudo mn -c
sudo python3 exclusao/topology.py
```

Saída esperada (semelhante à do multicast, trocando o nome do script):

```text
*** Starting exclusao/server.py
*** Starting p1 in 10.0.0.2
*** Starting p2 in 10.0.0.3
*** Starting p3 in 10.0.0.4
*** Network ready!!
mininet>
```

### 5.3. Testando request/release

O `command.py` da raiz **não precisa de nenhuma alteração** — ele só
encaminha texto cru para o canal de controle em `127.0.0.1:9000`, então
serve tanto para `send p1 ...` (multicast) quanto para `request p1` /
`release p1` (exclusão mútua), dependendo de qual `server.py` estiver
rodando na topologia ativa.

**Cenário simples — um processo pede e libera:**

```text
mininet> h1 python3 command.py request p1
mininet> h2 cat logs/process1.log
mininet> h1 python3 command.py release p1
```

No log de `p1` você deve ver, em sequência:

```text
[p1] REQUEST CS clock=1 state=WANTED timestamp=1
[p1] SEND REQUEST to=p2 timestamp=1
[p1] SEND REQUEST to=p3 timestamp=1
[p1] RECEIVE REPLY from=p2 ...
[p1] RECEIVE REPLY from=p3 ...
[p1] ENTER CS ...
[p1] HOLDING CS ... (aguardando comando 'release')
[p1] EXIT CS ...
```

**Cenário de disputa — dois processos pedem antes de o primeiro liberar:**

```text
mininet> h1 python3 command.py request p1
mininet> h1 python3 command.py request p2
mininet> h1 python3 command.py release p1
mininet> h3 cat logs/process2.log
```

No log de `p2` deve aparecer um `DEFER REPLY to=p1` (ou o inverso, a
depender de quem tinha o timestamp menor) seguido, só depois do `release`
de quem estava segurando a CS, por `ENTER CS`.

**Cenário dos três processos disputando ao mesmo tempo:**

```text
mininet> h1 python3 command.py request p1
mininet> h1 python3 command.py request p2
mininet> h1 python3 command.py request p3
mininet> h2 grep -E 'REQUEST|REPLY|DEFER|ENTER|EXIT' logs/process1.log
mininet> h3 grep -E 'REQUEST|REPLY|DEFER|ENTER|EXIT' logs/process2.log
mininet> h4 grep -E 'REQUEST|REPLY|DEFER|ENTER|EXIT' logs/process3.log
```

Nesse ponto, todos os três ficam bloqueados esperando `REPLY`. Você precisa
ir liberando um de cada vez para destravar a fila:

```text
mininet> h1 python3 command.py release p1
mininet> h1 python3 command.py release p2
mininet> h1 python3 command.py release p3
```

A ordem de entrada na CS (`ENTER CS`) deve seguir a ordem dos timestamps
lógicos de cada `REQUEST` — não necessariamente a ordem em que os comandos
`request` foram digitados, já que o relógio de cada processo parte de um
valor inicial diferente (`p1=0`, `p2=5`, `p3=10`).

### 5.4. Encerrando

```text
mininet> exit
```
```bash
sudo mn -c
```

---

## 6. Testes automáticos

```bash
python3 -B -m unittest exclusao.tests.test_mutex -v
```

Execute a partir da **raiz do repositório**. Não precisa de Mininet, `sudo`
nem sockets reais: os testes usam um "transporte" em memória (`Network`,
definido dentro do próprio arquivo de teste) que chama `handle_packet`
diretamente na ordem escolhida pelo teste — a mesma função usada em
produção, só sem a variabilidade de tempo de uma rede real.

Cinco cenários são cobertos:

| Teste | O que prova |
|---|---|
| `test_single_request_no_contention` | Sem disputa, o pedido é concedido de imediato pelos dois outros processos. |
| `test_concurrent_requests_priority_by_timestamp` | Dois pedidos concorrentes com timestamps diferentes: quem tem o timestamp menor entra primeiro; o outro é adiado e só entra depois do `release_cs()` do primeiro. |
| `test_tie_break_by_process_id` | Dois pedidos com o **mesmo** timestamp: o desempate por `(timestamp, nome)` é decidido corretamente pelo identificador do processo. |
| `test_three_way_concurrent_no_starvation` | Os três processos pedem "ao mesmo tempo": a ordem de entrada segue estritamente `(timestamp, id)` e cada processo adiado eventualmente entra (sem starvation) conforme os anteriores liberam. |
| `test_every_request_is_always_eventually_replied` | Um `REQUEST` recebido enquanto o destinatário está em `HELD` é adiado, não descartado — a contagem de `REPLY`s enviados confirma que a resposta que faltava sai exatamente no `release_cs()`. |

Saída esperada:

```text
Ran 5 tests in 0.00Xs

OK
```

---

## 7. Limitações conhecidas (por design)

Coerente com o enunciado ("versão minimalista", "não há falhas no sistema"):

- **sem detecção de falha, timeout ou retransmissão** — se um pacote se
  perdesse, o algoritmo travaria; isso é aceitável porque o enunciado
  garante ausência de falhas de canal;
- **um pedido por vez por processo** — chamar `request` duas vezes no
  mesmo processo sem um `release` no meio sobrescreve o pedido anterior;
  não há fila de pedidos locais, pois o enunciado não exige isso;
- **três processos fixos** (`p1`, `p2`, `p3`), assim como no projeto de
  multicast — não é um requisito da atividade generalizar para N processos.
