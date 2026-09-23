# Totally Ordered Multicast

Projeto acadêmico de **Sistemas Distribuídos** cujo objetivo é implementar um mecanismo de **multicast totalmente ordenado (Totally Ordered Multicast)**, 
tomando como referência os conceitos apresentados por Andrew S. Tanenbaum e Maarten van Steen em *Distributed Systems*.

O projeto utiliza **UDP** para a troca de mensagens entre processos e **Mininet** para emular a rede distribuída em uma única máquina Linux.

---

## 1. Objetivo

Este incremento implementa multicast totalmente ordenado entre `p1`, `p2` e `p3`,
seguindo o algoritmo de Lamport apresentado na seção 5.2.1 de *Distributed Systems*.
A arquitetura possui:

- um **servidor central**, usado para injetar mensagens no experimento;
- três **processos distribuídos**;
- comunicação por **UDP**;
- multicast entre os processos;
- timestamps lógicos;

O servidor apenas injeta uma solicitação no processo escolhido. Esse processo
origina o multicast, e os três participantes trocam as confirmações automaticamente.
O servidor não participa da fila de ordenação nem da contagem de ACKs.

---

## 2. Requisitos de sistema

### Sistema operacional

O Mininet depende de recursos do kernel Linux, portanto o ambiente recomendado é:

- Linux nativo;
- máquina virtual Linux; ou
- imagem oficial do Mininet.

### Dependências

São necessários:

- suporte a **network namespaces**;
- suporte a interfaces virtuais **veth**;
- privilégios de `root`;
- Python 3.12+ para este projeto;
- Mininet;
- Open vSwitch ou outro switch compatível.

O Mininet precisa ser executado com privilégios administrativos.

### 2.1 Instalação de dependências 

#### Ubuntu / Debian

Em distribuições Debian/Ubuntu recentes:

```bash
sudo apt update
sudo apt install mininet openvswitch-switch
```

Inicie o Open vSwitch, se necessário:

```bash
sudo systemctl enable --now openvswitch-switch
```

Teste:

```bash
sudo mn --test pingall
```

Outra opção é instalar o Mininet a partir do código-fonte seguindo a documentação oficial:

```text
https://github.com/mininet/mininet
https://mininet.org/download/
```

---

## 3. Como executar
### 3.1 Validar o Mininet

Antes de executar o projeto:
```bash
sudo mn --test pingall
```
O resultado esperado é:
```text
*** Results: 0% dropped
```

Antes de iniciar uma nova topologia:
```bash
sudo mn -c
```
Isso remove interfaces, namespaces e processos remanescentes de execuções anteriores.

### 3.2 Iniciar a topologia

Execute:

```bash
sudo python3 mininet_topology.py
```

Os logs ficam em `logs/` e são limpos a cada execução. A pasta e os arquivos
pertencem ao usuário que chamou `sudo`, identificado por `SUDO_UID` e `SUDO_GID`.
Os arquivos têm permissão `644`, permitindo editá-los sem abrir o editor como root.

A saída deverá conter algo semelhante a:

```text
*** Starting Process
*** Starting p1 in 10.0.0.2
*** Starting p2 in 10.0.0.3
*** Starting p3 in 10.0.0.4
*** Network ready!!
*** Starting CLI:
mininet>
```

## 3.3. Teste de conectividade
O próprio script já executa:
```python
net.pingAll()
```
Também é recomendado testar manualmente.

De `h1` para `h2`:

```bash
h1 ping -c 2 10.0.0.2
```

De `h2` para `h3`:

```bash
h2 ping -c 2 10.0.0.3
```
O resultado esperado é:

```text
0% packet loss
```

## 3.4. Testando Controller -> Server -> multicast

Execute os comandos no prompt do Mininet, dentro de `h1`, onde o servidor
escuta em `127.0.0.1:9000`:

```text
mininet> h1 python3 command.py send p1 hello p1
mininet> h1 python3 command.py send p2 hello p2
mininet> h1 python3 command.py send p3 hello p3
mininet> h2 cat logs/process1.log
mininet> h3 cat logs/process2.log
mininet> h4 cat logs/process3.log
```

Também é possível abrir o controlador interativo com `h1 python3 command.py`.
Ele acompanha os arquivos em `logs/`.

Cada solicitação gera um novo ID no processo de origem: `p1-1`, `p1-2` etc.
Enviar o mesmo texto novamente cria outra mensagem. Para comparar as entregas:

```text
mininet> h2 grep 'DELIVER' logs/process1.log
mininet> h3 grep 'DELIVER' logs/process2.log
mininet> h4 grep 'DELIVER' logs/process3.log
```

As três listas devem conter os mesmos IDs, conteúdos e timestamps na mesma ordem.
O campo `clock` pode diferir entre processos. Em uma execução nova, o primeiro
comando `send p1 hello p1` gera `p1-1` com timestamp `2`: o relógio de `p1` avança
de `0` para `1` na recepção da solicitação e para `2` no envio multicast.
Essa mensagem aparece em um evento `DELIVER` de cada processo.

## 3.4.1. Testes automáticos

Na pasta do projeto, execute:

```bash
python3 -B -m unittest discover -s tests -v
```

Os testes do algoritmo verificam ACK antes do DATA, desempate pelo identificador
do processo, bloqueio da cabeça da fila, mensagens repetidas e concorrência em
30 escalonamentos determinísticos. Os testes FIFO invertem datagramas DATA/ACK
e verificam que um remetente não bloqueia a recepção de outro.

O teste de integração inicia um servidor e três processos com sockets UDP reais
em localhost, envia comandos pelo `command.py` e compara as entregas dos três
logs. Ele usa portas temporárias, guarda os logs em uma pasta temporária e
encerra apenas os processos que iniciou. Não exige Mininet ou `sudo`, mas exige
que o ambiente permita criar sockets UDP.

## 3.5. Encerrando o projeto

Para sair do CLI:

```bash
exit
```
Depois é recomendado limpar qualquer estado residual:

```bash
sudo mn -c
```

---

## 4. Estrutura do projeto

```text
Multicast/
├── Message.py
├── command.py
├── fifo.py
├── mininet_topology.py
├── process.py
├── server.py
└── tests/
```

Durante a execução são gerados arquivos de log:

```text
Multicast/
└── logs/
    ├── server.log
    ├── process1.log
    ├── process2.log
    └── process3.log
```

## 5. Descrição dos arquivos

### `Message.py`

Define os tipos de pacotes utilizados pelo projeto.

```python
@dataclass(frozen=True)
class DataMessage:
    msg_id: str
    timestamp: int
    sender: str
    content: str
```
Representa uma mensagem com payload.

```python
@dataclass(frozen=True)
class AckMessage:
    msg_id: str
    timestamp: int
    sender: str
```

Representa uma mensagem de confirmação.

### `process.py`

Representa um processo do sistema distribuído.

Cada instância possui:

```text
process_id
name
ip
port
socket UDP
relógio de Lamport
contador local de mensagens
fila de DATA pendentes
conjunto de confirmações por msg_id
```

A porta padrão utilizada é:

```text
5000/UDP
```

Os relógios começam em `p1=0`, `p2=5` e `p3=10`. Não há relógios de parede ou
incrementos periódicos: cada recepção aplica `max(clock, timestamp) + 1`, e cada
envio multicast ou entrega à aplicação incrementa o relógio em `1`.

Um envio multicast é um evento lógico com um timestamp, copiado para os três
destinos por chamadas `sendto`, inclusive para o próprio remetente. Cada DATA
recebido entra na fila ordenada por `(timestamp, identificador numérico da origem)`
e gera um `AckMessage` multicast. O envio do ACK é outro evento: seu timestamp
é maior que o relógio registrado no recebimento do DATA.

As confirmações são armazenadas por `msg_id`, independentemente da chegada do
DATA. Assim, se `p2` recebe uma mensagem de `p1` e seu ACK chega a `p3` antes do
DATA original, `p3` guarda essa confirmação e a utiliza quando receber o DATA.

Somente a cabeça da fila pode ser entregue, após ACKs de `p1`, `p2` e `p3`,
incluindo o ACK local recebido pelo socket. Depois da entrega, a mensagem e
suas confirmações são removidas. Receber um ACK não gera outro ACK.

Os logs distinguem `SEND`, `RECEIVE` e `DELIVER`, com `clock` local e `timestamp`
original do pacote. A ordem total se aplica aos eventos `DELIVER`. DATA e ACK
de remetentes distintos podem ser recebidos com timestamps fora de ordem.
As três linhas `SEND` de um multicast compartilham o mesmo relógio e timestamp,
pois representam cópias do mesmo evento lógico para destinos diferentes.

### `fifo.py`

Implementa a ordem FIFO entre os processos sem mudar `DataMessage`, `AckMessage`
ou `Packet`. Na serialização UDP, acrescenta um campo `sequence`, compartilhado
por DATA e ACK e contado separadamente para cada destino.

O receptor mantém o próximo número esperado e um buffer por endereço de origem.
Datagramas adiantados aguardam os anteriores, sem impedir o recebimento de outros
remetentes. O campo de transporte é removido antes da desserialização do pacote.
Uma sequência já recebida é ignorada. Eventos `FIFO BUFFER` mostram datagramas
que precisaram aguardar reordenação.

Conforme definido para este incremento, não há perdas nem reinícios individuais
durante a execução do servidor. Não há retransmissão, timeout ou ACK de transporte:
os ACKs são exclusivamente os do algoritmo de Tanenbaum. Se um datagrama faltar,
a sequência posterior daquele canal ficará aguardando. Os contadores permanecem
em memória até o encerramento da execução.

Essa camada atua somente entre `p1`, `p2` e `p3`. A injeção do servidor mantém
o formato anterior, sem `sequence`; o processo escolhido a transforma em um novo
DATA multicast, com seu próprio ID e timestamp.

### `server.py`

Representa o servidor central utilizado para injetar mensagens no sistema.

O servidor possui dois canais UDP.

#### Canal da rede

```text
10.0.0.1:5000
```

É usado para enviar solicitações aos processos. Os ACKs do multicast são trocados
somente entre os três processos.

#### Canal de controle

```text
127.0.0.1:9000
```

É utilizado para enviar comandos ao servidor durante a execução.

O formato atualmente aceito é:

```text
send <processo> <mensagem>
```

Exemplo:

```text
send p1 Hello p1
```

A tabela de processos do servidor também está atualmente fixa em três nós:

```python
processes = {
    "p1": ("10.0.0.2", 5000),
    "p2": ("10.0.0.3", 5000),
    "p3": ("10.0.0.4", 5000)
}
```

Neste incremento, a tabela de participantes em `process.py` também é fixa em
três processos.

---

### `mininet_topology.py`

Cria a rede virtual utilizada no experimento.

A constante:

```python
NUM_PROCESS = 3
```

determina quantos processos distribuídos serão criados.

---

## 6. Topologia da rede

![[topology.png]]
