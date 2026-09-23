# Totally Ordered Multicast

Projeto acadêmico de **Sistemas Distribuídos** cujo objetivo é implementar um mecanismo de **multicast totalmente ordenado (Totally Ordered Multicast)**, tomando como referência os conceitos apresentados por Andrew S. Tanenbaum e Maarten van Steen em *Distributed Systems*.

O projeto utiliza **UDP** para a troca de mensagens entre processos e **Mininet** para emular a rede distribuída em uma única máquina Linux.

> **Estado atual:** o projeto ainda está na etapa de infraestrutura e comunicação básica. Já existem a topologia Mininet, o servidor central, os tipos de mensagem e um processo distribuído capaz de serializar, enviar, receber e desserializar pacotes UDP. O algoritmo de multicast totalmente ordenado ainda não foi implementado.

---

## 1. Objetivo

A arquitetura final pretende possuir:

- um **servidor central**, usado para injetar mensagens no experimento;
- `N` **processos distribuídos**;
- comunicação por **UDP**;
- multicast entre os processos;
- timestamps lógicos;
- confirmação e/ou controle de mensagens;
- uma fila de mensagens pendentes;
- um protocolo que garanta que todos os processos entreguem as mensagens na **mesma ordem total**.

A propriedade desejada no estágio final é:

```text
Se p1 entrega M1 antes de M2,
então todos os demais processos devem entregar M1 antes de M2.
```

Mesmo que a ordem física de chegada dos datagramas seja diferente em cada processo.

---

## 2. Estado atual da implementação

No estado atual, o fluxo implementado é:

```text
Mininet CLI
    |
    | comando de controle via UDP
    v
server.py
127.0.0.1:9000
    |
    | DataMessage via UDP
    v
process.py
10.0.0.X:5000
```

O servidor possui dois sockets com funções distintas:

```text
127.0.0.1:9000
    Canal de controle.
    Recebe comandos como:
    send p1 Hello

10.0.0.1:5000
    Canal da rede simulada.
    Envia mensagens aos processos
    e aguarda ACKs.
```

Cada processo distribuído utiliza:

```text
10.0.0.X:5000
```

para receber e enviar pacotes UDP.

### Funcionalidades já disponíveis

- criação automática de uma rede Mininet;
- criação de `N` hosts destinados aos processos;
- servidor central em `h1`;
- comunicação UDP;
- serialização de mensagens em JSON;
- desserialização de mensagens;
- definição de `DataMessage`;
- definição de `AckMessage`;
- envio de uma mensagem do controlador para o servidor;
- envio de uma `DataMessage` do servidor para um processo;
- logs independentes para servidor e processos.

### Ainda não implementado

- multicast entre os processos;
- algoritmo de ordenação total;
- relógios lógicos;
- fila de espera (*holdback queue*);
- geração dinâmica de timestamps;
- geração dinâmica de `msg_id`;
- ACK automático no `process.py`;
- retransmissão;
- tratamento de perda de pacotes;
- tratamento de falhas de processos.

---

## 3. Stack

O projeto utiliza:

| Tecnologia | Função |
|---|---|
| Python 3 | Implementação do servidor, processos e topologia |
| UDP / `socket` | Comunicação entre os nós |
| JSON | Serialização dos pacotes |
| `dataclasses` | Representação das mensagens |
| `threading` | Execução concorrente no servidor |
| Mininet | Emulação da rede |
| Open vSwitch | Switching virtual utilizado pelo Mininet |
| Linux network namespaces | Isolamento dos hosts virtuais |

### Versão do Python

`Message.py` utiliza a sintaxe:

```python
type Packet = DataMessage | AckMessage
```

Essa sintaxe requer **Python 3.12 ou superior**.

---

## 4. Requisitos de sistema

### Sistema operacional

O Mininet depende de recursos do kernel Linux, portanto o ambiente recomendado é:

- Linux nativo;
- máquina virtual Linux; ou
- imagem oficial do Mininet.

São necessários:

- suporte a **network namespaces**;
- suporte a interfaces virtuais **veth**;
- privilégios de `root`;
- Python 3.12+ para este projeto;
- Mininet;
- Open vSwitch ou outro switch compatível.

O Mininet precisa ser executado com privilégios administrativos.

---

## 5. Instalação

### 5.1 Arch Linux

No Arch Linux, o Mininet está disponível no AUR.

Com um AUR helper, por exemplo:

```bash
yay -S mininet
```

Verifique também se o Open vSwitch está disponível e ativo no sistema.

Uma instalação pode ser validada com:

```bash
sudo mn --test pingall
```

O resultado esperado é:

```text
*** Results: 0% dropped
```

### Verificando suporte a `veth`

O Mininet utiliza pares de interfaces `veth` para conectar hosts e switches.

Teste:

```bash
sudo modprobe veth
```

Depois:

```bash
lsmod | grep veth
```

Também é possível testar diretamente:

```bash
sudo ip link add veth-test1 type veth peer name veth-test2
```

Se funcionar, remova as interfaces:

```bash
sudo ip link delete veth-test1
```

#### Kernel atualizado sem reboot

No Arch Linux, depois de atualizar o kernel, pode ocorrer de:

```bash
uname -r
```

mostrar uma versão diferente da disponível em:

```bash
ls /usr/lib/modules/
```

Nesse caso, reinicie a máquina antes de usar o Mininet:

```bash
sudo reboot
```

---

### 5.2 Ubuntu / Debian

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

## 6. Estrutura do projeto

A estrutura esperada é:

```text
Multicast/
├── Message.py
├── mininet_topology.py
├── process.py
├── server.py
└── README.md
```

Durante a execução são gerados arquivos de log:

```text
Multicast/
├── server.log
├── process1.log
├── process2.log
├── process3.log
└── ...
```

> O nome `Message.py` é case-sensitive no Linux. Os arquivos atuais importam `Message`, portanto renomeá-lo para `message.py` exige atualizar os imports.

---

## 7. Descrição dos arquivos

### `Message.py`

Define os tipos de pacotes utilizados pelo projeto.

#### `DataMessage`

```python
@dataclass(frozen=True)
class DataMessage:
    msg_id: str
    timestamp: int
    sender: str
    content: str
```

Representa uma mensagem contendo dados.

Campos:

- `msg_id`: identificador da mensagem;
- `timestamp`: timestamp lógico;
- `sender`: identificador do emissor;
- `content`: conteúdo da mensagem.

#### `AckMessage`

```python
@dataclass(frozen=True)
class AckMessage:
    msg_id: str
    timestamp: int
    sender: str
```

Representa uma mensagem de confirmação.

#### `Packet`

```python
type Packet = DataMessage | AckMessage
```

Permite tratar os dois tipos de mensagem através de uma única anotação de tipo.

---

### `process.py`

Representa um processo do sistema distribuído.

Cada instância possui:

```text
process_id
name
ip
port
socket UDP
```

A porta padrão utilizada é:

```text
5000/UDP
```

As principais operações são:

#### `serialize_packet()`

Transforma um `DataMessage` ou `AckMessage` em JSON e depois em `bytes`.

```text
Packet
   |
   v
dict
   |
   v
JSON
   |
   v
bytes
```

#### `deserialize_packet()`

Executa o processo inverso:

```text
bytes
   |
   v
JSON
   |
   v
dict
   |
   v
DataMessage / AckMessage
```

#### `send_packet()`

Envia um `Packet` para um endereço UDP.

#### `receive_packet()`

Bloqueia esperando um datagrama e retorna:

```python
(packet, source)
```

#### `handle_packet()`

Identifica se a mensagem é um:

```text
DataMessage
```

ou:

```text
AckMessage
```

e imprime seu conteúdo.

#### `run()`

Executa o loop principal:

```text
receive_packet()
      |
      v
handle_packet()
      |
      v
receive_packet()
      |
     ...
```

---

### `server.py`

Representa o servidor central utilizado para injetar mensagens no sistema.

O servidor possui dois canais UDP.

#### Canal da rede

```text
10.0.0.1:5000
```

É usado para enviar mensagens aos processos e receber respostas.

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

O método:

```python
server_controller()
```

recebe esse comando e chama:

```python
send_message(process_name, message)
```

O servidor então cria uma `DataMessage` e envia para o processo selecionado.

Atualmente:

```python
msg_id = "server-test-1"
timestamp = 0
sender = "server"
```

são valores de teste.

A tabela de processos do servidor também está atualmente fixa em três nós:

```python
processes = {
    "p1": ("10.0.0.2", 5000),
    "p2": ("10.0.0.3", 5000),
    "p3": ("10.0.0.4", 5000)
}
```

Para suporte totalmente dinâmico a `N`, essa tabela deverá futuramente ser gerada a partir do número de processos.

---

### `mininet_topology.py`

Cria a rede virtual utilizada no experimento.

A constante:

```python
NUM_PROCESS = 3
```

determina quantos processos distribuídos serão criados.

O script:

1. cria uma instância do Mininet;
2. cria o controller `c0`;
3. cria o switch `s1`;
4. cria `h1` como servidor;
5. cria `N` hosts distribuídos;
6. conecta todos ao switch;
7. inicia a rede;
8. executa `pingAll()`;
9. inicia `server.py`;
10. inicia os processos;
11. abre o CLI do Mininet;
12. encerra os processos quando o CLI é fechado.

---

## 8. Topologia da rede

Para `N` processos, a topologia lógica é:

```text
                           c0
                           |
                           |
                          s1
            ______________|_______________
           /       /       |       \       \
          /       /        |        \       \
         h1      h2       h3        h4     ... h(N+1)
         |       |         |         |            |
      server     p1        p2        p3          pN
```

Todos os hosts estão na rede:

```text
10.0.0.0/24
```

### Endereçamento

O servidor sempre ocupa:

```text
h1
10.0.0.1
```

Para qualquer processo `p_i`:

```text
host = h(i + 1)

IP = 10.0.0.(i + 1)
```

Assim:

| Processo | Host Mininet | IP |
|---|---|---|
| server | h1 | 10.0.0.1 |
| p1 | h2 | 10.0.0.2 |
| p2 | h3 | 10.0.0.3 |
| p3 | h4 | 10.0.0.4 |
| ... | ... | ... |
| pN | h(N+1) | 10.0.0.(N+1) |

Todos os processos utilizam:

```text
UDP/5000
```

O servidor também utiliza:

```text
UDP/5000
```

na rede emulada e:

```text
UDP/9000
```

no loopback de `h1` para receber comandos de controle.

---

## 9. Modularidade para N processos

A criação dos hosts já é baseada em:

```python
NUM_PROCESS
```

Por exemplo:

```python
NUM_PROCESS = 5
```

produz:

```text
h1 -> server -> 10.0.0.1

h2 -> p1 -> 10.0.0.2
h3 -> p2 -> 10.0.0.3
h4 -> p3 -> 10.0.0.4
h5 -> p4 -> 10.0.0.5
h6 -> p5 -> 10.0.0.6
```

Entretanto, **o snapshot atual ainda não é completamente modular para `N`**.

Há duas limitações atuais:

1. `server.py` possui `p1`, `p2` e `p3` definidos manualmente;
2. a chamada de `process.py` em `mininet_topology.py` precisa estar compatível com os argumentos exigidos pelo processo.

A arquitetura pretendida é modular; essas partes devem ser generalizadas conforme o protocolo for evoluindo.

---

## 10. Ajuste necessário no snapshot atual

O `process.py` atual exige:

```bash
python3 process.py <process_id> <ip>
```

Porém, a versão atual de `mininet_topology.py` chama:

```python
f"python3 process.py {ip} "
```

ou seja, fornece apenas o IP.

Para executar os processos com a interface atual de `process.py`, a chamada da topologia precisa ser:

```python
host.cmd(
    f"python3 -u process.py "
    f"{process_id} {ip} "
    f"> process{process_id}.log 2>&1 &"
)
```

O `-u` é recomendado para desabilitar o buffering de `stdout` e permitir que os logs sejam atualizados imediatamente.

Sem esse ajuste, `process.py` encerrará exibindo sua mensagem de uso.

---

## 11. Como executar

### 11.1 Entrar na pasta

```bash
cd Multicast
```

Confira os arquivos:

```bash
ls
```

Resultado esperado:

```text
Message.py
mininet_topology.py
process.py
server.py
README.md
```

---

### 11.2 Validar o Mininet

Antes de executar o projeto:

```bash
sudo mn --test pingall
```

O resultado esperado é:

```text
*** Results: 0% dropped
```

---

### 11.3 Limpar execuções anteriores

Antes de iniciar uma nova topologia:

```bash
sudo mn -c
```

Isso remove interfaces, namespaces e processos remanescentes de execuções anteriores.

---

### 11.4 Iniciar a topologia

Execute:

```bash
sudo python3 mininet_topology.py
```

A saída deverá conter algo semelhante a:

```text
*** Add controller
*** Add switch
*** Add central Server
*** Adding distributed process
*** Starting network
*** Configuring hosts
h1 h2 h3 h4
*** Starting controller
c0
*** Starting 1 switches
s1 ...
*** Testing the connection
```

O `pingAll()` deve concluir com:

```text
*** Results: 0% dropped
```

Depois:

```text
*** Start server.py
*** Starting Process
*** Starting p1 in 10.0.0.2
*** Starting p2 in 10.0.0.3
*** Starting p3 in 10.0.0.4
*** Network ready!!
*** Starting CLI:
mininet>
```

---

## 12. Verificando os processos

Dentro do CLI do Mininet, **não digite `mininet>`**. Ele é apenas o prompt.

Correto:

```text
mininet> h2 ps aux | grep process.py
```

O comando digitado é apenas:

```bash
h2 ps aux | grep process.py
```

Confira cada processo:

```bash
h2 ps aux | grep process.py
h3 ps aux | grep process.py
h4 ps aux | grep process.py
```

---

## 13. Verificando os logs

Servidor:

```bash
h1 cat server.log
```

Processo 1:

```bash
h2 cat process1.log
```

Processo 2:

```bash
h3 cat process2.log
```

Processo 3:

```bash
h4 cat process3.log
```

Após iniciar corretamente, os logs dos processos devem conter algo como:

```text
[p1] running on 10.0.0.2:5000
```

---

## 14. Teste de conectividade

O próprio script já executa:

```python
net.pingAll()
```

Também é possível testar manualmente.

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

---

## 15. Testando Controller -> Server -> p1

Este é o principal teste suportado pelo estágio atual do projeto.

O fluxo esperado é:

```text
Mininet CLI
     |
     | UDP
     | "send p1 Hello p1"
     v
127.0.0.1:9000
     |
     v
server_controller()
     |
     v
send_message()
     |
     | DataMessage / UDP
     v
10.0.0.2:5000
     |
     v
p1
```

### Passo 1 — Verificar servidor

```bash
h1 cat server.log
```

Deve aparecer:

```text
[SERVER] UDP socket running on: 10.0.0.1:5000
[CONTROLLER] listening on: 127.0.0.1:9000
```

### Passo 2 — Verificar p1

```bash
h2 cat process1.log
```

Deve aparecer:

```text
[p1] running on 10.0.0.2:5000
```

### Passo 3 — Enviar comando ao servidor

No CLI do Mininet:

```bash
h1 python3 -c "import socket; s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM); s.sendto(b'send p1 Hello p1',('127.0.0.1',9000)); s.close()"
```

Esse comando **não envia diretamente para `p1`**.

Ele envia:

```text
send p1 Hello p1
```

para o canal de controle do servidor:

```text
127.0.0.1:9000
```

### Passo 4 — Conferir o servidor

```bash
h1 cat server.log
```

O log deve conter algo semelhante a:

```text
[CONTROLLER] command received: send p1 Hello p1
[SEND] ('10.0.0.2', 5000) - Hello p1
```

### Passo 5 — Conferir p1

```bash
h2 cat process1.log
```

O processo deverá registrar uma `DataMessage` semelhante a:

```text
[p1] RECEIVE <- ('10.0.0.1', 5000): DataMessage(...)

[p1] DATA MESSAGE
  id:        server-test-1
  sender:    server
  timestamp: 0
  content:   Hello p1
```

Isso comprova o caminho:

```text
Controller -> Server -> p1
```

---

## 16. Testando outros processos

### p2

```bash
h1 python3 -c "import socket; s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM); s.sendto(b'send p2 Hello p2',('127.0.0.1',9000)); s.close()"
```

Confira:

```bash
h3 cat process2.log
```

### p3

```bash
h1 python3 -c "import socket; s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM); s.sendto(b'send p3 Hello p3',('127.0.0.1',9000)); s.close()"
```

Confira:

```bash
h4 cat process3.log
```

---

## 17. Encerrando o projeto

Para sair do CLI:

```bash
exit
```

O `mininet_topology.py` então encerra:

- `server.py`;
- todos os `process.py`;
- a rede Mininet.

Depois é recomendado limpar qualquer estado residual:

```bash
sudo mn -c
```

---

## 18. Troubleshooting

### `Unknown command: mininet>`

Não escreva o texto `mininet>`.

Errado:

```text
mininet> mininet> h2 ping 10.0.0.3
```

Correto:

```text
mininet> h2 ping 10.0.0.3
```

---

### `process.py` não aparece no `ps`

Verifique:

```bash
h2 cat process1.log
```

O processo provavelmente encerrou devido a uma exceção ou argumentos incorretos.

---

### `ModuleNotFoundError: No module named 'Message'`

Verifique se o arquivo realmente se chama:

```text
Message.py
```

O Linux diferencia maiúsculas e minúsculas.

---

### `Error: Unknown device type`

Se o Mininet falhar ao criar:

```text
h1-eth0
s1-eth1
```

verifique o módulo `veth`:

```bash
sudo modprobe veth
```

Em Arch Linux, também confira:

```bash
uname -r
ls /usr/lib/modules/
```

Se as versões forem diferentes, reinicie o computador.

---

### Porta já em uso

Caso exista uma execução antiga:

```bash
sudo mn -c
```

Também é possível verificar processos Python:

```bash
ps aux | grep server.py
ps aux | grep process.py
```

---

## 19. Próximas etapas

A evolução natural da implementação é:

```text
[1] Topologia Mininet
        |
        v
[2] Comunicação UDP
        |
        v
[3] DataMessage / AckMessage
        |
        v
[4] Server -> Process
        |
        v
[5] Process -> Process
        |
        v
[6] Multicast básico
        |
        v
[7] Relógio lógico
        |
        v
[8] Holdback Queue
        |
        v
[9] Protocolo de ordenação
        |
        v
[10] Totally Ordered Multicast
```

O objetivo é manter a camada de comunicação (`send_packet`, `receive_packet`, serialização e desserialização) separada da futura lógica de ordenação.

---

## 20. Referências

O projeto utiliza como base conceitual os tópicos de comunicação, multicast e ordenação de eventos apresentados em:

- Andrew S. Tanenbaum e Maarten van Steen — *Distributed Systems*.
- Mininet — https://mininet.org/
- Mininet GitHub — https://github.com/mininet/mininet
- Mininet Installation Guide — https://github.com/mininet/mininet/blob/master/INSTALL
