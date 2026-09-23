# Totally Ordered Multicast

Projeto acadêmico de **Sistemas Distribuídos** cujo objetivo é implementar um mecanismo de **multicast totalmente ordenado (Totally Ordered Multicast)**, 
tomando como referência os conceitos apresentados por Andrew S. Tanenbaum e Maarten van Steen em *Distributed Systems*.

O projeto utiliza **UDP** para a troca de mensagens entre processos e **Mininet** para emular a rede distribuída em uma única máquina Linux.

---

## 1. Objetivo

A arquitetura final pretende possuir:

- um **servidor central**, usado para injetar mensagens no experimento;
- `N` **processos distribuídos**;
- comunicação por **UDP**;
- multicast entre os processos;
- timestamps lógicos;

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

## 3.4. Testando Controller -> Server -> p1



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
├── mininet_topology.py
├── process.py 
└── server.py
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
```

A porta padrão utilizada é:

```text
5000/UDP
```

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

A tabela de processos do servidor também está atualmente fixa em três nós:

```python
processes = {
    "p1": ("10.0.0.2", 5000),
    "p2": ("10.0.0.3", 5000),
    "p3": ("10.0.0.4", 5000)
}
```

> Feat: Deverá ser adicionado abstração para comportar N processos.

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
