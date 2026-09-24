from dataclasses import dataclass

@dataclass(frozen=True)
class DataMessage:
    msg_id: str
    timestamp: int
    sender: str
    content: str


@dataclass(frozen=True)
class AckMessage:
    msg_id: str
    timestamp: int
    sender: str
    
type Packet = DataMessage | AckMessage

# mensagens para o algoritmo de ricart e agrawala
@dataclass(frozen=True)
class RequestMessage:
    # pedido de acesso à recurso
    timestamp: int
    sender: str

@dataclass(frozen=True)
class ReplyMessage:
    # autorização, imediata ou adiada, para o remetente do 
    # request prosseguir 
    timestamp: int
    sender: str

@dataclass(frozen=True)
class TriggerMessage:
    # Comando injetado pelo servidor para provocar uma ação local
    # pedir ou liberar recurso em um processo específico
    action: str # "REQUEST" ou "RELEASE"
    timestamp: int
    sender: str = "server"


type Packet = DataMessage | AckMessage | RequestMessage | ReplyMessage | TriggerMessage