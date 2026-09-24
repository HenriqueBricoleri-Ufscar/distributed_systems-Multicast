"""
Testes do algoritmo de exclusão mútua de Ricart & Agrawala (exclusao/process.py).

Em vez de sockets/threads reais, cada teste usa um "transporte" em memória que
chama handle_packet() diretamente na ordem escolhida pelo teste. Isso reproduz
exatamente a mesma lógica usada em produção (a função handle_packet é idêntica),
mas com escalonamento 100% determinístico -- sem depender de timing de rede.
"""
import os
import sys
import unittest
import importlib.util

EXCLUSAO_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
ROOT_DIR = os.path.abspath(os.path.join(EXCLUSAO_DIR, ".."))
sys.path.insert(0, ROOT_DIR)  # para o Message.py da raiz

from Message import RequestMessage, ReplyMessage

# exclusao/process.py tem o mesmo nome de módulo que o process.py da raiz,
# então carregamos pelo caminho explícito para não pegar o módulo errado.
_spec = importlib.util.spec_from_file_location("exclusao_process", os.path.join(EXCLUSAO_DIR, "process.py"))
_exclusao_process = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_exclusao_process)

MutexProcess = _exclusao_process.MutexProcess
RELEASED = _exclusao_process.RELEASED
WANTED = _exclusao_process.WANTED
HELD = _exclusao_process.HELD


PEERS = {
    "p1": ("10.0.0.2", 5000),
    "p2": ("10.0.0.3", 5000),
    "p3": ("10.0.0.4", 5000),
}


class Network:
    """Roteia pacotes entre MutexProcess sem usar sockets. Também registra
    todo pacote entregue, para provarmos que toda REQUEST gera uma REPLY."""

    def __init__(self):
        self.processes: dict[str, MutexProcess] = {}
        self.delivered: list[tuple[str, str, object]] = []  # (origem, destino, pacote)

    def register(self, name: str) -> MutexProcess:
        process_id = int(name[1:])

        def transport(packet, dest_name, sender=name):
            self.delivered.append((sender, dest_name, packet))
            self.processes[dest_name].handle_packet(packet, (sender, 0))

        process = MutexProcess(process_id, PEERS[name][0], peers=PEERS, transport=transport)
        self.processes[name] = process
        return process

    def requests_sent(self):
        return [(src, dst, pkt) for src, dst, pkt in self.delivered if isinstance(pkt, RequestMessage)]

    def replies_sent(self):
        return [(src, dst, pkt) for src, dst, pkt in self.delivered if isinstance(pkt, ReplyMessage)]


class TestRicartAgrawala(unittest.TestCase):
    def setUp(self):
        self.net = Network()
        self.p1 = self.net.register("p1")
        self.p2 = self.net.register("p2")
        self.p3 = self.net.register("p3")

    # ------------------------------------------------------------------
    # Cenário 1: um único processo pede a CS, sem disputa.
    # ------------------------------------------------------------------
    def test_single_request_no_contention(self):
        self.p1.request_cs()  # transport síncrono => já retorna com CS concedida

        self.assertEqual(self.p1.state, HELD)
        self.assertEqual(self.p2.state, RELEASED)
        self.assertEqual(self.p3.state, RELEASED)
        # p1 recebeu REPLY de todos os outros -> pending_replies vazio
        self.assertEqual(self.p1.pending_replies, set())

    # ------------------------------------------------------------------
    # Cenário 2: p1 e p2 pedem "ao mesmo tempo"; timestamp de p1 é menor
    # (ele já estava com o clock mais baixo) -> p1 deve entrar primeiro e
    # p2 deve ficar bloqueado (DEFER) até p1 liberar.
    # ------------------------------------------------------------------
    def test_concurrent_requests_priority_by_timestamp(self):
        # p1 e p2 "pedem" simultaneamente, com timestamps diferentes: p1=1 é
        # menor e tem prioridade. Cada handle_packet já dispara o REPLY pela
        # rede automaticamente (via self.transport), então não precisamos
        # entregar réplicas manualmente.
        p1_req = RequestMessage(timestamp=1, sender="p1")
        p2_req = RequestMessage(timestamp=5, sender="p2")  # p2 começa com clock=5

        self.p1.state, self.p1.request_timestamp = WANTED, 1
        self.p1.pending_replies = {"p2", "p3"}
        self.p2.state, self.p2.request_timestamp = WANTED, 5
        self.p2.pending_replies = {"p1", "p3"}

        # p3 (neutro) recebe as duas requisições e responde as duas na hora
        self.p3.handle_packet(p1_req, ("p1", 0))  # -> REPLY automático para p1
        self.p3.handle_packet(p2_req, ("p2", 0))  # -> REPLY automático para p2

        # p1 e p2 trocam requests entre si
        self.p2.handle_packet(p1_req, ("p1", 0))  # p2 NÃO tem prioridade -> responde na hora
        self.p1.handle_packet(p2_req, ("p2", 0))  # p1 TEM prioridade (ts=1<5) -> ADIA a resposta

        self.assertIn("p2", self.p1.deferred)   # p1 adiou o pedido de p2
        self.assertEqual(self.p2.deferred, [])  # p2 não adiou nada, respondeu de imediato

        # p1 já tem REPLY de p3 (passo acima) e de p2 (imediata) -> entra na CS
        self.assertEqual(self.p1.state, HELD)

        # p2 só tem REPLY de p3; falta a de p1, que está adiada -> ainda espera
        self.assertEqual(self.p2.state, WANTED)
        self.assertEqual(self.p2.pending_replies, {"p1"})

        self.p1.release_cs()
        self.assertEqual(self.p1.state, RELEASED)

        # ao liberar, p1 finalmente respondeu ao pedido adiado de p2
        self.assertEqual(self.p2.state, HELD)

    # ------------------------------------------------------------------
    # Cenário 3: timestamps EMPATADOS -> desempate pelo identificador do
    # processo (menor nome vence), conforme o livro.
    # ------------------------------------------------------------------
    def test_tie_break_by_process_id(self):
        req_from_p2 = RequestMessage(timestamp=7, sender="p2")
        req_from_p3 = RequestMessage(timestamp=7, sender="p3")

        self.p2.state, self.p2.request_timestamp = WANTED, 7
        self.p3.state, self.p3.request_timestamp = WANTED, 7

        # p2 recebe o pedido empatado de p3: (7, "p2") < (7, "p3") -> p2 tem
        # prioridade, então p2 ADIA a resposta a p3.
        self.p2.handle_packet(req_from_p3, ("p3", 0))
        self.assertIn("p3", self.p2.deferred)

        # p3 recebe o pedido empatado de p2: (7, "p3") > (7, "p2") -> p3 NÃO
        # tem prioridade, deve responder imediatamente.
        sent_before = len(self.net.replies_sent())
        self.p3.handle_packet(req_from_p2, ("p2", 0))
        self.assertEqual(len(self.net.replies_sent()), sent_before + 1)
        self.assertEqual(self.net.replies_sent()[-1][:2], ("p3", "p2"))

    # ------------------------------------------------------------------
    # Cenário 4: os três processos pedem a CS "ao mesmo tempo" -> a ordem de
    # entrada deve seguir estritamente (timestamp, id) e ninguém fica sem
    # resposta (sem starvation).
    # ------------------------------------------------------------------
    def test_three_way_concurrent_no_starvation(self):
        order_of_entry = []

        for name, process in self.net.processes.items():
            original_deliver = process.deferred  # apenas para leitura, não usado

        # Timestamps propositalmente diferentes para definir uma ordem clara:
        # p2 (ts=2) < p3 (ts=3) < p1 (ts=4)
        requests = {
            "p1": RequestMessage(timestamp=4, sender="p1"),
            "p2": RequestMessage(timestamp=2, sender="p2"),
            "p3": RequestMessage(timestamp=3, sender="p3"),
        }

        for name, process in self.net.processes.items():
            process.state = WANTED
            process.request_timestamp = requests[name].timestamp
            process.pending_replies = {other for other in PEERS if other != name}

        # Cada processo recebe o REQUEST dos outros dois, na ordem que quiser
        # (o algoritmo não depende da ordem de chegada dos REQUESTs alheios).
        for name, process in self.net.processes.items():
            for other_name, req in requests.items():
                if other_name != name:
                    process.handle_packet(req, (other_name, 0))

        # p2 tem a menor prioridade (menor timestamp) -> ninguém deveria tê-la adiado
        self.assertNotIn("p2", self.p1.deferred)
        self.assertNotIn("p2", self.p3.deferred)

        # p1 tem a maior prioridade numérica (maior timestamp) -> foi adiado por
        # quem tem prioridade maior que ele (p2 e p3)
        self.assertIn("p1", self.p2.deferred)
        self.assertIn("p1", self.p3.deferred)

        # p2 deve conseguir fechar todos os REPLYs e entrar primeiro
        self.assertEqual(self.p2.pending_replies, set())
        self.assertEqual(self.p2.state, HELD)

        # p3 e p1 ainda esperam o REPLY de p2, que só chega quando p2 libera
        self.assertEqual(self.p3.state, WANTED)
        self.assertEqual(self.p1.state, WANTED)

        self.p2.release_cs()

        # depois de p2 liberar, p3 (próxima prioridade) deve completar e entrar
        self.assertEqual(self.p3.state, HELD)
        self.assertEqual(self.p1.state, WANTED)  # p1 ainda espera p3

        self.p3.release_cs()
        self.assertEqual(self.p1.state, HELD)

    # ------------------------------------------------------------------
    # Cenário 5: toda REQUEST enviada gera, cedo ou tarde, uma REPLY do
    # destinatário -- nunca fica sem resposta, mesmo quando adiada.
    # ------------------------------------------------------------------
    def test_every_request_is_always_eventually_replied(self):
        self.p1.request_cs()  # p1 entra e segura a CS

        # p2 pede a CS enquanto p1 está em HELD -> p1 deve adiar, nunca ignorar
        req_from_p2 = RequestMessage(timestamp=self.p2.clock + 1, sender="p2")
        self.p2.state, self.p2.request_timestamp = WANTED, req_from_p2.timestamp
        self.p2.pending_replies = {"p1", "p3"}

        self.p1.handle_packet(req_from_p2, ("p2", 0))
        self.p3.handle_packet(req_from_p2, ("p2", 0))

        self.assertIn("p2", self.p1.deferred)  # adiada, não perdida
        replies_before_release = len(self.net.replies_sent())

        self.p1.release_cs()

        # a liberação deve ter disparado a REPLY que faltava para p2
        self.assertEqual(len(self.net.replies_sent()), replies_before_release + 1)
        self.assertEqual(self.net.replies_sent()[-1][:2], ("p1", "p2"))


if __name__ == "__main__":
    unittest.main(verbosity=2)