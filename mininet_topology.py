import os

from mininet.net import Mininet
from mininet.node import Controller
from mininet.link import TCLink
from mininet.cli import CLI
from mininet.log import setLogLevel, info


NUM_PROCESS = 3


def prepare_log(filename):
    directory = os.path.dirname(filename)
    os.makedirs(directory, exist_ok=True)

    with open(filename, "w", encoding="utf-8") as log_file:
        if os.geteuid() == 0:
            uid = int(os.environ.get("SUDO_UID", os.getuid()))
            gid = int(os.environ.get("SUDO_GID", os.getgid()))
            os.chown(directory, uid, gid)
            os.fchown(log_file.fileno(), uid, gid)

        os.fchmod(log_file.fileno(), 0o644)


def build_network(server_script="server.py", process_script="process.py"):
    # server_script/process_script permitem apontar para outra implementação
    # (ex.: exclusion/server.py, exclusion/process.py) sem duplicar a topologia.
    net = Mininet(controller=Controller, link=TCLink)

    info("*** Add controller\n")
    net.addController("c0")

    info("*** Add switch\n")
    switch = net.addSwitch("s1")

    info("*** Add central Server\n")
    server = net.addHost("h1", ip="10.0.0.1/24")
    net.addLink(server, switch)

    info("*** Adding distributed process\n")
    processes = []

    for i in range(NUM_PROCESS):
        process_id = i + 1
        host_number = i + 2
        ip = f"10.0.0.{host_number}"

        host = net.addHost(f"h{host_number}", ip=f"{ip}/24")
        net.addLink(host, switch)
        processes.append(host)

    info("*** Starting network\n")
    net.start()

    info("*** Testing the connection\n")
    net.pingAll()

    info(f"*** Starting {server_script}\n") # mensagem de log para indicar que o servidor está sendo iniciado
    prepare_log("logs/server.log")
    server.cmd(f"python3 -u {server_script} > logs/server.log 2>&1 &") # inicia o servidor em segundo plano e redireciona a saída para o arquivo de log

    info("*** Starting processes\n")

    for i, host in enumerate(processes):
        process_id = i + 1
        ip = f"10.0.0.{i + 2}"

        info(f"*** Starting p{process_id} in {ip}\n")

        log_file = f"logs/process{process_id}.log"
        prepare_log(log_file)
        host.cmd(f"python3 -u {process_script} {process_id} {ip} > {log_file} 2>&1 &") # inicia o processo em segundo plano e redireciona a saída para o arquivo de log

    info("*** Checking processes\n")
    server_pid = server.cmd(f"pgrep -f 'python3 -u {server_script}'").strip() # verifica se o servidor está em execução e obtém o PID do processo do servidor

    if server_pid:
        info(f"*** Server running (PID {server_pid})\n")
    else:
        info(f"*** ERROR: {server_script} is not running\n")

    for i, host in enumerate(processes):
        process_id = i + 1
        process_pid = host.cmd(f"pgrep -f 'python3 -u {process_script} {process_id}'").strip()

        if process_pid:
            info(f"*** p{process_id} running (PID {process_pid})\n")
        else:
            info(f"*** ERROR: p{process_id} is not running\n")

    info("\n*** Network ready!!\n")
    info("*** Commands:\n")
    info("*** h1 cat logs/server.log\n")

    for i in range(NUM_PROCESS):
        process_id = i + 1
        host_number = i + 2
        info(f"*** h{host_number} cat logs/process{process_id}.log\n")

    info("\n")
    CLI(net)

    info("*** Finishing processes\n")
    server.cmd(f"pkill -f {server_script}")

    for host in processes:
        host.cmd(f"pkill -f {process_script}")

    net.stop()


if __name__ == "__main__":
    setLogLevel("info")
    build_network()