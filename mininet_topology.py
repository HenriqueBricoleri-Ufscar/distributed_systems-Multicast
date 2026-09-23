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


def build_network():
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

    info("*** Starting server.py\n")
    prepare_log("logs/server.log")
    server.cmd("python3 -u server.py > logs/server.log 2>&1 &")

    info("*** Starting processes\n")

    for i, host in enumerate(processes):
        process_id = i + 1
        ip = f"10.0.0.{i + 2}"

        info(f"*** Starting p{process_id} in {ip}\n")

        log_file = f"logs/process{process_id}.log"
        prepare_log(log_file)
        host.cmd(f"python3 -u process.py {process_id} {ip} > {log_file} 2>&1 &")

    info("*** Checking processes\n")
    server_pid = server.cmd("pgrep -f 'python3 -u server.py'").strip()

    if server_pid:
        info(f"*** Server running (PID {server_pid})\n")
    else:
        info("*** ERROR: server.py is not running\n")

    for i, host in enumerate(processes):
        process_id = i + 1
        process_pid = host.cmd(f"pgrep -f 'python3 -u process.py {process_id}'").strip()

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
    server.cmd("pkill -f server.py")

    for host in processes:
        host.cmd("pkill -f process.py")

    net.stop()


if __name__ == "__main__":
    setLogLevel("info")
    build_network()
