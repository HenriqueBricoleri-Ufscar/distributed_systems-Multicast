from mininet.net import Mininet
from mininet.node import Controller
from mininet.link import TCLink
from mininet.cli import CLI
from mininet.log import setLogLevel, info

NUM_PROCESS = 3

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
    
    process = []
    for i in range(NUM_PROCESS):
        host_number = i + 2
        ip = f"10.0.0.{host_number}"
        
        host = net.addHost(
            f"h{host_number}",
            ip = f"{ip}/24"
        )
        
        net.addLink(host, switch)
        
        process.append(host)
        
    info("*** Starting network\n")
    net.start()
    
    info("*** Testing the connection\n")
    net.pingAll()
    
    info("*** Start server.py\n")
    server.cmd(
        "python3 server.py"
        "> server.log 2>&1 &"
    )
    
    
    info("*** Starting Process\n")
    
    for i, host in enumerate(process):
        process_id = i+1
        ip = f"10.0.0.{i + 2}"
        
        info(
            f"*** Starting p{process_id} "
            f"in {ip}\n"
        )
        
        host.cmd(
            f"python3 process.py {ip} "
            f"> process{process_id}.log 2>&1 &"
        )
        
    info("*** Network ready!!\n")
    
    CLI(net)
    
    info("*** Finishing process!")
    server.cmd("pkill -f server.py")

    for host in process:
        host.cmd("pkill -f process.py")

    net.stop()
    
if __name__ == "__main__":

    setLogLevel("info")
    build_network()