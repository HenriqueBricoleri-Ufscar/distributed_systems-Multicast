import os
import sys

# Permite `from mininet_topology import build_network` mesmo rodando
# este arquivo a partir de dentro de exclusao/
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mininet.log import setLogLevel
from mininet_topology import build_network

if __name__ == "__main__":
    setLogLevel("info")
    build_network(
        server_script="exclusao/server.py",
        process_script="exclusao/process.py",
    )