ps -aux | grep -ie "python3 orchestration_server.py" | awk '{print $2}' | xargs kill -9
ps -aux | grep -ie "python3 node.py" | awk '{print $2}' | xargs kill -9
