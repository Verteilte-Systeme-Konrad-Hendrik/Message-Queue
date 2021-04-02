sh kill_system.sh

python3 orchestration_server.py &
sleep 1
N=$1
for i in $(seq 1 $N)
do
	python3 node.py &
	sleep 0.0
	echo "started ${i}"
done

echo "Done starting - system running"
