# multi-kafka-producer


To start

To start:
nohup java -jar kproducer.jar 30 5 90 5 30 > myproducer.out 2>&1 &

# kproducer.jar  userReqTimeSec MaxUserReqCount driverTimeSec MaxDriverAvailCount  weatherTimeSec


To stop:

ps aux | grep -i "kproducer.jar"

take the proicess id

kill -9 <process_id>
