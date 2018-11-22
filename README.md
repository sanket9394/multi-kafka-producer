# multi-kafka-producer


To start:
  kproducer.jar  \<userReqTimeSec> \<MaxUserReqCount> \<driverTimeSec> \<MaxDriverAvailCount>  \<weatherTimeSec> \<tripDataTimeSec>
  
Example:
  nohup java -jar kproducer.jar 30 5 90 5 30 > myproducer.out 2>&1 &

Command:

To stop:

ps aux | grep -i "kproducer.jar"

take the proicess id

kill -9 <process_id>
