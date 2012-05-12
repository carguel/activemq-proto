#!/bin/bash

classpath="target/classes"
for jar in lib/*
do
    classpath="$classpath:$jar"
done


java -Xmx400m -server -cp $classpath ConsumerTool  --url=tcp://localhost:61616 --topic=false --subject="TEST.FOO" --durable=false --maxium-messages=20000 --client-id=consumer1 --parallel-threads=1 --transacted=false --sleep-time=0 --verbose=true --ack-mode=AUTO_ACKNOWLEDGE --receive-time-out=0
