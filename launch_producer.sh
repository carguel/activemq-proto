#!/bin/bash

classpath="target/classes"
for jar in lib/*
do
    classpath="$classpath:$jar"
done

message_number=1000
message_size=100
host=localhost

while [ -n "$1" ]
do
    case "$1" in
        -n)
            shift
            message_number="$1"
            ;;

        -s)
            shift
            message_size="$1"
            ;;

        -h)
            shift
            host="$1"
            ;;
    esac

    shift
done


java -server -cp $classpath ProducerTool  --url=tcp://$host:61616 --topic=false --subject="TEST.FOO" --persistent=false --message-count=$message_number  --message-size=$message_size --parallel-threads=1  --time-to-live=0 --transacted=false --sleep-time=0 --verbose=false
