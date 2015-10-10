#!/bin/bash
echo `which java`

if [ -f target/jvsd-jar-with-dependencies.jar ]; then
    java -verbose:class -jar target/jvsd-jar-with-dependencies.jar $@ >output.txt
else
    echo "missing critical jar file, rebuild"
fi

