#!/bin/sh
java -Xmx512M -XX:MaxPermSize=512M -jar `dirname $0`/sbt-launch.jar "$@"