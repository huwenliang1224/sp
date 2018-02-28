#!/bin/sh

cd /root/project/sp

rm -rf /root/project/sp.jar

git pull
mvn clean package -DskipTests
cp /root/project/sp/target/sp.jar /root/project

zip -d sp.jar META-INF/*.RSA META-INF/*.DSA META-INF/*.SF
