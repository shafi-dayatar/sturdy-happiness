#!/bin/bash
#
# This script is used to start the server from a supplied config file
#

export SVR_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
echo "** starting from ${SVR_HOME} **"

echo server home = $SVR_HOME
#exit

#cd ${SVR_HOME}

JAVA_MAIN='gash.router.app.DemoApp'



# superceded by http://ww.oracle.com/technetwork/java/tuning-139912.html
JAVA_TUNE='-client -Djava.net.preferIPv4Stack=true'

read -p "Enter the IP you want to connect to  " ip
read -p "Enter the port to connect to  " port
while true
do
read -p "1. Read a file   2. Write a file  " action

if [ $action == "1" ]
then 
    action="get"
elif [ $action == "2" ]
then
    action="post"
fi

read -p "Enter File Path  " path
read -p "Enter File Name  " fname
JAVA_ARGS="$ip $port $action $path "$fname""
#echo -e "\n** config: ${JAVA_ARGS} **\n"
#echo $ip,$port,$action,$path,$fname
java ${JAVA_TUNE} -cp .:${SVR_HOME}/lib/'*':${SVR_HOME}/classes ${JAVA_MAIN} ${JAVA_ARGS} 

done
