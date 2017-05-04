# sturdy-happiness


Build Instructions:

1. Navigate to project directory and run the following command in the terminal:

sh build_pb_prabhu.sh

2. Navigate to project directory and run the command:

ant build
 
3. Navigate to ~/runtime and set the config values as follows:

{
    "nodeId": 1, 				
    "internalNode": false,
    "heartbeatDt": 3000,
    "workPort": 4167,
    "clusterId" : 4,
    "commandPort": 4168,
    "mysqlHost":"localhost", 	
    "mysqlPort": 3306,
    "mysqlUserName": "database user name", 			
    "mysqlPassword": "database password",
    "redisHost" : "localhost", 
    "redisPort" : 6379,
    "nextClusterId" :5,
    "clusterClientId" : 44,
    "threadCount": 3,
    "routing": [
        {
            "id": 2,
            "host": "localhost",
            "port": 4267,
            "cmdPort": 4268
        }
    ]
}

Note: Currently the set configuration is as follows:

1->2->3->4->5->6>1

This can be changed by modifying the key “routing”’s values.

4. Create a database named “cmpe275” and run the db script - “db_script” to set up relevant tables for the project

5. Run the the following to check for dependencies and db set up:

sh runDep.sh	

6. After redis is installed, run the redis service with the following command

redis-server

7. Initial setup and build is done and we can proceed to start the server

8. Run the below commands from project directory on separate terminals :

sh startServer.sh runtime/route-1.conf

sh startServer.sh runtime/route-2.conf

sh startServer.sh runtime/route-3.conf

sh startServer.sh runtime/route-4.conf

sh startServer.sh runtime/route-5.conf

sh startServer.sh runtime/route-6.conf


9. Leader election should happen fairly quickly and the terminal will indicate the leader status and follower status

10. Run client as follows:

sh runClient.sh


12. To check for work stealing proof, run the following command:

sh runTest.sh


12. Enter file name to read with extension. Example:

test.pdf

13. Enter number of read requests to fire. We recommend at least 500 messages. Example:

	1000

14. As proof, followers would be sending work stealing requests to each other and the following should be printed if a suitable message has been found and forwarded.

Found a message to steal



Dependencies:

JDBC jars
Apache Commons jars
MySQL database
Redis service


