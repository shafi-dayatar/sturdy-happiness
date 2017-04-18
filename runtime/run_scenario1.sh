sh ../startServer.sh route-1.conf 1> ../logs/server1.log 2> ../logs/server1.err &
sh ../startServer.sh route-2.conf 1> ../logs/server2.log 2> ../logs/server2.err &
sh ../startServer.sh route-5.conf 1> ../logs/server5.log 2> ../logs/server5.err &
sh ../startServer.sh route-6.conf 1> ../logs/server6.log 2> ../logs/server6.err &


Create Table chunks( id INT(11) NOT NULL AUTO_INCREMENT, file_id INT(11) NOT NULL,chunk_id INT(11) NOT NULL, chunk_data MEDIUMTEXT , PRIMARY KEY (id), Unique KEY(file_id, chunk_id));