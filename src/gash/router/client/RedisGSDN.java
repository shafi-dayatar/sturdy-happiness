package gash.router.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.ServerState;
import pipe.common.Common.Node;
import redis.clients.jedis.Jedis;

public class RedisGSDN {
	protected Logger logger = LoggerFactory.getLogger("Redis GSDN");

	private Jedis dbConnection;
	
	public RedisGSDN(String host, int port){
		dbConnection = new Jedis(host, 
				port);
	}
	
	public Node getLeader(int clusterId){
		Node node = null;
		try{
			dbConnection.select(0);// uses default database 
			String value = dbConnection.get(Integer.toString(clusterId));// use this to set leader on redis,
			String [] arr = value.split(":");
			Node.Builder nb = Node.newBuilder();
			nb.setNodeId(Integer.parseInt(arr[0]));
			nb.setHost(arr[1]);
			nb.setPort(Integer.parseInt(arr[2]));
			node = nb.build();
			logger.info("getting leader for cluster : " + clusterId);
		}catch(Exception e){
			logger.error("Error, while updating redis gsdn");
			e.printStackTrace();
		} 
		return node;
	}
}
