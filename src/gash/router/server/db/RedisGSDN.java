package gash.router.server.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.ServerState;
import pipe.common.Common.Node;
import redis.clients.jedis.Jedis;

public class RedisGSDN {
	protected Logger logger = LoggerFactory.getLogger("Redis GSDN");

	private Jedis dbConnection;
	
	public RedisGSDN(ServerState state){
		dbConnection = new Jedis(state.getConf().getRedisHost(), 
				state.getConf().getRedisPort());
	}
	
	public void updateLeader(int clusterId, String leaderNode){
		try{
			dbConnection.select(0);// uses default database 

			dbConnection.set(Integer.toString(clusterId), leaderNode);// use this to set leader on redis, 
			logger.info("Updating GSDN with new leader for cluster : " + clusterId);
		}catch(Exception e){
			logger.error("Error, while updating redis gsdn");
			e.printStackTrace();
		} 
	}
	
	public Node getLeader(int clusterId){
		Node node = null;
		try{
			dbConnection.select(0);// uses default database 
			String value = dbConnection.get(Integer.toString(clusterId));// use this to set leader on redis,
			String [] arr = value.split(":");
			Node.Builder nb = Node.newBuilder();
			nb.setNodeId(5);
			nb.setHost(arr[0]);
			nb.setPort(Integer.parseInt(arr[1]));
			node = nb.build();
			logger.info("getting leader for cluster : " + clusterId);
		}catch(Exception e){
			logger.error("Error, while updating redis gsdn");
			e.printStackTrace();
		} 
		return node;
	}
}
