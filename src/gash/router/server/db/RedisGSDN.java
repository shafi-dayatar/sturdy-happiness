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
		logger.info(" Host : " + state.getConf().getRedisHost() + "   Port: " + state.getConf().getRedisPort());
		dbConnection = new Jedis(state.getConf().getRedisHost(), 
				state.getConf().getRedisPort());
		dbConnection.select(0);
	}
	
	public synchronized void updateLeader(int clusterId, String leaderNode){
		try{
			// uses default database 
			dbConnection.set(Integer.toString(clusterId), leaderNode);// use this to set leader on redis, 
			logger.info("Updating GSDN with new leader for cluster : " + clusterId);
		}catch(Exception e){
			logger.error("Error, while updating redis gsdn");
			e.printStackTrace();
		} 
	}
	
	public synchronized Node getLeader(int clusterId){
		Node node = null;
		try{
			//dbConnection.select(0);// uses default database 
			String value = dbConnection.get(Integer.toString(clusterId));// use this to set leader on redis,
			if(null != value){
				String [] arr = value.split(":");
				Node.Builder nb = Node.newBuilder();
				nb.setNodeId(clusterId);
				nb.setHost(arr[0]);
				nb.setPort(Integer.parseInt(arr[1]));
				node = nb.build();
				logger.info("getting leader for cluster : " + clusterId);
			}
		}catch(Exception e){
			logger.error("Error, while fetchin leader for cluster : " + clusterId);
			e.printStackTrace();
		} 
		return node;
	}
}
