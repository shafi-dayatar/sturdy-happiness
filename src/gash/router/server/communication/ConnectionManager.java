package gash.router.server.communication;

import java.util.Hashtable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

public class ConnectionManager {
	
	protected Logger logger = LoggerFactory.getLogger("Connection Manager");
	private Hashtable<Integer, Channel> client_channel = new Hashtable<Integer, Channel>();
	
	
	public void setConnection(int clientId, Channel ch){
		if(!client_channel.containsKey(clientId)){
			closeListner(clientId, ch);
		    client_channel.put(clientId, ch);
		    logger.info("Got a new connection from client with node id: " + clientId);
		}
		
	}
	
	public Channel getConnection(int clientId){
		if(client_channel.containsKey(clientId))
			return client_channel.get(clientId);
		return null;
	}
	
	public void closeListner(final int clientId, Channel ch){
		ChannelFuture closeFuture = ch.closeFuture();

		   closeFuture.addListener(new ChannelFutureListener() {
		        @Override
		        public void operationComplete(ChannelFuture future) throws Exception {
		           client_channel.remove(clientId);
		           logger.info("Client closed connection, Removing connection channel from connectino Manager");
		        }
		    });
	}

}
