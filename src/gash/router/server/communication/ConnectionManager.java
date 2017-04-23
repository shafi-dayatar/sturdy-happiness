package gash.router.server.communication;

import java.util.Hashtable;
import java.util.Map;


import io.netty.channel.Channel;

public class ConnectionManager {
	
	private Hashtable<Integer, Channel> client_channel = new Hashtable<Integer, Channel>();
	
	
	public void setConnection(int clientId, Channel ch){
		if(!client_channel.contains(clientId))
		    client_channel.put(clientId, ch);
	}
	
	public Channel getConnection(int clientId){
		if(client_channel.contains(clientId))
			return client_channel.get(clientId);
		return null;
	}

}
