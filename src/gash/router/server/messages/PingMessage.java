package gash.router.server.messages;

import io.netty.channel.Channel;
import pipe.work.Work.WorkMessage;

public class PingMessage extends Message{
	
	
	public PingMessage(WorkMessage wm, Channel ch){
		setChannel(ch);
		unPackMessage(wm);
	}
	
	public void unPackMessage(WorkMessage msg){
		unPackHeader( msg.getHeader());	
		
	}
	
	public void respond(){
		System.out.println("In Ping Reply");
		WorkMessage.Builder wm = WorkMessage.newBuilder();
		//setReply(true);
		setMaxHops(10);
		//setReplyFrom(getDestinationId());
		setDestId(getNodeId());
		wm.setHeader(createHeader());
		wm.setPing(true);
		wm.setSecret(getSecret());
		//return wm.build();
	}
	
	public WorkMessage forward(){
		return null;
	}
	
	public WorkMessage processMessage(int nodeId){
		//System.out.println("nodeId" + nodeId + "getDestinationId" + getDestinationId());
		if(nodeId == getDestId()){
			//return respond();
		}
		return forward();
	}


}
