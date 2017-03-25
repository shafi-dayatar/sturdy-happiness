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
	
	public WorkMessage pingReply(){
		WorkMessage.Builder wm = WorkMessage.newBuilder();
		setReply(true);
		setReplyFrom(getDestinationId());
		setDestinationId(getNodeId());
		wm.setHeader(createHeader());
		wm.setPing(true);
		wm.setSecret(getSecret());
		return wm.build();
	}
	
	

}
