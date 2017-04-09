package gash.router.server.messages;

import io.netty.channel.Channel;
import pipe.common.Common.Header;
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
		setMaxHops(10);
		setReplyFrom(getDestinationId());
		setDestinationId(getNodeId());
		wm.setHeader(createHeader());
		wm.setPing(true);
		wm.setSecret(getSecret());
		return wm.build();
	}
	
	public WorkMessage forward(){
		if(getMaxHops() > 0){
			setMaxHops(getMaxHops() - 1);
			Header hd = createHeader();
			WorkMessage.Builder wb = WorkMessage.newBuilder();
			wb.setHeader(hd);
			wb.setSecret(new Integer(123123123));
			wb.setPing(true);
			return wb.build();
		}
		return null;
	}
	public WorkMessage processMessage(int nodeId){
		if(nodeId == getDestinationId()){
			if(isReply() == false){
			    return pingReply();
			}else{
				System.out.println("Got a reply from the node with id " + getReplyFrom());
				return null;
			}
		}
		return forward();
	}
	
	

}
