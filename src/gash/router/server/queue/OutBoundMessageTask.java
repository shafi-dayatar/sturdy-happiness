package gash.router.server.queue;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.ServerState;
import gash.router.server.edges.EdgeInfo;
import gash.router.server.messages.MessageHandler;
import gash.router.server.messages.MessageInterface;
import io.netty.channel.Channel;
import pipe.work.Work.WorkMessage;

public class OutBoundMessageTask implements Runnable{

	protected static Logger logger = LoggerFactory.getLogger("Discovery Message");
	
	WorkMessage wmsg;
	ServerState state;
	
    public OutBoundMessageTask(WorkMessage wm, ServerState state){
    	this.state = state;
    	this.wmsg = wm;
    	
    }
	

	@Override
	public void run() {
		// TODO Auto-generated method stub

		int destinationId = wmsg.getHeader().getDestination();	
		ArrayList<EdgeInfo> connectedNode = state.getEmon().getOutBoundChannel(destinationId);
		//logger.info("Message in outbound queue : " + wmsg.toString());
		
		//logger.info("Channle is  " + (ch != null));
		if (destinationId == -1){
			for(EdgeInfo ei : connectedNode){
                Channel ch = ei.getChannel();
				if(ch != null){
					ch.writeAndFlush(wmsg);
				}
				else{
					//todo:
					//If it is not able to send message to particular node,
					//it should update the message and set's destination to particular node.
					logger.error("ERROR - no channel found for destination id " +  ei.getRef());
				}
			}
			return;
	    }
		if ( connectedNode != null){
			Channel ch = connectedNode.get(0).getChannel();
			if(ch != null){
				ch.writeAndFlush(wmsg);
			}
			else{
				logger.error("ERROR - no channel found for destination id " +  destinationId);
				//this.addMessage(m);
				// To Do, should try for x no of times before discarding
			}
		}
	 

	}

}