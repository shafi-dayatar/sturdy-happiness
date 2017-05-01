package gash.router.server.queue;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.ServerState;
import gash.router.server.edges.EdgeInfo;
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
		logger.debug("Message in outbound queue : " + wmsg.toString());
		if (destinationId == -1){
			logger.info("Broadcast message found in queue");
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
		logger.info(" ------> sending to -- > destinationId " + destinationId );
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