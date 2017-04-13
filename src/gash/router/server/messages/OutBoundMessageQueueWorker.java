package gash.router.server.messages;

import java.util.ArrayList;

import gash.router.server.PrintUtil;
import gash.router.server.ServerState;
import gash.router.server.edges.EdgeInfo;
import io.netty.channel.Channel;
import pipe.work.Work.WorkMessage;

public class OutBoundMessageQueueWorker extends MessageQueue implements Runnable {

	public OutBoundMessageQueueWorker(ServerState state){
		this.setState(state);
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		while(isForever()){
			try{
				if(hasMessage()){
					//logger.info("Message in queues are : " + hasMessage() );
					processMessage();
					Thread.sleep(1000);
					
				}else{
					Thread.sleep(1000);
				}
			}catch(Exception e){
				logger.error("Error in OutBoundMessageQueue thread : " + e);
			}
		}

	}

	@Override
	public void processMessage() {
		// TODO Auto-generated method stub
		WorkMessage m = takeMessage();
		
		int destinationId = m.getHeader().getDestination();	
		ArrayList<EdgeInfo> connectedNode = getState().getEmon().getOutBoundChannel(destinationId);
		
		//logger.info("Channle is  " + (ch != null));
		if (destinationId == -1){
			for(EdgeInfo ei : connectedNode){
                Channel ch = ei.getChannel();
				if(ch != null){
					ch.writeAndFlush(m);
				}
				else{
					//todo:
					//If it is not able to send message to particular node,
					//it should update the message and set's destination to particular node.
				}
			}
			return;
	    }
		if ( connectedNode != null){
			Channel ch = connectedNode.get(0).getChannel();
			if(ch != null){
				ch.writeAndFlush(m);
			}
			else{
				//logger.error("no channel found for destination id " +  destinationId);
				//PrintUtil.printWork(m);
				this.addMessage(m);
				// To Do, should try for x no of times before discarding
			}
		}
	 

	}

	@Override
	public void sendMessage() {
		// TODO Auto-generated method stub

	}

}
