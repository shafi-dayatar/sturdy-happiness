package gash.router.server.messages;

import gash.router.server.PrintUtil;
import gash.router.server.ServerState;
import io.netty.channel.Channel;
import pipe.work.Work.WorkMessage;

public class OutBoundMessageQueue extends MessageQueue implements Runnable {

	public OutBoundMessageQueue(ServerState state){
		this.setState(state);
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		while(isForever()){
			try{
				if(hasMessage()){
					processMessage();
				}else{
					Thread.sleep(100);
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
		Channel ch = getState().getEmon().getOutBoundChannel(destinationId);
		if(ch != null){
			ch.writeAndFlush(m);
		}else{
			logger.error("no channel found for destination id " +  destinationId);
		}

	}

	@Override
	public void sendMessage() {
		// TODO Auto-generated method stub

	}

}
