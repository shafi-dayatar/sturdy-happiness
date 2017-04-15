package gash.router.server.messages;

import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.protobuf.Message;

import gash.router.server.ServerState;
import pipe.work.Work;
import pipe.work.Work.WorkMessage;

public class InBoundMessageQueueWorker extends MessageQueue implements Runnable{

	protected static Logger logger = LoggerFactory.getLogger("InBound Queue");
	public InBoundMessageQueueWorker(ServerState state){
		this.setState(state);
		
	}
	
	
	@Override
	public void processMessage() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void sendMessage() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		while(isForever()){
			if(hasMessage()){
				logger.info("I am here !");
				//fix type
				WorkMessage m = takeMessage();

				// Convert to appropraite type

				// Identify type of message
                MessageInterface message = MessageHandler.IdentifyMessage(m);
                //Client
                // process message/ reply to ping/ forward if it is not for current node
				//if it is a Ping (Common function to identify )
                //TODO : Override process message for PingMEssage , HeartBeatMessage etc
                message.processMessage(getState());
			
			}else{
				try{
					Thread.sleep(100);
				}catch(Exception e){
					System.out.println("Error while sleeping");
				}
			}
		}
		
	}

}
