package gash.router.server.messages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.protobuf.Message;

import gash.router.server.ServerState;

public class InBoundMessageQueue extends MessageQueue implements Runnable{

	protected static Logger logger = LoggerFactory.getLogger("InBound Queue");
	public InBoundMessageQueue(ServerState state){
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
				//fix type
				Message m = takeMessage();

				// Convert to appropraite type

				// Identify type of message

				// process message/ reply to ping/ forward if it is not for current node
				//if it is a Ping (Common function to identify )

				//


				logger.info("Class name is " + m.getClass().getName());
			
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
