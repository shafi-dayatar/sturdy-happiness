package gash.router.server.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.ServerState;
import gash.router.server.messages.MessageHandler;
import gash.router.server.messages.MessageInterface;
import pipe.work.Work.WorkMessage;

public class InBoundMessageTask implements Runnable {
	protected static Logger logger = LoggerFactory.getLogger("Discovery Message");
	WorkMessage wmsg;
	ServerState state;
	
    public InBoundMessageTask(WorkMessage wm, ServerState state){
    	this.state = state;
    	this.wmsg = wm;
    }
    public WorkMessage getWorkMessage(){
    	return wmsg;
	}

    @Override
    public void run() {
    	//logger.info("Got a Message in inbound queue");
    	//logger.info(wmsg.toString());
    	MessageInterface message = MessageHandler.IdentifyMessage(wmsg);
    	message.processMessage(state);
    }




}
