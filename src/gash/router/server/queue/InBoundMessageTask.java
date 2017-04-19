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

public class InBoundMessageTask implements Runnable {
	protected static Logger logger = LoggerFactory.getLogger("Discovery Message");
	WorkMessage wmsg;
	ServerState state;
	
    public InBoundMessageTask(WorkMessage wm, ServerState state){
    	this.state = state;
    	this.wmsg = wm;
    }

    @Override
    public void run() {
    	//logger.info("Got a Message in inbound queue");
    	//logger.info(wmsg.toString());
    	MessageInterface message = MessageHandler.IdentifyMessage(wmsg);
    	message.processMessage(state);
    }




}
