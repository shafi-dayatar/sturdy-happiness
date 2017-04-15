package gash.router.server.messages;

import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkMessage.MessageType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.PrintUtil;
public class MessageHandler {
	protected static Logger logger = LoggerFactory.getLogger("handler monitor");
	public static MessageInterface IdentifyMessage(WorkMessage msg){
		
		
		logger.info(" Msg recived ibn msg handler, type " + msg.getType());
		MessageInterface m = null;
		/*if (msg.hasPing()){
			//TODO: Set m to whichever type of message as below
			m = new PingMessage(msg, null);
		}*/
		switch(msg.getType()){
			case DISCOVERNODE :
				m =  new DiscoverMessage(msg);
				break;
		   	case DISCOVERNODEREPLY :
		   		m =  new DiscoverMessage(msg);
				break;
			case LEADERELECTION:
				m = new ElectionMessage(msg);
				break;
			case LEADERELECTIONREPLY:
				m = new ElectionMessage(msg);
				break;
			case HEARTBEAT:
				m = new LogAppend(msg);
				break;
			default:
			//case
			//hearbeat message ?

			break;
		}
		return m;
	}

}
