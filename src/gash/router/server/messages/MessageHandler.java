package gash.router.server.messages;

import pipe.work.Work.WorkMessage;

public class MessageHandler {
	
	public static MessageInterface IdentifyMessage(WorkMessage msg){
		MessageInterface m = null;
		if (msg.hasPing()){
			//TODO: Set m to whichever type of message as below
			m = new PingMessage(msg, null);
		}
		return m;
	}

}
