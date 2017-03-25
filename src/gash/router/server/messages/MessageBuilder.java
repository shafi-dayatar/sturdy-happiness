package gash.router.server.messages;

import io.netty.channel.Channel;
import pipe.work.Work.WorkMessage;

public class MessageBuilder {
	
	public static Message newBuilder(WorkMessage msg, Channel channel){
		Message m = null;
		if (msg.hasPing()){
			m = new PingMessage(msg, channel);
		}
		return m;
	}

}
