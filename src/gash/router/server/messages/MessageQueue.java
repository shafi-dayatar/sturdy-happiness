package gash.router.server.messages;


import java.util.concurrent.LinkedBlockingDeque;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.ServerState;
import pipe.work.Work.WorkMessage;

public abstract class MessageQueue {
	protected static Logger logger = LoggerFactory.getLogger("MessageQueue");
	private LinkedBlockingDeque<WorkMessage> message = new LinkedBlockingDeque<WorkMessage>();
	private boolean forever = true;
	private ServerState state;
	
	public void addMessage(WorkMessage m){
		if (message != null){
			message.add(m);
		}
	}
	public void forward(){
		//TODO:
	}
	
	public boolean hasMessage(){
		if (message == null || message.size() == 0)
			return false;
		return true;
	}
	
	public int queueSize(){
		if (message == null)
			return 0;
		return message.size();
	}
	
	public WorkMessage takeMessage(){
		WorkMessage  m = null;
		if (message == null)
			return m;
		try{
			m =  message.take();
		}catch(Exception e){
			logger.error("Failed while remove message with error " + e);
		}
		return m;
	}
	public abstract void processMessage();
	public abstract void sendMessage();

	public boolean isForever() {
		return forever;
	}

	public void setForever(boolean forever) {
		this.forever = forever;
	}

	public ServerState getState() {
		return state;
	}

	public void setState(ServerState state) {
		this.state = state;
	}

}
