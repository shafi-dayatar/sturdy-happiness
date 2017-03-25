package gash.router.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.container.RoutingConf;
import gash.router.server.edges.EdgeMonitor;
import gash.router.server.messages.MessageQueue;
import gash.router.server.tasks.TaskList;

public class ServerState {
	private RoutingConf conf;
	private EdgeMonitor emon;
	private TaskList tasks;
    private MessageQueue obmQueue;
    private MessageQueue ibmQueue;
    
    protected static Logger logger = LoggerFactory.getLogger("Server State");
    
	public RoutingConf getConf() {
		return conf;
	}

	public void setConf(RoutingConf conf) {
		this.conf = conf;
	}

	public EdgeMonitor getEmon() {
		return emon;
	}
	
	public MessageQueue getOutBoundMessageQueue() {
		return obmQueue;
	}
	
	public void setOutBoundMessageQueue(MessageQueue obmp) {
		this.obmQueue = obmp;
	}
	
	public MessageQueue getInBoundMessageQueue() {
		return ibmQueue;
	}
	
	public void setInBoundMessageQueue(MessageQueue ibmp) {
		this.ibmQueue = ibmp;
	}

	public void setEmon(EdgeMonitor emon) {
		this.emon = emon;
	}

	public TaskList getTasks() {
		return tasks;
	}

	public void setTasks(TaskList tasks) {
		this.tasks = tasks;
	}

}
