package gash.router.server;

import gash.router.server.states.Candidate;
import gash.router.server.states.NodeState;

import gash.router.server.states.ElectionTimer;
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
    private NodeState nodeState;
    private ElectionTimer electionTimer;

    public ServerState(){
    	//initially a candidate
    	this.nodeState = new Candidate();
    	this.electionTimer = new ElectionTimer(this.nodeState, 3, 10);
	}
	public ElectionTimer getElectionTimer(){
    	return electionTimer;
	}
	public NodeState getNodeState() {
		return nodeState;
	}
	public void setNodeState(NodeState nodeState){
		this.nodeState = nodeState;
	}

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
