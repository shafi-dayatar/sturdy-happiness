package gash.router.server;

import gash.router.server.states.Candidate;
import gash.router.server.states.RaftServerState;

import gash.router.server.states.ElectionTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.container.RoutingConf;
import gash.router.server.edges.EdgeMonitor;
import gash.router.server.messages.MessageQueue;
import gash.router.server.states.Candidate;
import gash.router.server.states.ElectionTimer;
import gash.router.server.states.Follower;
import gash.router.server.states.Leader;
import gash.router.server.states.RaftServerState;
import gash.router.server.tasks.TaskList;

public class ServerState {
	private RaftServerState state;
	private RaftServerState leader;
	private RaftServerState candidate;
	private RaftServerState follower;
	private int currentTerm = 0;
	private int votedFor = 0;

	private RoutingConf conf;
	private EdgeMonitor emon;
	private TaskList tasks;
    private MessageQueue obmQueue;
    private MessageQueue ibmQueue;
    private ElectionTimer electionTimer;

	public ElectionTimer getElectionTimer(){
    	return electionTimer;
	}

	protected static Logger logger = LoggerFactory.getLogger("Server State");
    
    public ServerState(){ 	
    	leader = new Leader(this);
    	candidate = new Candidate(this);
    	follower = new Follower(this);
    	state = follower;
		this.electionTimer = new ElectionTimer(follower, 3, 10);
    	//electionTimer = new ElectionTimer(this);	
    }
    
    
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

	public void becomeFollower(){
		state = follower;	
	}
	
	public void becomeCandidate(){
		state = candidate;
		state.startElection();
	}
	
	public void becomeLeader(){
		state = leader;
	}
	
	public void setCurrentTerm(int currentTerm){
        this.currentTerm = currentTerm;
    }
	
    public int getCurrentTerm(){
        return this.currentTerm;
    }
    
    public void setVotedFor(int votedFor){
        this.votedFor = votedFor;
    }
    
    public int getVotedFor(){
        return this.votedFor;
    }
}
