package gash.router.server;

import gash.router.server.states.Candidate;
import gash.router.server.states.RaftServerState;

import gash.router.server.states.ElectionTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.container.RoutingConf;
import gash.router.server.edges.EdgeMonitor;
import gash.router.server.log.LogInfo;
import gash.router.server.queue.MessageQueue;
import gash.router.server.states.Candidate;
import gash.router.server.states.ElectionTimer;
import gash.router.server.states.Follower;
import gash.router.server.states.Leader;
import gash.router.server.states.RaftServerState;
import gash.router.server.tasks.TaskList;
import pipe.work.Work.Node;

public class ServerState {
	private RaftServerState raftState;
	private Leader leader;
	private Candidate candidate;
	private Follower follower;
	private ElectionTimer electionTimer;
	private Thread electionTimerThread;
	private Thread leaderThread;

	private int currentTerm = 0;
	private int leaderNodeId;
	private boolean isLeaderKnown = false;

	private RoutingConf conf;
	private int nodeId;
	private EdgeMonitor emon;
	private TaskList tasks;
	private MessageQueue obmQueue;
	private MessageQueue ibmQueue;

	private IOUtility db = new IOUtility();

	private LogInfo log = new LogInfo();

	public ElectionTimer getElectionTimer() {
		return electionTimer;
	}

	protected static Logger logger = LoggerFactory.getLogger("Server State");

	public ServerState() {
		leader = new Leader(this);
		candidate = new Candidate(this);
		follower = new Follower(this);
		raftState = follower;
		// this.
	}

	public RoutingConf getConf() {
		return conf;
	}

	public void setConf(RoutingConf conf) {
		this.conf = conf;
		this.setNodeId(conf.getNodeId());
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

	public void becomeFollower() {
		logger.info("There is only two way I could become a follower, either I stepped down from candidate or,"
				+ " I found the Leader ");
		raftState = follower;
		//startElectionTimerThread();
		
	}

	public void becomeCandidate() {
		logger.debug("I am Candidate and I need to start leader election ");
		currentTerm++;
		electionTimer.setElectionStartTime(System.currentTimeMillis());
		candidate.startElection();
		raftState = candidate;
	}

	public RaftServerState getRaftState() {
		logger.debug("My Current State is :  " + this.raftState.getClass().getName());
		return this.raftState;
	}

	public void becomeLeader() {
		logger.info("Becoming leader for election term : " + currentTerm);
		raftState = leader;
		if (!leader.isLeader()) {
			leader.setLeader(true);
			leader.setNextAndMatchIndex();
			if (leaderThread == null)
				leaderThread = new Thread(leader);

			leaderThread.start();
			electionTimer.stopThread();
		}
	}

	public void setCurrentTerm(int currentTerm) {
		this.currentTerm = currentTerm;
	}

	public int getCurrentTerm() {
		return this.currentTerm;
	}

	public int getNodeId() {
		return nodeId;
	}

	public void setNodeId(int nodeId) {
		this.nodeId = nodeId;
	}

	public int getLastLogIndex() {
		// TODO Auto-generated method stub
		return 0;
	}

	public int getLastLogTerm() {
		// TODO Auto-generated method stub
		return 0;
	}

	public void stopElectionTimerThread() {
		electionTimer.stopThread();
		electionTimer = null;
	}

	public void startElectionTimerThread() {
		if (electionTimer == null){
			electionTimer = new ElectionTimer(this, getConf().getHeartbeatDt() + 150,
					getConf().getHeartbeatDt() + 500);
			electionTimer.setForever(true);
			electionTimerThread = new Thread(electionTimer);
			
			electionTimerThread.start();
		}
	}

	public void setLeaderId(int leaderNodeId) {
		// TODO Auto-generated method stub
		this.leaderNodeId = leaderNodeId;
	}

	public boolean isLeaderKnown() {
		return isLeaderKnown;
	}

	public void setLeaderKnown(boolean isLeaderKnown) {
		this.isLeaderKnown = isLeaderKnown;
	}

	public LogInfo getLog() {
		return log;
	}

	public void setLog(LogInfo log) {
		this.log = log;
	}

	public Node getLeaderNode() {
		// TODO Auto-generated method stub
		return null;
	}

	public IOUtility getDb() {
		return db;
	}

	public void setDb(IOUtility db) {
		this.db = db;
	}

}
