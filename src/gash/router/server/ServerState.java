package gash.router.server;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.container.RoutingConf;
import gash.router.server.communication.ConnectionManager;
import gash.router.server.db.ChunkRow;
import gash.router.server.db.RedisGSDN;
import gash.router.server.edges.EdgeMonitor;
import gash.router.server.log.LogInfo;
import gash.router.server.messages.DiscoverMessage;
import gash.router.server.queue.MessageQueue;
import gash.router.server.queue.ReadTaskQueue;
import gash.router.server.states.Candidate;
import gash.router.server.states.ElectionTimer;
import gash.router.server.states.Follower;
import gash.router.server.states.Leader;
import gash.router.server.states.RaftServerState;
import gash.router.server.tasks.TaskList;
import io.netty.channel.Channel;
import pipe.common.Common;
import pipe.common.Common.Node;
import pipe.common.Common.Response;
import pipe.work.Work;
import routing.Pipe;

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
	private RedisGSDN redis;

	private RoutingConf conf;
	private int nodeId;
	private EdgeMonitor emon;
	private TaskList tasks;
	private MessageQueue obmQueue;
	private MessageQueue ibmQueue;
	private ReadTaskQueue readTaskQueue;
	public ConnectionManager connectionManager = new ConnectionManager();

	private IOUtility db; //= new IOUtility();

	private LogInfo log = new LogInfo(this);

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
		startElectionTimerThread();
		
	}

	public void becomeCandidate() {
		logger.debug("I am Candidate and I need to start leader election ");
		currentTerm++;
		electionTimer.setElectionStartTime(System.currentTimeMillis());
		candidate.startElection();
		setLeaderKnown(false);
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
			setLeaderKnown(true);
			redis.updateLeader( getConf().getClusterId(),
					 DiscoverMessage.getCurrentIp() + ":" + getConf().getCommandPort());
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
	public int getLeaderId() {
		// TODO Auto-generated method stub
		return this.leaderNodeId;
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

	public RedisGSDN getRedis() {
		return redis;
	}

	public void setRedis(RedisGSDN redis) {
		this.redis = redis;
	}

	private int[] transformLocationAt(String location_at){

		String[] items = location_at.replaceAll("\\[", "").replaceAll("\\]", "").replaceAll("\\s", "").split(",");

		int[] locations = new int[items.length];
		for (int i = 0; i < items.length; i++) {
			try {
				locations[i] = Integer.parseInt(items[i]);
			} catch (NumberFormatException nfe) {
				//NOTE: write something here if you need to recover from formatting errors
				logger.error(" Exception while asserting servability. Location at may have been in invalid format ");

			}
		}
		return locations;
	}
	private int[] filterOutLeader(int[] locations){
		List<Integer> result = new ArrayList<Integer>();
		for (int i : locations){
			if(this.getNodeId() != i){
				result.add(i);
			}
		}
		int[] locs = new int[result.size()];
		int j =0;
		Iterator<Integer> iter = result.iterator();
		while (iter.hasNext()){
			locs[j] = iter.next();
			j++;
		}
		return locs;
	}
	private int getRandom(int[] arr){
		int rnd = new Random().nextInt(arr.length);
		return arr[rnd];
	}
	public int getRandomNodeWithChunk(int chunkid, int file_id){
		logger.info(" Getting node location for chunnk id :" + chunkid);
		ChunkRow chunkRow = getDb().getChunkRowById(chunkid, file_id);

		if(chunkRow!= null){
			int[] locations = transformLocationAt(chunkRow.getLocation_at());
			// now randomly pick a location and reroute the message to them

			locations = filterOutLeader(locations);
			if(locations.length == 0)
				return -1;

			return getRandom(locations);
		}
		logger.info(" chunk id not found ");
		return -1;

	}
	public boolean assertServability(Work.WorkMessage wmsg){
		Pipe.CommandMessage msg = wmsg.getReadCmdMessage();
		String filename = msg.getRequest().getRrb().getFilename();
		/*ChunkRow chunkRow = getDb().getChunkRowById(msg.getReq().getRrb().getChunkId(), );
		logger.info(" checking for " + chunkRow.getLocation_at() + " message req filename  " + filename);
		if(chunkRow!= null){

			if(chunkRow.getLocation_at().contains(Integer.toString(this.nodeId))) {
				logger.info(" node_id " + this.nodeId + " will steal for " + filename + " of type " + msg.getReq().getRequestType());
				//yes the node can steal this task now
				return true;
			} else {
				//
				// REROUTE TO A NODE WHICH CAN SERVE THE READ REQUEST
				//

				int[] locations = transformLocationAt(chunkRow.getLocation_at());
				// now randomly pick a location and reroute the message to them

				int randomNode = getRandom(locations);
				Work.WorkMessage.Builder wm = Work.WorkMessage.newBuilder(wmsg);

				Common.Header.Builder header = Common.Header.newBuilder(wmsg.getHeader());
				header.setDestination(randomNode);
				wm.setHeader(header.build());
				this.getOutBoundMessageQueue().addMessage(wm.build());
				return false;
			}

		}*/

		return false;
	}

	public ReadTaskQueue getReadTaskQueue() {
		return readTaskQueue;
	}

	public void setReadTaskQueue(ReadTaskQueue readTaskQueue) {
		this.readTaskQueue = readTaskQueue;
	}

	public void sendReadResponse(Channel channel, Response rsp, int clientNodeId){
		logger.info("Preparing to send read response for nodeid: "+ clientNodeId );
		Pipe.CommandMessage.Builder cmdMsg = Pipe.CommandMessage.newBuilder();
		Common.Header.Builder hd = Common.Header.newBuilder();
		hd.setNodeId(this.getNodeId());
		hd.setTime(System.currentTimeMillis());
		hd.setDestination(clientNodeId);
		cmdMsg.setResponse(rsp);
		cmdMsg.setHeader(hd);
		channel.writeAndFlush(cmdMsg.build());
		logger.info(" Sent read response to "  + cmdMsg.getHeader().getDestination());
	}

}
