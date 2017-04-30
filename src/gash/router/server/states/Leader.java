package gash.router.server.states;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Random;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.ServerState;
import gash.router.server.db.SqlClient;
import gash.router.server.messages.DiscoverMessage;
import gash.router.server.messages.FileChunk;
import gash.router.server.messages.LogAppend;
import pipe.common.Common;
import pipe.common.Common.Header;
import pipe.common.Common.Node;
import pipe.election.Election;
import pipe.election.Election.LeaderElection;
import pipe.work.Work;
import pipe.work.Work.Command;
import pipe.work.Work.FileChunkData;
import pipe.work.Work.LogAppendEntry;
import pipe.work.Work.LogEntry;
import pipe.work.Work.LogEntry.Builder;
import pipe.work.Work.LogEntry.DataAction;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkMessage.MessageType;
import routing.Pipe.Chunk;
import routing.Pipe.ChunkLocation;
import routing.Pipe.CommandMessage;
import routing.Pipe.ReadBody;
import routing.Pipe.ReadResponse;
import routing.Pipe.Response;
import routing.Pipe.Response.Status;
import routing.Pipe.TaskType;
import routing.Pipe.WriteBody;


/**
 * Created by rentala on 4/11/17.
 */
public class Leader implements RaftServerState, Runnable {

	protected static Logger logger = LoggerFactory.getLogger("Leader-State");
	private ServerState state;
	SqlClient sqlClient;
	private int replicationFactor = 3; // this should come from config file.

	// stores the next logIndex to be sent to each follower
	Hashtable<Integer, Integer> nextIndex = new Hashtable<Integer, Integer>();
	// stores the last logIndex response from each follower
	Hashtable<Integer, Integer> matchIndex = new Hashtable<Integer, Integer>();

	private boolean isLeader;

	public Leader(ServerState state) {
		this.state = state;
	}

	@Override
	public void declareLeader() {
		logger.info("Sending heartbeat Messages to followers");
		WorkMessage hearbeat = createHeartBeatMessage();
		logger.info("Heartbeat message for followers is : " + hearbeat.toString());
		state.getOutBoundMessageQueue().addMessage(hearbeat);
	}

	public synchronized void appendEntries(LogEntry.Builder logEntryBuilder) {
		int lastLogIndex = state.getLog().lastIndex();
		int lastLogTerm = state.getLog().lastLogTerm(lastLogIndex);
		int currentLogIndex = lastLogIndex + 1;
		int commitIndex = state.getLog().getCommitIndex();

		logEntryBuilder.setTerm(state.getCurrentTerm());
		logEntryBuilder.setLogId(currentLogIndex);
		LogEntry entry = logEntryBuilder.build();

		logger.info("last log index :  " + lastLogIndex);
		logger.info("Current Log Id is : " + currentLogIndex);
		state.getLog().appendEntry(currentLogIndex, entry);

		Set<Integer> keys = nextIndex.keySet();
		logger.info("Last Log index of leader is : " + lastLogIndex);
		logger.info("Log appends entry is : " + logEntryBuilder.toString());
		for (Integer nodeId : keys) {
			int nextlogindex = nextIndex.get(nodeId);
			logger.info("NextIndex Table is : " + nextIndex.toString());
			if (currentLogIndex >= nextlogindex) {
				logger.debug("Sending LogAppend to all connected and catched up followers");
				WorkMessage wm = LogAppend.createLogAppendEntry(state.getNodeId(), nodeId, state.getCurrentTerm(),
						lastLogIndex, lastLogTerm, commitIndex, entry);
				logger.info(wm.toString());
				state.getOutBoundMessageQueue().addMessage(wm);
			}
		}
	}

	@Override

	// check condition
	public synchronized void logAppend(LogAppendEntry logEntry) {
		// TODO Auto-generated method stub
		logger.info("Got logAppend response from follower: " + logEntry.toString());
		int logId;
		int nodeId = logEntry.getLeaderNodeId();
		if (!nextIndex.containsKey(nodeId)) {
			nextIndex.put(nodeId, logEntry.getPrevLogIndex() + 1);
			matchIndex.put(nodeId, 0);
		}
		if (logEntry.getSuccess()) {
			updateNextAndMatchIndex(nodeId, logEntry.getPrevLogIndex());
		} 
		logger.info("nextIndex.get(nodeId) = " + nextIndex.get(nodeId));
		logger.info("state.getLog().lastIndex() " + state.getLog().lastIndex());
		logger.info(" logEntry.getPrevLogIndex()" + logEntry.getPrevLogIndex());
		logger.info("condition : nextIndex.get(nodeId) <= state.getLog().lastIndex() " + 
				"&& logEntry.getPrevLogIndex() < state.getLog().lastIndex()" + 
				(nextIndex.get(nodeId) <= state.getLog().lastIndex()
				&& logEntry.getPrevLogIndex() < state.getLog().lastIndex()) );
		logger.info("nextIndex = " + nextIndex.toString());
		logger.info("MatchIndex = " + matchIndex.toString());
			
		if (nextIndex.get(nodeId) <= state.getLog().lastIndex()
				&& logEntry.getPrevLogIndex() < state.getLog().lastIndex()) {
			logger.info("My Last Index is : " + state.getLog().lastIndex());
			logId = nextIndex.get(nodeId);
			logger.info("Follower next index is " + logId);
			logger.info("Next index : " + nextIndex.toString() + " Match Index : " +  matchIndex.toString());
			LogEntry log = state.getLog().getEntry(logId);
			int lastLogIndex = log.getLogId();
			int lastLogTerm = log.getTerm();
			log = state.getLog().getEntry(logId);
			int commitIndex = state.getLog().getCommitIndex();
			WorkMessage wmsg = LogAppend.resendAppendRequest(state.getNodeId(), nodeId, state.getCurrentTerm(),
					commitIndex, logId, log, lastLogIndex, lastLogTerm);
			logger.info("Resending data: " + wmsg.toString());
			state.getOutBoundMessageQueue().addMessage(wmsg);
		}

	}

	/**
	 * When AppendRequest is successful for a node, update it's nextIndex to
	 * lastIndex+1 and matchIndex to lastIndex
	 */
	public synchronized void updateNextAndMatchIndex(int nodeId, int lastIndex) {

		logger.info("Updating next and match index for follower : " + nodeId + "\n" + "last log insert on node was"
				+ lastIndex);
		if (!matchIndex.containsKey(nodeId)) {
			// Next Index
			nextIndex.put(nodeId, state.getLog().lastIndex() + 1);
			// Match Index
			matchIndex.put(nodeId, (int) 0);
		}

		if (matchIndex.get(nodeId) <= lastIndex - 1) {
			int currentCommitIndex = state.getLog().getCommitIndex();
			logger.info("current commmit index is : " + currentCommitIndex);
			nextIndex.put(nodeId, lastIndex + 1);
			matchIndex.put(nodeId, lastIndex);

			logger.info("Match Index is : " + matchIndex.toString());
			logger.info("Next Index is : " + nextIndex.toString());
			Set<Integer> keys = matchIndex.keySet();
			int refelectedOn = 1;
			for (Integer id : keys) {
				if (currentCommitIndex <= matchIndex.get(id)) {
					refelectedOn++;
				}
			}
			logger.info("Log Entry appended on : " + refelectedOn + " servers..... ");
			if (refelectedOn >= (state.getEmon().getTotalNodes() / 2 + 1)
					&& state.getLog().getCommitIndex() < lastIndex) {
				logger.info("Time To Commit, everything looks perfect");
				updateCommitIndex(++currentCommitIndex);
			}
		}
	}

	public void updateCommitIndex(int lastIndex) {
		state.getLog().setCommitIndex(lastIndex);
	}

	public WorkMessage createHeartBeatMessage() {
		int lastIndex = state.getLog().lastIndex();
		WorkMessage.Builder wmb = WorkMessage.newBuilder();

		Header.Builder hdb = Header.newBuilder();
		hdb.setNodeId(state.getNodeId());
		hdb.setDestination(-1);
		hdb.setTime(System.currentTimeMillis());

		LogAppendEntry.Builder heartbeat = LogAppendEntry.newBuilder();
		heartbeat.setElectionTerm(state.getCurrentTerm());
		heartbeat.setLeaderNodeId(state.getNodeId());
		heartbeat.setPrevLogIndex(lastIndex);
		heartbeat.setPrevLogTerm(state.getLog().lastLogTerm(lastIndex));

		wmb.setSecret(5555);
		wmb.setType(MessageType.HEARTBEAT);
		wmb.setHeader(hdb);
		wmb.setLogAppendEntries(heartbeat);
		return wmb.build();

	}

	@Override
	public void heartbeat(LogAppendEntry heartbeat) {
		// TODO Auto-generated method stub
		if (state.getCurrentTerm() < heartbeat.getElectionTerm()) {
			// leader should step down, as it was previously elected and
			// something went wrong
			state.setCurrentTerm(heartbeat.getElectionTerm());
			setLeader(false);
			state.becomeFollower();
		}

	}

	@Override
	public void run() {
		while (isLeader) {

			declareLeader();
			try {
				logger.info("Will Send a hearbeat message in " + state.getConf().getHeartbeatDt());
				Thread.sleep(state.getConf().getHeartbeatDt());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public boolean isLeader() {
		return isLeader;
	}

	public void setLeader(boolean isLeader) {
		this.isLeader = isLeader;
	}

	@java.lang.Override
	public void requestVote(LeaderElection leaderElectionRequest) {

	}

	@java.lang.Override
	public void startElection() {

	}

	@java.lang.Override
	public void leaderElect() {

	}

	@java.lang.Override
	public void collectVote(Election.LeaderElectionResponse leaderElectionResponse) {
		/**
		 * if leader has been elected than there is no need to process other
		 * node response.
		 */
		return;

	}

	@Override
	public routing.Pipe.Response getFileChunkLocation(ReadBody request) {
		String fileName = request.getFilename();
		Integer [][] chunks = state.getDb().getChunks(fileName);
		
		routing.Pipe.Response.Builder res = Response.newBuilder();
		res.setFilename(fileName);
		res.setResponseType(TaskType.RESPONSEREADFILE);
		
		ReadResponse.Builder rr = ReadResponse.newBuilder();
		rr.setFilename(fileName);
		
		if (chunks[0].length == 1){
			if (chunks[0][0] == -1){
				logger.info("File not found on server");
				res.setStatus(Status.FILENOTFOUND);
			}
			else if (chunks[0][0] == -2){
				logger.info("File is incomplete, all chunks have not received ");
				res.setStatus(Status.INCOMPLETEFILE);
			}
				
		}else{
			res.setStatus(Status.SUCCESS);
			
	        Node.Builder node = Node.newBuilder();
			node.setHost(DiscoverMessage.getCurrentIp());
			node.setNodeId(state.getNodeId());
			node.setPort(state.getConf().getCommandPort());
			int i ;
			for( i= 0; i < chunks.length; i++){
				ChunkLocation.Builder chunkloc = ChunkLocation.newBuilder();
				chunkloc.addNode(node);
				chunkloc.setChunkid(chunks[i][1]);
				rr.addChunkLocation(chunkloc);
			}
			rr.setNumOfChunks(i);
			
		}
		res.setReadResponse(rr);
		
		return res.build();//new IOUtility().readFile(readBody);
	}

	@Override
	public Status writeFile(WriteBody request) {
		int fileId;
		try{
			synchronized (this){
				fileId = (int) state.getDb().getFileId(request.getFilename(), request.getFileExt(), request.getNumOfChunks());
			}
			if (fileId != -1) {
				String fileName = request.getFilename();
				LogEntry.Builder logEntryBuilder = LogEntry.newBuilder();
				logEntryBuilder.setAction(DataAction.INSERT);
				String chunk_value = Integer.toString(fileId) + ":" + fileName + ":" + request.getFileExt() + ":"
						+ Integer.toString(request.getChunk().getChunkId()) + ":";
				Command.Builder command = Command.newBuilder();
				command.setKey("chunk");
				ArrayList<Node> followers = state.getEmon().getOutBoundRouteTable();
				int loc = new Random().nextInt(followers.size());
				HashSet<Integer> location = new HashSet<Integer>();
				for (int i = 0; i < replicationFactor ; i++) {
					int currentLoc = (loc + i) % followers.size();
					Node node = followers.get(currentLoc);
					location.add(node.getNodeId());
					Chunk chunk = request.getChunk();
					WorkMessage msg = FileChunk.createFileWriteMessage(state.getNodeId(), node.getNodeId(), fileId, 
							chunk.getChunkId(), fileName, chunk.getChunkData());
					state.getOutBoundMessageQueue().addMessage(msg);	
				}
				chunk_value += location.toString() + ":" +request.getNumOfChunks();
				command.setValue(chunk_value);
				command.setClientId(999);
				logEntryBuilder.addData(command);
				appendEntries(logEntryBuilder);
			}
		}catch(Exception e){
			logger.info("Found exception while writing file");
			e.printStackTrace();
			return Status.ERROR;
		}
		return Status.SUCCESS;// IOUtility.writeFile(write);
	}

	public void setNextAndMatchIndex() {
		// reinitializeIndexes();
		// TODO Auto-generated method stub
		ArrayList<Node> nodes = state.getEmon().getOutBoundRouteTable();
		for (Node node : nodes) {
			nextIndex.put(node.getNodeId(), state.getLog().lastIndex() + 1);
			matchIndex.put(node.getNodeId(), 0);
		}

	}

	@Override
	public void readChunkData(FileChunkData chunk) {
		// TODO Auto-generated method stub

	}

	@Override
	public void writeChunkData(FileChunkData chunk) {
		// TODO Auto-generated method stub

	}

	@Override
	public void readChunkDataResponse(FileChunkData chunk) {
		// TODO Auto-generated method stub
		logger.info("Got A File Read Response");

	}

	@Override
	public void writeChunkDataResponse(FileChunkData chunk) {
		// TODO Auto-generated method stub
		logger.info("Got A file write response");

	}

	@Override
	public void stealWork() {
		//leader doesnt steal the work

	}

	@Override
	public CommandMessage getWork(int node_id) {
		return null;
	}
	// leader doesnt server read requests
	// routes it to a random available node
	@Override
	public void processReadRequest(CommandMessage cmdMsg) {

		int chunk_id = cmdMsg.getRequest().getRrb().getChunkId();
		int file_id = state.getDb().getFileId(cmdMsg.getRequest().getRrb().getFilename());
		int client_id = cmdMsg.getHeader().getNodeId();
		Common.Header.Builder hd = Common.Header.newBuilder();
		//set to whichever node it may set it to
		int[] destinations = state.getNodeLocations(chunk_id, file_id);
		int dest = state.getRandom(destinations);
		if(dest > 0 ){
			//chunk exists
			hd.setDestination(dest);
			hd.setNodeId(state.getNodeId());
			hd.setTime(System.currentTimeMillis());
			hd.setMessageId(client_id);
			//msgBuilder.setHeader(hd);
			CommandMessage.Builder cmdBuilder = CommandMessage.newBuilder();
			cmdBuilder.setHeader(hd.build());
			cmdBuilder.setMessage(state.ArrayToString(destinations));
			cmdBuilder.setRequest(cmdMsg.getRequest());
			state.getOutBoundReadTaskQueue().addMessage(cmdBuilder.build());
		}
		else{
			//invalid chunk id
			System.out.println(" Invalid chunk id recived while adding to InBoundReadTaskQueue");
		}
	}

	@Override
	public void appendEntries(ArrayList<Builder> logEntryBuilder) {
		// TODO Auto-generated method stub

	}

}
