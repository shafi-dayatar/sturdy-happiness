package gash.router.server.states;

import java.awt.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import gash.router.client.MessageClient;
import gash.router.server.IOUtility;
import gash.router.server.db.SqlClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.ServerState;
import gash.router.server.edges.EdgeMonitor;
import gash.router.server.log.LogInfo;
import gash.router.server.messages.LogAppend;
import pipe.common.Common.Header;
import pipe.election.Election;
import pipe.election.Election.LeaderElection;
import pipe.work.Work;
import pipe.work.Work.Command;
import pipe.work.Work.FileChunkData;
import pipe.work.Work.LogAppendEntry;
import pipe.work.Work.LogEntry;
import pipe.work.Work.LogEntry.*;
import pipe.work.Work.LogEntryList;
import pipe.work.Work.WorkMessage.MessageType;
import pipe.work.Work.Node;
import pipe.work.Work.WorkMessage;
import routing.Pipe;
import routing.Pipe.Chunk;
import routing.Pipe.ReadRequest;
import routing.Pipe.WriteRequest;

import com.google.protobuf.ByteString;

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
			
		if (nextIndex.get(nodeId) < state.getLog().lastIndex()
				&& logEntry.getPrevLogIndex() < state.getLog().lastIndex()) {
			logId = nextIndex.get(nodeId);
			LogEntry log = state.getLog().getEntry(logId);
			int lastLogIndex = log.getLogId();
			int lastLogTerm = log.getTerm();
			log = state.getLog().getEntry(logId);
			int commitIndex = state.getLog().getCommitIndex();
			WorkMessage wmsg = LogAppend.resendAppendRequest(state.getNodeId(), nodeId, state.getCurrentTerm(),
					commitIndex, logId, log, lastLogIndex, lastLogTerm);
			state.getOutBoundMessageQueue().addMessage(wmsg);
		}

	}

	/**
	 * After leader is elected, reinitialize nextIndex to lastIndex+1 and
	 * matchIndex to 0 of all followers
	 */

	private void reinitializeIndexes() {
		for (Node node : state.getEmon().getOutBoundRouteTable()) {
			// Next Index
			nextIndex.put(node.getNodeId(), state.getLog().lastIndex() + 1);
			// Match Index
			matchIndex.put(node.getNodeId(), (int) 0);
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
	public byte[] readFile(ReadRequest readBody) {
		return new IOUtility().readFile(readBody);
	}

	public WorkMessage createWriteFileMessage(ReadRequest writeMessage) {
		return null;
	}

	public WorkMessage createFileWriteMessage(int dest, int fileId, int chunkId, String FileName,
			ByteString chunkData) {
		WorkMessage.Builder msgBuilder = WorkMessage.newBuilder();
		msgBuilder.setSecret(9999999);
		msgBuilder.setType(MessageType.CHUNKFILEDATAWRITE);
		Header.Builder hd = Header.newBuilder();
		hd.setDestination(dest);
		hd.setNodeId(state.getNodeId());
		hd.setTime(System.currentTimeMillis());

		FileChunkData.Builder data = FileChunkData.newBuilder();
		data.setReplyTo(state.getNodeId());
		data.setFileId(fileId);
		data.setChunkId(chunkId);
		data.setFileName(FileName);
		data.setChunkData(chunkData);
		msgBuilder.setHeader(hd);
		msgBuilder.setChunkData(data);
		return msgBuilder.build();

	}

	@Override
	public int writeFile(WriteRequest write) {
		int fileId;
		synchronized (this) {
			fileId = (int) state.getDb().getFileId(write.getFilename(), write.getFileExt());
		}
		if (fileId != -1) {
			String fileName = write.getFilename();
			LogEntry.Builder logEntryBuilder = LogEntry.newBuilder();
			logEntryBuilder.setAction(DataAction.INSERT);
			String chunk_value = Integer.toString(fileId) + ":" + fileName + ":" + write.getFileExt() + ":"
					+ Integer.toString(write.getChunk().getChunkId()) + ":";
			Command.Builder command = Command.newBuilder();
			command.setKey("chunk");
			ArrayList<Node> followers = state.getEmon().getOutBoundRouteTable();
			int loc = new Random().nextInt(followers.size());
			for (int i = 0; i < 2; i++) {
				int currentLoc = (loc + i) % followers.size();
				Node node = followers.get(currentLoc);
				Chunk chunk = write.getChunk();
				WorkMessage msg = createFileWriteMessage(node.getNodeId(), fileId, chunk.getChunkId(), fileName,
						chunk.getChunkData());
				// logger.info(msg.toString());
				state.getOutBoundMessageQueue().addMessage(msg);
				chunk_value += node.getNodeId() + ",";
			}
			command.setValue(chunk_value);
			command.setClientId(999);
			logEntryBuilder.addData(command);
			appendEntries(logEntryBuilder);
		}

		return 0;// IOUtility.writeFile(write);
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
	public WorkMessage getWork() {
		return null;
	}

	@Override
	public void appendEntries(ArrayList<Builder> logEntryBuilder) {
		// TODO Auto-generated method stub

	}

}
