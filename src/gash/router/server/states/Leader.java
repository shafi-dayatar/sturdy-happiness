package gash.router.server.states;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import gash.router.server.db.SqlClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.ServerState;
import gash.router.server.edges.EdgeMonitor;
import gash.router.server.log.LogInfo;
import pipe.common.Common.Header;
import pipe.election.Election;
import pipe.election.Election.LeaderElection;
import pipe.work.Work;
import pipe.work.Work.Command;
import pipe.work.Work.LogAppendEntry;
import pipe.work.Work.LogEntry;
import pipe.work.Work.LogEntry.*;
import pipe.work.Work.LogEntryList;
import pipe.work.Work.WorkMessage.MessageType;
import pipe.work.Work.Node;
import pipe.work.Work.WorkMessage;
import routing.Pipe;
import routing.Pipe.Chunk;
import com.google.protobuf.ByteString;
/**
 * Created by rentala on 4/11/17.
 */
public class Leader implements RaftServerState, Runnable {

    protected static Logger logger = LoggerFactory.getLogger("Leader-State");
    private ServerState state;
    private LogInfo log;
    // stores the next logIndex to be sent to each follower
 	TreeMap<Integer, Integer> nextIndex = new TreeMap<Integer, Integer>();
 	// stores the last logIndex sent to each follower
 	TreeMap<Integer, Integer> matchIndex = new TreeMap<Integer, Integer>();
    private boolean isLeader;
    private SqlClient sqlClient;
    public Leader(ServerState state){
        this.state = state;
    }
    public void appendEntries(String entry){
        logger.info("appendEntries = " + entry);
    }
    
    /**
	 * Build AppendRequest for heart beat with 0 log entries
	 */
	public WorkMessage getAppendRequest(int fNode) {
		WorkMessage.Builder wmb = WorkMessage.newBuilder();
		Header.Builder hdb = Header.newBuilder();
		hdb.setNodeId(state.getNodeId());
		hdb.setTime(System.currentTimeMillis());
		hdb.setDestination(fNode);
		
	    wmb.setHeader(hdb.build());

	    LogAppendEntry.Builder le = LogAppendEntry.newBuilder();
		le.setElectionTerm(state.getCurrentTerm());
		le.setPrevLogIndex(log.lastIndex());
		le.setPrevLogTerm(log.lastIndex() != 0 ? log.getEntry(log.lastIndex()).getTerm() : 0);
		le.setLeaderCommitIndex(log.getCommitIndex());
		le.setLeaderNodeId(state.getNodeId());

		wmb.setType(WorkMessage.MessageType.LOGAPPENDENTRY);
		wmb.setSecret(11111);
		wmb.setLogAppendEntries(le.build());
		return wmb.build();
	}
    
    /**
	 * Build AppendRequest to send to a follower with log entries
	 * starting from logStartIndex to latestIndex
	 */
	public WorkMessage getAppendRequest(int fNode, int logStartIndex) {
		WorkMessage.Builder wmb = WorkMessage.newBuilder();
		Header.Builder hdb = Header.newBuilder();
		hdb.setNodeId(state.getNodeId());
		hdb.setTime(System.currentTimeMillis());
		hdb.setDestination(fNode);
		
	    wmb.setHeader(hdb.build());
	    
	    LogEntryList.Builder l = LogEntryList.newBuilder();
		l.addAllEntry(Arrays.asList(log.getEntries(logStartIndex)));
		
	    LogAppendEntry.Builder le = LogAppendEntry.newBuilder();
		le.setElectionTerm(state.getCurrentTerm());
		le.setPrevLogIndex(logStartIndex -1);
		le.setPrevLogTerm((logStartIndex - 1) != 0 ? log.getEntry(logStartIndex - 1).getTerm() : 0);
		le.setLeaderCommitIndex(log.getCommitIndex());
		le.setLeaderNodeId(state.getNodeId());
		le.setEntrylist(l.build());	    

	    wmb.setLogAppendEntries(le.build());
		wmb.setType(WorkMessage.MessageType.LOGAPPENDENTRY);
		wmb.setSecret(11111);
		return wmb.build();
	}

    /**
	 Insert a new log entry and send to candidates
	 */
	public void appendLogEntry(Command cmd, boolean isInsert) {
			LogEntry.Builder leb = LogEntry.newBuilder();
			if(isInsert) {
				leb.setAction(DataAction.INSERT);
				leb.setData(cmd);
				leb.setTerm(state.getCurrentTerm());
				log.appendEntry(leb.build());
			}
			
			sendAppendRequest(getAppendRequestForFollowers());
			
	}
	
	/**
	 * Send appendRequest to every node from leader
	 */
	public void sendAppendRequest(Map<Integer,WorkMessage> appendRequests) {
		for(Integer nodeId : appendRequests.keySet()) {
			if(appendRequests.get(nodeId) != null){}
				
		
				/*send to followers sendToNode(nodeId, appendRequests.get(nodeId)); */
		}
	}
	
	/**
	 * Build AppendRequests for all the followers with entries starting 
	 * from nextIndex for that follower
	 */
	public Map<Integer, WorkMessage> getAppendRequestForFollowers() {
		Map<Integer, WorkMessage> appendRequests = new HashMap<Integer, WorkMessage>();

		for (Entry<Integer, Integer> nodeIndex : nextIndex.entrySet()) {
			if (log.lastIndex() >= nodeIndex.getValue()) {
				appendRequests.put(
						nodeIndex.getKey(),
						getAppendRequest(nodeIndex.getKey(),
								nodeIndex.getValue()));
			}
		}
		return appendRequests;
	}
	
	
	/**
	 * After leader is elected, reinitialize nextIndex 
	 * to lastIndex+1 and matchIndex to 0 of all followers
	 */
	
	private void reinitializeIndexes() {
		for (Node node : state.getEmon().getOutBoundRouteTable()) {
			// Next Index
			nextIndex.put(node.getNodeId(), log.lastIndex() + 1);
			// Match Index
			matchIndex.put(node.getNodeId(), (int) 0);
		}
	}
	
	/**
	 * If follower rejects an AppendRequest due to log mismatch,
	 * decrement the follower's next index and return the
	 * decremented index
	 */
	public long getDecrementedNextIndex(int nodeId) {
		nextIndex.put(nodeId, nextIndex.get(nodeId) - 1);
		return nextIndex.get(nodeId);
	}
	
	/**
	 * When AppendRequest is successful for a node, update it's 
	 * nextIndex to lastIndex+1 and matchIndex to lastIndex
	 */
	public void updateNextAndMatchIndex(int nodeId) {
		if(!matchIndex.containsKey(nodeId)) {
			// Next Index
			nextIndex.put(nodeId, log.lastIndex() + 1);
			// Match Index
			matchIndex.put(nodeId, (int) 0);
		}
		
		if(matchIndex.get(nodeId) != log.lastIndex()) {
			nextIndex.put(nodeId, log.lastIndex() + 1);
			matchIndex.put(nodeId, log.lastIndex());
			// check if commit index is also increased
			updateCommitIndex();
		}
	}
	
	
	public void updateCommitIndex() {
		log.setCommitIndex(log.lastIndex());		
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
    	 * if leader has been elected than there is no need to process other node response.
    	 */
    	return;
        
    }

	@Override
	public void declareLeader() {
		WorkMessage hearbeat = createHeartBeatMessage();
		state.getOutBoundMessageQueue().addMessage(hearbeat);	
	}
	
	public WorkMessage createHeartBeatMessage(){
		WorkMessage.Builder wmb = WorkMessage.newBuilder();
		
		
		Header.Builder hdb = Header.newBuilder();
		hdb.setNodeId(state.getNodeId());
		hdb.setDestination(-1);
		hdb.setTime(System.currentTimeMillis());
		
		
		LogAppendEntry.Builder heartbeat = LogAppendEntry.newBuilder();
		heartbeat.setElectionTerm(state.getCurrentTerm());
		heartbeat.setLeaderNodeId(state.getNodeId());
		heartbeat.setPrevLogIndex(state.getLastLogIndex());
		heartbeat.setPrevLogTerm(state.getCurrentTerm() - 1);
		
		wmb.setSecret(5555);
		wmb.setType(MessageType.HEARTBEAT);
		wmb.setHeader(hdb);
		wmb.setLogAppendEntries(heartbeat);
		return wmb.build();

	}
	@Override
	public void heartbeat(LogAppendEntry heartbeat) {
		// TODO Auto-generated method stub
		if (state.getCurrentTerm()<heartbeat.getElectionTerm()){
			//leader should step down, as it was previously elected and something went wrong
			state.setCurrentTerm(heartbeat.getElectionTerm());
			state.becomeFollower();
		}
		
	}

	@Override
	public void readFile(Pipe.ReadBody readBody) {

	}

	@Override
	public void writeFile(Pipe.WriteBody readBody) {
		Chunk chunk = readBody.getChunk();
		ByteString bs = chunk.getChunkData();
		sqlClient.storefile(chunk.getChunkId(), bs.newInput(), readBody.getFilename());
	}

	@Override
	public void deleteFile(Pipe.ReadBody readBody) {

	}

	@Override
	public void logAppend(LogAppendEntry logEntry) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void run() {
		while(isLeader){
			declareLeader();
			try {
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

}

