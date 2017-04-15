package gash.router.server.states;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.Set;

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

/**
 * Created by rentala on 4/11/17.
 */
public class Leader implements RaftServerState, Runnable {

    protected static Logger logger = LoggerFactory.getLogger("Leader-State");
    private ServerState state;
    private LogInfo log;
    
    
    // stores the next logIndex to be sent to each follower
 	Hashtable<Integer, Integer> nextIndex = new Hashtable<Integer, Integer>();
 	// stores the last logIndex response from each follower
 	Hashtable<Integer, Integer> matchIndex = new Hashtable<Integer, Integer>();
 	
    private boolean isLeader;

    public Leader(ServerState state){
        this.state = state;
    }
    public synchronized void appendEntries(ArrayList<LogEntry.Builder> logEntryBuilder){
        //logger.info("appendEntries = " + entry);
        for (LogEntry.Builder builder : logEntryBuilder){
        	builder.setLogId(log.getLogIndex());
            builder.setTerm(state.getCurrentTerm());
            LogEntry entry  = builder.build();
            log.appendEntry(entry);
            Set<Integer> keys = nextIndex.keySet();
            for(Integer nodeId : keys){
            	int nextlogindex = nextIndex.get(keys);
            	if (state.getLastLogIndex() >= nextlogindex){
            		WorkMessage wm = createLogAppendEntry(nodeId, entry);
            		state.getOutBoundMessageQueue().addMessage(wm);
            	}
            }
        }
    }
    
    /**
	 * Build AppendRequest for heart beat with 0 log entries
	 */
    /*
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
	}*/
    
    
	/**
	 * Build AppendRequest to send to a follower with log entries
	 * starting from logStartIndex to latestIndex
	 */
	public WorkMessage resendAppendRequest(int fNode, int logIndex) {
		WorkMessage.Builder wmb = WorkMessage.newBuilder();
		Header.Builder hdb = Header.newBuilder();
		hdb.setNodeId(state.getNodeId());
		hdb.setTime(System.currentTimeMillis());
		hdb.setDestination(fNode);
		
	    wmb.setHeader(hdb.build());
	    
	    LogEntryList.Builder l = LogEntryList.newBuilder();
		l.addAllEntry(Arrays.asList(log.getEntries(logIndex)));
		
	    LogAppendEntry.Builder le = LogAppendEntry.newBuilder();
		le.setElectionTerm(state.getCurrentTerm());
		le.setPrevLogIndex(state.getLastLogIndex());
		le.setPrevLogTerm((logIndex - 1) != 0 ? log.getEntry(logIndex - 1).getTerm() : 0);
		le.setLeaderCommitIndex(log.getCommitIndex());
		le.setLeaderNodeId(state.getNodeId());
		le.setEntrylist(l.build());	    

	    wmb.setLogAppendEntries(le.build());
		wmb.setType(WorkMessage.MessageType.LOGAPPENDENTRY);
		wmb.setSecret(11111);
		return wmb.build();
	}

    
	@Override
	public void logAppend(LogAppendEntry logEntry) {
		// TODO Auto-generated method stub
		int nodeId = logEntry.getLeaderNodeId();
		if (logEntry.getSuccess()){
			updateNextAndMatchIndex(logEntry.getLeaderNodeId(), logEntry.getPrevLogIndex());
			if (logEntry.getPrevLogIndex() < state.getLastLogIndex()){
				resendAppendRequest(nodeId, nextIndex.get(nodeId)+1);
			}
		}else{
		   nextIndex.put(nodeId, nextIndex.get(nodeId)-1);
		   resendAppendRequest(nodeId, nextIndex.get(nodeId));
		}
	}
	
    private WorkMessage createLogAppendEntry(int nodeId, LogEntry logEntry) {

		// TODO Auto-generated method stub
		WorkMessage.Builder wmb = WorkMessage.newBuilder();
		Header.Builder hdb = Header.newBuilder();
		hdb.setNodeId(nodeId);
		hdb.setTime(System.currentTimeMillis());
		hdb.setDestination(-1);
		
		wmb.setHeader(hdb.build());
		
		LogEntryList.Builder entryList = LogEntryList.newBuilder();
	    entryList.addEntry(logEntry);
	    
	    LogAppendEntry.Builder logAppend = LogAppendEntry.newBuilder();
	    
	    logAppend.setElectionTerm(state.getCurrentTerm());
	    logAppend.setPrevLogIndex(state.getLastLogIndex());
	    logAppend.setPrevLogTerm(state.getLastLogTerm());
	    logAppend.setLeaderCommitIndex(log.getCommitIndex());
	    logAppend.setLeaderNodeId(state.getNodeId());
	    logAppend.setEntrylist(entryList.build());	    

	    wmb.setLogAppendEntries(logAppend.build());
		wmb.setType(WorkMessage.MessageType.LOGAPPENDENTRY);
		wmb.setSecret(9999);
		return wmb.build();
		
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
	public void updateNextAndMatchIndex(int nodeId, int lastIndex ) {
		if(!matchIndex.containsKey(nodeId)) {
			// Next Index
			nextIndex.put(nodeId, log.lastIndex() + 1);
			// Match Index
			matchIndex.put(nodeId, (int) 0);
		}
		
		if(matchIndex.get(nodeId) != lastIndex) {
			nextIndex.put(nodeId, lastIndex + 1);
			matchIndex.put(nodeId, lastIndex);
			// check if commit index is also increased
			//if matchIndex increased for most server then updateCommitIndex
			//updateCommitIndex();
		}
	}
	
	
	public void updateCommitIndex() {
		log.setCommitIndex(log.lastIndex());		
    }
	
/*    @java.lang.Override
    public void requestVote(LeaderElection leaderElectionRequest) {

    }

    @java.lang.Override
    public void startElection() {

    }

    @java.lang.Override
    public void leaderElect() {

    }*/

/*    @java.lang.Override
    public void collectVote(Election.LeaderElectionResponse leaderElectionResponse) {
    	*//**
    	 * if leader has been elected than there is no need to process other node response.
    	 *//*
    	return;
        
    }*/

/*	@Override
	public void declareLeader() {
		WorkMessage hearbeat = createHeartBeatMessage();
		state.getOutBoundMessageQueue().addMessage(hearbeat);	
	}*/
	
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
/*	@Override
	public void heartbeat(LogAppendEntry heartbeat) {
		// TODO Auto-generated method stub
		if (state.getCurrentTerm()<heartbeat.getElectionTerm()){
			//leader should step down, as it was previously elected and something went wrong
			state.setCurrentTerm(heartbeat.getElectionTerm());
			state.becomeFollower();
		}
		
	}*/

/*	@Override
	public void readFile(Pipe.ReadBody readBody) {

	}

	@Override
	public void writeFile(Pipe.WriteBody readBody) {

	}

	@Override
	public void deleteFile(Pipe.ReadBody readBody) {

	}*/


/*	@Override
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
	}*/


}

