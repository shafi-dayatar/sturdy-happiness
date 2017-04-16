package gash.router.server.states;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.ServerState;
import gash.router.server.log.LogInfo;
import pipe.common.Common.Header;
import pipe.election.Election;
import pipe.election.Election.LeaderElection;
import pipe.work.Work.LogAppendEntry;
import pipe.work.Work.LogEntry;
import pipe.work.Work.LogEntryList;
import pipe.work.Work.Node;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkMessage.MessageType;
import routing.Pipe;

/**
 * Created by rentala on 4/11/17.
 */
public class Leader implements RaftServerState, Runnable {

    protected static Logger logger = LoggerFactory.getLogger("Leader-State");
    private ServerState state;
    
    
    
    // stores the next logIndex to be sent to each follower
 	Hashtable<Integer, Integer> nextIndex = new Hashtable<Integer, Integer>();
 	// stores the last logIndex response from each follower
 	Hashtable<Integer, Integer> matchIndex = new Hashtable<Integer, Integer>();
 	
    private boolean isLeader;

    public Leader(ServerState state){
        this.state = state;
    }
    
	@Override
	public void declareLeader() {
		WorkMessage hearbeat = createHeartBeatMessage();
		state.getOutBoundMessageQueue().addMessage(hearbeat);	
	}
	
	@Override
    public synchronized void appendEntries(ArrayList<LogEntry.Builder> logEntryBuilder){
        //logger.info("appendEntries = " + entry);
        for (LogEntry.Builder builder : logEntryBuilder){
        	builder.setLogId(state.getLog().lastIndex());
            builder.setTerm(state.getCurrentTerm());
            LogEntry entry  = builder.build();
            Set<Integer> keys = nextIndex.keySet();
            for(Integer nodeId : keys){
            	
            	int nextlogindex = nextIndex.get(nodeId);
            	state.getLog().appendEntry(entry);
            	if (state.getLog().lastIndex() >= nextlogindex){
            		
            		//WorkMessage wm = createLogAppendEntry(nodeId, entry);
            		//state.getOutBoundMessageQueue().addMessage(wm);
            	}
            }
        }
    }
    
	public synchronized void appendEntries(LogEntry.Builder logEntryBuilder){
		int lastLogIndex = state.getLog().lastIndex();
		int lastLogTerm = state.getLog().lastLogTerm();
        int commitIndex = state.getLog().getCommitIndex();
        
		lastLogIndex = lastLogIndex == -1 ? 0 : lastLogIndex;
		lastLogTerm = lastLogTerm == -1 ? state.getCurrentTerm() : lastLogTerm;
				
		logEntryBuilder.setTerm(lastLogTerm);
		logEntryBuilder.setLogId(lastLogIndex+1);
		LogEntry entry  = logEntryBuilder.build();
		
		logger.info("last log index :  " + lastLogIndex + ",\n ");
		logger.info("Current Log Id is : " +  lastLogIndex+1);
        state.getLog().appendEntry(entry);
        state.getLog().setLastApplied(lastLogIndex+1);
        

        Set<Integer> keys = nextIndex.keySet();
        logger.info("Last Log index of leader is : " + lastLogIndex);
        for(Integer nodeId : keys){
            int nextlogindex = nextIndex.get(nodeId);
            logger.info("NextIndex Table is : " + nextIndex.toString());
            if (lastLogIndex + 1  >= nextlogindex){
            	logger.info("Sending LogAppend to all connected server");
            	WorkMessage wm = createLogAppendEntry(nodeId, lastLogIndex, lastLogTerm, commitIndex, entry);
            	logger.info("Message is : " +  wm.toString());
            	state.getOutBoundMessageQueue().addMessage(wm);
            }
        }
    }
	
	@Override
	public void logAppend(LogAppendEntry logEntry) {
		// TODO Auto-generated method stub
		logger.info("Got logAppend response from follower: " + logEntry.toString());
		int nodeId = logEntry.getLeaderNodeId();
		if (logEntry.getSuccess()){
			updateNextAndMatchIndex(logEntry.getLeaderNodeId(), logEntry.getPrevLogIndex());
			if (logEntry.getPrevLogIndex() < state.getLastLogIndex()){
				//resendAppendRequest(nodeId, nextIndex.get(nodeId)+1);
			}
		}else{
			int logId = getDecrementedNextIndex(nodeId);
		    //resendAppendRequest(nodeId,logId);
		}
	}
    
    
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
		l.addAllEntry(Arrays.asList(state.getLog().getEntries(logIndex)));
		
	    LogAppendEntry.Builder le = LogAppendEntry.newBuilder();
		le.setElectionTerm(state.getCurrentTerm());
		le.setPrevLogIndex(state.getLastLogIndex());
		le.setPrevLogTerm((logIndex - 1) != 0 ? state.getLog().getEntry(logIndex - 1).getTerm() : 0);
		le.setLeaderCommitIndex(state.getLog().getCommitIndex());
		le.setLeaderNodeId(state.getNodeId());
		le.setEntrylist(l.build());	    

	    wmb.setLogAppendEntries(le.build());
		wmb.setType(WorkMessage.MessageType.LOGAPPENDENTRY);
		wmb.setSecret(11111);
		return wmb.build();
	}

    

	
    private WorkMessage createLogAppendEntry(int nodeId, int lastLogIndex, 
    		int lastLogTerm, int commitIndex,
    		LogEntry logEntry) {

		// TODO Auto-generated method stub
		WorkMessage.Builder wmb = WorkMessage.newBuilder();
		Header.Builder hdb = Header.newBuilder();
		hdb.setNodeId(state.getNodeId());
		hdb.setTime(System.currentTimeMillis());
		hdb.setDestination(nodeId);
		
		wmb.setHeader(hdb.build());
		
		LogEntryList.Builder entryList = LogEntryList.newBuilder();
	    entryList.addEntry(logEntry);
	    
	    LogAppendEntry.Builder logAppend = LogAppendEntry.newBuilder();
	    
	    logAppend.setElectionTerm(state.getCurrentTerm());
	    logAppend.setPrevLogIndex(lastLogIndex);
	    logAppend.setPrevLogTerm(lastLogTerm);
	    logAppend.setLeaderCommitIndex(commitIndex);
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
			nextIndex.put(node.getNodeId(), state.getLog().lastIndex() + 1);
			// Match Index
			matchIndex.put(node.getNodeId(), (int) 0);
		}
	}
	
	/**
	 * If follower rejects an AppendRequest due to log mismatch,
	 * decrement the follower's next index and return the
	 * decremented index
	 */
	public int getDecrementedNextIndex(int nodeId) {
		int id = nextIndex.get(nodeId) - 1 ;
		nextIndex.put(nodeId, id);
		return id;
	}
	
	/**
	 * When AppendRequest is successful for a node, update it's 
	 * nextIndex to lastIndex+1 and matchIndex to lastIndex
	 */
	public void updateNextAndMatchIndex(int nodeId, int lastIndex ) {
		if(!matchIndex.containsKey(nodeId)) {
			// Next Index
			nextIndex.put(nodeId, state.getLog().lastIndex() + 1);
			// Match Index
			matchIndex.put(nodeId, (int) 0);
		}
		
		if(matchIndex.get(nodeId) == lastIndex - 1) {
			nextIndex.put(nodeId, lastIndex + 1);
			matchIndex.put(nodeId, lastIndex);
			// check if commit index is also increased
			//if matchIndex increased for most server then updateCommitIndex
			//updateCommitIndex();
		}
	}
	
	public void updateCommitIndex() {
		state.getLog().setCommitIndex(state.getLog().lastIndex());		
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
			setLeader(false);
			state.becomeFollower();
		}
		
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
	public void readFile(Pipe.ReadBody readBody) {

	}

	@Override
	public void writeFile(Pipe.WriteBody readBody) {

	}

	@Override
	public void deleteFile(Pipe.ReadBody readBody) {

	}

	public void setNextAndMatchIndex() {
		// TODO Auto-generated method stub
		ArrayList<Node> nodes = state.getEmon().getOutBoundRouteTable();
		for (Node node : nodes){
			nextIndex.put(node.getNodeId(), state.getLog().lastIndex()+1);
			matchIndex.put(node.getNodeId(), 0);
		}
		
	}	
}
