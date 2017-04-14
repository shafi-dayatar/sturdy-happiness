package gash.router.server.states;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

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
import pipe.work.Work.LogEntry;
import pipe.work.Work.LogEntry.*;
import poke.core.Mgmt.LogEntryList;
import poke.core.Mgmt.Management;
import poke.core.Mgmt.MgmtHeader;
import poke.core.Mgmt.RaftMessage;
import poke.server.managers.ConnectionManager;
import pipe.work.Work.Node;
import pipe.work.Work.WorkMessage;


/**
 * Created by rentala on 4/11/17.
 */
public class Leader implements RaftServerState {

    protected static Logger logger = LoggerFactory.getLogger("Leader-State");
    private ServerState state;
    private LogInfo log;
    // stores the next logIndex to be sent to each follower
 	TreeMap<Integer, Integer> nextIndex = new TreeMap<Integer, Integer>();
 	// stores the last logIndex sent to each follower
 	TreeMap<Integer, Integer> matchIndex = new TreeMap<Integer, Integer>();

    public Leader(ServerState state){
        this.state = state;
    }
    public void appendEntries(String entry){
        logger.info("appendEntries = " + entry);
    }
    
    /**
	 * Build AppendRequest to send to a follower with log entries
	 * starting from logStartIndex to lastIndx
	 * @param toNode
	 * @param logStartIndex
	 * @return Management response
	 */
	public WorkMessage getAppendRequest(int fNode, int logStartIndex) {
		Header.Builder hdb = Header.newBuilder();
		hdb.setNodeId(this.nodeId));
		hdb.setTime(System.currentTimeMillis());
		hdb.setDestination(fNode);
		
		WorkMessage.Builder wmb = WorkMessage.newBuilder();
	    wmb.setHeader(hdb);
	    
		RaftMessage.Builder rmb = RaftMessage.newBuilder();
		rmb.setAction(RaftMessage.Action.APPEND);
		rmb.setTerm(this.term);
		rmb.setPrevLogIndex(logStartIndex - 1);
		//logger.info("Log Start Index: " + logStartIndex);
		rmb.setPrevTerm((logStartIndex - 1) != 0 ? log.getEntry(logStartIndex - 1)
				.getTerm() : 0);
		rmb.setLogCommitIndex(log.getCommitIndex());

		LogEntryList.Builder le = LogEntryList.newBuilder();
		//logger.info("Current entries in log:"+ log.getEntries(logStartIndex).length);
		le.addAllEntry(Arrays.asList(log.getEntries(logStartIndex)));
		rmb.setEntries(le.build());

		Management.Builder mb = Management.newBuilder();
		mb.setHeader(mhb.build());
		mb.setRaftMessage(rmb.build());

		return mb.build();
	}

    /**
	 Insert a new log entry and send to candidates
	 */
	public void appendLogEntry(Command cmd, boolean isInsert) {
			LogEntry.Builder leb = LogEntry.newBuilder();
			if(isInsert) {
				leb.setAction(DataAction.INSERT);
				leb.setData(cmd);
				leb.setTerm(state.termId);
				log.appendEntry(leb.build());
			}
			ArrayList<Node> followers = new ArrayList<Node>(state.getEmon().getOutBoundRouteTable());
			sendAppendRequest(getAppendRequestForFollowers());
			
	}
	
	/**
	 * Send appendRequest to every node from leader
	 */
	public void sendAppendRequest(Map<Integer,WorkMessage> appendRequests) {
		for(Integer nodeId : appendRequests.keySet()) {
			if(appendRequests.get(nodeId) != null)
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
    public void logAppend() {

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
		// TODO Auto-generated method stub
		
	}

}
