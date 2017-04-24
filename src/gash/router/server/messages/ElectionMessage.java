package gash.router.server.messages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.ServerState;
import gash.router.server.states.RaftServerState;
import pipe.common.Common.Header;
import pipe.election.Election;
import pipe.election.Election.LeaderElection;
import pipe.election.Election.LeaderElectionResponse;
import pipe.work.Work;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkMessage.MessageType;

/**
 * Created by rentala on 4/12/17.
 */
public class ElectionMessage extends Message {

    protected static Logger logger = LoggerFactory.getLogger("Election Message");
    Work.WorkMessage.MessageType type = null;
    Election.LeaderElection leaderElectionRequest = null;
    Election.LeaderElectionResponse leaderElectionResponse = null;

    public ElectionMessage(Work.WorkMessage msg) {
        // TODO Auto-generated constructor stub
    	logger.info("Got Election message : " + msg.toString());
        unPackHeader( msg.getHeader());
        type = msg.getType();
        if(type == MessageType.LEADERELECTION){
            leaderElectionRequest = msg.getLeaderElectionRequest();
        }else if (type == MessageType.LEADERELECTIONREPLY){
        	leaderElectionResponse = msg.getLeaderElectionResponse();    	
        }
    }
    
    @java.lang.Override
    public void processMessage(ServerState state) {
    	RaftServerState serverState = state.getRaftState();
    	if (type == MessageType.LEADERELECTIONREPLY){ 
    		serverState.collectVote(leaderElectionResponse);
    	}else if (type == MessageType.LEADERELECTION){
    		serverState.requestVote(leaderElectionRequest);
    	}
        return;
    }
    
    
    public static WorkMessage createElectionMessage(int sourceId, int lastLogIndex, 
    		int lastLogTerm, int currentTerm){
		//Create Leader Election BroadCast Message:
		WorkMessage.Builder wmb = WorkMessage.newBuilder();
		Header.Builder hdb = Header.newBuilder();
		hdb.setNodeId(sourceId);
		hdb.setTime(System.currentTimeMillis());
		hdb.setDestination(-1);
	    wmb.setHeader(hdb);
	    
	    LeaderElection.Builder leb = LeaderElection.newBuilder();
	    leb.setLastLogIndex(lastLogIndex);
	    leb.setLastLogTerm(lastLogTerm);
	    leb.setTerm(currentTerm);
	    leb.setCandidateId(sourceId);
	    
		wmb.setLeaderElectionRequest(leb);
		wmb.setType(MessageType.LEADERELECTION);
		wmb.setSecret(10100);
		return wmb.build();
	}
    
    public static WorkMessage createVoteResponse(int destId, int sourceId, int term, boolean b) {
		logger.info("will I vote for " + destId + "?, and answer is : " + b);
		// TODO Auto-generated method stub
		WorkMessage.Builder wmb = WorkMessage.newBuilder();
		Header.Builder hdb = Header.newBuilder();
		hdb.setNodeId(sourceId);
		hdb.setTime(System.currentTimeMillis());
		hdb.setDestination(destId);
	    wmb.setHeader(hdb);
	    
	    LeaderElectionResponse.Builder leb = LeaderElectionResponse.newBuilder();
	    leb.setForTerm(term);
	    leb.setFromNodeId(sourceId);
	    leb.setVoteGranted(b);
	    
		wmb.setLeaderElectionResponse(leb);
		wmb.setType(MessageType.LEADERELECTIONREPLY);
		wmb.setSecret(10100);
		return wmb.build();
	}
  
}
