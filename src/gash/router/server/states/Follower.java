package gash.router.server.states;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.ServerState;
import gash.router.server.log.LogInfo;
import pipe.election.Election;
import pipe.election.Election.LeaderElection;
import pipe.election.Election.LeaderElectionResponse;
import pipe.work.Work;

import pipe.common.Common.Header;
import pipe.work.Work.LogAppendEntry;
import pipe.work.Work.LogAppendResponse;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkMessage.MessageType;


public class Follower implements RaftServerState {
    
	protected static Logger logger = LoggerFactory.getLogger("Follower-State");
	private ServerState state;
	//private List<integer, boolean> vote = new ArrayList<Integer, Boolean>();
    private ConcurrentHashMap<Integer, Integer> electionVotes =  new ConcurrentHashMap<Integer, Integer>();
    private LogInfo log;
    public Follower(ServerState state){
        this.state = state;
    }

    public void vote(){
        logger.info("voting .... ");
    }
    public void listenHeartBeat(){

    }
    
    public void toCandidate(){
        logger.info("Timed out ! To candidate state .... ");
    }


	public void requestVote(LeaderElection request) {
		// TODO Auto-generated method stub
		int logIndex = state.getLastLogIndex();
		int logTerm = state.getLastLogTerm();
		WorkMessage wm = null;
		
		if( request.getTerm() < state.getCurrentTerm() ||
				request.getLastLogTerm() < logTerm ||
				request.getLastLogIndex() < logIndex){
			/**
			 * vote will be false as: this candidate is lagging
			 */
			wm = createVoteResponse(request.getCandidateId(), state.getNodeId(), 
					request.getTerm(), false);
		}else{
			if(electionVotes.containsKey(request.getTerm())){
				wm  =  createVoteResponse(request.getCandidateId(), state.getNodeId(), 
						request.getTerm(), false);
			}
			else{
				electionVotes.put(request.getTerm(), request.getCandidateId());
				wm  =  createVoteResponse(request.getCandidateId(), state.getNodeId(), 
						request.getTerm(), true);
			}
			   
		}
		state.getOutBoundMessageQueue().addMessage(wm);
	}

	private WorkMessage createVoteResponse(int destId, int sourceId, int term, boolean b) {
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
	

	public void startElection() {
		// TODO Auto-generated method stub	
		
	}

	public void leaderElect() {
		// TODO Auto-generated method stub
		
	}

	public void logAppend() {
		// TODO Auto-generated method stub
		
	}
    @java.lang.Override
    public void collectVote(Election.LeaderElectionResponse leaderElectionResponse) {

    }

	@Override
	public void declareLeader() {
		// TODO Auto-generated method stub
		
	}
	
	class Vote {
		int candidateId;
		boolean vote;
		public Vote(int can, boolean vote){
			candidateId = can;
			this.vote = vote;
		}
	}
	
	/**
	 * Build appendResponse to send to the leader node
	 */
	public WorkMessage getAppendResponse(int lNode, boolean responseFlag) {

		WorkMessage.Builder wmb = WorkMessage.newBuilder();
		Header.Builder hdb = Header.newBuilder();
		hdb.setNodeId(state.getNodeId());
		hdb.setTime(System.currentTimeMillis());
		hdb.setDestination(lNode);
		
	    wmb.setHeader(hdb.build());

		LogAppendResponse.Builder lr = LogAppendResponse.newBuilder();
		lr.setElectionTerm(state.getCurrentTerm());
		
	    wmb.setLogAppendResponse(lr.build());
		wmb.setType(WorkMessage.MessageType.LOGAPPENDRESPONSE);
		wmb.setSecret(12222);
		return wmb.build();
	}
	
	/**
	 *	if commitIndex gets updated by leader,
	 * update the commitIndex of self and send message to Resource with 
	 * entries starting from lastApplied + 1 to commit index
	 */
	public void updateCommitIndex(int newCommitIndex) {
		log.setCommitIndex(newCommitIndex);
		if(log.getCommitIndex() >log.getLastApplied()) {
			log.setLastApplied(log.getCommitIndex());
		}
	}
}
