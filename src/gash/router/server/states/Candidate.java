package gash.router.server.states;


import java.util.ArrayList;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.ServerState;

import pipe.election.Election;
import pipe.work.Work;
import pipe.common.Common.Header;
import pipe.election.Election.LeaderElection;
import pipe.election.Election.LeaderElectionResponse;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkMessage.MessageType;
import pipe.work.Work.WorkMessageOrBuilder;

/**
 * Created by rentala on 4/11/17.
 */
public class Candidate implements RaftServerState {

    protected static Logger logger = LoggerFactory.getLogger("Candidate-State");
    private ServerState state;
	private Long startTime;
	private Long endTime;
	private Election election;
	
    public Candidate(ServerState serverState){
        this.state = serverState;
    }
    
    public void requestVote(LeaderElection leaderElectionRequest){
        logger.info("requestVote ");
    }

	public void startElection() {
		// TODO Auto-generated method stub	
		election = new Election(state.getCurrentTerm()+1, 
				state.getEmon().getTotalNodes(), state.getLastLogIndex(),
				state.getLastLogTerm());
		startTime = System.currentTimeMillis();		
		state.getOutBoundMessageQueue().addMessage(
				election.createElectionMessage());
	}
	
	public void leaderElect() {
		// TODO Auto-generated method stub
	}
	
	public void logAppend() {
		// TODO Auto-generated method stub
	}

	@java.lang.Override
	public void collectVote(LeaderElectionResponse leaderElection) {
		/**
		 *  vote is counted only if it from currentTerm, 
		 *  which will handle stale election response votes,
		 *  also duplicate vote response is handled
		 */
		
		logger.info("Got vote from : " + leaderElection.getNodeId());
		logger.info("Vote was : " + leaderElection.getVoteGranted());
		
		if (election.term == leaderElection.getForTerm()){
			if(election.voteFrom.add(leaderElection.getNodeId())){
				if(leaderElection.getVoteGranted()){
					election.voteCount++;
					if(election.checkElectionResult()){
						endTime = System.currentTimeMillis();
						logger.info("Leader was elected in " + (endTime - startTime));
						logger.info("Leader is " + state.getNodeId());
						logger.info("Election Result is " + election.toString());
						state.becomeLeader();
					}
					logger.info("Election Result is " + election.toString());
				}
			}
		}
		
	}

	public class Election {
		public int term;
		public int voteCount = 1;// self vote;
		public int voteRequired = 0;
		private int lastLogIndex = 0;
		private int lastLogTerm = 0;
		public HashSet<Integer> voteFrom = new HashSet<Integer>(); 
		public boolean successful = false;
		public Election(int term, int voteRequired, int lastLogIndex, int lastLogTerm){
			this.term = term;
			this.voteRequired = voteRequired/2 + 1;
			this.lastLogIndex = lastLogIndex;
			this.lastLogTerm = lastLogTerm;
		}
		
		public WorkMessage createElectionMessage(){
			//Create Leader Election BroadCast Message:
			WorkMessage.Builder wmb = WorkMessage.newBuilder();
			Header.Builder hdb = Header.newBuilder();
			hdb.setNodeId(state.getNodeId());
			hdb.setTime(System.currentTimeMillis());
			hdb.setDestination(-1);
		    wmb.setHeader(hdb);
		    
		    LeaderElection.Builder leb = LeaderElection.newBuilder();
		    leb.setLastLogIndex(state.getLastLogIndex());
		    leb.setLastLogTerm(state.getLastLogTerm());
		    leb.setTerm(state.getCurrentTerm() + 1);
		    leb.setCandidateId(state.getNodeId());
		    
			wmb.setLeaderElectionRequest(leb);
			wmb.setType(MessageType.LEADERELECTION);
			wmb.setSecret(10100);
			return wmb.build();
		}
		
		public boolean checkElectionResult(){
			if (voteCount >= voteRequired)
				successful = true;
			return successful;
		}
		
		public String toString(){
			return "ElectionTerm : "+ term + "\n" +
					"Total Votes : " + voteCount +  "\n" +
					"Total Votes Required : " + voteRequired + "\n" +
					"Election Status : " + successful; 
		}
	}


	@Override
	public void declareLeader() {
		// TODO Auto-generated method stub
		
	}


}
