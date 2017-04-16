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
import pipe.work.Work.LogAppendEntry;
import pipe.work.Work.LogEntry.Builder;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkMessage.MessageType;
import pipe.work.Work.WorkMessageOrBuilder;
import routing.Pipe;

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
    
    public void requestVote(LeaderElection request){
        logger.info("requestVote ");
        WorkMessage wm = createVoteResponse(request.getCandidateId(), state.getNodeId(), 
				request.getTerm(), false);
        state.getOutBoundMessageQueue().addMessage(wm);
        
    }

	public void startElection() {
		// TODO Auto-generated method stub	
		logger.info(" I am starting a election with term : " + state.getCurrentTerm() + 
		", Hopefully I will become leader ");
		
		election = new Election(state.getCurrentTerm(), 
				state.getEmon().getTotalNodes(), state.getLastLogIndex(),
				state.getLastLogTerm());
		startTime = System.currentTimeMillis();		
		state.getOutBoundMessageQueue().addMessage(
				election.createElectionMessage());
	}
	
	public void leaderElect() {
		// TODO Auto-generated method stub
	}
	

	@java.lang.Override
	public void collectVote(LeaderElectionResponse voteResponse) {
		/**
		 *  vote is counted only if it from currentTerm, 
		 *  which will handle stale election response votes,
		 *  also duplicate vote response is handled
		 */
		
		logger.info("Got vote from : " + voteResponse.getFromNodeId());
		logger.info("Vote was : " + voteResponse.getVoteGranted());
		
		if (election.term == voteResponse.getForTerm()){
			if(election.voteFrom.add(voteResponse.getFromNodeId())){
				if(voteResponse.getVoteGranted()){
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
			this.voteRequired = (voteRequired/2) + 1;
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
		    leb.setTerm(state.getCurrentTerm());
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

	@Override
	public void heartbeat(LogAppendEntry heartbeat) {
		// TODO Auto-generated method stub
		logger.info("Got a heartbeat message in While server was in Candidate state");
		logger.info("Current Elected Leader is :" + heartbeat.getLeaderNodeId() + 
				", for term : " + heartbeat.getElectionTerm() );
		state.getElectionTimer().resetElectionTimeOut();
		state.becomeFollower();
		state.setCurrentTerm(heartbeat.getElectionTerm());
		state.setLeaderId(heartbeat.getLeaderNodeId());
		state.setLeaderKnown(true);
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

	@Override
	public void logAppend(LogAppendEntry logEntry) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void appendEntries(ArrayList<Builder> logEntryBuilder) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void appendEntries(Builder logEntryBuilder) {
		// TODO Auto-generated method stub
		
	}


}
