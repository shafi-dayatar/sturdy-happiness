package gash.router.server.states;


import java.util.ArrayList;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.ServerState;
import gash.router.server.messages.ElectionMessage;
import pipe.election.Election.LeaderElection;
import pipe.election.Election.LeaderElectionResponse;
import pipe.work.Work.FileChunkData;
import pipe.work.Work.LogAppendEntry;
import pipe.work.Work.LogEntry.Builder;
import pipe.work.Work.WorkMessage;
import routing.Pipe.CommandMessage;
import routing.Pipe.ReadBody;
import routing.Pipe.Response;
import routing.Pipe.Response.Status;
import routing.Pipe.WriteBody;

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
        WorkMessage wm = null;
        if (request.getTerm() > state.getCurrentTerm()){
        	wm = ElectionMessage.createVoteResponse(request.getCandidateId(), state.getNodeId(), 
    				request.getTerm(), true);
        	state.becomeFollower();
        }else{
        	wm = ElectionMessage.createVoteResponse(request.getCandidateId(), state.getNodeId(), 
				request.getTerm(), false);
        }
        state.getOutBoundMessageQueue().addMessage(wm);
        
    }

	public void startElection() {
		// TODO Auto-generated method stub	
		logger.debug(" I am starting a election for term : " + state.getCurrentTerm() + 
		", Will I become leader ???");
		int lastIndex  = state.getLog().lastIndex();
		logger.info("Last index on follower was : " + lastIndex);
		logger.info(state.getLog().log.toString());
		int lastTerm = state.getLog().lastLogTerm(lastIndex);
		election = new Election(state.getCurrentTerm(), 
				state.getEmon().getTotalNodes(), lastIndex,
				lastTerm);
		startTime = System.currentTimeMillis();	
		
		logger.debug("Sending vote request to peers in cluster");
		
		
		WorkMessage  wm = ElectionMessage.createElectionMessage(state.getNodeId(), 
				lastIndex, lastTerm, state.getCurrentTerm());
		
		state.getOutBoundMessageQueue().addMessage(wm);
	}
	
	public void leaderElect() {
		// TODO Auto-generated method stub
	}
	

	@java.lang.Override
	public void collectVote(LeaderElectionResponse voteResponse) {
		/**
		 *  vote will be counted only if it is from currentTerm, 
		 *  which will handle stale election response votes from previous term,
		 *  No Duplicate vote is allowed.
		 */
		
		logger.info("Got vote response from : " + voteResponse.getFromNodeId());
		logger.info("Vote was in favor of me: " + voteResponse.getVoteGranted());
		
		if (election.term == voteResponse.getForTerm()){
			if(election.voteFrom.add(voteResponse.getFromNodeId())){
				if(voteResponse.getVoteGranted()){
					synchronized(election) {
					   election.voteCount++;
					}
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
			this.voteRequired = (voteRequired/2) + 1;
			this.lastLogIndex = lastLogIndex;
			this.lastLogTerm = lastLogTerm;
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
		if (heartbeat.getElectionTerm() > state.getCurrentTerm()){
			logger.info("Got a heartbeat message in While server was in Candidate state");
			logger.info("Current Elected Leader is :" + heartbeat.getLeaderNodeId() + 
					", for term : " + heartbeat.getElectionTerm() );
			state.getElectionTimer().resetElectionTimeOut();
			state.becomeFollower();
			state.setCurrentTerm(heartbeat.getElectionTerm());
			state.setLeaderId(heartbeat.getLeaderNodeId());
			state.setLeaderKnown(true);
		}
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

	}

	@Override
	public void writeChunkDataResponse(FileChunkData chunk) {

	}


	@Override
	public void stealWork() {
		//Candidate doesnt steal work
	}

	@Override
	public Response getFileChunkLocation(ReadBody request) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status writeFile(WriteBody writeBody) {
		// TODO Auto-generated method stub
		return Status.NOLEADER;
	}

	@Override
	public CommandMessage getWork() {
		// TODO Auto-generated method stub
		return null;
	}

}
