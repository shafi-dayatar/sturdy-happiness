package gash.router.server.states;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.ServerState;
import pipe.election.Election;
import pipe.election.Election.LeaderElection;
import pipe.election.Election.LeaderElectionResponse;
import pipe.work.Work;

import pipe.common.Common.Header;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkMessage.MessageType;


/**
 * Created by rentala on 4/11/17.
 */
public class Follower implements RaftServerState {
    
	protected static Logger logger = LoggerFactory.getLogger("Follower-State");
	private ServerState state;
	//private List<integer, boolean> vote = new ArrayList<Integer, Boolean>();
    private ConcurrentHashMap<Integer, ArrayList<Vote>> electionVotes =  new ConcurrentHashMap<Integer, ArrayList<Vote>>();
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
				request.getLastLogTerm() < state.getLastLogTerm() ||
				request.getLastLogIndex() < state.getLastLogIndex()
				){
			/**
			 * vote will be false as: this candidate is lagging
			 */
			wm = createVoteResponse(request.getCandidateId(), request.getTerm(), false);
		}else{
			if(!electionVotes.contains(request.getTerm())){
				ArrayList<Vote> votes= new ArrayList<Vote>();
				votes.add(new Vote(request.getCandidateId(),
						true));
				electionVotes.put(request.getTerm(),votes);
			}
			else{
				ArrayList<Vote> previousVotes = electionVotes.get(request.getTerm());
				for(Vote v: previousVotes){
					if (v.candidateId == request.getCandidateId()){
						//cannot vote again for this candidate
						wm = createVoteResponse(request.getCandidateId(), request.getTerm(), false);
						state.getOutBoundMessageQueue().addMessage(wm);
						return;
					}
				}
				previousVotes.add(new Vote(request.getCandidateId(),
						true));
			}
			wm  =  createVoteResponse(request.getCandidateId(), request.getTerm(), true);
		}
		state.getOutBoundMessageQueue().addMessage(wm);
	}

	private WorkMessage createVoteResponse(int candidateId, int term, boolean b) {
		// TODO Auto-generated method stub
		WorkMessage.Builder wmb = WorkMessage.newBuilder();
		Header.Builder hdb = Header.newBuilder();
		hdb.setNodeId(state.getNodeId());
		hdb.setTime(System.currentTimeMillis());
		hdb.setDestination(candidateId);
	    wmb.setHeader(hdb);
	    
	    LeaderElectionResponse.Builder leb = LeaderElectionResponse.newBuilder();
	    leb.setForTerm(term);
	    leb.setNodeId(candidateId);
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
}
