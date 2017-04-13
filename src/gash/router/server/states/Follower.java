package gash.router.server.states;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.ServerState;
import pipe.election.Election;
import pipe.election.Election.LeaderElection;
import pipe.work.Work;

import pipe.common.Common.Header;
import pipe.work.Work.WorkMessage;


/**
 * Created by rentala on 4/11/17.
 */
public class Follower implements RaftServerState {
    
	protected static Logger logger = LoggerFactory.getLogger("Follower-State");
	private ServerState state;
	//private List<integer, boolean> vote = new ArrayList<Integer, Boolean>();
    private ConcurrentHashMap<Integer, ArrayList<Vote>> voted =  new ConcurrentHashMap<Integer, ArrayList<Vote>>();
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


	public void requestVote(LeaderElection leaderElectionRequest) {
		// TODO Auto-generated method stub
		int logIndex = state.getLastLogIndex();
		int logTerm = state.getLastLogTerm();
		WorkMessage wm = null;
		
		if( leaderElectionRequest.getTerm() < state.getCurrentTerm() ||
				leaderElectionRequest.getLastLogTerm() < state.getLastLogTerm() ||
				leaderElectionRequest.getLastLogIndex() < state.getLastLogIndex()
				){
			/**
			 * vote will be false as: this candidate is lagging
			 */
			//wm = createVoteRejectMessage();
		}else{
			if(!voted.contains(leaderElectionRequest.getTerm())){
				ArrayList<Vote> votes= new ArrayList<Vote>();
				votes.add(new Vote(leaderElectionRequest.getCandidateId(),
						true));
				voted.put(leaderElectionRequest.getTerm(),votes);
				return;
			}
			else{
				ArrayList<Vote> previousVote = voted.get(leaderElectionRequest.getTerm());
				for(Vote v: previousVote){
					if (v.candidateId == leaderElectionRequest.getCandidateId()){
						//have voted for 
						return;
					}
				}
			}
		}
		
 
		
		
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
