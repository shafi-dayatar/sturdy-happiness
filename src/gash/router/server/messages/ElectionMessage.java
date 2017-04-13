package gash.router.server.messages;

import gash.router.server.ServerState;
import gash.router.server.states.RaftServerState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pipe.election.Election;
import pipe.work.Work;
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
}
