package gash.router.server.messages;

import gash.router.server.ServerState;
import gash.router.server.states.RaftServerState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pipe.election.Election;
import pipe.work.Work;

/**
 * Created by rentala on 4/12/17.
 */
public class ElectionMessage extends Message {

    protected static Logger logger = LoggerFactory.getLogger("Election Message");
    Work.WorkMessage.MessageType type = null;
    Election.LeaderElection leaderElection = null;

    public ElectionMessage(Work.WorkMessage msg) {
        // TODO Auto-generated constructor stub
        unPackHeader( msg.getHeader());
        type = msg.getType();
        if(msg.hasLeaderElectionRequest()){
            leaderElection = msg.getLeaderElectionRequest();
        }
    }
    @java.lang.Override
    public Work.WorkMessage processMessage(ServerState state) {
        RaftServerState serverState = state.getState();
        return serverState.collectVote(leaderElection);
    }
}
