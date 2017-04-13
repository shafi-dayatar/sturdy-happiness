package gash.router.server.states;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.ServerState;
import pipe.election.Election;
import pipe.work.Work;

/**
 * Created by rentala on 4/11/17.
 */
public class Leader implements RaftServerState {

    protected static Logger logger = LoggerFactory.getLogger("Leader-State");
    private ServerState state;

    public Leader(ServerState state){
        this.state = state;
    }
    public void appendEntries(String entry){
        logger.info("appendEntries = " + entry);
    }
	

    @java.lang.Override
    public void requestVote() {

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
    public void collectVote(Election.LeaderElection leaderElection) {
        
    }

}
