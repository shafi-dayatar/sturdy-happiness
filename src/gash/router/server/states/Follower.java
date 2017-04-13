package gash.router.server.states;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.ServerState;
import pipe.election.Election;
import pipe.work.Work;

/**
 * Created by rentala on 4/11/17.
 */
public class Follower implements RaftServerState {
    protected static Logger logger = LoggerFactory.getLogger("Follower-State");
    private ServerState state;

    public Follower(){

    }
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
    public Work.WorkMessage collectVote(Election.LeaderElection leaderElection) {
        return null;
    }

}
