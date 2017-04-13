package gash.router.server.states;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.ServerState;
import pipe.election.Election;
import pipe.work.Work;

/**
 * Created by rentala on 4/11/17.
 */
public class Candidate implements RaftServerState {
    protected static Logger logger = LoggerFactory.getLogger("Candidate-State");
    private ServerState state;

    public Candidate(){

    }
    public Candidate(ServerState state){
        this.state = state;
    }
    public void requestVote(){
        logger.info("requestVote ");

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

    public void vote(){
        logger.info("voting .... ");

    }

}
