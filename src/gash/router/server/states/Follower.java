package gash.router.server.states;

/**
 * Created by rentala on 4/11/17.
 */
public class Follower extends NodeState {
    public Follower(){

    }

    public void vote(){
        logger.info("voting .... ");

    }
    public void listenHeartBeat(){

    }
    public void toCandidate(){
        logger.info("Timed out ! To candidate state .... ");

    }
}