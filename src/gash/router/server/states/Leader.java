package gash.router.server.states;

/**
 * Created by rentala on 4/11/17.
 */
public class Leader extends NodeState {
    public Leader(){

    }
    public void appendEntries(String entry){
        logger.info("appendEntries = " + entry);
    }

}