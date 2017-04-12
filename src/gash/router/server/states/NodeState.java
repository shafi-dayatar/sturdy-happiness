package gash.router.server.states;

/**
 * Created by rentala on 4/11/17.
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Parent state class that defines every node - Leader Candidate Follower
 */
public abstract class NodeState {
    protected static Logger logger = LoggerFactory.getLogger("state");
    public NodeState(){

    }
    //initial term is always 0
    private int currentTerm = 0;
    public void setCurrentTerm(int currentTerm){
        this.currentTerm = currentTerm;
    }
    public int getCurrentTerm(){
        return this.currentTerm;
    }
    //initial term is always 0
    private int votedFor = 0;
    public void setVotedFor(int votedFor){
        this.votedFor = votedFor;
    }
    public int getVotedFor(){
        return this.votedFor;
    }
    public void vote(){

    }

}
