package gash.router.server.states;

/**
 * Created by rentala on 4/11/17.
 */


import pipe.election.Election;
import pipe.work.Work;

/**
 *  Parent state class that defines every node - Leader Candidate Follower
 */
public interface RaftServerState {

    public void requestVote();
    public void startElection();
    public void leaderElect();
    public void logAppend();
    public void collectVote(Election.LeaderElection leaderElection);

}
