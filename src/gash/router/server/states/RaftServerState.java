package gash.router.server.states;

/**
 * Created by rentala on 4/11/17.
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Parent state class that defines every node - Leader Candidate Follower
 */
public interface RaftServerState {

    public void requestVote();
	public void startElection();
	public void leaderElect();
	public void logAppend();
	public void collectVote();
}

