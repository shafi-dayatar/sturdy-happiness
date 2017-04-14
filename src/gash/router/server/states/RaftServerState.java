package gash.router.server.states;

/**
 * Created by rentala on 4/11/17.
 */


import pipe.election.Election;
import pipe.election.Election.LeaderElection;
import pipe.election.Election.LeaderElectionResponse;
import pipe.work.Work;
import pipe.work.Work.LogAppendEntry;
import routing.Pipe;

/**
 *  Parent state class that defines every node - Leader Candidate Follower
 */
public interface RaftServerState {

    public void requestVote(LeaderElection leaderElectionRequest);
    public void startElection();
    public void leaderElect();
    public void logAppend(LogAppendEntry logEntry);
	public void collectVote(LeaderElectionResponse leaderElectionResponse);
	public void declareLeader();
	public void heartbeat(LogAppendEntry hearbeat);
	void readFile(Pipe.ReadBody readBody);
    void writeFile(Pipe.WriteBody writeBody);
    void deleteFile(Pipe.ReadBody readBody);

}
