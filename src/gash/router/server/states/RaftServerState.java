package gash.router.server.states;

import java.util.ArrayList;

/**
 * Created by rentala on 4/11/17.
 */


import pipe.election.Election;
import pipe.election.Election.LeaderElection;
import pipe.election.Election.LeaderElectionResponse;
import pipe.work.Work;
import pipe.work.Work.FileChunkData;
import pipe.work.Work.LogAppendEntry;
import pipe.work.Work.LogEntry;
import routing.Pipe;
import routing.Pipe.ReadBody;
import routing.Pipe.WriteBody;

/**
 *  Parent state class that defines every node - Leader Candidate Follower
 */
public interface RaftServerState {

	//change this method name to electionVoteResponse
	public void requestVote(LeaderElection leaderElectionRequest);
    public void startElection();
    public void leaderElect();
    public void logAppend(LogAppendEntry logEntry);
	public void collectVote(LeaderElectionResponse leaderElectionResponse);
	public void declareLeader();
	public void heartbeat(LogAppendEntry hearbeat);
	void appendEntries(ArrayList<LogEntry.Builder> logEntryBuilder);
	void appendEntries(LogEntry.Builder logEntryBuilder);
	// if it returns -1 - all good, else returns the chunk id it failed to write

    //void deleteFile(Pipe.ReadBody readBody);
	public void readChunkData(FileChunkData chunk);
	public void writeChunkData(FileChunkData chunk);
	public void readChunkDataResponse(FileChunkData chunk);
	public void writeChunkDataResponse(FileChunkData chunk);
	routing.Pipe.Response getFileChunkLocation(ReadBody request);
	int writeFile(WriteBody writeBody);

	public void stealWork();
    Work.WorkMessage getWork();

}
