package gash.router.server.states;

import java.util.ArrayList;

import pipe.common.Common.Response;
import pipe.common.Common.ReadBody;
import pipe.common.Common.WriteBody;
import pipe.common.Common.Response.Status;
import pipe.election.Election.LeaderElection;
import pipe.election.Election.LeaderElectionResponse;
import pipe.work.Work.FileChunkData;
import pipe.work.Work.LogAppendEntry;
import pipe.work.Work.LogEntry;
import routing.Pipe;

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



	public void readChunkData(FileChunkData chunk);
	public void writeChunkData(FileChunkData chunk);
	public void readChunkDataResponse(FileChunkData chunk);
	public void writeChunkDataResponse(FileChunkData chunk);
	Response getFileChunkLocation(ReadBody request);
	Status writeFile(WriteBody writeBody);

	public void stealWork();
    Pipe.CommandMessage getWork();

}
