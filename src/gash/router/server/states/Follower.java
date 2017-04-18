package gash.router.server.states;

import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import gash.router.server.db.SqlClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.PrintUtil;
import gash.router.server.ServerState;
import gash.router.server.log.LogInfo;
import pipe.election.Election;
import pipe.election.Election.LeaderElection;
import pipe.election.Election.LeaderElectionResponse;
import pipe.work.Work;

import pipe.common.Common.Header;
import pipe.work.Work.FileChunkData;
import pipe.work.Work.LogAppendEntry;
import pipe.work.Work.LogAppendResponse;
import pipe.work.Work.LogEntry;
import pipe.work.Work.LogEntry.Builder;
import pipe.work.Work.LogEntryList;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkMessage.MessageType;
import routing.*;
import routing.Pipe;
import com.google.protobuf.ByteString;
import gash.router.server.IOUtility;
/**
 * Created by rentala on 4/11/17.
 */

/**
 * TO DO:
 * 1) As leader gets elected it should reset electionVotes from previous term, for memory performance;
 * 2) 
 *
 */

public class Follower implements RaftServerState {
    
	protected static Logger logger = LoggerFactory.getLogger("Follower-State");
	private ServerState state;
	//private List<integer, boolean> vote = new ArrayList<Integer, Boolean>();
    private ConcurrentHashMap<Integer, Integer> electionVotes =  new ConcurrentHashMap<Integer, Integer>();
    private LogInfo log;
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

    //change this method name to electionVoteResponse
	public void requestVote(LeaderElection request) {
		// TODO Auto-generated method stub
		int logIndex = state.getLog().getLastApplied();
		int logTerm = state.getLog().lastLogTerm();
		WorkMessage wm = null;
		
		if( request.getTerm() < state.getCurrentTerm() ||
				request.getLastLogTerm() < logTerm ||
				request.getLastLogIndex() < logIndex){
			/**
			 * vote will be false as: this candidate is lagging
			 */
			wm = createVoteResponse(request.getCandidateId(), state.getNodeId(), 
					request.getTerm(), false);
		}else{
			if(electionVotes.containsKey(request.getTerm())){
				wm  =  createVoteResponse(request.getCandidateId(), state.getNodeId(), 
						request.getTerm(), false);
			}
			else{
				state.setCurrentTerm(request.getTerm());
				electionVotes.put(request.getTerm(), request.getCandidateId());
				wm  =  createVoteResponse(request.getCandidateId(), state.getNodeId(), 
						request.getTerm(), true);
			}
			   
		}
		state.getOutBoundMessageQueue().addMessage(wm);
	}

	private WorkMessage createVoteResponse(int destId, int sourceId, int term, boolean b) {
		logger.info("will I vote for " + destId + " for term : " + term +"?, and answer is : " + b);
		// TODO Auto-generated method stub
		WorkMessage.Builder wmb = WorkMessage.newBuilder();
		Header.Builder hdb = Header.newBuilder();
		hdb.setNodeId(sourceId);
		hdb.setTime(System.currentTimeMillis());
		hdb.setDestination(destId);
	    wmb.setHeader(hdb);
	    
	    LeaderElectionResponse.Builder leb = LeaderElectionResponse.newBuilder();
	    leb.setForTerm(term);
	    leb.setFromNodeId(sourceId);
	    leb.setVoteGranted(b);
	    
		wmb.setLeaderElectionResponse(leb);
		wmb.setType(MessageType.LEADERELECTIONREPLY);
		wmb.setSecret(10100);
		return wmb.build();
	}
	

	public void startElection() {
		// TODO Auto-generated method stub	
		
	}

	public void leaderElect() {
		// TODO Auto-generated method stub
		
	}

	
    @java.lang.Override
    public void collectVote(Election.LeaderElectionResponse leaderElectionResponse) {

    }

	@Override
	public void declareLeader() {
		// TODO Auto-generated method stub
		
	}
	
	class Vote {
		int candidateId;
		boolean vote;
		public Vote(int can, boolean vote){
			candidateId = can;
			this.vote = vote;
		}
	}

	
	/**
	 * Build appendResponse to send to the leader node
	 */
	public WorkMessage getAppendResponse(int lNode, boolean responseFlag) {

		WorkMessage.Builder wmb = WorkMessage.newBuilder();
		Header.Builder hdb = Header.newBuilder();
		hdb.setNodeId(state.getNodeId());
		hdb.setTime(System.currentTimeMillis());
		hdb.setDestination(lNode);
		
	    wmb.setHeader(hdb.build());

		LogAppendResponse.Builder lr = LogAppendResponse.newBuilder();
		lr.setElectionTerm(state.getCurrentTerm());
		
	    wmb.setLogAppendResponse(lr.build());
		wmb.setType(WorkMessage.MessageType.LOGAPPENDRESPONSE);
		wmb.setSecret(12222);
		return wmb.build();
	}
	
	/**
	 *	if commitIndex gets updated by leader,
	 * update the commitIndex of self and send message to Resource with 
	 * entries starting from lastApplied + 1 to commit index
	 */
	public void updateCommitIndex(int newCommitIndex) {
		log.setCommitIndex(newCommitIndex);
		if(log.getCommitIndex() >log.getLastApplied()) {
			log.setLastApplied(log.getCommitIndex());
		}
	}


	@Override
	public void heartbeat(LogAppendEntry heartbeat) {
		logger.info("Got a heartbeat message in While server was in Follower state");
		logger.info("Current Elected Leader is :" + heartbeat.getLeaderNodeId() + 
				", for term : " + heartbeat.getElectionTerm() );
		state.getElectionTimer().resetElectionTimeOut();
		state.setLeaderId(heartbeat.getLeaderNodeId());
		state.becomeFollower();
		state.setCurrentTerm(heartbeat.getElectionTerm());
		state.setLeaderKnown(true);
	}

	@Override
	public byte[] readFile(Pipe.ReadBody readBody) {
		return null;//IOUtility.readFile(readBody);
	}

	@Override
	public int writeFile(Pipe.WriteBody readBody) {
		return 0;//IOUtility.writeFile(readBody);
	}

	@Override
	public void deleteFile(Pipe.ReadBody readBody) {

	}

	@Override
	public void logAppend(LogAppendEntry logEntry) {
		// TODO Auto-generated method stub
		WorkMessage wm = null;
		int lastIndex = state.getLog().lastIndex();
		int lastTerm= state.getLog().lastLogTerm();

		logger.info("Follower Recieved Log Entry" + logEntry.toString());
		//follower has not received any update from current Leader,
		//could be a stale message
		if(logEntry.getElectionTerm() < state.getCurrentTerm() && 
				lastIndex != logEntry.getPrevLogIndex()){
			if (lastTerm != logEntry.getPrevLogTerm()){
				//need to delete logs entry
			}

			wm = createLogAppendResponse(logEntry.getLeaderNodeId(),
					lastIndex, state.getCurrentTerm(), false);
		}
		else{
			//reply false, with currentTerm.
			if (logEntry.hasEntrylist()){
				LogEntryList lgl = logEntry.getEntrylist();
				List<LogEntry> entries = lgl.getEntryList();
				for (LogEntry entry: entries ){
					state.getLog().appendEntry(entry);	
					lastIndex = entry.getLogId();
				}
				if (state.getLog().lastIndex() != state.getLastLogIndex()){
					
				}
			}
			if(logEntry.getLeaderCommitIndex() > state.getLog().getCommitIndex()){
				state.getLog().setCommitIndex(Math.min(logEntry.getLeaderCommitIndex(),
						lastIndex));
			}
			wm = createLogAppendResponse(logEntry.getLeaderNodeId(),
					lastIndex, state.getCurrentTerm(), true);
		}
		state.getOutBoundMessageQueue().addMessage(wm);
	}
	
	public WorkMessage createLogAppendResponse(int destId, int currentIndex, 
			int currentTerm, boolean success){
		WorkMessage.Builder msgBuilder = WorkMessage.newBuilder();
		msgBuilder.setType(MessageType.LOGAPPENDENTRY);
		msgBuilder.setSecret(9999);
		
		Header.Builder header = Header.newBuilder();
		header.setDestination(destId);
		header.setNodeId(state.getNodeId());
		header.setTime(System.currentTimeMillis());
		
		msgBuilder.setHeader(header);
		
		LogAppendEntry.Builder logAppend = LogAppendEntry.newBuilder();
		logAppend.setElectionTerm(currentTerm);
		logAppend.setSuccess(success);
		logAppend.setPrevLogIndex(currentIndex);
		logAppend.setLeaderNodeId(state.getNodeId());
		
		msgBuilder.setLogAppendEntries(logAppend);
		return msgBuilder.build();
	}

	@Override
	public void appendEntries(ArrayList<Builder> logEntryBuilder) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void appendEntries(Builder logEntryBuilder) {
		// TODO Auto-generated method stub
	}

	@Override
	public void readChunkData(FileChunkData chunk) {
		// TODO Auto-generated method stub
		
		String file_name =  "./data/" + chunk.getFileName() + "_" + chunk.getFileId() + "_" + chunk.getChunkId();
		File file = new File(file_name);
	    byte[] fileData = new byte[(int) file.length()];
	    try{
	    	DataInputStream dis = new DataInputStream(new FileInputStream(file));
	    	dis.readFully(fileData);
	    	dis.close();
	    	
	    	WorkMessage.Builder msgBuilder = WorkMessage.newBuilder();
	    	msgBuilder.setSecret(9999999);
	    	msgBuilder.setType(MessageType.CHUNKFILEDATAREADRESPONSE);
	    	Header.Builder hd = Header.newBuilder();
	    	hd.setDestination(chunk.getReplyTo());
	    	hd.setNodeId(state.getNodeId());
	    	hd.setTime(System.currentTimeMillis());
	    	
	    	FileChunkData.Builder data =  FileChunkData.newBuilder();
	    	data.setFileId(chunk.getFileId());
	    	data.setChunkId(chunk.getChunkId());
	    	data.setFileName(chunk.getFileName());
	    	data.setChunkData(ByteString.copyFrom(fileData));
	    	data.setSuccess(true);
	    	
	    	msgBuilder.setHeader(hd);
	    	msgBuilder.setChunkData(data);
	    	state.getOutBoundMessageQueue().addMessage(msgBuilder.build());
	    	
	    }catch(Exception e){
	    	
	    }
        
	}

	@Override
	public void writeChunkData(FileChunkData chunk) {
		// TODO Auto-generated method stub
		logger.info("Got a write chunk request");
		String file_name =  "./data/" + chunk.getFileName() + "_" + chunk.getFileId() + "_" + chunk.getChunkId();
        FileOutputStream writer = null;
        try {
        	writer = new FileOutputStream(file_name);
        	writer.write(chunk.getChunkData().toByteArray());
        	writer.close();
        	
        	WorkMessage.Builder msgBuilder = WorkMessage.newBuilder();
	    	msgBuilder.setSecret(9999999);
	    	msgBuilder.setType(MessageType.CHUNKFILEDATAREADRESPONSE);
	    	Header.Builder hd = Header.newBuilder();
	    	hd.setDestination(chunk.getReplyTo());
	    	hd.setNodeId(state.getNodeId());
	    	hd.setTime(System.currentTimeMillis());
	    	
	    	FileChunkData.Builder data =  FileChunkData.newBuilder();
	    	data.setFileId(chunk.getFileId());
	    	data.setChunkId(chunk.getChunkId());
	    	data.setFileName(chunk.getFileName());
	    	data.setSuccess(true);
	    	msgBuilder.setHeader(hd);
	    	msgBuilder.setChunkData(data);
	    	state.getOutBoundMessageQueue().addMessage(msgBuilder.build());
        } catch (Exception e) {
        	// TODO Auto-generated catch block
        	logger.info("Write chunk request failed due to : ");
	    	e.printStackTrace();
        	e.printStackTrace();
        }
	}

	@Override
	public void readChunkDataResponse(FileChunkData chunk) {
		// TODO Auto-generated method stub
		logger.info("Got A File Read Response");
		
	}

	@Override
	public void writeChunkDataResponse(FileChunkData chunk) {
		// TODO Auto-generated method stub
		logger.info("Got A file write response");
		
	}	


}
