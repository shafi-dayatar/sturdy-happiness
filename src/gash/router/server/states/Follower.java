package gash.router.server.states;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import gash.router.server.ServerState;
import gash.router.server.messages.ElectionMessage;
import gash.router.server.messages.LogAppend;
import pipe.common.Common.Header;
import pipe.election.Election;
import pipe.election.Election.LeaderElection;
import pipe.work.Work.FileChunkData;
import pipe.work.Work.LogAppendEntry;
import pipe.work.Work.LogAppendResponse;
import pipe.work.Work.LogEntry;
import pipe.work.Work.LogEntry.Builder;
import pipe.work.Work.LogEntryList;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkMessage.MessageType;
import routing.Pipe;
import pipe.common.Common.ReadBody;
import pipe.common.Common.Response;
import pipe.common.Common.Response.Status;
import pipe.common.Common.WriteBody;

/**
 * TO DO:
 * 1) As leader gets elected it should reset electionVotes from previous term, for memory performance;
 * 2) 
 *
 */

public class Follower implements RaftServerState {
    
	protected static Logger logger = LoggerFactory.getLogger("Follower-State");
	private ServerState state;
	
    private ConcurrentHashMap<Integer, Integer> electionVotes =  new ConcurrentHashMap<Integer, Integer>();
    
    public Follower(ServerState state){
        this.state = state;
    }
    
    public void toCandidate(){
        logger.info("Timed out ! To candidate state .... ");
    }

    //change this method name to electionVoteResponse
	public void requestVote(LeaderElection request) {
		// TODO Auto-generated method stub
		int logIndex = state.getLog().lastIndex();
		int logTerm = state.getLog().lastLogTerm(logIndex);
		WorkMessage wm = null;
		
		if( request.getTerm() < state.getCurrentTerm() ||
				request.getLastLogTerm() < logTerm ||
				request.getLastLogIndex() < logIndex){
			/**
			 * vote will be false as: this candidate is lagging
			 */
			wm = ElectionMessage.createVoteResponse(request.getCandidateId(), state.getNodeId(), 
					request.getTerm(), false);
		}else{
			if(electionVotes.containsKey(request.getTerm())){
				wm  =  ElectionMessage.createVoteResponse(request.getCandidateId(), state.getNodeId(), 
						request.getTerm(), false);
			}
			else{
				state.setCurrentTerm(request.getTerm());
				electionVotes.put(request.getTerm(), request.getCandidateId());
				wm  =  ElectionMessage.createVoteResponse(request.getCandidateId(), state.getNodeId(), 
						request.getTerm(), true);
			}
			   
		}
		state.getOutBoundMessageQueue().addMessage(wm);
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
		state.getLog().setCommitIndex(newCommitIndex);
		if(newCommitIndex > state.getLog().lastIndex()) {
			state.getLog().setLastApplied(newCommitIndex);
			
		}
	}


	@Override
	public void heartbeat(LogAppendEntry heartbeat) {
		logger.info("Heartbeat details : " +  heartbeat.toString());
		logger.info("Received a heartbeat message in Follower state");
		logger.info("Current Elected Leader is :" + heartbeat.getLeaderNodeId() + 
				", for term : " + heartbeat.getElectionTerm() );
		int followerLastIndex = state.getLog().lastIndex();
		int lastIndex = heartbeat.getPrevLogIndex();
		state.getElectionTimer().resetElectionTimeOut();
		state.setLeaderId(heartbeat.getLeaderNodeId());
		state.setCurrentTerm(heartbeat.getElectionTerm());
		state.setLeaderKnown(true);
		if(followerLastIndex < lastIndex ){
			WorkMessage wm = LogAppend.createLogAppendResponse(state.getNodeId(), heartbeat.getLeaderNodeId(),
					followerLastIndex, state.getCurrentTerm(), true);
			state.getOutBoundMessageQueue().addMessage(wm);
			logger.info("My leader known index is : " + lastIndex +
					", and my last known index is :" + state.getLog().lastIndex() );
			logger.info("requesting log append entries : " + wm.toString());
			
		}
		
		//need to send a response to server provide followers log information
	}



	@Override
	public synchronized void logAppend(LogAppendEntry logEntry) {
		// TODO Auto-generated method stub
		WorkMessage wm = null;
		int lastIndex = state.getLog().lastIndex();
		int lastTerm= state.getLog().lastLogTerm(lastIndex);

		logger.info("Follower Recieved Log Entry" + logEntry.toString());

		if(logEntry.getElectionTerm() < state.getCurrentTerm() && 
				lastIndex != logEntry.getPrevLogIndex()){
			if (lastTerm != logEntry.getPrevLogTerm()){
				//need to delete logs entry
			}

			wm = LogAppend.createLogAppendResponse(state.getNodeId(), logEntry.getLeaderNodeId(),
					lastIndex, state.getCurrentTerm(), false);
		}
		else{
			//reply false, with currentTerm.
			if (logEntry.hasEntrylist()){
				LogEntryList lgl = logEntry.getEntrylist();
				List<LogEntry> entries = lgl.getEntryList();
				for (LogEntry entry: entries ){
					lastIndex = entry.getLogId();
					state.getLog().appendEntry(lastIndex, entry);	
					
				}
				if (state.getLog().lastIndex() != state.getLastLogIndex()){
					
				}
			}
			if(logEntry.getLeaderCommitIndex() > state.getLog().getCommitIndex()){
				state.getLog().setCommitIndex(Math.min(logEntry.getLeaderCommitIndex(),
						lastIndex));
			}
			wm = LogAppend.createLogAppendResponse(state.getNodeId(), logEntry.getLeaderNodeId(),
					lastIndex, state.getCurrentTerm(), true);
		}
		logger.info("Log Append response from follower would  be : " + wm.toString());
		state.getOutBoundMessageQueue().addMessage(wm);
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
		String ext[] = chunk.getFileName().split("\\.");
		String file_name =  "./data/" + ext[0] + "_" + chunk.getFileId() + "_" + chunk.getChunkId();
		logger.info(file_name + " ---> serving read chunk data request for it. ");
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
	    	hd.setDestination(state.getLeaderId());
	    	hd.setNodeId(state.getNodeId());
	    	hd.setTime(System.currentTimeMillis());
	    	
	    	FileChunkData.Builder data =  FileChunkData.newBuilder();
	    	data.setFileId(chunk.getFileId());
	    	data.setChunkId(chunk.getChunkId());
	    	data.setFileName(chunk.getFileName());
	    	data.setChunkData(ByteString.copyFrom(fileData));
			data.setReplyTo(chunk.getReplyTo());
	    	data.setSuccess(true);
	    	
	    	msgBuilder.setHeader(hd);
	    	msgBuilder.setChunkData(data);
	    	state.getOutBoundMessageQueue().addMessage(msgBuilder.build());
	    	
	    }catch(Exception e){
	        logger.info("Got error while reading chunk data in follower: " );
	        e.printStackTrace();
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
	    	msgBuilder.setType(MessageType.CHUNKFILEDATAWRITERESPONSE);
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

    }

    @Override
    public void writeChunkDataResponse(FileChunkData chunk) {

    }

    @Override
	public void stealWork() {
		//Follower can steal work
		try
		{
			if(state.isLeaderKnown()){
				WorkMessage.Builder msgBuilder = WorkMessage.newBuilder();
				msgBuilder.setSecret(9999999);
				msgBuilder.setType(MessageType.WORKSTEALREQUEST);
				Header.Builder hd = Header.newBuilder();
				hd.setDestination(state.getLeaderId());
				hd.setNodeId(state.getNodeId());
				hd.setTime(System.currentTimeMillis());
				msgBuilder.setHeader(hd);
				state.getOutBoundMessageQueue().addMessage(msgBuilder.build());

			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.info(" stealWork request failed due to : ");
			e.printStackTrace();
		}

	}

	@Override
	public Pipe.CommandMessage getWork(int node_id) {
		//TODO : Add logic to check if the work message can be handled by the requested node id
		return state.getInBoundReadTaskQueue().getQueuedMessage(Integer.toString(node_id));
	}

	@Override
	public void processReadRequest(Pipe.CommandMessage cmdMsg) {
		FileChunkData.Builder chBuilder = FileChunkData.newBuilder();
		chBuilder.setFileName(cmdMsg.getRequest().getRrb().getFilename());
		int chunk_id = cmdMsg.getRequest().getRrb().getChunkId();
		chBuilder.setChunkId(chunk_id);
		int fileId = state.getDb().getFileId(cmdMsg.getRequest().getRrb().getFilename());
		chBuilder.setFileId(fileId);
		chBuilder.setReplyTo(cmdMsg.getHeader().getMessageId());
		this.readChunkData(chBuilder.build());
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

	@Override
	public Response getFileChunkLocation(ReadBody request) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status writeFile(WriteBody writeBody) {
		// TODO Auto-generated method stub
		return Status.NOLEADER;
	}


}
