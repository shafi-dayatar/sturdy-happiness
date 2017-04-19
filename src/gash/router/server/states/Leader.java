package gash.router.server.states;

import java.awt.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import gash.router.client.MessageClient;
import gash.router.server.IOUtility;
import gash.router.server.db.SqlClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.ServerState;
import gash.router.server.edges.EdgeMonitor;
import gash.router.server.log.LogInfo;
import pipe.common.Common.Header;
import pipe.election.Election;
import pipe.election.Election.LeaderElection;
import pipe.work.Work;
import pipe.work.Work.Command;
import pipe.work.Work.FileChunkData;
import pipe.work.Work.LogAppendEntry;
import pipe.work.Work.LogEntry;
import pipe.work.Work.LogEntry.*;
import pipe.work.Work.LogEntryList;
import pipe.work.Work.WorkMessage.MessageType;
import pipe.work.Work.Node;
import pipe.work.Work.WorkMessage;
import routing.Pipe;
import routing.Pipe.Chunk;
import routing.Pipe.ReadRequest;
import routing.Pipe.WriteRequest;

import com.google.protobuf.ByteString;
/**
 * Created by rentala on 4/11/17.
 */
public class Leader implements RaftServerState, Runnable {

    protected static Logger logger = LoggerFactory.getLogger("Leader-State");
    private ServerState state;
	SqlClient sqlClient;
	private int replicationFactor = 3; // this should come from config file.



    // stores the next logIndex to be sent to each follower
 	Hashtable<Integer, Integer> nextIndex = new Hashtable<Integer, Integer>();
 	// stores the last logIndex response from each follower
 	Hashtable<Integer, Integer> matchIndex = new Hashtable<Integer, Integer>();
 	
    private boolean isLeader;

    public Leader(ServerState state){
        this.state = state;
    }

	@Override
	public void declareLeader() {
		logger.info("Sending heartbeat Messages to followers");
		WorkMessage hearbeat = createHeartBeatMessage();
		state.getOutBoundMessageQueue().addMessage(hearbeat);
	}

	@Override
    public synchronized void appendEntries(ArrayList<LogEntry.Builder> logEntryBuilder){
        //logger.info("appendEntries = " + entry);
        for (LogEntry.Builder builder : logEntryBuilder){
        	builder.setLogId(state.getLog().lastIndex());
            builder.setTerm(state.getCurrentTerm());
            LogEntry entry  = builder.build();
            Set<Integer> keys = nextIndex.keySet();
            for(Integer nodeId : keys){
            	int nextlogindex = nextIndex.get(nodeId);
            	state.getLog().appendEntry(entry);
            	if (state.getLog().lastIndex() >= nextlogindex){
            		
            		//WorkMessage wm = createLogAppendEntry(nodeId, entry);
            		//state.getOutBoundMessageQueue().addMessage(wm);
            	}
            }
        }
    }
    
	public synchronized void appendEntries(LogEntry.Builder logEntryBuilder){
		int lastLogIndex = state.getLog().lastIndex();
		int lastLogTerm = state.getLog().lastLogTerm();
        int commitIndex = state.getLog().getCommitIndex();
        
		lastLogIndex = lastLogIndex == -1 ? 0 : lastLogIndex;
		lastLogTerm = lastLogTerm == -1 ? state.getCurrentTerm() : lastLogTerm;
				
		logEntryBuilder.setTerm(lastLogTerm);
		logEntryBuilder.setLogId(lastLogIndex+1);
		LogEntry entry  = logEntryBuilder.build();
		
		logger.info("last log index :  " + lastLogIndex + ",\n ");
		logger.info("Current Log Id is : " +  lastLogIndex+1);
        state.getLog().appendEntry(entry);
        state.getLog().setLastApplied(lastLogIndex+1);
        

        Set<Integer> keys = nextIndex.keySet();
        logger.info("Last Log index of leader is : " + lastLogIndex);
        for(Integer nodeId : keys){
            int nextlogindex = nextIndex.get(nodeId);
            logger.info("NextIndex Table is : " + nextIndex.toString());
            if (lastLogIndex + 1  >= nextlogindex){
            	logger.info("Sending LogAppend to all connected server");
            	WorkMessage wm = createLogAppendEntry(nodeId, lastLogIndex, lastLogTerm, commitIndex, entry);
            	logger.info("Message is : " +  wm.toString());
            	state.getOutBoundMessageQueue().addMessage(wm);
            }
        }
    }
	
	@Override
	public void logAppend(LogAppendEntry logEntry) {
		// TODO Auto-generated method stub
		logger.info("Got logAppend response from follower: " + logEntry.toString());
		int nodeId = logEntry.getLeaderNodeId();
		if (logEntry.getSuccess()){
			updateNextAndMatchIndex(logEntry.getLeaderNodeId(), logEntry.getPrevLogIndex());
			if (logEntry.getPrevLogIndex() < state.getLastLogIndex()){
				//resendAppendRequest(nodeId, nextIndex.get(nodeId)+1);
			}
		}else{
			int logId = getDecrementedNextIndex(nodeId);
		    //resendAppendRequest(nodeId,logId);
		}
	}
    
    
	/**
	 * Build AppendRequest to send to a follower with log entries
	 * starting from logStartIndex to latestIndex
	 */
	public WorkMessage resendAppendRequest(int fNode, int logIndex) {
		WorkMessage.Builder wmb = WorkMessage.newBuilder();
		Header.Builder hdb = Header.newBuilder();
		hdb.setNodeId(state.getNodeId());
		hdb.setTime(System.currentTimeMillis());
		hdb.setDestination(fNode);
		
	    wmb.setHeader(hdb.build());
	    
	    LogEntryList.Builder l = LogEntryList.newBuilder();
		l.addAllEntry(Arrays.asList(state.getLog().getEntries(logIndex)));
		
	    LogAppendEntry.Builder le = LogAppendEntry.newBuilder();
		le.setElectionTerm(state.getCurrentTerm());
		le.setPrevLogIndex(state.getLastLogIndex());
		le.setPrevLogTerm((logIndex - 1) != 0 ? state.getLog().getEntry(logIndex - 1).getTerm() : 0);
		le.setLeaderCommitIndex(state.getLog().getCommitIndex());
		le.setLeaderNodeId(state.getNodeId());
		le.setEntrylist(l.build());	    

	    wmb.setLogAppendEntries(le.build());
		wmb.setType(WorkMessage.MessageType.LOGAPPENDENTRY);
		wmb.setSecret(11111);
		return wmb.build();
	}
	
    private WorkMessage createLogAppendEntry(int nodeId, int lastLogIndex, 
    		int lastLogTerm, int commitIndex,
    		LogEntry logEntry) {

		// TODO Auto-generated method stub
		WorkMessage.Builder wmb = WorkMessage.newBuilder();
		Header.Builder hdb = Header.newBuilder();
		hdb.setNodeId(state.getNodeId());
		hdb.setTime(System.currentTimeMillis());
		hdb.setDestination(nodeId);
		
		wmb.setHeader(hdb.build());
		
		LogEntryList.Builder entryList = LogEntryList.newBuilder();
	    entryList.addEntry(logEntry);
	    
	    LogAppendEntry.Builder logAppend = LogAppendEntry.newBuilder();
	    
	    logAppend.setElectionTerm(state.getCurrentTerm());
	    logAppend.setPrevLogIndex(lastLogIndex);
	    logAppend.setPrevLogTerm(lastLogTerm);
	    logAppend.setLeaderCommitIndex(commitIndex);
	    logAppend.setLeaderNodeId(state.getNodeId());
	    logAppend.setEntrylist(entryList.build());	    

	    wmb.setLogAppendEntries(logAppend.build());
		wmb.setType(WorkMessage.MessageType.LOGAPPENDENTRY);
		wmb.setSecret(9999);
		return wmb.build();
		
	}
	
	
	/**
	 * After leader is elected, reinitialize nextIndex 
	 * to lastIndex+1 and matchIndex to 0 of all followers
	 */
	
	private void reinitializeIndexes() {
		for (Node node : state.getEmon().getOutBoundRouteTable()) {
			// Next Index
			nextIndex.put(node.getNodeId(), state.getLog().lastIndex() + 1);
			// Match Index
			matchIndex.put(node.getNodeId(), (int) 0);
		}
	}
	
	/**
	 * If follower rejects an AppendRequest due to log mismatch,
	 * decrement the follower's next index and return the
	 * decremented index
	 */
	public int getDecrementedNextIndex(int nodeId) {
		int id = nextIndex.get(nodeId) - 1 ;
		nextIndex.put(nodeId, id);
		return id;
	}
	
	/**
	 * When AppendRequest is successful for a node, update it's 
	 * nextIndex to lastIndex+1 and matchIndex to lastIndex
	 */
	public void updateNextAndMatchIndex(int nodeId, int lastIndex ) {
		if(!matchIndex.containsKey(nodeId)) {
			// Next Index
			nextIndex.put(nodeId, state.getLog().lastIndex() + 1);
			// Match Index
			matchIndex.put(nodeId, (int) 0);
		}
		
		if(matchIndex.get(nodeId) == lastIndex - 1) {
			nextIndex.put(nodeId, lastIndex + 1);
			matchIndex.put(nodeId, lastIndex);
			// check if commit index is also increased
			//if matchIndex increased for most server then updateCommitIndex
			logger.info("Match Index is : " + matchIndex.toString());
			logger.info("Next Index is : " + nextIndex.toString());
			Set<Integer> keys = matchIndex.keySet();
			int refelectedOn = 0;
			for(Integer id : keys){
				if(lastIndex <= matchIndex.get(id)){
					refelectedOn++;
				}
			}
			logger.info("Log Entry appended on : " + refelectedOn +" servers..... ");
			if (refelectedOn >= (state.getEmon().getTotalNodes()/2 + 1)){
				logger.info("Time To Commit, everything looks perfect");
				updateCommitIndex(lastIndex);
			}
		}
	}
	
	
	public void updateCommitIndex(int lastIndex) {
		state.getLog().setCommitIndex(lastIndex);		
    }

	public WorkMessage createHeartBeatMessage(){
		WorkMessage.Builder wmb = WorkMessage.newBuilder();
		
		
		Header.Builder hdb = Header.newBuilder();
		hdb.setNodeId(state.getNodeId());
		hdb.setDestination(-1);
		hdb.setTime(System.currentTimeMillis());
		
		
		LogAppendEntry.Builder heartbeat = LogAppendEntry.newBuilder();
		heartbeat.setElectionTerm(state.getCurrentTerm());
		heartbeat.setLeaderNodeId(state.getNodeId());
		heartbeat.setPrevLogIndex(state.getLastLogIndex());
		heartbeat.setPrevLogTerm(state.getCurrentTerm() - 1);
		
		wmb.setSecret(5555);
		wmb.setType(MessageType.HEARTBEAT);
		wmb.setHeader(hdb);
		wmb.setLogAppendEntries(heartbeat);
		return wmb.build();

	}

	@Override
	public void heartbeat(LogAppendEntry heartbeat) {
		// TODO Auto-generated method stub
		if (state.getCurrentTerm()<heartbeat.getElectionTerm()){
			//leader should step down, as it was previously elected and something went wrong
			state.setCurrentTerm(heartbeat.getElectionTerm());
			setLeader(false);
			state.becomeFollower();
		}
		
	}

	@Override
	public void run() {
		while(isLeader){
			declareLeader();
			try {
				logger.info("Will Send a hearbeat message in " + state.getConf().getHeartbeatDt());
				Thread.sleep(state.getConf().getHeartbeatDt());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

    public boolean isLeader() {
		return isLeader;
	}

	public void setLeader(boolean isLeader) {
		this.isLeader = isLeader;
	}

	@java.lang.Override
	public void requestVote(LeaderElection leaderElectionRequest) {

	}

	@java.lang.Override
	public void startElection() {

	}

	@java.lang.Override
	public void leaderElect() {

	}

	@java.lang.Override
	public void collectVote(Election.LeaderElectionResponse leaderElectionResponse) {
		/**
		 * if leader has been elected than there is no need to process other node response.
		 */
		return;

	}

	@Override
	public byte[] readFile(ReadRequest readBody) {
		return new IOUtility().readFile(readBody);
	}
	
	
	public WorkMessage createWriteFileMessage(ReadRequest writeMessage){
		return null;
	}

	
	public WorkMessage createFileWriteMessage(int dest, int fileId, int chunkId, String FileName, ByteString chunkData){
		WorkMessage.Builder msgBuilder = WorkMessage.newBuilder();
    	msgBuilder.setSecret(9999999);
    	msgBuilder.setType(MessageType.CHUNKFILEDATAWRITE);
    	Header.Builder hd = Header.newBuilder();
    	hd.setDestination(dest);
    	hd.setNodeId(state.getNodeId());
    	hd.setTime(System.currentTimeMillis());
    	
    	FileChunkData.Builder data =  FileChunkData.newBuilder();
    	data.setReplyTo(state.getNodeId());
    	data.setFileId(fileId);
    	data.setChunkId(chunkId);
    	data.setFileName(FileName);
    	data.setChunkData(chunkData);
    	msgBuilder.setHeader(hd);
    	msgBuilder.setChunkData(data);
    	return msgBuilder.build();
    	
	}
	
	@Override
	public int writeFile(WriteRequest write) {
		

		int fileId = (int)state.getDb().getFileId(write.getFilename(), write.getFileExt());
		
		if (fileId != -1) {
			String fileName = write.getFilename();
			LogEntry.Builder logEntryBuilder = LogEntry.newBuilder();
			logEntryBuilder.setAction(DataAction.INSERT);
			Command.Builder command = Command.newBuilder();
			command.setClientId(999);
			command.setKey("FileId");
			command.setValue(Integer.toString(fileId));
			logEntryBuilder.addData(command);
			command.setKey("Filename");
			command.setValue(fileName);
			logEntryBuilder.addData(command);
			command.setKey("FileExt");
			command.setValue(write.getFileExt());
			logEntryBuilder.addData(command);
			command.setKey("chunk_id");
			command.setValue(Integer.toString(write.getChunk().getChunkId()));
			logEntryBuilder.addData(command);
			command.setKey("located_at");
			ArrayList<Node> followers = state.getEmon().getOutBoundRouteTable();
			ArrayList<String> location = new ArrayList<String>(); 
			int loc = new Random().nextInt(followers.size());
			for(int i = 0; i< 2; i++){
				int currentLoc = (loc+i) % followers.size();
				Node node = followers.get(currentLoc);
				//new IOUtility(node.getIpAddr()).writeFile(write);
				Chunk chunk = write.getChunk();
				WorkMessage msg  = createFileWriteMessage(node.getNodeId(), fileId, chunk.getChunkId(), 
						fileName, chunk.getChunkData());
				logger.info(msg.toString());
				state.getOutBoundMessageQueue().addMessage(msg);
				String addr =  node.getNodeId() + ":" + node.getIpAddr() + ":4" + node.getNodeId() + "68";
				location.add(addr);
		     }
			command.setValue(location.toString());
			logEntryBuilder.addData(command);
			appendEntries(logEntryBuilder);
		}
		
		
		
		return 0;//IOUtility.writeFile(write);
	}

	public void setNextAndMatchIndex() {
		// TODO Auto-generated method stub
		ArrayList<Node> nodes = state.getEmon().getOutBoundRouteTable();
		for (Node node : nodes){
			nextIndex.put(node.getNodeId(), state.getLog().lastIndex()+1);
			matchIndex.put(node.getNodeId(), 0);
		}
		
	}

	@Override
	public void readChunkData(FileChunkData chunk) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeChunkData(FileChunkData chunk) {
		// TODO Auto-generated method stub
		
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
