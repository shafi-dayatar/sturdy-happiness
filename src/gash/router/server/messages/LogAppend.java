package gash.router.server.messages;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.ServerState;
import gash.router.server.states.RaftServerState;
import pipe.common.Common.Header;
import pipe.work.Work.LogAppendEntry;
import pipe.work.Work.LogEntry;
import pipe.work.Work.LogEntryList;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkMessage.MessageType;

public class LogAppend extends Message {
	protected static Logger logger = LoggerFactory.getLogger("LogAppendEntry Message");
	
    MessageType type = null;
    LogAppendEntry logEntry = null;

    public LogAppend(WorkMessage msg) {
        // TODO Auto-generated constructor stub
        unPackHeader( msg.getHeader());
        type = msg.getType();
        logEntry = msg.getLogAppendEntries();
    }
    
    @java.lang.Override
    public void processMessage(ServerState state) {
    	RaftServerState serverState = state.getRaftState();
    	if (type == MessageType.HEARTBEAT){ 
    		serverState.heartbeat(logEntry);
    	}else if (type == MessageType.LOGAPPENDENTRY){		
    		serverState.logAppend(logEntry);
    	}
        return;
    }
    
	public static WorkMessage createLogAppendResponse(int sourceId, int destId, int currentIndex, 
			int currentTerm, boolean success){
		WorkMessage.Builder msgBuilder = WorkMessage.newBuilder();
		msgBuilder.setType(MessageType.LOGAPPENDENTRY);
		msgBuilder.setSecret(9999);
		
		Header.Builder header = Header.newBuilder();
		header.setDestination(destId);
		header.setNodeId(sourceId);
		header.setTime(System.currentTimeMillis());
		
		msgBuilder.setHeader(header);
		
		LogAppendEntry.Builder logAppend = LogAppendEntry.newBuilder();
		logAppend.setElectionTerm(currentTerm);
		logAppend.setSuccess(success);
		logAppend.setPrevLogIndex(currentIndex);
		logAppend.setLeaderNodeId(sourceId);
		
		msgBuilder.setLogAppendEntries(logAppend);
		return msgBuilder.build();
	}
	
	 
		/**
		 * Build AppendRequest to send to a follower with log entries
		 * starting from logStartIndex to latestIndex
		 */
		public static WorkMessage resendAppendRequest(int leaderId, int followerId,
				int currentTerm, int commitIndex, int logIndex, 
				LogEntry log,int lastLogIndex, int lastLogTerm ) {
			
			WorkMessage.Builder wmb = WorkMessage.newBuilder();
			Header.Builder hdb = Header.newBuilder();
			hdb.setNodeId(leaderId);
			hdb.setTime(System.currentTimeMillis());
			hdb.setDestination(followerId);
			
		    wmb.setHeader(hdb.build());
		    
		    LogEntryList.Builder l = LogEntryList.newBuilder();
			l.addAllEntry(Arrays.asList(log));
			
		    LogAppendEntry.Builder le = LogAppendEntry.newBuilder();
			le.setElectionTerm(currentTerm);
			le.setPrevLogIndex(lastLogIndex);
			le.setPrevLogTerm(lastLogTerm);
			le.setLeaderCommitIndex(commitIndex);
			le.setLeaderNodeId(leaderId);
			le.setEntrylist(l.build());	    

		    wmb.setLogAppendEntries(le.build());
			wmb.setType(WorkMessage.MessageType.LOGAPPENDENTRY);
			wmb.setSecret(11111);
			return wmb.build();
		}
		
	    public static WorkMessage createLogAppendEntry(int leaderId,int followerId, int currentTerm, 
	    		int lastLogIndex, int lastLogTerm, int commitIndex,
	    		LogEntry logEntry) {

			// TODO Auto-generated method stub
			WorkMessage.Builder wmb = WorkMessage.newBuilder();
			Header.Builder hdb = Header.newBuilder();
			hdb.setNodeId(leaderId);
			hdb.setTime(System.currentTimeMillis());
			hdb.setDestination(followerId);
			
			wmb.setHeader(hdb.build());
			
			LogEntryList.Builder entryList = LogEntryList.newBuilder();
		    entryList.addEntry(logEntry);
		    
		    LogAppendEntry.Builder logAppend = LogAppendEntry.newBuilder();
		    
		    logAppend.setElectionTerm(currentTerm);
		    logAppend.setPrevLogIndex(lastLogIndex);
		    logAppend.setPrevLogTerm(lastLogTerm);
		    logAppend.setLeaderCommitIndex(commitIndex);
		    logAppend.setLeaderNodeId(leaderId);
		    logAppend.setEntrylist(entryList.build());	    

		    wmb.setLogAppendEntries(logAppend.build());
			wmb.setType(WorkMessage.MessageType.LOGAPPENDENTRY);
			wmb.setSecret(9999);
			return wmb.build();
			
		}
}
