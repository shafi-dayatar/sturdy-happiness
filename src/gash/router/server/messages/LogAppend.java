package gash.router.server.messages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.PrintUtil;
import gash.router.server.ServerState;
import gash.router.server.states.RaftServerState;
import pipe.election.Election;
import pipe.work.Work.LogAppendEntry;
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
}
