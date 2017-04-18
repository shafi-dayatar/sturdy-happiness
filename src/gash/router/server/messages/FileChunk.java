package gash.router.server.messages;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.ServerState;
import gash.router.server.states.RaftServerState;
import pipe.work.Work.FileChunkData;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkMessage.MessageType;

public class FileChunk extends Message {
	
protected static Logger logger = LoggerFactory.getLogger("LogAppendEntry Message");
	
    MessageType type = null;
    FileChunkData chunk = null;
  

    public FileChunk(WorkMessage msg) {
        // TODO Auto-generated constructor stub
        unPackHeader( msg.getHeader());
        type = msg.getType();
        chunk = msg.getChunkData();
    }
    
    @java.lang.Override
    public void processMessage(ServerState state) {
    	RaftServerState serverState = state.getRaftState();
    	switch(type){
    	case CHUNKFILEDATAREAD:
    		serverState.readChunkData(chunk);
    		break;
    	case CHUNKFILEDATAWRITE:
    		serverState.writeChunkData(chunk);
    		break;
    	case CHUNKFILEDATAREADRESPONSE:
    		serverState.readChunkDataResponse(chunk);
    		break;
    	case CHUNKFILEDATAWRITERESPONSE:
    		serverState.writeChunkDataResponse(chunk);
    		break;


    	}
    	if (type == MessageType.CHUNKFILEDATAREAD){ 
    		serverState.readChunkData(chunk);
    	}else if (type == MessageType.CHUNKFILEDATAWRITE){
    		serverState.writeChunkData(chunk);
    	} 
        return;
    }

}