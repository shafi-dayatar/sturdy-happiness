package gash.router.server.messages;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import gash.router.server.ServerState;
import gash.router.server.states.RaftServerState;
import pipe.common.Common.Header;
import pipe.work.Work.FileChunkData;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkMessage.MessageType;
import routing.Pipe;

import java.nio.channels.Channel;

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
			sendReadResponse(state);
    		break;
    	case CHUNKFILEDATAWRITERESPONSE:
    		//serverState.writeChunkDataResponse(chunk);
    		break;


    	}
    	if (type == MessageType.CHUNKFILEDATAREAD){ 
    		serverState.readChunkData(chunk);
    	}else if (type == MessageType.CHUNKFILEDATAWRITE){
    		serverState.writeChunkData(chunk);
    	} 
        return;
    }
    private void sendReadResponse(ServerState state){
    	System.out.println( " Chunk at sendReadRespone ");
    	System.out.println( " chunkid " + chunk.getChunkId() + " fileid:  " + chunk.getFileId() + " fname:  " + chunk.getFileName());
		io.netty.channel.Channel channel = state.connectionManager.getConnection(chunk.getReplyTo());
		Pipe.Response.Builder respBuilder = Pipe.Response.newBuilder();
		respBuilder.setResponseType(Pipe.TaskType.RESPONSEREADFILE);
		Pipe.ReadResponse.Builder readRespBuilder = Pipe.ReadResponse.newBuilder();
		readRespBuilder.setFilename(chunk.getFileName());
		Pipe.Chunk.Builder chunkBuilder = Pipe.Chunk.newBuilder();
		chunkBuilder.setChunkId(chunk.getChunkId());
		chunkBuilder.setChunkData(chunk.getChunkData());
		readRespBuilder.setChunk(chunkBuilder.build());
		respBuilder.setReadResponse(readRespBuilder.build());
		state.sendReadResponse(channel, respBuilder.build(), chunk.getReplyTo());
	}
	public static WorkMessage createFileWriteMessage(int source, int dest, int fileId, int chunkId, String FileName,
			ByteString chunkData) {
		WorkMessage.Builder msgBuilder = WorkMessage.newBuilder();
		msgBuilder.setSecret(9999999);
		msgBuilder.setType(MessageType.CHUNKFILEDATAWRITE);
		Header.Builder hd = Header.newBuilder();
		hd.setDestination(dest);
		hd.setNodeId(source);
		hd.setTime(System.currentTimeMillis());

		FileChunkData.Builder data = FileChunkData.newBuilder();
		data.setReplyTo(source);
		data.setFileId(fileId);
		data.setChunkId(chunkId);
		data.setFileName(FileName);
		data.setChunkData(chunkData);
		msgBuilder.setHeader(hd);
		msgBuilder.setChunkData(data);
		return msgBuilder.build();

	}

}