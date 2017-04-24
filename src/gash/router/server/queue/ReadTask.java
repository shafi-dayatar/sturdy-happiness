package gash.router.server.queue;

import gash.router.server.ServerState;
import pipe.common.Common;
import pipe.work.Work;
import pipe.work.Work.WorkMessage;
import routing.Pipe.CommandMessage;

public class ReadTask implements Runnable{
	
	CommandMessage cmd;
	ServerState state;
	
    public ReadTask(CommandMessage cmdM, ServerState state){
    	this.state = state;
    	this.cmd = cmdM;
    }
	
	
	@Override
    public void run() {
		processRead(state, cmd);
    }
	
	public void processRead(ServerState state, CommandMessage cmdMsg){
	       Work.WorkMessage.Builder msgBuilder = Work.WorkMessage.newBuilder();
	        msgBuilder.setSecret(9999999);
	        msgBuilder.setType(Work.WorkMessage.MessageType.CHUNKFILEDATAREAD);

	        Work.FileChunkData.Builder chBuilder = Work.FileChunkData.newBuilder();
	        chBuilder.setFileName(cmdMsg.getReq().getRrb().getFilename());
	        int chunk_id = cmdMsg.getReq().getRrb().getChunkId();
	        chBuilder.setChunkId(chunk_id);
	        int fileId = state.getDb().getFileId(cmdMsg.getReq().getRrb().getFilename());
	        chBuilder.setFileId(fileId);
	        chBuilder.setReplyTo(cmdMsg.getHeader().getNodeId());

	        msgBuilder.setChunkData(chBuilder.build());

	        Common.Header.Builder hd = Common.Header.newBuilder();
	        //set to whichever node it may set it to
	        int dest = state.getRandomNodeWithChunk(chunk_id);
	        if(dest > 0 ){
	            //chunk exists
	            hd.setDestination(dest);
	            hd.setNodeId(state.getNodeId());
	            hd.setTime(System.currentTimeMillis());
	            msgBuilder.setHeader(hd);
	            state.getOutBoundMessageQueue().addMessage(msgBuilder.build());
	            //exeService.execute(new InBoundMesageTask(msgBuilder.build(), state));
	        }
	        else{
	            //invalid chunk id
	            System.out.println(" Invalid chunk id recived while adding to ReadTaskQueue");
	        }
	}
}
