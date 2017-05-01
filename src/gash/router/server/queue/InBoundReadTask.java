package gash.router.server.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.ServerState;
import routing.Pipe.CommandMessage;


public class InBoundReadTask implements Runnable, ReadTask{
	protected static Logger logger = LoggerFactory.getLogger("Discovery Message");

	CommandMessage cmd;
	ServerState state;

    public InBoundReadTask(CommandMessage cmdM, ServerState state){
    	this.state = state;
    	this.cmd = cmdM;
    }
    public CommandMessage getCmd(){
    	return cmd;
	}
	
	
	@Override
    public void run() {
    	state.getRaftState().processReadRequest(cmd);
    }

    /*
	public void processRead(ServerState state, CommandMessage cmdMsg){
	       Work.WorkMessage.Builder msgBuilder = Work.WorkMessage.newBuilder();
	        msgBuilder.setSecret(9999999);
	        msgBuilder.setType(Work.WorkMessage.MessageType.CHUNKFILEDATAREAD);

	        Work.FileChunkData.Builder chBuilder = Work.FileChunkData.newBuilder();
	        chBuilder.setFileName(cmdMsg.getRequest().getRrb().getFilename());
	        int chunk_id = cmdMsg.getRequest().getRrb().getChunkId();
	        chBuilder.setChunkId(chunk_id);
	        int fileId = state.getDb().getFileId(cmdMsg.getRequest().getRrb().getFilename());
	        chBuilder.setFileId(fileId);
	        chBuilder.setReplyTo(cmdMsg.getHeader().getNodeId());

	        msgBuilder.setChunkData(chBuilder.build());

	        Common.Header.Builder hd = Common.Header.newBuilder();
	        //set to whichever node it may set it to
	        int dest = state.getRandomNodeWithChunk(chunk_id, fileId);
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
	            System.out.println(" Invalid chunk id recived while adding to InBoundReadTaskQueue");
	        }
	}*/
}
