package gash.router.server.tasks;

/**
 * Created by rentala on 4/23/17.
 */

import gash.router.server.ServerState;
import gash.router.server.customexecutor.ExtendedExecutor;
import gash.router.server.queue.InBoundMessageTask;
import gash.router.server.queue.MessageQueue;
import pipe.common.Common;
import pipe.work.Work;
import routing.Pipe;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ReadTaskQueue {

    ThreadPoolExecutor exeService;
    ServerState state;
    LinkedBlockingQueue blockingQueue;

    public ReadTaskQueue(ServerState state, int threadCount){
        this.state = state;
        this.blockingQueue= new LinkedBlockingQueue();
        exeService = new ExtendedExecutor(threadCount, threadCount, 0L, TimeUnit.MILLISECONDS, this.blockingQueue, state);
    }

    public void addMessage(Pipe.CommandMessage cmdMsg) {
    	
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