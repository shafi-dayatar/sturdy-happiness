package gash.router.server.queue;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by rentala on 4/23/17.
 */

import gash.router.server.ServerState;
import gash.router.server.customexecutor.ExtendedExecutor;
import routing.Pipe;

public class InBoundReadTaskQueue {

    ThreadPoolExecutor exeService;
    ServerState state;
    LinkedBlockingQueue blockingQueue;

    public InBoundReadTaskQueue(ServerState state, int threadCount){
        this.state = state;
        this.blockingQueue= new LinkedBlockingQueue();
        exeService = new ExtendedExecutor(threadCount, threadCount, 0L, TimeUnit.MILLISECONDS, this.blockingQueue, state);
    }

    public void addMessage(Pipe.CommandMessage cmdMsg) {
    	exeService.execute(new InBoundReadTask(cmdMsg, state));
    }

    public synchronized Pipe.CommandMessage getQueuedMessage(String node_id){
        try
        {
            Pipe.CommandMessage msg = (Pipe.CommandMessage)blockingQueue.peek();
            //becasue message contains the location of the requested message
            if(msg.getMessage().contains(node_id)){
                return (Pipe.CommandMessage)blockingQueue.poll();
            }
            return null;
        }
        catch (Exception e)
        {
            System.out.println(" --->     Execption in getQueuedMessage " + e.getMessage());
            e.printStackTrace();
            return null;
        }

    }
}