package gash.router.server.queue;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by rentala on 4/23/17.
 */

import gash.router.server.ServerState;
import gash.router.server.customexecutor.ExtendedExecutor;
import pipe.work.Work;
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
            System.out.println(" \n \n \n " +
                    "\n \n Message queue size is  . .. . " + exeService.getQueue().size() + " \n \n \n");
            if(exeService.getQueue().size() > 0){
                ReadTask task = (ReadTask)exeService.getQueue().peek();
                //becasue message contains the location of the requested message
                Pipe.CommandMessage cmd = task.getCmd();
                System.out.println(" Cmd message can be served by  " + cmd.getMessage() );
                System.out.println(" Requested by  " + node_id );
                if(cmd.getRequest().getRrb().getChunkLocations().contains(node_id)){
                    System.out.println(" Found a message to steal !!! ");
                    ReadTask stolenTask = (ReadTask)exeService.getQueue().poll();
                    return stolenTask.getCmd();
                }
            }
            System.out.println(" No message to steal in queue. Size is " + exeService.getQueue().size());
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