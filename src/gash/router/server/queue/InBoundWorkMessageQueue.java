package gash.router.server.queue;

import java.util.concurrent.*;

import gash.router.server.ServerState;
import gash.router.server.customexecutor.ExtendedExecutor;
import pipe.work.Work.WorkMessage;

public class InBoundWorkMessageQueue implements MessageQueue {
	
	ThreadPoolExecutor exeService;
    ServerState state;
    LinkedBlockingQueue blockingQueue;
    
    public InBoundWorkMessageQueue(ServerState state, int threadCount){
	    this.state = state;
	    this.blockingQueue= new LinkedBlockingQueue();
    	exeService = new ExtendedExecutor(threadCount, threadCount, 0L, TimeUnit.MILLISECONDS, this.blockingQueue, state);
    }
    
    public void addMessage(WorkMessage wm) {
    	exeService.execute(new InBoundMessageTask(wm, state));
    }
    
    public WorkMessage getQueuedMessage(){
        InBoundMessageTask task = (InBoundMessageTask)blockingQueue.poll();
        return task.getWorkMessage();
    }

}