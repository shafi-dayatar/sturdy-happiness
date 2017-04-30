package gash.router.server.queue;

import java.util.concurrent.*;

import gash.router.server.ServerState;
import gash.router.server.customexecutor.ExtendedExecutor;
import pipe.work.Work.WorkMessage;

public class InBoundWorkMessageQueue implements MessageQueue {

    ExecutorService exeService;
    ServerState state;
    
    public InBoundWorkMessageQueue(ServerState state, int threadCount){
	    this.state = state;
        exeService = Executors.newFixedThreadPool(threadCount);
    }
    
    public void addMessage(WorkMessage wm) {
    	exeService.execute(new InBoundMessageTask(wm, state));
    }

}