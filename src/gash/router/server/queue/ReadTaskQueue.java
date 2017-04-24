package gash.router.server.queue;

/**
 * Created by rentala on 4/23/17.
 */

import gash.router.server.ServerState;
import gash.router.server.customexecutor.ExtendedExecutor;
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
    	exeService.execute(new ReadTask(cmdMsg, state));
    }

}