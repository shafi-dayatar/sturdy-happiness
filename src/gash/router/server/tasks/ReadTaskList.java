package gash.router.server.tasks;

/**
 * Created by rentala on 4/23/17.
 */

import gash.router.server.ServerState;
import gash.router.server.customexecutor.ExtendedExecutor;
import gash.router.server.queue.InBoundMessageTask;
import gash.router.server.queue.MessageQueue;
import pipe.work.Work;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ReadTaskList implements MessageQueue {

    ThreadPoolExecutor exeService;
    ServerState state;
    LinkedBlockingQueue blockingQueue;

    public ReadTaskList(ServerState state, int threadCount){
        this.state = state;
        this.blockingQueue= new LinkedBlockingQueue();
        exeService = new ExtendedExecutor(threadCount, threadCount, 0L, TimeUnit.MILLISECONDS, this.blockingQueue, state);
    }

    public void addMessage(Work.WorkMessage wm) {
        exeService.execute(new InBoundMessageTask(wm, state));
    }

}