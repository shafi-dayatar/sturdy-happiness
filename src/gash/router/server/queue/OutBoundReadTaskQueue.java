package gash.router.server.queue;

import gash.router.server.ServerState;
import routing.Pipe;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by rentala on 4/28/17.
 */
public class OutBoundReadTaskQueue {
    ExecutorService exeService;
    ServerState state;

    public OutBoundReadTaskQueue(ServerState state, int threadCount){
        this.state = state;
        exeService = Executors.newFixedThreadPool(threadCount);
    }

    public void addMessage(Pipe.CommandMessage cm) {
        exeService.execute(new OutBoundReadTask(cm, state));
    }

}
