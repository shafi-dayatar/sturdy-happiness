package gash.router.server.customexecutor;

import gash.router.server.ServerState;
import pipe.work.Work.WorkMessage;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by rentala on 4/22/17.
 */
public class ExtendedExecutor extends ThreadPoolExecutor {
    ServerState state;
    public ExtendedExecutor(int i, int i1, long l, TimeUnit timeUnit, BlockingQueue<Runnable> blockingQueue, ServerState state) {
        super(i, i1, l, timeUnit, blockingQueue);
        this.state = state;
    }
    /*protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        if (t == null) {
            try {
                if(this.getQueue().size() == 0 )
                {
                    //steal work
                    state.getRaftState().stealWork();
                }
            } catch (Exception ce) {
                t = ce;
            }
        }
        if (t != null)
            System.out.println(t);
    }*/
}
