package gash.router.server.states;

import java.util.concurrent.ThreadLocalRandom;

// nextInt is normally exclusive of the top value,
// so add 1 to make it inclusive

/**
 * Created by rentala on 4/11/17.
 */
public class ElectionTimer implements Runnable {
    private RaftServerState state;
    private long timerValue;
    public ElectionTimer(RaftServerState state, int min,int max){
        this.state = state;
        this.timerValue = ThreadLocalRandom.current().nextLong(min, max + 1) * 1000;
    }
    @Override
    public void run() {
        long currentTime = System.currentTimeMillis();
        while(currentTime < currentTime + this.timerValue){
            //timer
        }
        //timed out
        //state.toCandidate();

    }
}
