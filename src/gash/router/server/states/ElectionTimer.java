package gash.router.server.states;

import java.util.concurrent.ThreadLocalRandom;

// nextInt is normally exclusive of the top value,
// so add 1 to make it inclusive

/**
 * Created by rentala on 4/11/17.
 */
public class ElectionTimer implements Runnable {
    private NodeState state;
    private int timerValue;
    public ElectionTimer(NodeState state, int min,int max){
        this.state = state;
        this.timerValue = ThreadLocalRandom.current().nextInt(min, max + 1);

    }
    @Override
    public void run() {

    }
}
