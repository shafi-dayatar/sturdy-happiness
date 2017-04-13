package gash.router.server.states;



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.ServerState;
import io.netty.util.internal.ThreadLocalRandom;


// nextInt is normally exclusive of the top value,
// so add 1 to make it inclusive

/**
 * Created by rentala on 4/11/17.
 */
public class ElectionTimer implements Runnable {
	protected static Logger logger = LoggerFactory.getLogger("Election Timer");
    private ServerState state;
    private long timerValue;
    public ElectionTimer(ServerState state, int min,int max){
        this.state = state;
        this.timerValue = ThreadLocalRandom.current().nextLong(min, max + 1) * 1000;
    }

    
	@Override
    public void run() {
        long electionTimeOut  = System.currentTimeMillis() + this.timerValue;
        long currentTime = System.currentTimeMillis();
        while( currentTime < electionTimeOut){
            //timer
        	currentTime = System.currentTimeMillis();
        	logger.info("Election will start in " + (electionTimeOut - currentTime));
        	try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
        logger.info("Election TimedOut, changing state to candidate");

        state.becomeCandidate();
    }
	
}
