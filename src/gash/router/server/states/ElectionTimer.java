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
    private int maxRandom;
    private int minRandom;
    private long electionTimeOut;
    private boolean forever = true;
    public ElectionTimer(ServerState state, int min,int max){
        this.state = state;
        maxRandom = max;
        minRandom = min;
        this.timerValue = ThreadLocalRandom.current().nextLong(min, max + 1) * 1000;
        electionTimeOut  = System.currentTimeMillis() + this.timerValue;
 
    }

	@Override
    public void run() {
        
        while(forever){
        	long currentTime = System.currentTimeMillis();
        	while(forever && currentTime < electionTimeOut){
        		currentTime = System.currentTimeMillis();
        		logger.info("Election will start in " + (electionTimeOut - currentTime));
        		try {
        			Thread.sleep(100);
        		} catch (InterruptedException e) {
        			// TODO Auto-generated catch block
        			e.printStackTrace();
        		}
        	}
        	logger.info("Election Timeout");
        	if(forever && state.getEmon().getTotalNodes() >= 3){
        		logger.info("Changing from Follower state to Candidate");
        		if (state.getRaftState() instanceof Follower)   
        		state.becomeCandidate();
        	}
        	setElectionTimeOut();
        }
    }
	
	public void setElectionTimeOut(){
		this.timerValue = ThreadLocalRandom.current().nextLong(minRandom, maxRandom + 1) * 1000;
        electionTimeOut  = System.currentTimeMillis() + this.timerValue;
	}	
	
	public void stopThread(){
		forever = false;
	}
	
	
}
