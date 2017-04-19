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
    private long electionStartTime;
    private long electionResolutionTime;
    private boolean forever = true;
    
    public ElectionTimer(ServerState state, int min,int max){
        this.state = state;
        maxRandom = max;
        minRandom = min;
        this.timerValue = ThreadLocalRandom.current().nextLong(min, max + 1);
        electionTimeOut  = System.currentTimeMillis() + this.timerValue;
        electionResolutionTime = 100;
    }

	@Override
    public void run() {
        
        while(forever){
        	long currentTime = System.currentTimeMillis();
        	while(forever && currentTime < electionTimeOut){
        		currentTime = System.currentTimeMillis();
        		logger.debug("Election should end in : "  + ((electionStartTime + electionResolutionTime) - currentTime));
        		if(currentTime > electionStartTime + electionResolutionTime &&
        				state.getRaftState() instanceof Candidate){
        			logger.info("Election took too long, probably no leader was elected, "
        					+ "hence stepping down from candidate");
        			state.becomeFollower();
        		}
        		logger.debug("Election will start in " + (electionTimeOut - currentTime));
        		try {
        			Thread.sleep(100);
        		} catch (InterruptedException e) {
        			// TODO Auto-generated catch block
        			e.printStackTrace();
        		}
        	}
        	logger.info("Election Timeout"); 
        	if(forever && state.getEmon().getTotalNodes() >= 4){
        		logger.info("I am connected to :" + state.getEmon().getTotalNodes());
        		if (state.getRaftState() instanceof Follower)   
        		    state.becomeCandidate();
        		
        	}
        	resetElectionTimeOut();
        	try {
    			Thread.sleep(100);
    		} catch (InterruptedException e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    		}
        }
    }
	
	public void resetElectionTimeOut(){
		this.timerValue = ThreadLocalRandom.current().nextLong(minRandom*1000, maxRandom *1000 + 1);
        electionTimeOut  = System.currentTimeMillis() + this.timerValue;
        logger.info("Election will start in millisecs:  " + (electionTimeOut - System.currentTimeMillis()));
	}	
	
	public void stopThread(){
		forever = false;
	}

	public long getElectionStartTime() {
		return electionStartTime;
	}

	public void setElectionStartTime(long electionStartTime) {
		this.electionStartTime = electionStartTime;
	}

	public long getElectionResolutionTime() {
		return electionResolutionTime;
	}

	public void setElectionResolutionTime(long electionResolutionTime) {
		this.electionResolutionTime = electionResolutionTime;
	}
	
	
}
