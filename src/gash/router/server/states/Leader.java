package gash.router.server.states;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.ServerState;
import pipe.common.Common.Header;
import pipe.election.Election;
import pipe.election.Election.LeaderElection;
import pipe.work.Work;
import pipe.work.Work.LogAppendEntry;
import pipe.work.Work.LogAppendEntry.Builder;
import pipe.work.Work.WorkMessage.MessageType;
import pipe.work.Work.WorkMessage;

/**
 * Created by rentala on 4/11/17.
 */
public class Leader implements RaftServerState, Runnable {

    protected static Logger logger = LoggerFactory.getLogger("Leader-State");
    private ServerState state;
    private boolean isLeader;

    public Leader(ServerState state){
        this.state = state;
    }
    public void appendEntries(String entry){
        logger.info("appendEntries = " + entry);
    }
	

    @java.lang.Override
    public void requestVote(LeaderElection leaderElectionRequest) {

    }

    @java.lang.Override
    public void startElection() {

    }

    @java.lang.Override
    public void leaderElect() {

    }

    @java.lang.Override
    public void collectVote(Election.LeaderElectionResponse leaderElectionResponse) {
    	/**
    	 * if leader has been elected than there is no need to process other node response.
    	 */
    	return;
        
    }

	@Override
	public void declareLeader() {
		WorkMessage hearbeat = createHeartBeatMessage();
		state.getOutBoundMessageQueue().addMessage(hearbeat);	
	}
	
	public WorkMessage createHeartBeatMessage(){
		WorkMessage.Builder wmb = WorkMessage.newBuilder();
		
		
		Header.Builder hdb = Header.newBuilder();
		hdb.setNodeId(state.getNodeId());
		hdb.setDestination(-1);
		hdb.setTime(System.currentTimeMillis());
		
		
		LogAppendEntry.Builder heartbeat = LogAppendEntry.newBuilder();
		heartbeat.setElectionTerm(state.getCurrentTerm());
		heartbeat.setLeaderNodeId(state.getNodeId());
		heartbeat.setPrevLogIndex(state.getLastLogIndex());
		heartbeat.setPrevLogTerm(state.getCurrentTerm() - 1);
		
		wmb.setSecret(5555);
		wmb.setType(MessageType.HEARTBEAT);
		wmb.setHeader(hdb);
		wmb.setLogAppendEntries(heartbeat);
		return wmb.build();

	}
	@Override
	public void heartbeat(LogAppendEntry hearbeat) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void logAppend(LogAppendEntry logEntry) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void run() {
		while(isLeader){
			declareLeader();
			try {
				Thread.sleep(state.getConf().getHeartbeatDt());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	public boolean isLeader() {
		return isLeader;
	}
	public void setLeader(boolean isLeader) {
		this.isLeader = isLeader;
	}

}
