package gash.router.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.edges.EdgeInfo;
import gash.router.server.edges.EdgeMonitor;
import io.netty.channel.Channel;
import pipe.work.Work.WorkMessage;

public class OutBoundMessageProcessor implements Runnable{
	private ServerState state;
	private EdgeMonitor emon;
	private boolean forever = true;
	protected static Logger logger = LoggerFactory.getLogger("outbound processor");
	
	public OutBoundMessageProcessor(ServerState state) {
		if (state == null)
			throw new RuntimeException("state is null");
		this.state = state;
		this.emon = state.getEmon();
	}
	
	public void shutdown() {
		forever = false;
	}
	
	@Override
	public void run() {
		//logger.info("Thread started");
		while (forever) {
			try {
				logger.info("Thread Running: messages in queue are : " + state.numOutEnqueued() );
				if(state.numOutEnqueued() > 0){
					WorkMessage msg = state.processOutMessage();
					logger.info("message is coming in queue");
					PrintUtil.printWork(msg);
					EdgeInfo destination = emon.getOutBoundChannel(msg.getHeader().getDestination());
					if (null != destination){
						Channel ch = destination.getChannel();
						if(ch != null){
							ch.writeAndFlush(msg);
						}
					}else{
						logger.error("Cannot forward message to destination as no connection is available");
					}
				}else{
					Thread.sleep(100);
				}				
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
