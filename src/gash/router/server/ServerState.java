package gash.router.server;

import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.netty.channel.Channel;
import gash.router.container.RoutingConf;
import gash.router.server.edges.EdgeMonitor;
import gash.router.server.tasks.TaskList;
import pipe.work.Work.Task;
import pipe.work.Work.WorkMessage;
import routing.Pipe.CommandMessage;

public class ServerState {
	private RoutingConf conf;
	private EdgeMonitor emon;
	private TaskList tasks;
	private LinkedBlockingDeque<WorkMessage> workMessageInBound = new LinkedBlockingDeque<WorkMessage>();
    private LinkedBlockingDeque<WorkMessage> workMessageOutBound = new LinkedBlockingDeque<WorkMessage>();
    private OutBoundMessageProcessor obmp;
    protected static Logger logger = LoggerFactory.getLogger("Server State");
    
	public RoutingConf getConf() {
		return conf;
	}

	public void setConf(RoutingConf conf) {
		this.conf = conf;
	}

	public EdgeMonitor getEmon() {
		return emon;
	}
	
	public OutBoundMessageProcessor getOutBoundMessageProcessor() {
		return obmp;
	}
	public void setOutBoundMessageProcessor(OutBoundMessageProcessor obmp) {
		this.obmp = obmp;
	}

	public void setEmon(EdgeMonitor emon) {
		this.emon = emon;
	}

	public TaskList getTasks() {
		return tasks;
	}

	public void setTasks(TaskList tasks) {
		this.tasks = tasks;
	}
	
	public void addInMessage(WorkMessage t) {
		logger.info("Adding Message to queue");
		workMessageInBound.add(t);
	}

	public int numEnqueued() {
		return workMessageInBound.size();
	}

	public WorkMessage processInMessage() {
		try {
		    return workMessageInBound.take();
		}catch(InterruptedException e){
			logger.error("failed to process work message ", e);
		}
		return null;
	}
	
	public void addOutMessage(WorkMessage t) {
		logger.error("Should print something when ping is called");
		workMessageOutBound.add(t);
	}

	public int numOutEnqueued() {
		if (workMessageOutBound == null)
		     return 0;
		return workMessageOutBound.size();
	}

	public WorkMessage processOutMessage() {
		try {
		    return workMessageOutBound.take();
		}catch(InterruptedException e){
			logger.error("failed to process work message ", e);
		}
		return null;
	}

}
