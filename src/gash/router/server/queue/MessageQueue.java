package gash.router.server.queue;

import pipe.work.Work.WorkMessage;

public interface MessageQueue {
	void addMessage(WorkMessage wmsg);
}
