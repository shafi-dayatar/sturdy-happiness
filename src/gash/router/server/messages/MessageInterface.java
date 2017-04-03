package gash.router.server.messages;




//TODO

import pipe.work.Work.WorkMessage;

// Add Message Class methods here
//
public interface MessageInterface {
    public WorkMessage processMessage(int nodeId);
    void discard();
    WorkMessage forward();
    void reply();
}
