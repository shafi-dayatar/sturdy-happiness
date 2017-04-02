package gash.router.server.messages;


//TODO

import pipe.work.Work;

// Add Message Class methods here
//
public interface MessageInterface {
    public boolean processMessage();
    void discard();
    Work.WorkMessage forward();
    void reply();
}
