package gash.router.server.messages;




import gash.router.server.ServerState;

//TODO

import pipe.work.Work.WorkMessage;

// Add Message Class methods here
//
public interface MessageInterface {
    public void processMessage(ServerState state);
}
