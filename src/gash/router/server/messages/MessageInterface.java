package gash.router.server.messages;




import gash.router.server.ServerState;

// Add Message Class methods here
//
public interface MessageInterface {
    public void processMessage(ServerState state);
}
