package gash.router.server.messages;

import gash.router.server.ServerState;
import pipe.work.Work;

public class HeartBeatMessage extends Message{

    @java.lang.Override
    public Work.WorkMessage processMessage(ServerState state) {
        return super.processMessage(state);
    }
}
