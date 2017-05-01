package gash.router.server.queue;

import routing.Pipe;

/**
 * Created by rentala on 4/30/17.
 */
public interface ReadTask {
    Pipe.CommandMessage getCmd();
}
