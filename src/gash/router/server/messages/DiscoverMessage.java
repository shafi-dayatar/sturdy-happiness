package gash.router.server.messages;

import pipe.work.Work;

/**
 * Created by rentala on 4/8/17.
 */
public class DiscoverMessage extends Message {
    public Work.WorkMessage processMessage(int nodeId){
        System.out.println("[x] Discover message received ");
        forward();
        return respond();
    }
    public Work.WorkMessage respond(){
        Work.WorkMessage.Builder wm = Work.WorkMessage.newBuilder();
        setReply(true);
        setReplyFrom(getDestinationId());
        setDestinationId(getNodeId());
        wm.setHeader(createHeader());
        wm.setPing(true);
        wm.setSecret(getSecret());
        System.out.println("[x] Responding to discover message ....");
        return wm.build();
    }
}
