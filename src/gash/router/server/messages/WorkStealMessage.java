package gash.router.server.messages;

import gash.router.server.ServerState;
import gash.router.server.states.RaftServerState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pipe.common.Common;
import pipe.work.Work;
import pipe.work.Work.WorkMessage;

/**
 * Created by rentala on 4/22/17.
 */
public class WorkStealMessage extends Message {

    protected static Logger logger = LoggerFactory.getLogger("WorkStealMessage ");

    WorkMessage msg = null;


    public WorkStealMessage(WorkMessage msg) {
        // TODO Auto-generated constructor stub
       this.msg = msg;
    }

    @java.lang.Override
    public void processMessage(ServerState state) {
        RaftServerState serverState = state.getRaftState();
        switch(msg.getType()){
            case WORKSTEALREQUEST:
                //check if it has any in its queue and respond
               WorkMessage wmsg = state.getRaftState().getWork();
                if(wmsg != null){
                    WorkMessage.Builder wmsgBuilder = WorkMessage.newBuilder();
                    wmsgBuilder.setSecret(9999999);
                    wmsgBuilder.setType(WorkMessage.MessageType.WORKSTEALRESPONSE);
                    wmsgBuilder.setStolenWork(wmsg);
                    //build header
                    Common.Header.Builder hd = Common.Header.newBuilder();
                    hd.setDestination(msg.getHeader().getNodeId());
                    hd.setNodeId(state.getNodeId());
                    hd.setTime(System.currentTimeMillis());
                    wmsgBuilder.setHeader(hd);
                    state.getOutBoundMessageQueue().addMessage(wmsgBuilder.build());
                }

                break;
            case WORKSTEALRESPONSE:
                WorkMessage work = msg.getStolenWork();
                state.getInBoundMessageQueue().addMessage(work);
                break;

        }

        return;
    }

}