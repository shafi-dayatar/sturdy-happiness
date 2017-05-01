package gash.router.server.messages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.ServerState;
import gash.router.server.states.RaftServerState;
import pipe.common.Common;
import pipe.work.Work;
import pipe.work.Work.WorkMessage;
import routing.Pipe;

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
                logger.info(" Recived steal request from Node: " + msg.getHeader().getNodeId());
               Pipe.CommandMessage stolenCmdMessage = state.getRaftState().getWork(msg.getHeader().getNodeId());
                if(stolenCmdMessage != null){
                    logger.info(" Sending stolen message to Node : " + msg.getHeader().getNodeId());
                    Pipe.CommandMessage.Builder cmdBuilder = Pipe.CommandMessage.newBuilder(stolenCmdMessage);
                    cmdBuilder.setRequest(stolenCmdMessage.getRequest());
                    Common.Header.Builder hdBuilder = Common.Header.newBuilder();
                    hdBuilder.setMessageId(stolenCmdMessage.getHeader().getMessageId());
                    hdBuilder.setDestination(msg.getHeader().getNodeId());
                    hdBuilder.setNodeId(state.getNodeId());
                    hdBuilder.setMaxHops(stolenCmdMessage.getHeader().getMaxHops());
                    hdBuilder.setTime(stolenCmdMessage.getHeader().getTime());
                    cmdBuilder.setHeader(hdBuilder.build());
                    state.getOutBoundReadTaskQueue().addMessage(cmdBuilder.build());
                }

                break;
            case WORKSTEALRESPONSE:
                logger.info(" \n -------  \n ------ \n \n \n  Recived for a stolen message from Node: " + msg.getHeader().getNodeId());
                Pipe.CommandMessage stolenMessage = msg.getReadCmdMessage();
                state.getInBoundReadTaskQueue().addMessage(stolenMessage);
                break;

        }

        return;
    }

}