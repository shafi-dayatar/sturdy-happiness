package gash.router.server.queue;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.ServerState;
import gash.router.server.edges.EdgeInfo;
import io.netty.channel.Channel;
import routing.Pipe.CommandMessage;


public class OutBoundReadTask implements Runnable, ReadTask{
     protected static Logger logger = LoggerFactory.getLogger("OutBoundReadTask Message");

     CommandMessage cmd;
     ServerState state;
     public OutBoundReadTask(CommandMessage cmdM, ServerState state){
          this.state = state;
          this.cmd = cmdM;
     }


     @Override
     public void run() {
          outBoundProcess();
     }
     void outBoundProcess(){
          // TODO Auto-generated method stub
          try{
               int destinationId = cmd.getHeader().getDestination();
               ArrayList<EdgeInfo> connectedNode = state.getEmon().getOutBoundChannel(destinationId);

               if (destinationId == -1){
                    for(EdgeInfo ei : connectedNode){
                         Channel ch = ei.getCmdChannel();
                         if(ch != null){
                              ch.writeAndFlush(cmd);
                         }
                         else{
                              //todo:
                              //If it is not able to send message to particular node,
                              //it should update the message and set's destination to particular node.
                              logger.error("ERROR - no channel found for destination id " +  ei.getRef());
                         }
                    }
                    return;
               }
               logger.info(" ------> sending cmd message to -- > destinationId " + destinationId );
               if ( connectedNode != null){
                    Channel ch = connectedNode.get(0).getCmdChannel();
                    if(ch != null){
                         ch.writeAndFlush(cmd);
                    }
                    else{
                         logger.error("ERROR - no channel found for destination id " +  destinationId);
                         //this.addMessage(m);
                         // To Do, should try for x no of times before discarding
                    }
               }
          }
          catch (Exception e)
          {
               logger.error(" Exception while fwding command message ::::::::   " + e.getMessage());
               e.printStackTrace();
          }



     }

     @Override
     public CommandMessage getCmd() {
          return cmd;
     }
}
