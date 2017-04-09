package gash.router.server.messages;


import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.ServerState;
import gash.router.server.edges.EdgeInfo;
import pipe.common.Common.Header;
import pipe.work.Work;
import pipe.work.Work.Discovery;
import pipe.work.Work.Node;
import pipe.work.Work.WorkMessage;

/**
 * Created by rentala on 4/8/17.
 */
public class DiscoverMessage extends Message {
	protected static Logger logger = LoggerFactory.getLogger("Discovery Message");
	Discovery discovery = null;
	
	
	public void unPackMessage(WorkMessage msg){
		unPackHeader( msg.getHeader());
		if(msg.hasDiscovery()){
			discovery = msg.getDiscovery();
		}
	}
	public WorkMessage createMessage(){
		return null;
	}
	
    public Work.WorkMessage processMessage(ServerState state){
       logger.info("Got a discover message from " + getNodeId());
        if( discovery != null){
        	if (discovery.hasLeader()){
        		WorkMessage msg = createMessage();
        		logger.info("Message came to leader " + getNodeId());
        		// create new message for leader and update leader status in edgemonitor;
        	} else if(discovery.hasNode()){
        		logger.info("Message came to : " + getDestinationId() + " from : " + getNodeId());
        		// if leader is known pass leader details or else routing table from a node
        		//create new header by interchanging destination with sender id and set ack bit
        		Node newNode = discovery.getNode();
        		EdgeInfo newEdge = new EdgeInfo(newNode.getNodeId(), newNode.getIpAddr(),
        				newNode.getWorkPort());
        		state.getEmon().onAdd(newEdge);
        		
        		int senderId = getDestinationId();
        		setDestinationId(getNodeId());
        		setNodeId(senderId);
        		Discovery.Builder dsb = Discovery.newBuilder(); 
        		List<Node> nodes = state.getEmon().getOutBoundRouteTable();
        		int i = 1;
        		for (Node n : nodes){
        			dsb.setRoutingTable(i++, n);
        		}
        		WorkMessage.Builder wmb = WorkMessage.newBuilder();
        		Header hd = createHeader();
        		wmb.setHeader(hd);
        		wmb.setAck(true);
        		wmb.setSecret(123456);
        		wmb.setDiscovery(dsb);
        		WorkMessage wm = wmb.build();
        		state.getOutBoundMessageQueue().addMessage(wm);
        		
        	}else {
        	    // got message from leader or other node with routing table
           		ArrayList<Node> nodes = (ArrayList<Node>) discovery.getRoutingTableList();
        		for (Node n : nodes){
        			EdgeInfo ei = new EdgeInfo(n.getNodeId(), n.getIpAddr(), n.getWorkPort());        			
        			state.getEmon().onAdd(ei);
        		}
        	}
        	
        	
        }
        
        return null;
    }
    public void respond(){
        Work.WorkMessage.Builder wm = Work.WorkMessage.newBuilder();
        //setReply(true);
        //setReplyFrom(getDestinationId());
        setDestinationId(getNodeId());
        wm.setHeader(createHeader());
        wm.setPing(true);
        wm.setSecret(getSecret());
        System.out.println("[x] Responding to discover message ....");
        //return wm.build();
    }
}
