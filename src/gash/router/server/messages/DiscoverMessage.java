package gash.router.server.messages;


import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.container.RoutingConf.RoutingEntry;
import gash.router.server.ServerState;
import gash.router.server.edges.EdgeInfo;
import pipe.common.Common.Header;
import pipe.work.Work;
import pipe.work.Work.Discovery;
import pipe.work.Work.Node;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkMessage.MessageType;

/**
 * Created by rentala on 4/8/17.
 */
public class DiscoverMessage extends Message {
	protected static Logger logger = LoggerFactory.getLogger("Discovery Message");
	MessageType type = null;
	Discovery discovery = null;
	
	public DiscoverMessage(WorkMessage msg) {
		// TODO Auto-generated constructor stub
		logger.info("Got Discover Node Message : " + msg.toString());
		
		unPackHeader( msg.getHeader());
		type = msg.getType();
		if(msg.hasDiscovery()){
			discovery = msg.getDiscovery();
		}
	}
	
	public WorkMessage createMessage(){
		return null;
	}
	
    public void processMessage(ServerState state){
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
        		state.getEmon().addNewEdgeInfo(newNode.getNodeId(), newNode.getIpAddr(),
        				newNode.getWorkPort());
        		int senderId = getDestinationId();
        		setDestinationId(getNodeId());
        		setNodeId(senderId);
        		Discovery.Builder dsb = Discovery.newBuilder(); 
        		List<Node> nodes = state.getEmon().getOutBoundRouteTable();
        		for (Node n : nodes){
        			if (n.getNodeId() != getDestinationId()){
        			    dsb.addRoutingTable(n);
        			}
        		}
        		WorkMessage.Builder wmb = WorkMessage.newBuilder();
        		Header hd = createHeader();
        		wmb.setHeader(hd);
        		//wmb.setAck(true);
        		wmb.setSecret(123456);
        		wmb.setDiscovery(dsb);
        		wmb.setType(MessageType.DISCOVERNODEREPLY);
        		WorkMessage wm = wmb.build();
        		logger.info("Sending Routing table: " + wm.toString());
        		state.getOutBoundMessageQueue().addMessage(wm);
        		
        		
        	}else {
        	    // got message from leader or other node with routing table
        		logger.info("Got a reply from server : " + getNodeId() );
        		
           		List<Node> nodes =  discovery.getRoutingTableList();
        		for (Node n : nodes){
        			
        			boolean newEdge = state.getEmon().addNewEdgeInfo(n.getNodeId(), n.getIpAddr(), 
        					n.getWorkPort());
        			if(newEdge){
        				String ip_address = null;
						try {
							ip_address = InetAddress.getLocalHost().getHostAddress();
						} catch (UnknownHostException e) {
							// TODO Auto-generated catch block
							logger.error("Error in decting ip_address of this node server : ");
							e.printStackTrace();
						}
					    ip_address = ip_address == null ? "localhost" : ip_address;
        				/*WorkMessage wm = createDiscoverMessage(state.getNodeId(), 
        						n.getNodeId(), ip_address, state.getConf().getWorkPort());
        				logger.info("Found new node sending discoverreply " + wm.toString());
        				state.getOutBoundMessageQueue().addMessage(wm);*/
        				
        			}
        		}
        		
        	}	
        }

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
    
    public WorkMessage createDiscoverMessage(int sourceId, int destId, String sourceIp, 
    		int sourcePort ){
    	
		WorkMessage.Builder wmb = WorkMessage.newBuilder();
		Header.Builder hdb = Header.newBuilder();
		hdb.setNodeId(sourceId);
		hdb.setDestination(destId);
		hdb.setTime(System.currentTimeMillis());
		wmb.setHeader(hdb.build());
		wmb.setSecret(1111);
		
		Discovery.Builder db = Discovery.newBuilder(); 
		Node.Builder discover = Node.newBuilder(); 
		discover.setNodeId(sourceId);
		discover.setIpAddr(sourceIp);
		discover.setWorkPort(sourcePort);
		db.setNode(discover.build());
		wmb.setType(MessageType.DISCOVERNODEREPLY);
		wmb.setDiscovery(db.build());
		return wmb.build();
    }
}
