

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.ServerState;
import pipe.common.Common.Header;
import pipe.common.Common.Node;
import pipe.work.Work.Discovery;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkMessage.MessageType;

/**
 * Created by rentala on 4/8/17.
 */
public class DiscoverMessage extends Message {
	protected static Logger logger = LoggerFactory.getLogger("Discovery Message");
	MessageType type = null;
	Discovery discovery = null;
	int secret = 111;
	static String ipAddress = getCurrentIp();
	long startTime = System.currentTimeMillis();
	public DiscoverMessage(WorkMessage msg) {
		// TODO Auto-generated constructor stub
		logger.info("Got Discover Node Message : " + msg.toString());
		
		unPackHeader( msg.getHeader());
		type = msg.getType();
		if(msg.hasDiscovery()){
			discovery = msg.getDiscovery();
		}
	}
	
    public void processMessage(ServerState state){
       logger.info("Got a discover message from " + getNodeId());
       
       if( discovery != null){
        	
        	if(discovery.hasNode()){
        		logger.info("Replying node" + getNodeId() + "with routing table:");
        		logger.debug("Message Details are : " + discovery.toString());
        		//logger.info(" Time passed before message processing -(delivery + network + inboundqueue ) : " + 
        		//(System.currentTimeMillis() - getTimestamp() ));
        		
        		Node newNode = discovery.getNode();
        		state.getEmon().addNewEdgeInfo(newNode.getNodeId(), newNode.getHost(),
        				newNode.getPort());
        		//Send routing table to requestor;
        		Discovery.Builder dsb = Discovery.newBuilder(); 
        		List<Node> nodes = state.getEmon().getOutBoundRouteTable();
        		for (Node n : nodes){
        			if (n.getNodeId() != getDestId() ){
        			    dsb.addRoutingTable(n);
        			}
        		}
        		WorkMessage wm = discoverMessageReply( getDestId(), getNodeId(), getTimestamp(),
        				dsb);
        		logger.info("Time Taken to create routing table message: " + (System.currentTimeMillis() - startTime));
        		state.getOutBoundMessageQueue().addMessage(wm);
        		logger.info("Sending Routing table: " + wm.toString());
        		
        		
        	}else {
        	    // got reply from node with routing table
        		logger.info("Got a reply from server : " + getNodeId() );
        		//logger.info("Round Trip time of the discovery message was : " +(System.currentTimeMillis()-
        		//		getTimestamp()) + " millisecs");
        		//log this 
           		List<Node> nodes =  discovery.getRoutingTableList();
        		for (Node n : nodes){
        			if (n.getNodeId() != state.getNodeId()){
        				boolean newEdge = state.getEmon().addNewEdgeInfo(n.getNodeId(), n.getHost(), 
        						n.getPort());
        				if(newEdge){
        					WorkMessage wm = discoverMessage(state.getNodeId(), 
        							n.getNodeId(), state.getConf().getWorkPort());
        					logger.info("Found new node sending discoverreply " + wm.toString());
        					state.getOutBoundMessageQueue().addMessage(wm);

        				}
        			}
        		}
        		
        	}	
        }
    }
	
	public static WorkMessage discoverMessage(int sourceId, int destId, int sourcePort  ){
		//todo should read all entries
		
		logger.info("Creating Node Discovery Message for DestId : " + destId);
        ipAddress = (ipAddress == null ? "localost" : ipAddress );
		WorkMessage.Builder wmb = WorkMessage.newBuilder();
		wmb.setType(MessageType.DISCOVERNODE);
		wmb.setSecret(1111);
		
		Header.Builder hdb = Header.newBuilder();
		hdb.setNodeId(sourceId);
		hdb.setDestination(destId);
		hdb.setTime(System.currentTimeMillis());
		wmb.setHeader(hdb.build());
		
		Discovery.Builder discovery = Discovery.newBuilder();
		Node.Builder node = Node.newBuilder(); 
		node.setNodeId(sourceId);
		node.setHost(ipAddress);
		node.setPort(sourcePort);
		discovery.setNode(node.build());

		wmb.setDiscovery(discovery);
		
		logger.debug("Discover Message : " + wmb.toString() );
		return wmb.build();
		
	}
	
	public static WorkMessage discoverMessageReply(int sourceId, int destId, 
			long timestamp, Discovery.Builder dsb ){
    	
		WorkMessage.Builder wmb = WorkMessage.newBuilder();
		Header.Builder hdb = Header.newBuilder();
		hdb.setNodeId(sourceId);
		hdb.setDestination(destId);
		hdb.setTime(timestamp);
		wmb.setHeader(hdb.build());
		
		wmb.setSecret(1111);
		wmb.setType(MessageType.DISCOVERNODEREPLY);
		wmb.setDiscovery(dsb.build());
		return wmb.build();
    }
	
	public static String getCurrentIp() {
        try {
            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface
                    .getNetworkInterfaces();
            while (networkInterfaces.hasMoreElements()) {
                NetworkInterface ni = (NetworkInterface) networkInterfaces
                        .nextElement();
                Enumeration<InetAddress> nias = ni.getInetAddresses();
                while(nias.hasMoreElements()) {
                    InetAddress ia= (InetAddress) nias.nextElement();
                    if ( 
                      !ia.isLoopbackAddress()
                     && ia instanceof Inet4Address) {
                        return ia.getHostAddress();
                    }
                }
            }
        } catch (SocketException e) {
            logger.error("unable to get current IP " + e.getMessage(), e);
        }
        return null;
    }
}
