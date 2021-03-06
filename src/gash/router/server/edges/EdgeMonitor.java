/**
 * Copyright 2016 Gash.
 *
 * This file and intellectual content is protected under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package gash.router.server.edges;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.container.RoutingConf.RoutingEntry;
import gash.router.server.ServerState;
import gash.router.server.communication.CommConnection;
import gash.router.server.messages.DiscoverMessage;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import pipe.common.Common.Header;
import pipe.work.Work.Heartbeat;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkState;

public class EdgeMonitor implements EdgeListener, Runnable {
	protected static Logger logger = LoggerFactory.getLogger("edge monitor");

	private EdgeList outboundEdges;
	private EdgeList inboundEdges;
	private long dt = 2000;
	private ServerState state;
	private boolean forever = true;

	public EdgeMonitor(ServerState state) {
		if (state == null)
			throw new RuntimeException("state is null");

		this.outboundEdges = new EdgeList();
		this.inboundEdges = new EdgeList();
		this.state = state;
		this.state.setEmon(this);

		if (state.getConf().getRouting() != null) {
			for (RoutingEntry e : state.getConf().getRouting()) {
				EdgeInfo ei = outboundEdges.addNode(e.getId(), e.getHost(), e.getPort(), e.getCmdPort());
				onAdd(ei); //try to connect thru creating channel.
			}
		}

		// cannot go below 2 sec
		if (state.getConf().getHeartbeatDt() > this.dt)
			this.dt = state.getConf().getHeartbeatDt();
	}

	public void createInboundIfNew(int ref, String host, int port, int cmdPort) {
		inboundEdges.createIfNew(ref, host, port, cmdPort);
	}

	private WorkMessage createHB(EdgeInfo ei) {
		WorkState.Builder sb = WorkState.newBuilder();
		sb.setEnqueued(-1);
		sb.setProcessed(-1);

		Heartbeat.Builder bb = Heartbeat.newBuilder();
		bb.setState(sb);

		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(state.getConf().getNodeId());
		hb.setDestination(-1);
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb);
		wb.setSecret(new Integer(123123123));
		wb.setBeat(bb);

		return wb.build();
	}

	public void shutdown() {
		forever = false;
	}
	
	@Override
	public void run() {
		while (forever) {
			try {
				logger.debug("Routing table is : ");
				for (EdgeInfo ei : this.outboundEdges.map.values()) {
					if (ei.getChannel() == null){
						onAdd(ei);
					}
					if (ei.isActive() && ei.getChannel() != null) {	
						//WorkMessage wm = createHB(ei); this will come from leader
						//ChannelFuture cf = ei.getChannel().writeAndFlush(wm);
						
					} else {
						// TODO create a client to the node
						logger.info("trying to connect to node " + ei.getRef());
					}
					logger.debug(ei.toString());
					
				}
				Thread.sleep(dt);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				logger.error("Thread Interupted while type to update routing table ");
				e.printStackTrace();
			}
		}
	}

	private CommConnection makeCmdChannel(EdgeInfo ei){
		try {
			if(ei.getCmdPort() != 0){
				logger.info("making cmd connection with : " + ei.getHost() + " on cmd port : " + ei.getCmdPort()  );
				return new CommConnection(ei.getHost(), ei.getCmdPort());
			}
		}catch (Exception e){
			logger.error("Command Port connetion failed !!!!! Server is down!! nodeid =  " + ei.getRef()+" reason: " + e );
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public synchronized void onAdd(final EdgeInfo ei) {
		// TODO check connection
		try{
			if (ei != null && ei.getChannel() == null){
				logger.info("making connection with : " + ei.getHost() + " on port : " + ei.getPort()  );
				CommConnection cc = new CommConnection(ei.getHost(), ei.getPort());

				CommConnection cmdCC = makeCmdChannel(ei);
				if(cmdCC != null){
					ei.setCmdChannel(cmdCC.connect());

				}
				ei.setChannel(cc.connect());
				ei.setActive(true);
				ChannelFuture closeFuture = ei.getChannel().closeFuture();

				   closeFuture.addListener(new ChannelFutureListener() {
				        @Override
				        public void operationComplete(ChannelFuture future) throws Exception {
				           onRemove(ei);
				        }
				    });
				ChannelFuture cmdCloseFuture = ei.getChannel().closeFuture();

				cmdCloseFuture.addListener(new ChannelFutureListener() {
					@Override
					public void operationComplete(ChannelFuture future) throws Exception {
						onRemove(ei);
					}
				});
				long startTime = System.currentTimeMillis();   
				WorkMessage wm  = DiscoverMessage.discoverMessage(state.getNodeId(), ei.getRef(),
						state.getConf().getWorkPort(), state.getConf().getCommandPort());
				logger.info("Time Taken to create Initial discovery Message : " + (System.currentTimeMillis() - startTime));
				state.getOutBoundMessageQueue().addMessage(wm);
			}
		}
		catch(Exception e){
			logger.error("Cannot connect to host!! Server is down!! nodeid =  " + ei.getRef()+" reason: " + e );
			logger.error(" or maybe it failed as it tried to connect to cmd port ?");
			e.printStackTrace();
		}
	}

	@Override
	public synchronized void onRemove(EdgeInfo ei) {
		// TODO ?
		 logger.info("Connection was terminated from node : " + ei.getRef());
		 logger.info("removing it from routing table");
		outboundEdges.removeNode(ei.getRef());
		
	}
	public ArrayList<EdgeInfo> getOutBoundChannel(int nodeId){
		//Single Directional 
		//logger.info("Getting Channle for destination : " +  nodeId);
		ArrayList<EdgeInfo> ei = null;
		
		try{
			if (outboundEdges != null && outboundEdges.map.size() > 0){
				if (nodeId == -1){

					return new ArrayList<EdgeInfo>(outboundEdges.map.values());
				}
				ei = new ArrayList<EdgeInfo>();
				ei.add(outboundEdges.map.get(nodeId));
				return ei;
			}
		}catch(Exception e){
			logger.error("Getting Error while looking for outbound channel with error : " + e);
		}
		return null;
	}
	
	public ArrayList<pipe.common.Common.Node> getOutBoundRouteTable(){
		return outboundEdges.getRoutingTable();	
	}
	
	public int getTotalNodes(){
		return outboundEdges.map.size() + 1;//including self	
	}
	
	public boolean addNewEdgeInfo(int ref, String host, int port, int cmdPort){
		logger.info("Got a new connection from  nodeid : " + ref + " ip :" + host +  " port : " + port  );
		if (outboundEdges.hasNode(ref)){
			EdgeInfo ei = outboundEdges.getNode(ref);
			logger.info("Trying to make reverse connection");
			onAdd(ei);
			return false;
		}
			
		EdgeInfo ei = outboundEdges.createIfNew(ref, host, port, cmdPort);
		if (!ei.isActive()) {
			logger.info("Trying to make reverse connection");
		    onAdd(ei);
		    
		}
		return true;
	}
}
