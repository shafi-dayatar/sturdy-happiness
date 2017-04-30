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
package gash.router.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.container.RoutingConf;
import gash.router.server.communication.CommConnection;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import pipe.common.Common;
import pipe.common.Common.Node;
import routing.Pipe;
import routing.Pipe.CommandMessage;
import routing.Pipe.ReadBody;
import routing.Pipe.Response;
import routing.Pipe.Response.Status;
import routing.Pipe.TaskType;
import routing.Pipe.WriteBody;


/**
 * The message handler processes json messages that are delimited by a 'newline'
 * 
 * TODO replace println with logging!
 * 
 * @author gash
 * 
 */
public class CommandHandler extends SimpleChannelInboundHandler<CommandMessage> {
	protected static Logger logger = LoggerFactory.getLogger("cmd");
	protected RoutingConf conf;

	ServerState serverState;

	public CommandHandler(RoutingConf conf) {
		if (conf != null) {
			this.conf = conf;
		}
	}
	public CommandHandler(RoutingConf conf, ServerState serverState) {
		if (conf != null) {
			this.conf = conf;
		}
		this.serverState = serverState;
	}



	/**
	 * override this method to provide processing behavior. This implementation
	 * mimics the routing we see in annotating classes to support a RESTful-like
	 * behavior (e.g., jax-rs).
	 * 
	 * @param msg
	 */

	public void handleMessage(CommandMessage msg, Channel channel) {
		if (msg == null) {
			// TODO add logging
			System.out.println("ERROR: Unexpected content - " + msg);
			return;
		}
		logger.info("Request received at command server : " + msg.toString());
		if(msg.hasRequest()){
			switch(msg.getRequest().getRequestType()){
			case REQUESTWRITEFILE:
				WriteBody writeReq = msg.getRequest().getRwb();
				Status status = serverState.getRaftState().writeFile(writeReq);
				sendWriteResponse(channel, msg, status);
				//forwardWriteRequest();
				break;
			case REQUESTREADFILE:
				ReadBody readReq = msg.getRequest().getRrb();
				Response res = null;
				if(readReq.hasChunkId()){
					serverState.connectionManager.setConnection(msg.getHeader().getNodeId(), channel);
					logger.info("Request from client is : " + msg.toString());
					serverState.getInBoundReadTaskQueue().addMessage(msg);
				}else{
					res = serverState.getRaftState().getFileChunkLocation(readReq);
					serverState.sendReadResponse(channel, res, msg.getHeader().getNodeId());
				}
				break;
			default:
				logger.info(">>>>> unhandled request at server, just loggin the request, " + msg.toString());
				break;
			}
		}else if(msg.hasPing()){
			handlePing(msg, channel);
		}else{
			logger.info(">>>>>Unsupport msg received from client  msg detail is : " + msg.toString());
		}

	}


	private void handlePing(CommandMessage msg, Channel channel){
		logger.info("----------got a ping message------------:" + msg.toString());
		int destinationId = msg.getHeader().getDestination();
		int clusterClientId = serverState.getConf().getClusterClientId();
		int nextClusterId = serverState.getConf().getNextClusterId();
		if (destinationId == serverState.getConf().getClusterId()){
			if(msg.getHeader().getNodeId() == clusterClientId){
				sendPingResponse(msg, channel);
				logger.info("Client Pings own cluster, respond directly to client");
			}else{
				Channel ch = serverState.connectionManager.getConnection(nextClusterId);
				if(ch != null){
					sendPingResponse(msg, ch);
					logger.info("Other cluster is trying to ping, sending response, forwarding to next Cluster");
				}
			}
		}else if(destinationId == clusterClientId){
			Channel ch = serverState.connectionManager.getConnection(destinationId);
			if(ch != null){
				forwardPingMessage(msg, ch);
				logger.info("Forwarding ping respone to client");
			}else{
				logger.info("Cannot forward ping to client, no connection available");
			}
		}else{
			logger.info("Forwarding to cluster id : " + nextClusterId);
			Channel ch = serverState.connectionManager.getConnection(nextClusterId);
			if (ch == null){
				Node node = serverState.getRedis().getLeader(nextClusterId);
				logger.debug("creating new channgel for  :  "  + node.toString());//+ node.toString()) ;
				try{
					CommConnection cc = new CommConnection(node.getHost(), node.getPort());
					ch = cc.connect();
					serverState.connectionManager.setConnection(nextClusterId, ch);
				}
				catch(Exception e){
					logger.error("Cannot make connection to next Cluster");
				}
			}
			if(ch != null){
				logger.debug("forwording on Channel:  " + ch.toString() );
				forwardPingMessage(msg, ch);
				logger.info("forward Msg compeleted");
			}else{
				logger.info("No channel available for clusterId:" + nextClusterId);
			}

		}
	}

	private void forwardPingMessage(CommandMessage msg, Channel ch) {
		CommandMessage.Builder cmdMsg = CommandMessage.newBuilder(msg);
		Header.Builder hdb = Header.newBuilder(msg.getHeader());
		int maxHops = msg.getHeader().getMaxHops();
		if (maxHops == 0){
			logger.info("MaxHops has reached zero, grounding this message: " + msg.toString());
			return;
		}
		if( maxHops > 0 ){
			hdb.setMaxHops(maxHops - 1);
		}
		cmdMsg.setHeader(hdb);
		cmdMsg.setPing(true);
		ch.writeAndFlush(cmdMsg.build());
	}

	private void forwardToNextCluster(CommandMessage msg, Channel ch) {
		// TODO Auto-generated method stub
		CommandMessage.Builder cmdMsg = CommandMessage.newBuilder(msg);
		Header.Builder hdb = Header.newBuilder(msg.getHeader());
		int maxHops = msg.getHeader().getMaxHops();
		if (maxHops == 0){
			logger.info("MaxHops has reached zero, grounding this message: " + msg.toString());
			return;
		}
		if( maxHops > 0 ){
			hdb.setMaxHops(maxHops - 1);
		}
		cmdMsg.setHeader(hdb);
		cmdMsg.setPing(true);
		ch.writeAndFlush(cmdMsg.build());

	}

	private void sendPingResponse(CommandMessage msg, Channel ch) {
		// TODO Auto-generated method stub
		CommandMessage.Builder cmdMsg = CommandMessage.newBuilder();
		Header.Builder hdb = Header.newBuilder(msg.getHeader());
		hdb.setDestination(msg.getHeader().getNodeId());
		hdb.setNodeId(msg.getHeader().getDestination());
		hdb.setMaxHops(10);
		cmdMsg.setHeader(hdb);
		cmdMsg.setPing(true);
		ch.writeAndFlush(cmdMsg.build());
	}

	private void sendWriteResponse(Channel channel, CommandMessage cmdMessage, Status status){
		logger.info("Preparing to send write response for nodeid: "+ cmdMessage.getHeader().getNodeId() );
		CommandMessage.Builder cmdMsg = CommandMessage.newBuilder(cmdMessage);
		Common.Header.Builder hd = Common.Header.newBuilder();
		hd.setNodeId(serverState.getNodeId());
		hd.setTime(System.currentTimeMillis());
		hd.setDestination(cmdMessage.getHeader().getNodeId());

		Response.Builder response = Response.newBuilder();
		response.setResponseType(TaskType.RESPONSEWRITEFILE);
		response.setStatus(status);

		WriteResponse.Builder writeRespBuilder = WriteResponse.newBuilder();
		writeRespBuilder.addChunkId(cmdMessage.getRequest().getRwb().getChunk().getChunkId());
		response.setWriteResponse(writeRespBuilder.build());
		cmdMsg.setHeader(hd);
		channel.writeAndFlush(cmdMsg.build());
	}
	/*
	private Pipe.Response.Builder buildReadResponse(Pipe.Request request, byte[] chunkContent){
		logger.info("Building read response for request chunk content length: " + chunkContent.length);
		Pipe.Response.Builder response = Pipe.Response.newBuilder();
		response.setResponseType(request.getRequestType());
		response.setStatus(Pipe.Response.Status.Success);
		response.setReadResponse(RResponseBuild(request, chunkContent).build());
		//failure case
		response.setStatus(Pipe.Response.Status.Failure);
		//response.setReadResponse(buildReadResponse().build());
		return response;

	}

	private Pipe.Response.Builder buildWriteResponse(Pipe.Request request, int chunkId){
		logger.info("Building write response for request, missing chunk id is : " + chunkId);
		Pipe.Response.Builder response = Pipe.Response.newBuilder();
		response.setResponseType(request.getRequestType());
		Pipe.WriteResponse.Builder writeRespBuilder = Pipe.WriteResponse.newBuilder();
		response.setWriteResponse(writeRespBuilder.build());
		//write is successful
		response.setStatus(Pipe.Response.Status.Success);
		return response;
	}

	private Pipe.ReadResponse.Builder RResponseBuild(Pipe.Request request, byte[] bytes){
		Pipe.ReadResponse.Builder readRespBuilder = Pipe.ReadResponse.newBuilder();
		readRespBuilder.setFilename(request.getRrb().getFilename());
		readRespBuilder.setFileExt("");
		readRespBuilder.setFileId(Long.toString(request.getRrb().getFileId()));
		readRespBuilder.setNumOfChunks(1);
		Chunk.Builder chunkB = Chunk.newBuilder();
		chunkB.setChunkData(ByteString.copyFrom(bytes));
		chunkB.setChunkId(0);
		readRespBuilder.setChunk(chunkB.build());
		//multiple
		readRespBuilder.addChunkLocation(buildChunkLocation().build());
		return readRespBuilder;
	}
	private Pipe.ChunkLocation.Builder buildChunkLocation(){
		Pipe.ChunkLocation.Builder chunkLocBuilder = Pipe.ChunkLocation.newBuilder();
		//chunkLocBuilder.setChunkid(0);
		//Pipe.Node.Builder node = buildNode();
		//chunkLocBuilder.setNode(this.conf.getNodeId(), node.build());
		return chunkLocBuilder;
	}
	private Pipe.Node.Builder buildNode(){
		Pipe.Node.Builder node = Pipe.Node.newBuilder();
		setHost(node);
		node.setNodeId(this.conf.getNodeId());
		node.setPort(this.conf.getHeartbeatDt());
		return node;
	}
	private void setHost(Pipe.Node.Builder node){
		InetAddress IP= null;
		try {
			IP = InetAddress.getLocalHost();
			node.setHost(IP.getHostAddress());
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}

	/**
	 * a message was received from the server. Here we dispatch the message to
	 * the client's thread pool to minimize the time it takes to process other
	 * messages.
	 * 
	 * @param ctx
	 *            The channel the message was received from
	 * @param msg
	 *            The message
	 */
	protected void channelRead0(ChannelHandlerContext ctx, CommandMessage msg) throws Exception {
		handleMessage(msg, ctx.channel());
	}


	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		logger.error("Unexpected exception from downstream.", cause);
		ctx.close();
	}

}