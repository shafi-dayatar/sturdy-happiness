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
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import pipe.common.Common;
import pipe.common.Common.Failure;
import pipe.work.Work.Command;
import pipe.work.Work.LogEntry;
import pipe.work.Work.LogEntry.DataAction;
import routing.Pipe;
import routing.Pipe.CommandMessage;
import routing.Pipe.ReadBody;
import routing.Pipe.WriteBody;
import routing.Pipe.Chunk;
import routing.Pipe.Response;
import com.google.protobuf.ByteString;
import java.net.InetAddress;
import java.net.UnknownHostException;


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
	//logger.info("Request received at server : " + msg.toString());
		if(msg.hasReq()){
			
			switch(msg.getReq().getRequestType()){
		
		
		    case REQUESTWRITEFILE:
		    	WriteBody writeReq = msg.getReq().getRwb();
		    	int missingChunk = serverState.getRaftState().writeFile(writeReq);
		    	break;
		    case REQUESTREADFILE:
		    	ReadBody readReq = msg.getReq().getRrb();
		    	Response res = null;
		    	if(readReq.hasChunkId()){
		    		
		    	}else{
		    		res = serverState.getRaftState().getFileChunkLocation(readReq);
		    	}
		    	sendReadResponse(channel, res, msg.getHeader().getNodeId());
		    default:
		    	break;
			}
		}else if(msg.hasPing()){
			
		}else{
			logger.info("Unsupport msg received from client  msg detail is : " + msg.toString());
		}

	}
		//try {
			// TODO How can you implement this without if-else statements?
			/*if (msg.hasReq()){
				switch (msg.getReq().getRequestType()){
					case READFILE:
						if(msg.getReq().hasRrb()){
							byte[] chunkContent = serverState.getRaftState().readFile(msg.getReq().getRrb());
							sendReadResponse(channel, msg, chunkContent);

							logger.info(" end of READFILE");
						}
						break;
					case WRITEFILE:
						if(msg.getReq().hasRwb()){
							int missingChunk = serverState.getRaftState().writeFile(msg.getReq().getRwb());
							sendWriteResponse(channel, msg, missingChunk);
							logger.info(" end of WRITEFILE");
						}
						break;
					case UPDATEFILE:

						//same as write no difference
						if(msg.getReq().hasRwb()){
							int missingChunk = serverState.getRaftState().writeFile(msg.getReq().getRwb());
							sendWriteResponse(channel, msg, missingChunk);
						}

						break;
					case DELETEFILE:
						if(msg.getReq().hasRwb()){
							serverState.getRaftState().deleteFile(msg.getReq().getRrb());
						}
						break;

				}
			}

		} catch (Exception e) {
			// TODO add logging
			logger.info("error stack trace in handleMessage - CommandHanler: ");
			e.printStackTrace();
			Failure.Builder eb = Failure.newBuilder();
			eb.setId(conf.getNodeId());
			eb.setRefId(msg.getHeader().getNodeId());
			eb.setMessage(e.getMessage());
			CommandMessage.Builder rb = CommandMessage.newBuilder(msg);
			rb.setErr(eb);
			channel.write(rb.build());
		}
		logger.info(" flushing . . . .. ");
		System.out.flush();
	}*/

	private void sendReadResponse(Channel channel, Response rsp, int clientNodeId){
		logger.info("Preparing to send read response for nodeid: "+ clientNodeId );
		CommandMessage.Builder cmdMsg = CommandMessage.newBuilder();
		Common.Header.Builder hd = Common.Header.newBuilder();
		hd.setNodeId(serverState.getNodeId());
		hd.setTime(System.currentTimeMillis());
		hd.setDestination(clientNodeId);
		cmdMsg.setResp(rsp);
		cmdMsg.setHeader(hd);
		channel.writeAndFlush(cmdMsg.build());
	}
    /*
	private void sendWriteResponse(Channel channel, CommandMessage cmdMessage, int chunkId){
		logger.info("Preparing to send write response for nodeid: "+ cmdMessage.getHeader().getNodeId() );
		CommandMessage.Builder cmdMsg = CommandMessage.newBuilder(cmdMessage);
		Common.Header.Builder hd = Common.Header.newBuilder();
		hd.setNodeId(serverState.getNodeId());
		hd.setTime(System.currentTimeMillis());
		hd.setDestination(-1);
		cmdMsg.setResp(buildWriteResponse(cmdMessage.getReq(), chunkId).build());
		cmdMsg.setHeader(hd);
		channel.writeAndFlush(cmdMsg.build());
	}

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