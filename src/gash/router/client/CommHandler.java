/*
 * copyright 2016, gash
 * 
 * Gash licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package gash.router.client;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import routing.Pipe.CommandMessage;
import routing.Pipe.ReadBody;
import routing.Pipe.ReadResponse;
import routing.Pipe.Response;

import javax.xml.bind.Unmarshaller;



/**
 * A client-side netty pipeline send/receive.
 * 
 * Note a management client is (and should only be) a trusted, private client.
 * This is not intended for public use.
 * 
 * @author gash
 * 
 */
public class CommHandler extends SimpleChannelInboundHandler<CommandMessage> {
	protected static Logger logger = LoggerFactory.getLogger("connect");
	protected ConcurrentMap<String, CommListener> listeners = new ConcurrentHashMap<String, CommListener>();
	//private volatile Channel channel;
	
	private String host;
	private int port;
	//private  MessageClient mc = new MessageClient(host,port);
	private MessageClient mc = new MessageClient();
	public CommHandler() {
		//this.mc =mc;
	}

	/**
	 * Notification registration. Classes/Applications receiving information
	 * will register their interest in receiving content.
	 * 
	 * Note: Notification is serial, FIFO like. If multiple listeners are
	 * present, the data (message) is passed to the listener as a mutable
	 * object.
	 * 
	 * @param listener
	 */
//	public void take(String host, int port){
//		this.host =host;
//		this.port = port;
//	}
	public void addListener(CommListener listener) {
		if (listener == null)
			return;

		listeners.putIfAbsent(listener.getListenerID(), listener);
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
	public void handleMessage(CommandMessage msg, Channel channel) {
		if (msg == null) {
			// TODO add logging
			System.out.println("ERROR: Unexpected content - " + msg);
			return;
		}
//		for(String listenerId : listeners.keySet()){
//			CommListener commListener = listeners.get(listenerId);
//			commListener.onMessage(msg);
//
//		}
		System.out.println("im in");
	//logger.info("Request received at server : " + msg.toString());
		if(msg.hasResp()){
			
			switch(msg.getResp().getResponseType()){
		
		
		    case RESPONSEREADFILE:
		    	//System.out.println("I'm here");
		    	ReadResponse readRes = msg.getResp().getReadResponse();
		    	System.out.println(readRes.getNumOfChunks());
		    	mc.sendfileReadRequests(msg);
		    	//System.out.println("chunkloccount"+readRes.getChunkLocationCount()+" loc list "+readRes.getChunkLocationList().toString()+"");
		    	break;
		    case REQUESTREADFILE:
		    	ReadBody readReq = msg.getReq().getRrb();
		    	Response res = null;
		    	if(readReq.hasChunkId()){
		    		
		    	}else{
		    		//res = serverState.getRaftState().getFileChunkLocation(readReq);
		    	}
		    	//sendReadResponse(channel, res, msg.getHeader().getNodeId());
		    default:
		    	break;
			}
		}else if(msg.hasPing()){
			
		}else{
			logger.info("Unsupport msg received from client  msg detail is : " + msg.toString());
		}

	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, CommandMessage msg) throws Exception {
		System.out.println(" got response ! ");
		handleMessage(msg,ctx.channel());
		/*System.out.println("--> got incoming message");
		for (String id : listeners.keySet()) {
			CommListener cl = listeners.get(id);
			// TODO this may need to be delegated to a thread pool to allow
			// async processing of replies
			cl.onMessage(msg);
		}*/
	}
	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
		System.out.println("--> user event: " + evt.toString());
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		logger.error("Unexpected exception from channel.", cause);
		ctx.close();
	}

}