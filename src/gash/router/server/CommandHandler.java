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
import pipe.common.Common.Failure;
import pipe.work.Work.Command;
import pipe.work.Work.LogEntry;
import pipe.work.Work.LogEntry.DataAction;
import routing.Pipe.CommandMessage;
import routing.Pipe.TaskType;
import routing.Pipe.Chunk;
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

		PrintUtil.printCommand(msg);
		//Test Log Message
		LogEntry.Builder logEntryBuilder = LogEntry.newBuilder();
		Command.Builder command = Command.newBuilder();
		command.setClientId(999);
		command.setKey("Filename");
		command.setValue("no1.txt");
		logEntryBuilder.setAction(DataAction.INSERT);
		logEntryBuilder.setData(command);
		logger.info("Got Request from client, pushing it to leader");
		serverState.getRaftState().appendEntries(logEntryBuilder);
		

		try {
			// TODO How can you implement this without if-else statements?
			if (msg.hasReq()){
				switch (msg.getReq().getRequestType()){
					case READFILE:
						if(msg.getReq().hasRrb()){
							serverState.getRaftState().readFile(msg.getReq().getRrb());
						}

						break;
					case WRITEFILE:
						if(msg.getReq().hasRwb()){
							serverState.getRaftState().writeFile(msg.getReq().getRwb());
						}
						break;
					case UPDATEFILE:
						if(msg.getReq().hasRwb()){
							serverState.getRaftState().writeFile(msg.getReq().getRwb());
						}
						break;
					case DELETEFILE:
						if(msg.getReq().hasRwb()){
							serverState.getRaftState().deleteFile(msg.getReq().getRrb());
						}
						break;
				}
			}
			if (msg.hasPing()) {
				logger.info("ping from " + msg.getHeader().getNodeId());
			} else if (msg.hasMessage()) {
				logger.info(msg.getMessage());
			} else {
			}

		} catch (Exception e) {
			// TODO add logging
			Failure.Builder eb = Failure.newBuilder();
			eb.setId(conf.getNodeId());
			eb.setRefId(msg.getHeader().getNodeId());
			eb.setMessage(e.getMessage());
			CommandMessage.Builder rb = CommandMessage.newBuilder(msg);
			rb.setErr(eb);
			channel.write(rb.build());
		}

		System.out.flush();
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