package gash.router.server.messages;

import gash.router.server.ServerState;
import io.netty.channel.Channel;
import pipe.common.Common.Header;
import pipe.work.Work.WorkMessage;

public class Message implements MessageInterface {

	private int nodeId;
	private int destinationId;
	private boolean reply;
	private int replyFrom;
	private long timestamp;
	private int maxHops;
	private Channel channel;
	private int secret = 123245;
	

	
	public void unPackHeader(Header hd){
		nodeId = hd.getNodeId();
		destinationId = hd.getDestination();
		timestamp = hd.getTime();
		maxHops = hd.getMaxHops();
	}


	public Header createHeader(){
		Header.Builder hd = Header.newBuilder();
		hd.setDestination(destinationId);
		hd.setMaxHops(maxHops);
		hd.setTime(timestamp);
		hd.setNodeId(nodeId);
		return hd.build();
	}
	
	public WorkMessage forward(){
		return null;
	}

	/*
	 * Setters and Getters
	 */
	public int getNodeId() {
		return nodeId;
	}
	public void setNodeId(int nodeId) {
		this.nodeId = nodeId;
	}
	public int getDestId() {
		return destinationId;
	}
	public void setDestId(int destinationId) {
		this.destinationId = destinationId;
	}
	public boolean isReply() {
		return reply;
	}
	public void setReply(boolean reply) {
		this.reply = reply;
	}
	public int getReplyFrom() {
		return replyFrom;
	}
	public void setReplyFrom(int replyFrom) {
		this.replyFrom = replyFrom;
	}
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	public int getMaxHops() {
		return maxHops;
	}
	public void setMaxHops(int maxHops) {
		this.maxHops = maxHops;
	}
	public int getSecret() {
		return secret;
	}
	public void setSecret(int secret) {
		this.secret = secret;
	}
	public Channel getChannel() {
		return channel;
	}
	public void setChannel(Channel channel) {
		this.channel = channel;
	}
	@Override
	public void processMessage(ServerState state) {
		// TODO Auto-generated method stub
		//return null;
	}

}
