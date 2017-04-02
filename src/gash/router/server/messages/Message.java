package gash.router.server.messages;

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
	private MessageType type;
	private Channel channel;
	private int secret = 123245;
	
	public MessageType getType() {
		return type;
	}

	public void setType(MessageType type) {
		this.type = type;
	}
	
	public void unPackHeader(Header hd){
		nodeId = hd.getNodeId();
		destinationId = hd.getDestination();
		timestamp = hd.getTime();
		maxHops = hd.getMaxHops();
	}

	public boolean processMessage(){
		return false;
	}

	@java.lang.Override
	public void discard() {

	}

	public Header createHeader(){
		Header.Builder hd = Header.newBuilder();
		hd.setDestination(destinationId);
		hd.setMaxHops(maxHops);
		hd.setTime(timestamp);
		hd.setNodeId(nodeId);
		if (reply){
			hd.setReplyFrom(replyFrom);
		}	
		return hd.build();
	}
	
	public WorkMessage forward(){
		maxHops = maxHops - 1;
		Header hd = createHeader();
		WorkMessage.Builder wb = WorkMessage.newBuilder();
		
		return wb.build();
		
	}

	@java.lang.Override
	public void reply() {

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
	public int getDestinationId() {
		return destinationId;
	}
	public void setDestinationId(int destinationId) {
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

}
