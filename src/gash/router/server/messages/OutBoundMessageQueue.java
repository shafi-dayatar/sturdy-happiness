package gash.router.server.messages;

public class OutBoundMessageQueue extends MessageQueue implements Runnable {

	private boolean forever = true;
	@Override
	public void run() {
		// TODO Auto-generated method stub
		while(forever){
			try{
			if(hasMessage()){
				
			}else{
				Thread.sleep(100);
			}
			}catch(Exception e){
				logger.error("Error in OutBoundMessageQueue thread : " + e);
			}
		}

	}

	@Override
	public void processMessage() {
		// TODO Auto-generated method stub

	}

	@Override
	public void sendMessage() {
		// TODO Auto-generated method stub

	}

}
