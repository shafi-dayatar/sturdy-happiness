import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.ArrayList;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.protobuf.ByteString;

import gash.router.client.CommConnection;
import gash.router.client.MessageClient;

public class WriteFileTest {

	  static MessageClient mc;

	  @BeforeClass
	  public static void testSetup() {
	    mc = new MessageClient("localhost", 4268);
	  }
	  
	  @AfterClass
	  public static void testCleanup() {
	    // Do your cleanup here like close URL connection , releasing resources etc
	  }

	  @Test
	  public void testMultiply() throws NullPointerException, Exception {
	    File file = new File("/Users/shafidayatar/Desktop/Gash-EnterpriseApp-275/sturdy-happiness/build_pb_shafi.sh");
	    ArrayList<ByteString> chunks = mc.chunkFile(file);
	    for(int i = 1;i <= 1000;i++){
	    	
	    CommConnection.getInstance().enqueue(mc.buildWCommandMessage("build_pb_shafi_"+i, chunks));}
	    Thread.sleep(10000);
	  }
}
