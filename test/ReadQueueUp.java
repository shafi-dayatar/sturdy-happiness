/**
 * Created by rentala on 4/29/17.
 */

import gash.router.client.MessageClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import gash.router.app.DemoApp;
import java.util.Scanner;

import static org.junit.Assert.assertEquals;


public class ReadQueueUp {
    static MessageClient mc;
    static DemoApp da;
    static Scanner s;

    @BeforeClass
    public static void testSetup() {
        System.out.println("Enter host name: ");
        s = new Scanner(System.in);
        String host = s.nextLine();
        System.out.println("Enter port number: ");
        String port = s.nextLine();
        int portNo = Integer.parseInt(port);
        mc = new MessageClient(3, host, portNo);
    }
    @Test
    public void readFile() throws Exception {
        System.out.println("Enter file name: ");
        String filename = s.nextLine();

        System.out.println("Enter number of requests: ");
        int reqs = s.nextInt();

        while(reqs > 0){
            mc.fileOperation("get", "./", filename.trim());
            reqs--;
            System.out.println("Sent request no : " + reqs);
        }
    }
    @AfterClass
    public static void testCleanup() {
        // Do your cleanup here like close URL connection , releasing resources etc
    }
}
