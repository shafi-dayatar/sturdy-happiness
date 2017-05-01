/**
 * Created by rentala on 4/29/17.
 */
package gash.router.client;

import java.util.Scanner;

import gash.router.app.DemoApp;
import pipe.common.Common;




public class ReadQueueUp {
    static MessageClient mc;
    static DemoApp da;
    static Scanner s;

    public static void testSetup() {
        RedisGSDN redis = new RedisGSDN("localhost", 6379);
        Common.Node node = redis.getLeader(4);
        mc = new MessageClient(node.getNodeId(), node.getHost(),
                node.getPort());
    }

    public static void main(String[] args) {
        testSetup();
        readFile();
    }

    public static void readFile() {
        try{
            s = new Scanner(System.in);
            System.out.println("Enter file name: ");
            String filename = s.nextLine();

            System.out.println("Enter number of requests: ");
            int reqs = s.nextInt();

            while(reqs > 0){
                mc.fileOperation("get", "./", filename.trim());
                reqs--;
                System.out.println("Sent request no : " + reqs);
            }

        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void testCleanup() {
        // Do your cleanup here like close URL connection , releasing resources etc
    }
}
