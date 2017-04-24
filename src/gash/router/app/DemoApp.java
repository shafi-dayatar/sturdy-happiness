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
package gash.router.app;

import java.io.File;
import java.util.Scanner;

import gash.router.client.CommListener;
import gash.router.client.MessageClient;
import gash.router.client.RedisGSDN;
import pipe.common.Common.Node;
import routing.Pipe.CommandMessage;

public class DemoApp implements CommListener {
	private MessageClient mc;
	private String leaderHost="";
	private int leaderPort=0;
	public DemoApp(MessageClient mc) {
		init(mc);
	}

	private void init(MessageClient mc) {
		this.mc = mc;
		this.mc.addListener(this);
	}

	private void ping(Scanner scan) {
		// test round-trip overhead (note overhead for initial connection)
		System.out.println("Enter Destination NodeId : ");
		int dest = scan.nextInt();
		System.out.println("Enter number of pings : ");
		int N = scan.nextInt();
		long[] dt = new long[N];
		long st = System.currentTimeMillis(), ft = 0;
		for (int n = 0; n < N; n++) {
			mc.ping(dest);
			ft = System.currentTimeMillis();
			dt[n] = ft - st;
			st = ft;
		}
		System.out.println("Round-trip ping times (msec)");
		for (int n = 0; n < N; n++)
			System.out.print(dt[n] + " ");
		System.out.println("");
	}
	
	private void uploadFolder(Scanner scan) {
		// TODO Auto-generated method stub
		System.out.print("Enter Folder Path : ");
		final File folder = new File(scan.nextLine().trim());
		mc.makeFileList("post", folder);
		
	}

	private void uploadMultipleFiles(Scanner scan) {
		// TODO Auto-generated method stub
		while(true){
		System.out.print("Enter File Path : ");
		String filePath = scan.nextLine().trim();
		System.out.print("Enter File Name : ");
		String fileName = scan.nextLine().trim();
		mc.fileOperation("post", filePath, fileName);
		}
		
	}

	private void uploadFile(Scanner scan) {
		// TODO Auto-generated method stub
		System.out.print("Enter File Path : ");
		String filePath = scan.nextLine().trim();
		System.out.print("Enter File Name : ");
		String fileName = scan.nextLine().trim();
		mc.fileOperation("post", filePath, fileName);
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	

	@Override
	public String getListenerID() {
		return "demo";
	}

	@Override
	public void onMessage(CommandMessage msg) {

		/*System.out.println("---> Got response RequestType : " + msg.getResp().getResponseType());
		System.out.println("---> Got response RequestType : " + msg.getResp().getStatus());
		if( msg.getResp().getResponseType() == Pipe.TaskType.READFILE){
			this.mc.onReadRequest(msg);
		}
		if( msg.getResp().getResponseType() == Pipe.TaskType.WRITEFILE){
			this.mc.onWriteRequest(msg);
		}*/
	}
	public void readFile(Scanner scan){
		System.out.print("Enter File Name: ");
		String fileName = scan.nextLine();
		mc.fileOperation("get", "./", fileName.trim());
	}

	/**
	 * sample application (client) use of our messaging service
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		RedisGSDN redis = new RedisGSDN("192.168.1.4", 6379);

		 loop: while(true){

			int nodeId;
			Scanner scan = new Scanner(System.in);
			System.out.println("Enter cluster details :");
			System.out.print("Enter Node id: ");
			nodeId = Integer.parseInt(scan.nextLine());		
			Node node = redis.getLeader(nodeId);
			MessageClient mc = new MessageClient(node.getNodeId(), node.getHost(),
					node.getPort());
			DemoApp da  = new DemoApp(mc);
			loop1: while(true){
				System.out.println("\n\n===============================================");
				System.out.println("===============================================\n");
				System.out.println("1. Upload a single file");
				System.out.println("2. Upload multiple files");
				System.out.println("3. upload all files from a directory");
				System.out.println("4: Read a File");
				System.out.println("5. Ping Node");
				System.out.println("6. Restart");
				System.out.println("7. Exit");
				System.out.println("\n===============================================");
				System.out.print("Enter Command no. :");
				int option = Integer.parseInt(scan.nextLine());
				System.out.println("Enter Option is : " + option);
				switch(option){
				case 1:
					System.out.println("You have selected option one");
					da.uploadFile(scan);
					break;
				case 2:
					da.uploadMultipleFiles(scan);
					break;
				case 3:
					da.uploadFolder(scan);
					break;
				case 4:
					da.readFile(scan);
					break;
				case 5:
					da.ping(scan);
					break;
				case 6:
					break loop1;
				case 7:
					break loop;
				default :
					break;
				}
			}
			System.out.println("Thank you for using our system!!! Hope to see you soon!");
		}
	}

	
}