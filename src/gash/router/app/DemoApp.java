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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

import com.google.protobuf.ByteString;

import gash.router.client.CommConnection;
import gash.router.client.CommListener;
import gash.router.client.MessageClient;
import gash.router.server.messages.Message;
import routing.Pipe;
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

	private void ping(int N) {
		// test round-trip overhead (note overhead for initial connection)
		final int maxN = 10;
		long[] dt = new long[N];
		long st = System.currentTimeMillis(), ft = 0;
		for (int n = 0; n < N; n++) {
			mc.ping();
			ft = System.currentTimeMillis();
			dt[n] = ft - st;
			st = ft;
		}

		System.out.println("Round-trip ping times (msec)");
		for (int n = 0; n < N; n++)
			System.out.print(dt[n] + " ");
		System.out.println("");
	}
	

	@Override
	public String getListenerID() {
		return "demo";
	}

	@Override
	public void onMessage(CommandMessage msg) {

		System.out.println("---> Got response RequestType : " + msg.getResp().getResponseType());
		System.out.println("---> Got response RequestType : " + msg.getResp().getStatus());
		if( msg.getResp().getResponseType() == Pipe.TaskType.READFILE){
			this.mc.onReadRequest(msg);
		}
		if( msg.getResp().getResponseType() == Pipe.TaskType.WRITEFILE){
			this.mc.onWriteRequest(msg);
		}
	}

	/**
	 * sample application (client) use of our messaging service
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		// 0 ip
		// 1 port
		// 2 get/post
		// 3 file path

		try {
			for(String s : args){
				System.out.println(s);
			}
			if(args.length <2){
				System.out.println("Enter valid inputs - 1 -> ip 2 -> port");
			}
			MessageClient mc = new MessageClient(args[0], Integer.parseInt(args[1]));
			DemoApp da = new DemoApp(mc);
			if(args.length == 2){
				da.mc.ping();
			}

			if(args.length == 4){
				da.mc.fileOperation(args[2], args[3], -1);
			}
			if(args.length == 5){
				da.mc.fileOperation(args[2], args[3], Long.parseLong(args[4]));
			}


		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}