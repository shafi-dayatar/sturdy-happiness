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
package gash.router.client;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import pipe.common.Common;
import pipe.common.Common.Header;
import routing.Pipe;
import routing.Pipe.Request;
import routing.Pipe.WriteBody;
import routing.Pipe.Chunk;
import routing.Pipe.TaskType;
import routing.Pipe.CommandMessage;
//import routing.Pipe.WhoIsLeader;

/**
 * front-end (proxy) to our service - functional-based
 * 
 * @author gash
 * 
 */
public class MessageClient {
	// track requests
	private int curID = 0;
	protected static Logger logger = LoggerFactory.getLogger("Client");
	public MessageClient(String host, int port) {
		init(host, port);
	}

	private void init(String host, int port) {
		CommConnection.initConnection(host, port);
	}

	public void addListener(CommListener listener) {
		CommConnection.getInstance().addListener(listener);
	}
	public void askForLeader() {
		// construct the message to send
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(999);
		hb.setTime(System.currentTimeMillis());
		hb.setDestination(-1);
		
		//WhoIsLeader.Builder wl=WhoIsLeader.newBuilder();
		//wl.setAskleader(true);
		
		CommandMessage.Builder rb = CommandMessage.newBuilder();
		rb.setHeader(hb);
		//rb.setWhoisleader(wl);
		
		System.out.println("im sending message");
		try {
			// direct no queue
			// CommConnection.getInstance().write(rb.build());

			// using queue
			CommConnection.getInstance().enqueue(rb.build());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public void ping() {
		// construct the message to send
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(999);
		hb.setTime(System.currentTimeMillis());
		hb.setDestination(-1);

		CommandMessage.Builder rb = CommandMessage.newBuilder();
		rb.setHeader(hb);
		rb.setPing(true);

		try {
			// direct no queue
			// CommConnection.getInstance().write(rb.build());

			// using queue
			CommConnection.getInstance().enqueue(rb.build());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	// Save File to server
	private File readFileByPath(String filePath){
		File file = null;
		try
		{
			file = new File(filePath);
		}
		catch (Exception e){
			System.out.println("File not found !!");
		}
		return file;

	}
	public void fileOperation(String action, String filePath){
		if(action == "get"){

		}

		if(action == "post"){
			File file = readFileByPath(filePath);
			if(file == null){
				return;
			}
			ArrayList<ByteString> chunks = chunkFile(file);
			if(chunks == null){
				return;
			}
			CommandMessage commandMessage = buildCommandMessage(file, chunks);
			try
			{
				CommConnection.getInstance().enqueue(commandMessage);
			}
			catch (Exception e) {
				e.printStackTrace();
				System.out.println("Couldnt sent to the system");
				return;
			}
			finally {
				CommConnection.getInstance().release();
				System.out.println("Connection released exiting ! ");
			}

		}
		System.out.println("Enter valid inputs - 3 -> 'get' or 'post' ");
		return;
	}
	private ArrayList<ByteString> chunkFile(File file) {
		ArrayList<ByteString> chunkedFile = new ArrayList<ByteString>();
		int sizeOfFiles = 254 * 254; // equivalent 64KB ~
		byte[] buffer = new byte[sizeOfFiles];

		try {
			BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
			int tmpBuffer = 0;
			while ((tmpBuffer = bis.read(buffer)) > 0) {
				ByteString byteString = ByteString.copyFrom(buffer, 0, tmpBuffer);
				chunkedFile.add(byteString);
			}
			return chunkedFile;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	private CommandMessage buildCommandMessage(File file, ArrayList<ByteString> chunks )
	{
		CommandMessage.Builder command = CommandMessage.newBuilder();
		try
		{
			Request.Builder msg = Request.newBuilder();
			msg.setRequestType(TaskType.WRITEFILE);
			WriteBody.Builder rwb  = WriteBody.newBuilder();
			rwb.setFileExt(file.getName().substring(file.getName().lastIndexOf(".") + 1));
			rwb.setFilename(file.getName());
			rwb.setNumOfChunks(chunks.size());
			for(ByteString chunk : chunks){
				Chunk.Builder chunkBuilder = Chunk.newBuilder();
				chunkBuilder.setChunkId(nextId());
				chunkBuilder.setChunkSize(chunk.size());
				chunkBuilder.setChunkData(chunk);
				rwb.setChunk(chunkBuilder.build());
			}
			msg.setRwb(rwb);

			Pipe.Node.Builder node = Pipe.Node.newBuilder();

			node.setHost(InetAddress.getLocalHost().getHostAddress());

			node.setPort(8000);
			node.setNodeId(-1);
			//msg.setClient(node);
			command.setReq(msg.build());
			return command.build();
		}
		catch (Exception e)
		{
			System.out.println(" Sending write request failed :");
			e.printStackTrace();
			return command.build();
		}
	}
	public void writeFile(String filename, ByteString chunkData, int noOfChunks, int chunkId) {

		logger.info("Printing byte size"+chunkData.size());
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(999);
		hb.setTime(System.currentTimeMillis());
		hb.setDestination(-1);


		Chunk.Builder chb=Chunk.newBuilder();
		chb.setChunkId(chunkId);
		chb.setChunkData(chunkData);
		chb.setChunkSize(chunkData.size());

		WriteBody.Builder wb= WriteBody.newBuilder();
		wb.setFileId("1");
		wb.setFilename(filename);
		wb.setChunk(chb);
		wb.setNumOfChunks(noOfChunks);

		Request.Builder rb = Request.newBuilder();
		//request type, read,write,etc
		rb.setRequestType(TaskType.WRITEFILE ); // operation to be
														// performed
		rb.setRwb(wb);
		CommandMessage.Builder cb = CommandMessage.newBuilder();
		// Prepare the CommandMessage structure
		cb.setHeader(hb);
		cb.setReq(rb.build());

		// Initiate connection to the server and prepare to save file
		try {
			CommConnection.getInstance().enqueue(cb.build());
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Problem connecting to the system");
		}

	}

	/*public void release() {
		CommConnection.getInstance().release();
	}*/

	/**
	 * Since the service/server is asychronous we need a unique ID to associate
	 * our requests with the server's reply
	 * 
	 * @return
	 */
	private synchronized int nextId() {
		return ++curID;
	}
}