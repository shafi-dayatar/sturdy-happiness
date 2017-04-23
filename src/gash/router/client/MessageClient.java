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

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import org.apache.commons.io.IOUtils;

import pipe.common.Common;
import pipe.common.Common.Header;
import routing.Pipe;
import routing.Pipe.Chunk;
import routing.Pipe.CommandMessage;
//import routing.Pipe.CommandMessage.MessageType;
import routing.Pipe.Request;
import routing.Pipe.TaskType;
import routing.Pipe.WriteBody;
//import routing.Pipe.WriteRequest;

import com.google.protobuf.ByteString;
//import routing.Pipe.WhoIsLeader;

/**
 * front-end (proxy) to our service - functional-based
 * 
 * @author gash
 * 
 */
public class MessageClient {
	// track requests
	private int messageId = 1;
	private static int fileId = 0;
	private static int chunkId = 0;
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

	private String fileoutput = "output";

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

	public void onWriteRequest(CommandMessage msg) {
		// System.out.println(" Write request message: "+
		// msg.getResp().getStatus());

		System.out.println(" done with write request  . .. . .");
		System.out.flush();

	}

	public void onReadRequest(CommandMessage msg) {
		// System.out.println(" Reaad request message: "+
		// msg.getResp().getStatus());
		// System.out.println(" No of chunks: "+
		// msg.getResp().getReadResponse().getNumOfChunks());

		try {

			File file = new File(fileoutput);
			file.createNewFile();
			ArrayList<ByteString> byteString = new ArrayList<ByteString>();
			// byteString.add(msg.getResp().getReadResponse().getChunk().getChunkData());
			FileOutputStream outputStream = new FileOutputStream(file);
			ByteString bs = ByteString.copyFrom(byteString);
			System.out.println(bs.size());
			outputStream.write(bs.toByteArray());
			outputStream.flush();
			outputStream.close();
			System.out.println(" Wrote file: ");

		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println(" flushing  . .. . .");
		System.out.flush();
	}

	// Save File to server

	private File readFileByPath(String filePath) {
		File file = null;
		try {
			file = new File(filePath);
		} catch (Exception e) {
			System.out.println("File not found !!");
		}
		return file;

	}

	public void fileOperation(String action, String filePath, String file_name) {
		System.out.println("Actions recived: " + action + " " + filePath);
		ClassLoader classLoader = getClass().getClassLoader();
		File file = new File(filePath+"/"+file_name);
		if (action.contains("get") && file_name != null) {
			this.fileoutput = filePath;
			CommandMessage commandMessage = buildRCommandMessage(file_name);
			try {
				System.out.println("Enueued read request.....");
				CommConnection.getInstance().enqueue(commandMessage);
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("Couldnt sent read request to the system");
				return;
			}
		} else if (action.contains("post")) {
			// File file = readFileByPath(filePath);
			if (file_name == null) {
				return;
			}
			
			ArrayList<ByteString> chunks = chunkFile(file);
			if (chunks == null) {
				return;
			}
			int chunksNum = chunks.size();
			try {
				for(int i =0;i< chunks.size();i++){
					CommandMessage commandMessage = buildWCommandMessage(file, chunks.get(i),chunksNum);
				System.out.println("Enueued file .....");
				CommConnection.getInstance().enqueue(commandMessage);
				}
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("Couldnt sent to the system");
				return;
			} finally {
				// CommConnection.getInstance().release();
				// System.out.println("Connection released exiting ! ");
			}

		} else {
			System.out.println("Enter valid inputs - 3 -> 'get' or 'post' or file id > 0");
			return;
		}

	}

	public ArrayList<ByteString> chunkFile(File file) {
		ArrayList<ByteString> chunkedFile = new ArrayList<ByteString>();
		float len = file.length();
		int sizeOfChunk = 1024;
		byte[] buffer = new byte[sizeOfChunk];
		int noOfChunks = (int) Math.ceil(len / (double) sizeOfChunk);

		try {
			BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
			while (len > 0) {
				bis.read(buffer);
				if (len < sizeOfChunk) {
					byte[] leftData = new byte[(int) sizeOfChunk];
					chunkedFile.add(ByteString.copyFrom(leftData));
					len =0;
				} else {
					len = len - sizeOfChunk;
					logger.info("chunk"+len);
					chunkedFile.add(ByteString.copyFrom(buffer));
				}
			}
			
			return chunkedFile;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	private CommandMessage buildRCommandMessage(String file_name) {
		CommandMessage.Builder command = CommandMessage.newBuilder();
		try {
			// Request.Builder msg = Request.newBuilder();
			// msg.setRequestType(TaskType.READFILE);
			// Pipe.ReadBody.Builder rrb = Pipe.ReadBody.newBuilder();
			// rrb.setFilename(file_name);
			// msg.setRrb(rrb.build());

//			Pipe.Node.Builder node = Pipe.Node.newBuilder();
//
//			node.setHost(InetAddress.getLocalHost().getHostAddress());
//
//			node.setPort(8000);
//			node.setNodeId(-1);
			// msg.setClient(node);
			Header.Builder header = Header.newBuilder();
			header.setNodeId(1);
			header.setTime(0);
			command.setHeader(header);
			// command.setReq(msg.build());
			return command.build();
		} catch (Exception e) {
			System.out.println(" Sending read request failed :");
			e.printStackTrace();
			return command.build();
		}
	}

	public CommandMessage buildWCommandMessage(File file, ByteString chunk, int noOfChunks) {
		CommandMessage.Builder command = CommandMessage.newBuilder();
		try {
			/// *Request.Builder msg = Request.newBuilder();
			Request.Builder req = Request.newBuilder();
			req.setRequestType(TaskType.REQUESTWRITEFILE);
			WriteBody.Builder rwb = WriteBody.newBuilder();
			String ext[] =  file.getName().toString().split("\\.");
			rwb.setFileExt(ext[1]);
			rwb.setFilename(file.getName());
			rwb.setNumOfChunks(noOfChunks);
			int i = 1;
			//rwb.setFileId(++fileId);
				Chunk.Builder chunkBuilder = Chunk.newBuilder();
				chunkBuilder.setChunkId(++chunkId);
				chunkBuilder.setChunkSize(chunk.size());
				chunkBuilder.setChunkData(chunk);
				rwb.setChunk(chunkBuilder.build());

			req.setRwb(rwb.build());
			Header.Builder header = Header.newBuilder();
			header.setNodeId(1);
			header.setTime(System.currentTimeMillis());
			command.setReq(req);
			command.setHeader(header);
			System.out.println(chunkId);
			return command.build();
		} catch (Exception e) {
			System.out.println(" Sending write request failed :");
			e.printStackTrace();
			return command.build();
		}
	}

//	public void writeFile(String filename, ByteString chunkData, int noOfChunks, int chunkId) {
//
//		logger.info("Printing byte size" + chunkData.size());
//		Header.Builder hb = Header.newBuilder();
//		hb.setNodeId(999);
//		hb.setTime(System.currentTimeMillis());
//		hb.setDestination(-1);
//
//		Chunk.Builder chb = Chunk.newBuilder();
//		chb.setChunkId(chunkId);
//		chb.setChunkData(chunkData);
//		chb.setChunkSize(chunkData.size());
//
//		WriteRequest.Builder wb = WriteRequest.newBuilder();
//		wb.setFileId("1");
//		wb.setFilename(filename);
//		wb.setChunk(chb);
//		wb.setNumOfChunks(noOfChunks);
//		CommandMessage.Builder cb = CommandMessage.newBuilder();
//		// Prepare the CommandMessage structure
//		cb.setHeader(hb);
//		cb.setMessageType(MessageType.REQUESTWRITEFILE);
//		cb.setRequestWrite(wb);
//
//		// Initiate connection to the server and prepare to save file
//		try {
//			CommConnection.getInstance().enqueue(cb.build());
//		} catch (Exception e) {
//			e.printStackTrace();
//			logger.error("Problem connecting to the system");
//		}
//
//	}

	/*
	 * public void release() { CommConnection.getInstance().release(); }
	 */

	/**
	 * Since the service/server is asychronous we need a unique ID to associate
	 * our requests with the server's reply
	 * 
	 * @return
	 */
}