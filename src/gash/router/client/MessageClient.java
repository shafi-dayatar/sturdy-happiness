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
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import pipe.common.Common.Chunk;
import pipe.common.Common.ChunkLocation;
import pipe.common.Common.Header;
import pipe.common.Common.ReadBody;
import pipe.common.Common.ReadResponse;
//import routing.Pipe.CommandMessage.MessageType;
import pipe.common.Common.Request;
import pipe.common.Common.TaskType;
import pipe.common.Common.WriteBody;
//import routing.Pipe.WriteRequest;
import routing.Pipe.CommandMessage;

/**
 * front-end (proxy) to our service - functional-based
 * 
 * @author gash
 * 
 */
public class MessageClient {
	// track requests
	int sizeOfChunk = 1024 * 1024;
	private int messageId = 1;
	private static int fileId = 0;
	private static int chunkId = 0;
	private int clientId=44;
	protected static Logger logger = LoggerFactory.getLogger("Client");

	public MessageClient(int clusterId, String host, int port) {
		init( clusterId, host, port);

	}
	public MessageClient(){};
	private void init(int clusterId, String host, int port) {
		CommConnection.initConnection(clusterId, host, port);
	}

	public void addListener(CommListener listener) {
		CommConnection.getInstance().addListener(listener);
	}

	private String fileoutput = "output";

	public void ping(int destinationId) {
		// construct the message to send
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(clientId);
		hb.setTime(System.currentTimeMillis());
		hb.setDestination(destinationId);

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
			File file = new File(filePath + "/" + file_name);
			this.chunkId = 0;
			if (file_name == null) {
				return;
			}
			//createFileChunks(file);
			ArrayList<ByteString> chunks = chunkFile(file);
			if (chunks == null) {
				return;
			}
			int chunksNum = chunks.size();
			try {
				for (int i = 0; i < chunks.size(); i++) {
					CommandMessage commandMessage = buildWCommandMessage(file, chunks.get(i), chunksNum);
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

		byte[] buffer = new byte[sizeOfChunk];
		int noOfChunks = (int) Math.ceil(len / (double) sizeOfChunk);

		try {
			BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
			while (len > 0) {	
				if (len < sizeOfChunk) {
					byte[] leftData = new byte[(int) len];
					bis.read(leftData);
					chunkedFile.add(ByteString.copyFrom(leftData));
					len = 0;
				} else {
					bis.read(buffer);
					len = len - sizeOfChunk;
					logger.info("chunk" + len);
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

			// Pipe.Node.Builder node = Pipe.Node.newBuilder();
			//
			// node.setHost(InetAddress.getLocalHost().getHostAddress());
			//
			// node.setPort(8000);
			// node.setNodeId(-1);
			// msg.setClient(node);
			Request.Builder req = Request.newBuilder();
			req.setRequestType(TaskType.REQUESTREADFILE);
			ReadBody.Builder rrb = ReadBody.newBuilder();
			rrb.setFilename(file_name);

			req.setRrb(rrb.build());
			Header.Builder header = Header.newBuilder();
			header.setNodeId(1);
			header.setTime(System.currentTimeMillis());
			command.setRequest(req);
			command.setHeader(header);
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
			String ext[] = file.getName().toString().split("\\.");
			rwb.setFileExt(ext[1]);
			rwb.setFilename(ext[0]);
			rwb.setNumOfChunks(noOfChunks);
			System.out.println(chunkId);
			Chunk.Builder chunkBuilder = Chunk.newBuilder();
			chunkBuilder.setChunkId(chunkId++);
			chunkBuilder.setChunkSize(chunk.size());
			chunkBuilder.setChunkData(chunk);
			rwb.setChunk(chunkBuilder.build());

			req.setRwb(rwb.build());
			Header.Builder header = Header.newBuilder();
			header.setNodeId(1);
			header.setTime(System.currentTimeMillis());
			command.setRequest(req);
			command.setHeader(header);
			return command.build();
		} catch (Exception e) {
			System.out.println(" Sending write request failed :");
			e.printStackTrace();
			return command.build();
		}
	}
	public void sendfileReadRequests(CommandMessage msg) {
		// TODO Auto-generated method stub
		ReadResponse readRes = msg.getResponse().getReadResponse();
		List<ChunkLocation> list = new ArrayList<>(readRes.getChunkLocationList());
		try {
			int listSize = list.size();
			for(int j =0;j<listSize;j++){
				int node_id = list.get(j).getNode(0).getNodeId();
				String host = list.get(j).getNode(0).getHost();
				int port = list.get(j).getNode(0).getPort();
				String file_name = readRes.getFilename();
				int chunkId = list.get(j).getChunkId();
				CommandMessage commandMessage = buildRCommandMessage(file_name, node_id, host, port, chunkId);
				CommConnection.getInstance().enqueue(commandMessage);
				System.out.println("Sent Read Request .....");
				System.out.println(chunkId+" chunk"+ "   j "+j+ "    list  "+list.get(j).toString());
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Couldnt sent to the system");
			return;
		}
	}
		
		public CommandMessage buildRCommandMessage(String file_name,int node_id, String host, int port, int chunkId){
		CommandMessage.Builder command = CommandMessage.newBuilder();
		try{
		Request.Builder req = Request.newBuilder();
		req.setRequestType(TaskType.REQUESTREADFILE);
		ReadBody.Builder rrb = ReadBody.newBuilder();
		rrb.setFilename(file_name);
		rrb.setChunkId(chunkId);

		req.setRrb(rrb.build());
		Header.Builder header = Header.newBuilder();
		header.setNodeId(clientId);
		header.setDestination(node_id);
		header.setTime(System.currentTimeMillis());
		command.setRequest(req);
		command.setHeader(header);
		return command.build();
	} 
		catch (Exception e) {
			System.out.println(" Sending individual read requests failed :");
			e.printStackTrace();
			return command.build();
		}
		
	}
		public void makeFileList(String action, File folder) {
			// TODO Auto-generated method stub
			for (final File fileEntry : folder.listFiles()) {
		        if (fileEntry.isDirectory()) {
		            makeFileList(action,fileEntry);
		        } else {
		            fileOperation(action,folder.toString(),fileEntry.getName());
		        }
		    }
		}

	
	
	// public void writeFile(String filename, ByteString chunkData, int
	// noOfChunks, int chunkId) {
	//
	// logger.info("Printing byte size" + chunkData.size());
	// Header.Builder hb = Header.newBuilder();
	// hb.setNodeId(999);
	// hb.setTime(System.currentTimeMillis());
	// hb.setDestination(-1);
	//
	// Chunk.Builder chb = Chunk.newBuilder();
	// chb.setChunkId(chunkId);
	// chb.setChunkData(chunkData);
	// chb.setChunkSize(chunkData.size());
	//
	// WriteRequest.Builder wb = WriteRequest.newBuilder();
	// wb.setFileId("1");
	// wb.setFilename(filename);
	// wb.setChunk(chb);
	// wb.setNumOfChunks(noOfChunks);
	// CommandMessage.Builder cb = CommandMessage.newBuilder();
	// // Prepare the CommandMessage structure
	// cb.setHeader(hb);
	// cb.setMessageType(MessageType.REQUESTWRITEFILE);
	// cb.setRequestWrite(wb);
	//
	// // Initiate connection to the server and prepare to save file
	// try {
	// CommConnection.getInstance().enqueue(cb.build());
	// } catch (Exception e) {
	// e.printStackTrace();
	// logger.error("Problem connecting to the system");
	// }
	//
	// }

	
	/*
	 * public void release() { CommConnection.getInstance().release(); }
	 */

}