package gash.router.server.log;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Hashtable;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.ServerState;
import pipe.work.Work.Command;
import pipe.work.Work.LogEntry;



public class LogInfo implements LogOperations {


	protected static Logger logger = LoggerFactory.getLogger("logging");
	
	public Hashtable<Integer, LogEntry> log;
	private Integer commitIndex; // change it to atomic integer
	private Integer lastApplied;// change it to atomic integer 

	private int thresholdSize = 1;
	private String logStoreDir = "./resources/files";
	private ServerState state;
	
	public LogInfo(ServerState state) {
		log = new Hashtable<Integer, LogEntry>();
		commitIndex = (int) 0;
		lastApplied = (int) 0;
		this.state = state;
		restoreLogSegment();
	}
	
	/**
	 * Increment the commit index by given integer.
	 * @param i
	 * @return long - updated commitIndex
	 */
	public int incCommitIndex(int i) {
		commitIndex += i;
		return commitIndex;
	}
	
	/**
	 * Returns current value of commitIndex
	 * @return long commitIndex
	 */
	public int getCommitIndex() {
		return commitIndex;
	}
	
	/**
	 * Set commitIndex to passed value
	 * @param commitIndex
	 */
	public synchronized void setCommitIndex(Integer commitIndex) {
		logger.info("Committing log at :" + commitIndex);
		LogEntry  la = log.get(commitIndex);

		String filename = null, fileExt = null, locatedAt = null;
		int fileId =-1, chunkId = -1, totalChunks = 0 ;
		System.out.println("Should insert this log in mysql database for future reads : " + la.toString());
		List<Command> commands =  la.getDataList();
		for(Command cmd :commands){
			cmd.getClientId();
			String [] logEntry = cmd.getValue().split(":");
			fileId = Integer.parseInt(logEntry[0]);
			filename = logEntry[1];
			fileExt = logEntry[2];
			chunkId = Integer.parseInt(logEntry[3]);
			locatedAt = logEntry[4];
			totalChunks = Integer.parseInt(logEntry[5]);
		}
		
		state.getDb().insertLogEntry(la.getLogId(), fileId, filename, fileExt, chunkId, locatedAt, totalChunks);
		this.commitIndex = commitIndex;
		if (commitIndex % thresholdSize == 0){
			storeLogSegment();
		}
	}
	
	public void storeLogSegment() {

		long lastIndex = lastIndex();
		long commitIndex = getCommitIndex();
		String fileName = "Raft.log";

		try {
			File file = new File(logStoreDir,fileName);
			if(!file.exists()) {
				file.createNewFile(); 
			}
			ObjectOutputStream o = new ObjectOutputStream(new FileOutputStream(file));
			o.writeLong(lastIndex);
			o.writeLong(commitIndex);
			o.writeObject(log);
			o.close();
		} catch(IOException e) {
			logger.error(e.getMessage());
		}
	}
	
	@SuppressWarnings("unchecked")
	public void restoreLogSegment() {

		String fileName = "Raft.log";

		try {
			log.remove(lastIndex());

			File file = new File(logStoreDir,fileName);
			if(file.exists()){

				ObjectInputStream o = new ObjectInputStream(new FileInputStream(file));
				this.lastApplied = (int) o.readLong();
				this.commitIndex = (int) o.readLong();
				try {
					this.log = (Hashtable<Integer, LogEntry>) o.readObject();
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				o.close();
			}
		} catch(IOException e) {
			logger.error(e.getMessage());
		}
	}

	public void setLastApplied(Integer lastApplied) {
		this.lastApplied = lastApplied;
	}

	
	/**
	 * returns size of the in-memory log
	 * @return long size
	 */
	@Override
	public long size() {
		return log.size();
	}

	/**
	 * Append a single entry at the end of the log. Checks if the log size
	 * reached threshold after appending. If yes, store the log in new file.
	 * 
	 * @param LogEntry entry
	 * @return long index
	 */
	@Override
	public synchronized void  appendEntry(int logIndex, LogEntry entry) {
		log.put(logIndex, entry);
		lastApplied = logIndex;
	}
	
	/**
	 * Append a list of entries at the end of the log. Checks if the log size
	 * reached threshold after appending each entry. If yes, store the log in 
	 * a new file.
	 * 
	 * @param LogEntry[] entries
	 * @return Long index - last index
	 */


	@Override
	public int firstIndex() {
		return log.get(0).getLogId();
	}

	@Override
	public int lastIndex() {
		return lastApplied;
	}
	
	@Override
	public int lastLogTerm(int index) {
		int lastLog = lastIndex();
		if (log.size()>0)
			return log.get(index).getTerm();
		return 0;
	}

	/**
	 * Retrieves a single log entry present at provided index. If index is less
	 * than start index of in-memory log, the entry would be retrieved from the
	 * file
	 * 
	 * @param Long index
	 * @return LogEntry
	 */
	@Override
	public LogEntry getEntry(int index) {
		if(index < 0)
			return null;
		else
			return log.get(index);
	}
	
	/**
	 * Retrieves all the entries starting from given start index until the end of 
	 * the log. If startIndex < firstIndex of the in-memory log, file search is done
	 * and the required entries are retrieved.
	 * 
	 * @param startIndex
	 * @return LogEntry[] - array of log entries in order from startIndex to lastIndex
	 */
	public LogEntry[] getEntries(int startIndex, int lastIndex) {
		if(startIndex < 0)
			return null;
		
		int size = (int) (lastIndex - startIndex)+1;
		LogEntry[] entries = new LogEntry[size];
		int i = 0;
		
		while(i < size) {
			entries[i++] = log.get(startIndex++);
		}
		
		return entries;
	}
	
	/**
	 * Remove all the entries from log starting with startIndex to
	 * lastIndex
	 * @param startIndex
	 */
	public void removeEntry(int startIndex) {
		if(startIndex < 0)
			return;
		
		if(startIndex < firstIndex()) {
			log.clear();
		}
		for(long i = startIndex; i <= lastIndex(); i++)
			log.remove(i);		
	}
	
}
	
	
	/**
	 * Retrieves log segment from single/multiple files starting from startIndex to 
	 * endIndex
	 * 
	 * @param startIndex
	 * @param endIndex
	 * @return TreeMap of requested log
	 * 
	 */
//	public TreeMap<Long, LogEntry> retrieveLogSegment(long startIndex, long endIndex) {
//		TreeMap<Long, LogEntry> bufferMap = new TreeMap<Long, LogEntry>();
//		TreeMap<Long, LogEntry> tmpMap = new TreeMap<Long, LogEntry>();
//		
//		File dir = new File(logStoreDir);
//		File[] logFiles = dir.listFiles();
//		
//		try{
//			for(File file: logFiles) {
//				int fileLastIndex = Integer.parseInt(file.getName().split("_")[1]);
//				int fileStartIndex = fileLastIndex - thresholdSize + 1;
//				
//				if(fileLastIndex >= startIndex) {
//					ObjectInputStream oi = new ObjectInputStream(new FileInputStream(file));
//					if(fileStartIndex < startIndex) {
//						tmpMap = (TreeMap<Long, LogEntry>)oi.readObject();
//						
//						while(startIndex <= fileLastIndex) {
//							bufferMap.put(startIndex, tmpMap.get(startIndex));
//							startIndex++;
//						}
//						
//						tmpMap.clear();
//					} else {
//						bufferMap.putAll((TreeMap<Long, LogEntry>)oi.readObject());
//						
//					}
//					oi.close();
//				} else {
//					continue;
//				}
//			}
//			
//		} catch(IOException e) {
//			//logger.error(e.getMessage());
//			System.out.println(e.getMessage());
//		} catch (ClassNotFoundException e) {
//			//logger.error(e.getMessage());
//			System.out.println(e.getMessage());
//		}
//		
//		return bufferMap;
//	}
//	
//	public TreeMap<Long, LogEntry> removeLogSegment(long startIndex) {
//		File dir = new File(logStoreDir);
//		File[] logFiles = dir.listFiles();
//		TreeMap<Long, LogEntry> tmpMap = new TreeMap<Long, LogEntry>();
//		
//		try{
//			for(File file: logFiles) {
//				int fileLastIndex = Integer.parseInt(file.getName().split("_")[1]);
//				int fileStartIndex = fileLastIndex - thresholdSize + 1;
//				
//				if(fileLastIndex >= startIndex) {
//					ObjectInputStream oi = new ObjectInputStream(new FileInputStream(file));
//					if(fileStartIndex <= startIndex) {
//						tmpMap = (TreeMap<Long, LogEntry>)oi.readObject();
//						file.delete();
//					} else {
//						file.delete();
//					}
//					oi.close();
//				} else {
//					continue;
//				}
//			}
//			
//		} catch(IOException e) {
//			//logger.error(e.getMessage());
//			System.out.println(e.getMessage());
//		} catch (ClassNotFoundException e) {
//			//logger.error(e.getMessage());
//			System.out.println(e.getMessage());
//		}
//		return tmpMap;
//	}
//	
//
//	

	
	
//	// storing logs in file when size exceeds the limit
//	
//		/**
//		 * Checks if the map size has reached the threshold value.
//		 * @return boolean 
//		 */
//		public boolean isSegmentLimitReached() {
//			if(size() != 1 && (size()-1) % thresholdSize == 0) 
//				return true;
//			return false;
//		}
//		
//		/**
//		 * Stores the log present in memory to a file.
//		 * File name convention - RaftLog_[lastIndex of the log to be stored in the file]
//		 * 
//		 */
//		public void storeLogSegment() {
//			LogEntry lastEntry = log.get(lastIndex());
//			
//			long lastIndex = lastIndex() - 1;
//			String fileName = "RaftLog_" + lastIndex;
//			
//			try {
//				log.remove(lastIndex());
//				
//				File file = new File(logStoreDir,fileName);
//				
//				if(!file.exists()) {
//					file.createNewFile(); 
//				}
//				
//				ObjectOutputStream o = new ObjectOutputStream(new FileOutputStream(file));
//				o.writeObject(log);
//				o.close();
//				
//				log.clear();
//				log.put(lastIndex+1, lastEntry);
//				
//			} catch(IOException e) {
//				log.put(lastIndex+1, lastEntry);
//				//logger.error(e.getMessage());
//				System.out.println(e.getMessage());
//			}
//		}


