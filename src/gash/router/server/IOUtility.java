package gash.router.server;

import gash.router.server.db.ChunkRow;
import gash.router.server.db.SqlClient;
import routing.Pipe;
import routing.Pipe.WriteBody;

import com.google.protobuf.ByteString;

/**
 * Created by rentala on 4/16/17.
 */
public class IOUtility {

    static SqlClient sqlClient = new SqlClient();

	public static long getFileId(String filename, String fileExt, int totalChunks) {
		return sqlClient.createIfNotExistFileId(filename, fileExt, totalChunks);
	}
	public static boolean insertLogEntry(int log_id, int fileId, String filename, String fileExt, int chunk_id, 
			String locatedAt, int totalChunks){
		return sqlClient.insertLog(log_id, fileId, filename,  fileExt, chunk_id, locatedAt, totalChunks);
	}

	public int getFileId(String fileName) {
		return sqlClient.getFileId(fileName);
	}

	public Integer[][] getChunks(String fileName) {
		// TODO Auto-generated method stub
		return sqlClient.getChunks(fileName);
		
	}
	public ChunkRow[] getChunkRows(String fileName) {
		// TODO Auto-generated method stub
		return sqlClient.getChunkRows(fileName);

	}

	public ChunkRow getChunkRowById(int id) {
		// TODO Auto-generated method stub
		return sqlClient.getChunkRowById(id);

	}

}
