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

    public static int writeFile(WriteBody read){
        Pipe.Chunk chunk = read.getChunk();
        ByteString bs = chunk.getChunkData();
        return sqlClient.storefile(chunk.getChunkId(), bs.newInput(), read.getFilename());
    }

	public static long getFileId(String filename, String fileExt) {
		return sqlClient.createIfNotExistFileId(filename, fileExt);
	}
	public static boolean insertLogEntry(int log_id, int fileId, String filename, String fileExt, int chunk_id, 
			String locatedAt){
		return sqlClient.insertLog(log_id, fileId, filename,  fileExt, chunk_id, locatedAt);
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

}
