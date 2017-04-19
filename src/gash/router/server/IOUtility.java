package gash.router.server;

import gash.router.server.db.SqlClient;
import routing.Pipe;
import routing.Pipe.ReadRequest;
import routing.Pipe.WriteRequest;

import com.google.protobuf.ByteString;

/**
 * Created by rentala on 4/16/17.
 */
public class IOUtility {

    static SqlClient sqlClient = new SqlClient();

    public static int writeFile(WriteRequest read){
        Pipe.Chunk chunk = read.getChunk();
        ByteString bs = chunk.getChunkData();
        return sqlClient.storefile(chunk.getChunkId(), bs.newInput(), read.getFilename());
    }

    public byte[] readFile(ReadRequest read){
        return sqlClient.getFile(read.getFilename());
        //return sqlClient.getFile((int)readBody.getFileId());
    }

	public static long getFileId(String filename, String fileExt) {
		// TODO Auto-generated method stub
		return sqlClient.createIfNotExistFileId(filename, fileExt);
	}
	public static boolean insertLogEntry(int log_id, int fileId, String filename, String fileExt, int chunk_id, 
			String locatedAt){
		return sqlClient.insertLog(log_id, fileId, filename,  fileExt, chunk_id, locatedAt);
	}

}
