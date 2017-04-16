package gash.router.server;

import gash.router.server.db.SqlClient;
import routing.Pipe;
import com.google.protobuf.ByteString;

/**
 * Created by rentala on 4/16/17.
 */
public class IOUtility {
    static SqlClient sqlClient =  new SqlClient();
    public static int writeFile(Pipe.WriteBody readBody){
        Pipe.Chunk chunk = readBody.getChunk();
        ByteString bs = chunk.getChunkData();
        return sqlClient.storefile(chunk.getChunkId(), bs.newInput(), readBody.getFilename());
    }
    public static byte[] readFile(Pipe.ReadBody readBody){
        return sqlClient.getFile((int)readBody.getFileId());
    }

}
