package gash.router.server.db;

/**
 * Created by rentala on 4/23/17.
 */
public class ChunkRow {
    private int id;
    private int file_id;
    private int chunk_id;
    private String location_at;
    private int chunk_size;
    public ChunkRow(int id, int file_id, int chunk_id, String location_at, int chunk_size){
        setId(id);
        setFile_id(file_id);
        setChunk_id(chunk_id);
        setLocation_at(location_at);
        setChunk_size(chunk_size);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getFile_id() {
        return file_id;
    }

    public void setFile_id(int file_id) {
        this.file_id = file_id;
    }

    public int getChunk_id() {
        return chunk_id;
    }

    public String getLocation_at() {
        return location_at;
    }

    public void setLocation_at(String location_at) {
        this.location_at = location_at;
    }

    public void setChunk_id(int chunk_id) {
        this.chunk_id = chunk_id;
    }

    public int getChunk_size() {
        return chunk_size;
    }

    public void setChunk_size(int chunk_size) {
        this.chunk_size = chunk_size;
    }
}
