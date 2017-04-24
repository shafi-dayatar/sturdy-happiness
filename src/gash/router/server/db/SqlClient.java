package gash.router.server.db;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import routing.Pipe;

import java.io.*;
import java.net.URL;
import java.sql.*;

public class SqlClient{
	
	protected Logger logger = LoggerFactory.getLogger("SQL Client");
    final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    final String  db_url = "jdbc:mysql://localhost/cmpe275";
    File conf;
    //  Database credentials
    String USER = "root";
    String PASSWORD = "password";

    Connection connection = null;
    Statement stmt = null;
    PreparedStatement insertStatement = null;
    PreparedStatement readByFname = null;
    PreparedStatement readByFId = null;
    PreparedStatement deletStatement = null;
    PreparedStatement getFileId = null;
    PreparedStatement fileNameInsert = null;
    
    
    public SqlClient(){
        System.out.println("Establishing database connection :");
        long startTime = System.currentTimeMillis();
        checkDependency();
        //loadConfig();
        establishConnection();
        prepareStatements();
        System.out.println("Time Taken to make a db connection: " + (System.currentTimeMillis() - startTime));
    }
    
    private void loadConfig(){
        try {
            URL path = SqlClient.class.getResource("db.conf");
            File conf = new File(path.getFile());
            InputStream IS = null;
            IS = new FileInputStream(conf);
            USER = IS.toString().split(":")[0];
            PASSWORD = IS.toString().split(":")[1];
        } catch (FileNotFoundException e) {
            System.out.println("Failed to load config ? or IS error");
            e.printStackTrace();
        }
    }
    

    /*public SqlClient(String hostname){
        System.out.println(" HOST NAME " + hostname);
    	//db_url = "jdbc:mysql://" + hostname + "/cmpe275";
        checkDependency();
        establishConnection();
        prepareStatements();
    }*/

    private void checkDependency(){
        try
        {
            Class.forName("com.mysql.jdbc.Driver");
            Class.forName("org.apache.commons.io.IOUtils");
        }
        catch (ClassNotFoundException e) {
            System.out.println("Missing Dependency !!!!!!!!!!!!!!!!!");
            e.printStackTrace();
        }
    }
    private void establishConnection(){
        try
        {
            connection = DriverManager.getConnection(db_url,USER,PASSWORD);
        }
        catch (SQLException e)
        {
            System.out.println(" Connection Failed! Check output console");
            e.printStackTrace();
        }
        if (connection != null) {
            System.out.println("You made it, take control your database now!");
        } else {
            System.out.println("Failed to make connection!");
        }
    }
    private void prepareStatements(){
        try{
            insertStatement = connection.prepareStatement("INSERT INTO FILES(CHUNK_ID, FILENAME, CONTENT) " +
                    "VALUES(?,?,?)");
            deletStatement = connection.prepareStatement("DELETE from FILES where ID = ? ");
            readByFname = connection.prepareStatement("SELECT CONTENT from FILES where FILENAME = ? LIMIT 1 ");
            readByFId =  connection.prepareStatement("SELECT CONTENT from FILES where ID = ? ");
            getFileId = connection.prepareStatement("SELECT id from files where name =? and file_ext= ?");
            fileNameInsert = connection.prepareStatement("INSERT INTO FILES (name, file_ext, total_chunks) values (?,?,?)");
    		
            
        }
        catch (Exception e){
            System.out.println(" PrepareStatements failed !");
            e.printStackTrace();
        }

    }
    
    public int createIfNotExistFileId(String fileName, String fileExt, int totalChunks){
    	long startTime = System.currentTimeMillis();
    	int file_id = -1; 
    	try{
    	getFileId.setString(1, fileName);
    	getFileId.setString(2, fileExt);
    	ResultSet rs = getFileId.executeQuery();
    	
    	if(rs.next()) {
    		file_id = rs.getInt(1);
    	}else{
    		fileNameInsert.setString(1, fileName);
    		fileNameInsert.setString(2, fileExt);
    		fileNameInsert.setInt(3, totalChunks);
    		int statement = fileNameInsert.executeUpdate();
    		rs = fileNameInsert.getGeneratedKeys();
    		if (rs.next()){
    			file_id =  rs.getInt(1);
    		}
    	}
    	}catch(Exception e){
    		System.out.println("FileName insertion failed");
    		e.printStackTrace();
    	}
    	long timeTaken =  System.currentTimeMillis() - startTime;
    	System.out.println("Take taken to execute query is :" + timeTaken);
    	
    	
    	return file_id;
    }

	public boolean insertLog(int logId, int fileId, String filename, String fileExt, int chunk_id, 
			String locatedAt, int total_chunks) {
		// TODO Auto-generated method stub
		boolean status = false;
		try{
			//todo: should create file name with same file_id from log, otherwise there will be inconsistency;
			int file_id = createIfNotExistFileId(fileId, filename, fileExt, total_chunks);
			PreparedStatement chunk_loc = connection.prepareStatement("insert into chunks (id, file_id, chunk_id, location_at)"
				+ " values(?,?,?,?)");
			chunk_loc.setInt(1, logId);
			chunk_loc.setInt(2, file_id);
			chunk_loc.setInt(3, chunk_id);
			chunk_loc.setString(4, locatedAt);
			int statement = chunk_loc.executeUpdate();
			ResultSet rs = chunk_loc.getGeneratedKeys();
			if(rs.next()) {
				System.out.println("Log Append Successfully committed in database");
				status = true;
			}
		}catch(Exception e){
			System.out.println("File get failed");
            e.printStackTrace();
		}
		
		return status;
	}

	private int createIfNotExistFileId(int fileId, String filename, String fileExt, int totalChunks) {
		long startTime = System.currentTimeMillis();
    	int file_id = -1; 
    	try{
    	getFileId.setString(1, filename);
    	getFileId.setString(2, fileExt);
    	ResultSet rs = getFileId.executeQuery();
    	
    	if(rs.next()) {
    		file_id = rs.getInt(1);
    	}else{
    		 PreparedStatement fileNameInsert = connection.prepareStatement("INSERT INTO FILES (id ,name, file_ext, total_chunks) values (?,?,?,?)");
    		 fileNameInsert.setInt(1, fileId);
    		fileNameInsert.setString(2, filename);
    		fileNameInsert.setString(3, fileExt);
    		fileNameInsert.setInt(4, totalChunks);
    		int statement = fileNameInsert.executeUpdate();
    		rs = fileNameInsert.getGeneratedKeys();
    		if (rs.next()){
    			file_id =  rs.getInt(1);
    		}
    	}
    	}catch(Exception e){
    		System.out.println("FileName insertion failed");
    		e.printStackTrace();
    	}
    	long timeTaken =  System.currentTimeMillis() - startTime;
    	System.out.println("Take taken to execute query is :" + timeTaken);
    	
    	
    	return file_id;
	}

	public int getFileId(String fileName) {
		int file_id = -1; 
    	try{
    		String[]str  = fileName.split("\\.");
    		getFileId.setString(1, str[0]);
    		getFileId.setString(2, str[1]);
    		ResultSet rs = getFileId.executeQuery();

    		if(rs.next()) {
    			file_id = rs.getInt(1);
    		}
    	}catch(Exception e){
    		logger.info("Error while finding filename in db");
    		e.printStackTrace();
    	}
		return file_id;
	}
	
	public ChunkRow getChunkRowById(int chunk_id){
        ChunkRow data = null;
        try {
            PreparedStatement chunksQuery = connection.prepareStatement("select file_id, chunk_id, chunk_size, location_at, id "
                    + "from chunks where chunk_id = ?");
            chunksQuery.setInt(1, chunk_id);
            ResultSet rs = chunksQuery.executeQuery();

            if(rs.next()){
            	data = new ChunkRow(rs.getInt(5), rs.getInt(1), rs.getInt(2), rs.getString(4), rs.getInt(3));
                /*data.setFile_id(rs.getInt(1));
                data.setChunk_id(rs.getInt(2));
                data.setChunk_size(rs.getInt(3));
                data.setLocation_at(rs.getString(4));*/
            }
            return data;
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return data;
    }
    public ChunkRow[] getChunkRows(String fileName) {
        // TODO Auto-generated method stub
        ChunkRow[] data = null;
        try {
            PreparedStatement fileQuery = connection.prepareStatement("select id, name, total_chunks "
                    + "from files where name = ? and file_ext = ?");
            String [] str  = fileName.split(".");
            fileQuery.setString(1, str[0]);
            fileQuery.setString(2, str[1]);
            ResultSet rs = fileQuery.executeQuery();
            if (rs.next()){
                int total_chunks = rs.getInt(2);
                data = new ChunkRow[total_chunks];
                PreparedStatement chunksQuery = connection.prepareStatement("select file_id, chunk_id, chunk_size, location_at "
                        + "from chunks where file_id = ?");
                chunksQuery.setInt(1, rs.getInt(1));
                rs = chunksQuery.executeQuery();

                int i = 1;
                while(rs.next()){
                    data[i].setFile_id(rs.getInt(1));
                    data[i].setChunk_id(rs.getInt(2));
                    data[i].setChunk_size(rs.getInt(3));
                    data[i].setLocation_at(rs.getString(4));
                    i++;
                }
                return data;
            }
            return data;
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return data;
    }
    
    
	public Integer[][] getChunks(String fileName) {
		// TODO Auto-generated method stub
		System.out.println("Filename" + fileName);
		Integer[][] data = null;
		try {
			PreparedStatement fileQuery = connection.prepareStatement("select id, name, total_chunks "
					+ "from files where name = ? and file_ext = ?");
			String [] str  = fileName.split("\\.");
			fileQuery.setString(1, str[0]);
			fileQuery.setString(2, str[1]);
			ResultSet rs = fileQuery.executeQuery();
			
			if (rs.next()){
				System.out.println("1st" + rs.getInt(1) + "2nd" + rs.getString(2) + "3rd" + rs.getInt(3));
				int total_chunks = rs.getInt(3);
				System.out.println("total chunks"+total_chunks);
				data = new Integer [total_chunks][4];
				PreparedStatement chunksQuery = connection.prepareStatement("select file_id, chunk_id, chunk_size, location_at "
					+ "from chunks where file_id = ?");		
				chunksQuery.setInt(1, rs.getInt(1));
				rs = chunksQuery.executeQuery();
				 
				int i = 0;
				while(rs.next()){
					data[i][0] = rs.getInt(1);
					data[i][1] = rs.getInt(2);
					data[i][2] = rs.getInt(3);
                    //data[i][3] = rs.getInt(4);
					i++;
				}
				if (i != total_chunks){
					data = new Integer [1][1];
					data[0][0] = -2;
				}
				return data;
			}
			data = new Integer [1][1];
			data[0][0] = -1;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return data;
	}
	
	public Integer [] chunkLocation(String filename, int chunkId , int fileId ){
		Integer[] data = null;
		try {
			if (fileId == -1){
				PreparedStatement fileQuery = connection.prepareStatement("select name, total_chunks "
						+ "from files where name = ? and file_ext = ?");
				String [] str = filename.split(".");
				fileQuery.setString(1, str[0]);
				fileQuery.setString(2, str[1]);
				ResultSet rs = fileQuery.executeQuery();
				if (rs.next()){
					fileId = rs.getInt(1);
				}
			}
			PreparedStatement chunksQuery = connection.prepareStatement("select location_at "
					+ "from chunks where file_id = ?");		
			chunksQuery.setInt(1, fileId);
			chunksQuery.setInt(2, chunkId);
			ResultSet rs = chunksQuery.executeQuery();
			if (rs.next()){
				String arr = rs.getString(1);
				String[] items = arr.replaceAll("\\[", "").replaceAll("\\]", "").replaceAll("\\s", "").split(",");

				data = new Integer[items.length];

				for (int i = 0; i < items.length; i++) {
				    try {
				        data[i] = Integer.parseInt(items[i]);
				    } catch (NumberFormatException nfe) {
				        //NOTE: write something here if you need to recover from formatting errors
				    };
				}
			}
				
		}catch(Exception e){
			
		}
		return data;
	}

}


