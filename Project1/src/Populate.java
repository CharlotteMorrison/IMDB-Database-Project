import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Populate {

	//database credentials
	String url = "jdbc:mysql://localhost:3306/IMDB?rewriteBatchedStatements=true";
	String user = "root";
	String password = "Isabella1";
	Connection connection = null;
	Statement statement = null;		
	
	public void populate() throws SQLException {
		
		connection = DriverManager.getConnection(url, user, password);
		statement = connection.createStatement();		
		
		String nameFile = "src/ImportFiles/nameBasics.tsv";
		populateNames(nameFile, connection, statement);

        
		String movieFile = "src/ImportFiles/titleBasics.tsv";
		populateMovies(movieFile, connection, statement);
		
		String principalsFile = "src/ImportFiles/titlePrincipals.tsv";
		populatePrincipals(principalsFile, connection, statement);		
	}
	
	public void populateNames(String file, Connection connection, Statement statement) throws SQLException {
	    String fileName = file; 
        String line = null;
    	int count = 0;
    	
        try {
            FileReader fileReader =  new FileReader(fileName);
            BufferedReader bufferedReader =  new BufferedReader(fileReader);

            bufferedReader.readLine(); //skips the title line for me
            
        	String sql = "insert into Names (Identifier, Primary_Name, Birth_Year, Death_Year) values (?,?,?,?)";            
        	PreparedStatement ps = connection.prepareStatement(sql);    
        	//int counter = 0;
        	
            while((line = bufferedReader.readLine()) != null ) {            	
            	//counter++;
            	String[] actor = line.split("\\t");

            	String identifier = actor[0];
            	String actorName = actor[1];
            	actorName = actorName.replace("'","\\'");
            	String birthYear = actor[2];
            	String deathYear = actor[3];            	

            	if (birthYear.equals("\\N")) {
            		birthYear = null;
            	}
            	if (deathYear.equals("\\N")) {
            		deathYear = null;
            	}

            	final int batchSize = 1000;

            	ps.setString(1, identifier);
            	ps.setString(2, actorName);
            	ps.setString(3, birthYear);
            	ps.setString(4, deathYear);
            	ps.addBatch();
            		
            	if(++count % batchSize == 0) {
            			ps.executeBatch();
            			System.out.println("Name Batch Loaded..." + count);
            	}
            
            } 
        	ps.executeBatch(); // insert remaining records
        	ps.close();
            bufferedReader.close();     
            System.out.println("Actor/Actress/Director Tables Loaded");
        }
        catch(FileNotFoundException ex) {
            System.out.println("Unable to open file '" +fileName + "'");                
        }
        catch(IOException ex) {
            System.out.println("Error reading file '" + fileName + "'");                  
        }       
        System.out.println("Names table populated.");
    }
	
	public void populateMovies(String file, Connection connection, Statement statement) throws SQLException {
		
        String fileName = file; 
        String line = null;		

        
        try {
        	FileReader fileReader =  new FileReader(fileName);
            BufferedReader bufferedReader =  new BufferedReader(fileReader);

            bufferedReader.readLine(); //skips the title line for me

        	String insertMovie = "insert into Movies (Movie_ID, Identifier, Title_Type, Primary_Title, Original_Title," 
        			+ "Is_Adult, Start_Year, End_Year, Run_time) values (?,?,?,?,?,?,?,?, ?) ";	
        	PreparedStatement ps1 = connection.prepareStatement(insertMovie); 
        	
			
			String insertGenre = "insert into Genre (Genre) values (?);";
			PreparedStatement ps2 = connection.prepareStatement(insertGenre);  
            
			String insertGM = "insert into Movie_Genres (Movie_ID, Genre_ID) values (?,?)";
            PreparedStatement ps3 =connection.prepareStatement(insertGM);       
            
            final int batchSize = 1000;
            final int batchSize2 = 3000;
            int count = 0;
            int count2 = 0;
            int count3 =0;
        	List<String> genreLookup = new ArrayList<String>();
        	
            while((line = bufferedReader.readLine()) != null) {
            	String[] movie = line.split("\\t");
            	String [] genre = movie[8].split(",");
            	count++;//increments a value for the primary key
            	//add movie record to database
            	//create an ID base off count
            	String movieID = Integer.toString(count);
            	//fix for nulls
            	String runtime = movie[7];
            	if (runtime.equals("\\N")) {runtime = null;}            	
            	String endYear = movie[6];
            	if (endYear.equals("\\N")) {endYear = null;}            	
            	String startYear = movie[5];
            	if (startYear.equals("\\N")) {startYear = null;}
            	
            	//fix for quotes
            	String movieName = movie[2];
            	//movieName = movieName.replace("'","\\'");
            	String ogName = movie[3];
            	//ogName = ogName.replace("'", "\\'");

            	//insert movie
          	
               	ps1.setString(1, movieID);
            	ps1.setString(2, movie[0]);
            	ps1.setString(3, movie[1]);
            	ps1.setString(4, movieName);
            	ps1.setString(5, ogName);
            	ps1.setString(6, movie[4]);
            	ps1.setString(7, startYear);
            	ps1.setString(8, endYear);
            	ps1.setString(9, runtime);
            	ps1.addBatch();
            	
            	if(++count3 % batchSize == 0) {
        			ps1.executeBatch();
        			System.out.println("Movie Batch Loaded..." + count3);
            	}	

            	//check if genre is in table, if not add it and link it to movie
            	for(int i = 0; i < genre.length; i++)
            	{
            		//test if genre is in array, get array index            		
            		int genreID = genreLookup.indexOf(genre[i]) + 1;
            		if (genreID == 0) { 
            			ps2.setString(1, genre[i]);
            			ps2.executeUpdate();
            			
            			genreLookup.add(genre[i]);
            			genreID = genreLookup.indexOf(genre[i]) + 1;
            		}
        			String genreIDValue = Integer.toString(genreID);
        			
                	//update Movie_genres with both genre and movie id  
                	ps3.setString(1, movieID);
                	ps3.setString(2, genreIDValue);
                	ps3.addBatch();             	           		
            	}        	
            	if(++count2 % batchSize2 == 0) {
        			ps3.executeBatch();
        			System.out.println("Movie/Genre Batch Loaded..." + count2);
            	}	         	
            }
            ps1.executeBatch();
            ps3.executeBatch();
            bufferedReader.close();
            System.out.println("Movies, Genres and Movie_Genres tables populated.");
        }
        catch(FileNotFoundException ex) {
        	ex.printStackTrace();
        }
        catch(IOException ex) {
        	ex.printStackTrace();
        }
	}

	public void populatePrincipals(String file, Connection connection, Statement statement) throws SQLException {
        String fileName = file; 
        String line = null;		
        int count = 0;

        try {
        	FileReader fileReader =  new FileReader(fileName);
            BufferedReader bufferedReader =  new BufferedReader(fileReader);
            
            String insertActor = "insert into ActorRoles (Movie_Identifier, Name_Identifier, Category_ID , Ordering, Character_Role) values (?,?,?,?,?)";
        	PreparedStatement ps1 = connection.prepareStatement(insertActor);  
        	
        	String insertDirector = "insert into Directors (Movie_Identifier, Name_Identifier, Category_ID , Ordering) values (?,?,?,?)";
        	PreparedStatement ps2 = connection.prepareStatement(insertDirector);  
        	
        	String insertCategory = "insert into Category (Category_ID, Category_Name) values (?,?);";
        	PreparedStatement ps3 = connection.prepareStatement(insertCategory);  
        	
        	String insertStaff = "insert into Staff (Movie_Identifier, Name_Identifier, Category_ID , Ordering) values (?,?,?,?)";
        	PreparedStatement ps4 = connection.prepareStatement(insertStaff); 
        	
            final int batchSize = 1000;
 
            int catCount = 0;
            int actCount = 0;
            int dirCount = 0;
            int stfCount = 0;

        	List<String> categoryLookup = new ArrayList<String>();  
        	
            bufferedReader.readLine(); //skips the title line for me
            
            while((line = bufferedReader.readLine()) != null) {
            	count++;
            	String[] principals = line.split("\\t");
            	String category = principals[3];

                //set up the category list and get the id value (might save time hardcoding the three needed...)
            		
            	int categoryID = categoryLookup.indexOf(category) + 1;

            	if (categoryID == 0) { 
            		catCount++;
            		ps3.setString(1, Integer.toString(catCount));
           			ps3.setString(2, category);
           			ps3.executeUpdate();
            			
           			categoryLookup.add(category);
           			categoryID = categoryLookup.indexOf(category) + 1;
           		}
            		
        		String categoryIDValue = Integer.toString(categoryID); 
        			
        			//load directors table
        		if (category.equals("director")) {
            		ps2.setString(1, principals[0]);
            		ps2.setString(2, principals[2]);
            		ps2.setString(3, categoryIDValue);
            		ps2.setString(4, principals[1]);
                     	ps2.addBatch();
            			
                   	if(++dirCount % batchSize == 0) {
                		ps2.executeBatch();
                		System.out.println("Director Batch Loaded..." + dirCount);
                   	}       				
        		}
        		else if (category.equals("actor") || category.equals("actress")){
        			//else load the actor table, first identify the characters
        			String[] characters = principals[5].toString().split("\",\"");
        			
	                for(int i = 0; i < characters.length; i++) {
	                	//clean up the formatting
	                	String role = characters[i].replaceAll("[\\[\\]\"]", "");      

	            		ps1.setString(1, principals[0]);
	            		ps1.setString(2, principals[2]);
	            		ps1.setString(3, categoryIDValue);
	            		ps1.setString(4, principals[1]);
	            		ps1.setString(5, role);
	            		ps1.addBatch();
	            		
	                   	if(++actCount % batchSize == 0) {
	               			ps1.executeBatch();
	               			System.out.println("Actor Batch Loaded..." + actCount);
	                   	}	                		
	               	}
        		} 
        		else {
        			ps4.setString(1, principals[0]);
            		ps4.setString(2, principals[2]);
            		ps4.setString(3, categoryIDValue);
            		ps4.setString(4, principals[1]);
                    ps4.addBatch();
            			
                   	if(++stfCount % batchSize == 0) {
                		ps4.executeBatch();
                		System.out.println("Staff Batch Loaded..." + stfCount);
                   	}       				       			
        		}
            	
            }
    		ps1.executeBatch();
            ps2.executeBatch();
            ps4.executeBatch();
            
            bufferedReader.close();
            System.out.println("Actor/Actress/Director tables populated.");
        }
        catch(FileNotFoundException ex) {
        	ex.printStackTrace();
        }
        catch(IOException ex) {
        	ex.printStackTrace();
        }
	}

}
