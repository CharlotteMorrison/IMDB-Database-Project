import java.sql.*;

public class TableCreate {
	
	//database credentials
	String url = "jdbc:mysql://localhost:3306/";
	String user = "root";
	String password = "Isabella1";
	Connection connection = null;
	Statement statement = null;		

	public void tableCreate(String name) throws SQLException {
			
		try {

			connection = DriverManager.getConnection(url +"?autoReconnect=true&useSSL=false", user, password);
			statement = connection.createStatement();

			String drop = "drop database if exists " + name;
			statement.executeUpdate(drop);
			
			String sql = "create database " + name; 
			statement.executeUpdate(sql);	
					
			createFields(name); 
			//added the indexes later to improve the speed.  still very slow!
			createIndex(name);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			try {
				if(statement != null) {
					statement.close();
				}
				if(connection != null) {
					connection.close();
				}
			}
			catch (Exception e){
				e.printStackTrace();
			}
		}
	}
	
	private void createFields(String name) throws SQLException {
		url = url + name + "?autoReconnect=true&useSSL=false";
		try {
			connection = DriverManager.getConnection(url, user, password);
			statement = connection.createStatement();
			
			String movies = "create table Movies (Movie_ID int(12) not null unique, Identifier varchar(15), Title_Type varchar(50), Primary_Title varchar(1500), "
					+ "Original_Title varchar(1500), Is_Adult boolean, Start_Year int(4), End_Year int(4), Run_Time int(12), primary key (Identifier))";
			
			String movieGenre = "create table Movie_Genres (Movie_Genres_ID int(12) not null auto_increment, "
					+ "Movie_ID int(12), foreign key (Movie_ID) references Movies(Movie_ID), "
					+ "Genre_ID int(12), foreign key (Genre_ID) references Genre(Genre_ID), primary key (Movie_Genres_ID))";
			
			String genre = "create table Genre (Genre_ID int(12) not null auto_increment, Genre varchar(100), primary key (Genre_ID))";
			
			String names = "create table Names (Name_ID int(12) not null auto_increment unique, Identifier varchar(15), Primary_Name varchar(255), "
					+ "Birth_Year int(4), Death_Year int(4), primary key (Identifier))";
			
			String category = "create table Category (Category_ID int(12) not null, Category_Name varchar(255), primary key (Category_ID))";
			
			//the character roles were originally in a separate table- but for processing consideration, I have kept them in here
			String actorRoles = "create table ActorRoles (ActorRole_ID int(12) not null auto_increment, Movie_Identifier varchar(15), Name_Identifier varchar(15),"
					+"Category_ID int(12), Ordering int(5), Character_Role varchar(1500), primary key (ActorRole_ID))";
			
			String directors = "create table Directors (Director_ID int(12) not null auto_increment, Movie_Identifier varchar(15), Name_Identifier varchar(15),"
					+"Category_ID int(12), Ordering int(5), primary key (Director_ID))";
			
			String staff = "create table Staff (Staff_ID int(12) not null auto_increment, Movie_Identifier varchar(15), Name_Identifier varchar(15),"
					+"Category_ID int(12), Ordering int(5), primary key (Staff_ID))";
			
			String ratings = "create table Ratings (Movie_Identifier varchar(15) not null unique, Total_Ranking decimal(4,2) , Num_Votes int(12)) ";

			statement.executeUpdate(movies);
			statement.executeUpdate(genre);	
			statement.executeUpdate(movieGenre);
			statement.executeUpdate(names);
			statement.executeUpdate(category);
			statement.executeUpdate(actorRoles);
			statement.executeUpdate(directors);
			statement.executeUpdate(staff);
			statement.executeUpdate(ratings);
			
			System.out.println("Database created successfully.");
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	public void dropDatabase (String name) {
		url = url + name + "?autoReconnect=true&useSSL=false";
		try {

			connection = DriverManager.getConnection(url, user, password);
			statement = connection.createStatement();

			String drop = "drop database if exists " + name;
			statement.executeUpdate(drop);
			
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			try {
				if(statement != null) {
					statement.close();
				}
				if(connection != null) {
					connection.close();
				}
			}
			catch (Exception e){
				e.printStackTrace();
			}
		}
		System.out.println("Database: " + name + " dropped.");
	}


	private void createIndex(String name) throws SQLException {
		url = url + name + "?autoReconnect=true&useSSL=false";
		try {
			connection = DriverManager.getConnection(url, user, password);
			statement = connection.createStatement();	
			//Check and see if the indexes created, if not, create, after
			String alter1 = "ALTER TABLE actorroles ADD INDEX (Movie_Identifier)"; 
			String alter2 = "ALTER TABLE actorroles ADD INDEX (Name_Identifier)";   
			String alter3 = "ALTER TABLE ratings ADD INDEX (Movie_Identifier)";
			
			statement.executeUpdate(alter1);
			statement.executeUpdate(alter2);	
			statement.executeUpdate(alter3);			
			
			System.out.println("Indexes created successfully.");
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}
