
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;



public class Query {

	//database credentials
	private String url = "jdbc:mysql://localhost:3306/IMDB?rewriteBatchedStatements=true&autoReconnect=true&useSSL=false";
	private String user = "root";
	private String password = "Isabella1";
	private Connection connection = null;
	private Statement statement = null;		

	
	
	public void query() throws SQLException {
		connection = DriverManager.getConnection(url, user, password);
		statement = connection.createStatement();
		
        if (connection != null) {
            System.out.println("Creating: Bacon Number View. " );
        }
		
		String sql = "CREATE OR REPLACE VIEW baconNumber AS "  
				+ "	SELECT ar.Name_Identifier AS bacon , ar.Movie_Identifier AS movie, arr.Name_Identifier AS actor " 
				+ "	FROM actorroles ar " 
				+ "	JOIN actorroles arr " 
				+ "	ON ar.Movie_Identifier = arr.Movie_Identifier " 
				+ "	AND ar.Name_Identifier != arr.Name_Identifier ";
		
		try {
			
			PreparedStatement ps = connection.prepareStatement(sql);			
			ps.executeQuery();			
			
		} catch (SQLException e) {
			e.printStackTrace();
		}			
		System.out.println("Bacon Number View Created. " );
		
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
	
	public void directorByGenre (String genre) throws SQLException {

		connection = DriverManager.getConnection(url, user, password);
		statement = connection.createStatement();
		
        if (connection != null) {
            System.out.println("Connected to the database: Fetching Directors with films in the " + genre + " genre.");
        }
		
		String sql = "SELECT n.Primary_Name, m.Primary_Title , m.Start_Year "
				+ " FROM names  n, directors d, movies m, movie_genres mg, genre g "
				+ " WHERE g.Genre = ? "
				+ " AND g.Genre_ID = mg.Genre_ID "
				+ " AND mg.Movie_ID = m.Movie_ID "
				+ " AND d.Movie_Identifier = m.Identifier "
			    + " AND d.Name_Identifier = n.Identifier "
			    + " AND m.Start_Year % 4 = 0";
		try {
			
			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setString(1, genre);
			
			ResultSet rs = ps.executeQuery();
			
			while(rs.next()) {
				String name = rs.getString("Primary_Name");
				String title = rs.getString("Primary_Title");
				String year = rs.getString("Start_Year");
				System.out.println(name + "\t" + title + "\t" + year);
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
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
	
	public void actorInMovie (String movie) throws SQLException {
		
		connection = DriverManager.getConnection(url, user, password);
		statement = connection.createStatement();
		
        if (connection != null) {
            System.out.println("Connected to the database: Fetching actors in the movie " + movie + ".");
        }
		
		String sql = "SELECT n.Primary_Name " 
				+ "	FROM movies m, actorroles ar, names n" 
				+ "	WHERE m.Primary_Title = ? "  
				+ " AND ar.Movie_Identifier = m.Identifier " 
				+ " AND ar.Name_Identifier = n.Identifier";
		
		try {
			
			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setString(1, movie);
			
			ResultSet rs = ps.executeQuery();
			
			while(rs.next()) {
				String name = rs.getString("Primary_Name");
				System.out.println(name);
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
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
	
	public void yearError (int before, int after) throws SQLException {
		connection = DriverManager.getConnection(url, user, password);
		statement = connection.createStatement();
		
        if (connection != null) {
            System.out.println("Connected to the database: Fetching Actors with roles prior to "  + before + " and after " + after +  "." );
        }
		
		String sql = "SELECT DISTINCT n.Primary_Name " 
				+ "	FROM names n " 
				+ "	where n.Identifier " 
				+ "    IN (SELECT ar.Name_Identifier " 
				+ "		FROM actorroles ar, movies m " 
				+ "        WHERE m.Start_Year < ? " 
				+ "        AND ar.Movie_Identifier = m.Identifier) " 
				+ "	AND n.Identifier " 
				+ "    IN (SELECT ar.Name_Identifier " 
				+ "		FROM actorroles ar, movies m " 
				+ "        WHERE m.Start_Year > ? " 
				+ "        AND ar.Movie_Identifier = m.Identifier)";
		
		try {
			
			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setInt(1, before);
			ps.setInt(2, after);
			
			ResultSet rs = ps.executeQuery();
			
			while(rs.next()) {
				String name = rs.getString("Primary_Name");
				System.out.println(name);
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}		
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
	
	public void directedMoreThan (int amount) throws SQLException {
		connection = DriverManager.getConnection(url, user, password);
		statement = connection.createStatement();
		
        if (connection != null) {
            System.out.println("Connected to the database: Fetching directors with more than  " + amount + " films directed.");
        }
		
		String sql = "SELECT n.Primary_Name, COUNT(*) AS number "  
				+ "	FROM names n, directors d "  
				+ " WHERE d.Name_Identifier = n.Identifier "  
				+ " GROUP BY d.Name_Identifier " 
				+ " HAVING COUNT(*) >= ? "  
				+ " ORDER BY COUNT(*) DESC";
		
		try {
			
			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setInt(1, amount);
			
			ResultSet rs = ps.executeQuery();
			
			while(rs.next()) {
				String name = rs.getString("Primary_Name");
				String count = rs.getString("number");
				System.out.println(name + " directed " + count + " movies.");
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}	
		
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
	
	public void twoPlusRolesA () throws SQLException {
		connection = DriverManager.getConnection(url, user, password);
		statement = connection.createStatement();
		
        if (connection != null) {
            System.out.println("Connected to the database: Fetching actors/actresses with 2 or more roles in a film (count). ");
        }
		
		String sql = "SELECT n.Primary_Name, m.Primary_Title, COUNT(Distinct ar.Character_Role) AS number"  
				+ "	FROM movies m, names n, actorroles ar " 
				+ "	WHERE  n.Identifier = ar.Name_Identifier " 
				+ "	AND ar.Movie_Identifier = m.Identifier " 
				+ "	GROUP BY n.Identifier, m.Identifier " 
				+ "	HAVING COUNT(Distinct ar.Character_Role) >= 2";
		
		try {
			
			PreparedStatement ps = connection.prepareStatement(sql);
			
			ResultSet rs = ps.executeQuery();
			
			while(rs.next()) {
				String name = rs.getString("Primary_Name");
				String title = rs.getString("Primary_Title");
				String roles = rs.getString("number");
				System.out.println(name + "\t" + title + "\t" + roles);
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
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
	
	public void twoPlusRolesB () throws SQLException {
		connection = DriverManager.getConnection(url, user, password);
		statement = connection.createStatement();
		
        if (connection != null) {
            System.out.println("Connected to the database: Fetching actor/actress with two or more roles in a film, listing roles. " );
        }
		
		String sql = "SELECT n.Primary_Name, m.Primary_Title, ar.Character_Role "  
				+ " FROM Names n, Movies m, actorroles ar, " 
				+ "	(SELECT n.Identifier AS thisName, m.Primary_Title AS thisMovie " 
				+ "	FROM Names n, Movies m, actorroles ar " 
				+ "	WHERE n.Identifier = ar.Name_Identifier AND ar.Movie_Identifier = m.Identifier " 
				+ "	GROUP BY n.Identifier, m.Primary_Title " 
				+ "	HAVING COUNT(*) >= 2) x " 
				+ " WHERE n.Identifier = ar.Name_Identifier and m.Identifier = ar.Movie_Identifier " 
				+ " AND x.thisName = n.Identifier and x.thisMovie = m.Primary_Title";
		
		try {
			
			PreparedStatement ps = connection.prepareStatement(sql);
			
			ResultSet rs = ps.executeQuery();
			
			while(rs.next()) {
				String name = rs.getString("Primary_Name");
				String title = rs.getString("Primary_Title");
				String roles = rs.getString("number");
				System.out.println(name + "\t" + title + "\t" + roles);
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}	
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
	
	public void femaleOnlyA () throws SQLException {
		connection = DriverManager.getConnection(url, user, password);
		statement = connection.createStatement();
		
        if (connection != null) {
            System.out.println("Connected to the database: Fetching number of movies with female only cast in the database by year." );
        }
		
		String sql = "SELECT m.Start_Year, COUNT(*)  AS number "  
				+ "	FROM movies m, actorroles ar, category c " 
				+ " WHERE c.Category_Name = 'Actress' " 
				+ " AND ar.Category_ID = c.Category_ID " 
				+ " AND ar.Movie_Identifier = m.Identifier "
				+ " AND m.Start_Year IS NOT NULL "
				+ " GROUP BY m.Start_Year ORDER BY m.Start_Year DESC";
		
		try {
			
			PreparedStatement ps = connection.prepareStatement(sql);
			
			ResultSet rs = ps.executeQuery();
			
			while(rs.next()) {
				String year = rs.getString("Start_Year");
				String count = rs.getString("number");
				System.out.println(year + "\t" + count);
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}	
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

	public void femaleOnlyB () throws SQLException {
		connection = DriverManager.getConnection(url, user, password);
		statement = connection.createStatement();
		
        if (connection != null) {
            System.out.println("Connected to the database: Fetching number and percentage of movies with female only cast in the database by year. " );
        }
		//didn't grab the category id from the database- just hard coded this because this query takes forever to run.
		String sql = "select m.Start_Year AS Year, "  
				+ "	CONCAT(FORMAT(((count(*) - male_count)/COUNT(*))*100, 2), '%') AS Percentage, " 
				+ " COUNT(*) - male_count AS Female_Only_Cast " 
				+ "	FROM movies m "  
				+ "	INNER JOIN actorroles ar "  
				+ " ON m.Identifier = ar.Movie_Identifier, " 
				+ " (SELECT m.Start_Year AS year, " 
				+ "	Count(*) AS male_count " 
				+ " FROM movies m, actorroles ar " 
				+ " WHERE ar.Category_ID = \"7\" "
				+ " AND m.Identifier = ar.Movie_Identifier " 
				+ " GROUP BY m.Start_Year) AS male_set " 
				+ "	WHERE m.Start_Year = male_set.year "
				+ " AND m.Start_Year IS NOT NULL "
				+ "	GROUP BY m.start_year ";
		
		try {
			
			PreparedStatement ps = connection.prepareStatement(sql);
			
			ResultSet rs = ps.executeQuery();
			
			while(rs.next()) {
				String year = rs.getString("Year");
				String percentage = rs.getString("Percentage");
				String female = rs.getString("Female_Only_Cast");
				System.out.println(year + "\t" + percentage + "\t" + female);
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}	
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
	
	public void largestCast () throws SQLException {
		connection = DriverManager.getConnection(url, user, password);
		statement = connection.createStatement();
		
        if (connection != null) {
            System.out.println("Connected to the database: Fetching the movie with the largest cast size in the database. " );
        }
		
		String sql = "SELECT m.Primary_Title, COUNT(*) " 
				+ "	AS Total_Cast_Size " 
				+ "	FROM Movies m " 
				+ "	INNER JOIN actorroles ar " 
				+ "	ON m.Identifier = ar.Movie_Identifier " 
				+ "	GROUP BY m.identifier, m.Primary_Title " 
				+ "	HAVING COUNT(*) >= ALL (SELECT COUNT(*) FROM actorroles GROUP BY Movie_Identifier) ";
		
		try {
			
			PreparedStatement ps = connection.prepareStatement(sql);
			
			ResultSet rs = ps.executeQuery();
			
			while(rs.next()) {
				String title = rs.getString("Primary_Title");
				String size = rs.getString("Total_Cast_Size");
				System.out.println(title + "\t" + size);
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}		
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
	
	public void mostPerDecade () throws SQLException {
		connection = DriverManager.getConnection(url, user, password);
		statement = connection.createStatement();
		
        if (connection != null) {
            System.out.println("Connected to the database: Fetching the decade  with the most films produced." );
        }
		
		String sql = "SELECT y.start_year AS decade, SUM(c.total) AS movie_count "
				+ " FROM (SELECT DISTINCT start_year FROM Movies) AS y, "
				+ " (SELECT start_year, COUNT(*) AS total FROM Movies GROUP BY start_year) AS c "
				+ " WHERE c.start_year >= y.start_year AND c.start_year < (y.start_year + 10) "
				+ " GROUP BY y.start_year HAVING SUM(c.total) >= ALL "
				+ " (SELECT SUM(c.total) FROM (SELECT DISTINCT start_year FROM Movies) AS y, "
				+ " (SELECT start_year, COUNT(*) AS total FROM Movies GROUP BY start_year) AS c "
				+ " WHERE c.start_year >= y.start_year AND c.start_year < (y.start_year + 10) "
				+ " GROUP BY y.start_year)";
		
		try {
			
			PreparedStatement ps = connection.prepareStatement(sql);
			
			ResultSet rs = ps.executeQuery();
			
			while(rs.next()) {
				String decade = rs.getString("decade");
				String count = rs.getString("movie_count");
				int endDate = Integer.parseInt(decade) + 10;
				System.out.println(decade + "-" + endDate + " = " + count + " movies");
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
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
	
	public void baconNumber () throws SQLException {
		connection = DriverManager.getConnection(url, user, password);
		statement = connection.createStatement();
		
        if (connection != null) {
            System.out.println("Connected to the database: Fetching the number of actors/actresses with a Kevin Bacon number of 2 " );
        }
		
        String sql = "SELECT COUNT(*) AS count" 
        		+ " FROM baconNumber a JOIN baconNumber b ON a.actor = b.bacon " 
        		+ " WHERE a.bacon = 'nm0000102';";
        
		try {
			
			PreparedStatement ps = connection.prepareStatement(sql);

			ResultSet rs = ps.executeQuery();
			
			while(rs.next()) {
				String count = rs.getString("count");

				System.out.println("Number of actors/actresses with a Bacon factor of 2:" + "\t" + count);
			}
			
		} catch (SQLException e) {

			e.printStackTrace();
		}		
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
	
	public void baconDegree (String name) throws SQLException {
		
		connection = DriverManager.getConnection(url, user, password);
		statement = connection.createStatement();
		
        if (connection != null) {
            System.out.println("Connected to the database: Seaching for " + name + " identifier." );
        }
		
		String sql1 = "SELECT Identifier FROM names WHERE Primary_Name = ? ";
		String sqlB1 = "SELECT movie, actor FROM baconNumber WHERE bacon = 'nm0000102' AND actor = ?";
		String sqlB2 = "SELECT 	a.movie movie1, a.actor actor1, b.movie movie2, b.actor actor2 " 
				+ " FROM baconNumber a JOIN baconNumber b ON a.actor = b.bacon " 
				+ " WHERE a.bacon = 'nm0000102' AND b.actor = ?";
		String sqlB3 = "SELECT 	a.movie movie1, a.actor actor1, b.movie movie2, b.actor actor2," 
				+ " c.movie movie3, c.actor actor3, d.movie movie4, d.actor actor4 " 
				+ " FROM baconNumber a JOIN baconNumber b ON a.actor = b.bacon"
				+ " JOIN baconNumber c ON b.actor = c.bacon JOIN baconNumber d ON c.actor = d.bacon " 
				+ " WHERE a.bacon = 'nm0000102' AND d.actor = ? ";
		String id = null;
		try {
			
			PreparedStatement ps = connection.prepareStatement(sql1);
			ps.setString(1, name);
			
			ResultSet rs = ps.executeQuery();

			rs.next();
			id = rs.getString("Identifier");

			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println(name + " \t" + id);
		
		try {
			
			PreparedStatement ps = connection.prepareStatement(sqlB1);
			ps.setString(1, id);
			
			ResultSet rs = ps.executeQuery();
			
			if (rs.next() == false) {    
			    ps = connection.prepareStatement(sqlB2);
			    ps.setString(1, id);
			    rs = ps.executeQuery();
			    
			    if (rs.next() == false) {
				    ps = connection.prepareStatement(sqlB3);
				    ps.setString(1, id);
				    rs = ps.executeQuery();
				    
				    if (rs.next() == false) {
				    	System.out.println("keep looking");
				    } else {
				    	System.out.println(name + "  is Bacon Level 3.");
				    }			    	
			    } else {
			    	System.out.println(name + "  is Bacon Level 2.");
			    }		    
			} else {
				System.out.println(name + "  is Bacon Level 1.");
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}	
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
	
	public void ratingsAndVotes () throws SQLException, IOException {
		connection = DriverManager.getConnection(url, user, password);
		statement = connection.createStatement();
		BufferedWriter	bw = new BufferedWriter(new FileWriter("src/ratings.csv"));
		
        if (connection != null) {
            System.out.println("Connected to the database: Creating ranking file.  " );
        }
		
		String sql = "SELECT Total_Ranking, Num_Votes FROM ratings LIMIT 255";
		bw.write("# of Votes, Ranking\n");
		try {
			
			PreparedStatement ps = connection.prepareStatement(sql);

			ResultSet rs = ps.executeQuery();
			
			while(rs.next()) {
				String ranking = rs.getString("Total_Ranking");
				String votes = rs.getString("Num_Votes");
				bw.write(votes + "," + ranking + "\n");
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			if(statement != null) {
				statement.close();
			}
			if(connection != null) {
				connection.close();
			}
			bw.close();
		}
		catch (Exception e){
			e.printStackTrace();
		}
		System.out.println("Ranking file created.");
	}
	
}
