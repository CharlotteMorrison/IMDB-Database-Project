
import java.io.IOException;
import java.sql.*;
public class Driver {


	public static void main(String[] args) throws SQLException, IOException {

/*
		TableCreate table = new TableCreate();		
		
		try {
			table.tableCreate("IMDB");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		
		Populate files = new Populate();
		
		files.populate();
*/		
		Query query = new Query();
/*		
		query.directorByGenre("Film-Noir");
		
		query.actorInMovie("Officer \'444\'");
		
		query.yearError(1900, 2000);
		
		query.directedMoreThan(100);
		
		query.twoPlusRolesA();
		
		query.twoPlusRolesB();
	
		query.femaleOnlyA();
		
		query.femaleOnlyB();
		
		query.largestCast();
		
		query.mostPerDecade();
	
		query.baconNumber();
		
		query.baconDegree("Sean Connery");
		
		query.baconDegree("Humphrey Bogart");
		
		query.baconDegree("Spencer Tracy");
		
		query.baconDegree("Shirley Temple");
*/		
		query.ratingsAndVotes();
		
		//table.dropDatabase("IMDB");
		
	}

}
