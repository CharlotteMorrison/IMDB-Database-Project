
import java.sql.*;
public class Driver {


	public static void main(String[] args) throws SQLException {

		/*
		TableCreate table = new TableCreate();		
		
		try {
			table.tableCreate("IMDB");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		*/		

		Populate files = new Populate();
		files.populate();
		
		
		
	}

}
