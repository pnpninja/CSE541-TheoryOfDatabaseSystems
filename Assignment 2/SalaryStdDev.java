import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SalaryStdDev {

	public static void main(String[] args) {		
		
		String url = "jdbc:db2://localhost:50000/";
	    if(args.length != 4) {
	    	System.exit(1);
	    }else {
	    	try {
	    		url = url+args[0];
	    		String username = args[2];
	    		String password = args[3];
	    		Connection con;
	    	    Statement stmt;
	    	    ResultSet rs;
		    	float empSal;
		    	float sumSalary = 0;
		    	float sumSalarySquared = 0;
		    	float nosRows = 0;
		    	Class.forName("com.ibm.db2.jcc.DB2Driver");
		    	con = DriverManager.getConnection (url, username, password); 
		    	stmt = con.createStatement();
		    	rs = stmt.executeQuery("SELECT SALARY FROM "+args[1]);
		    	while (rs.next()) {
		            empSal = rs.getFloat(1);
		            sumSalary += empSal;
		            sumSalarySquared += empSal*empSal;
		            nosRows += 1;
		           
		        }
		    	double stddev = Math.sqrt((sumSalarySquared/nosRows) - ((sumSalary/nosRows) * (sumSalary/nosRows)));
		    	System.out.println("Standard deviation = "+stddev);
		    	rs.close();
		    	stmt.close();
		    	con.commit();
		    	con.close();
		    	
		    }catch(ClassNotFoundException e) {
		    	System.out.println("Driver not found!");
		    	e.printStackTrace();
		    }catch (SQLException e) {
		    	System.out.println("Unable to establish connection to - "+url+" with given credentials - ");
		    	e.printStackTrace();
			}
	    }
	    
	    
	}

}
