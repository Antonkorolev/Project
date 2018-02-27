import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Collections;
import java.util.Properties;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;

public class SimpleDb 
{
	private static HashMap<String,String> Map1 = new HashMap<>(); // collection for save data from veksha
	private static HashMap<String,String> Map2 = new HashMap<>(); // collection for save data from veksha
	private static ArrayList<Double> res = new ArrayList<>(); // collection for save sla
	private static double percentile;  //percentile from file settings
	private static String sqlSiebel = null;//SQL query from file settings for siebel
	private static String sqlveksha = null;//SQL query from file settings for veksha 
	
    public static void main(String[] args) {  
        getSettings("C:\\Users\\Antonio\\Desktop\\settings\\settings.txt");
        selectfromSiebel();
        selecctfromVeksha();
        masDate(Map1, Map2);
        System.out.println("Percentille is " + Percentile(res,percentile));
        
    }
    //settings from file
    public static void getSettings(String context) {
    	 String settingsFileName = context;
         Properties prop = new Properties();
         File fileProps = new File(settingsFileName);
         try {
             prop.load(new FileInputStream(fileProps));
             
             if (!prop.getProperty("sqlSiebel").isEmpty()) {
            	 sqlSiebel = prop.getProperty("sqlSiebel");
             } else {
                System.out.println("Can't find SQL query for siebel");
             }
            
             if (!prop.getProperty("sqlveksha").isEmpty()) {
            	 sqlveksha = prop.getProperty("sqlveksha");
             } else {
                System.out.println("Can't find SQL query for veksha");
             }
             if (!prop.getProperty("percentile").isEmpty()) {
            	 percentile =Double.parseDouble(prop.getProperty("percentile"));
             } else {
                System.out.println("Can't find SQL query for veksha");
             }
         } catch (Exception e) {
        	 System.out.println("Can't do this");
         } 
    }
  //SQL query in veksha siebel
    private static void selectfromSiebel() {//String name
        try {
            Class.forName("org.postgresql.Driver");
            String url = "jdbc:postgresql://localhost:5432/siebel";
            String login = "postgres";
            String password = "postgres";
            Connection con = DriverManager.getConnection(url, login, password);
            try {
                Statement stmt = con.createStatement();
                ResultSet rs = stmt.executeQuery(sqlSiebel);
                while (rs.next()) {              
                    Map2.put(rs.getString("rquid"), rs.getString("time"));
                }               
                rs.close();
                stmt.close();
            } finally {
                con.close();
            }
        } catch (Exception e) {
        	 System.out.println("Can't execute SQL query for siebel");
        }
    }
    //SQL query in veksha
    private static void selecctfromVeksha() {//String name
        try {
            Class.forName("org.postgresql.Driver");
            String url = "jdbc:postgresql://localhost:5432/veksha";
            String login = "postgres";
            String password = "postgres";
            Connection con = DriverManager.getConnection(url, login, password);
            try {
                Statement stmt = con.createStatement();
                ResultSet rs = stmt.executeQuery(sqlveksha);
                while (rs.next()) {
                    Map1.put(rs.getString("rquid"), rs.getString("time"));                 
                }       
                rs.close();
                stmt.close();
            } finally {
                con.close();
            }
        } catch (Exception e) {
        	System.out.println("Can't execute SQL query for siebel");
        }
    }
    // find sla and save in collection
    private static ArrayList<Double> masDate(HashMap<String,String> map1,HashMap<String,String> map2){
    	SimpleDateFormat Format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"); 
    	  try{
    		  for (Map.Entry<String, String> entry1 : map1.entrySet()){
    			  for (Map.Entry<String, String> entry2 : map2.entrySet()) {
    				 if(entry1.getKey().equals(entry2.getKey())) {    				
    					double z = ((double)Format.parse(entry2.getValue()).getTime() - (double)Format.parse(entry1.getValue()).getTime())/1000;    				
    					res.add(z);
    				 }
    		  }
    		  }
       	  }catch(Exception e) {
    		  System.out.println("Can't find collection");
    	  }
    	return res;//
    }
//find percentile
	private static double Percentile(ArrayList<Double> res, double percentile){
			double result = 0;
		try {
			Collections.sort(res); 			
			double index = percentile*(res.size() - 1); 
			int lower = (int)Math.floor(index); 
			double fraction = index - lower; 		
			result = res.get(lower) + fraction*(res.get(lower+1) - res.get(lower)); 
		}catch(Exception e) {
			System.out.println("Can't find percentile");
		}
		return result;
		} 

}