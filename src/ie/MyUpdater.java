package ie;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class MyUpdater {
	private Connection connection = null;
	private Statement statement = null;
	private PreparedStatement preparedstatement = null;
	private ResultSet resultset = null;
	Twokenize tokenizer = new Twokenize();
	
	public void UpdateEvent(ArrayList<String> labels,String twitter_tweet_id){
		// select from datbase where twitter_tweet_id equals to input
		String tweet = getTweet(twitter_tweet_id);
		// tokenize, 
		ArrayList<String> tokenized = (ArrayList<String>) tokenizer.tokenizeRawTweetText(tweet); 
		
		// matching with labels in order
		doRestructure(labels, tokenized);
		
		// restructure text to match database event column structure (name,place,time,info)
		// insert into database evetn
	}
	
	
	/**
	 * Private functions 
	 */
	private String getTweet(String twitter_tweet_id){
		String retval ="";
		Long tweetid = Long.valueOf(twitter_tweet_id);
		try{
			preparedstatement = connection.prepareStatement("SELECT tweet from raw_tweet where twitter_tweet_id = ?");
			preparedstatement.setLong(1, tweetid);
			resultset = preparedstatement.executeQuery();
			while(resultset.next()){
				return resultset.getString("tweet");
			}
		}catch(SQLException e){
			e.printStackTrace();
		}
		return retval;
	}
	
	private void doRestructure(ArrayList<String> labels,ArrayList<String> tokenized){
		StringBuffer sbname = new StringBuffer();
		StringBuffer sbplace = new StringBuffer();
		StringBuffer sbtime = new StringBuffer();
		StringBuffer sbinfo = new StringBuffer();
		
		for (int i = 0; i < labels.size(); i++) {
			String label = labels.get(i);
			if(label.equals("i-name")){
				sbname.append(tokenized.get(i)+ " ");
			}else if(label.equals("i-place")){
				sbplace.append(tokenized.get(i)+ " ");
			}else if(label.equals("i-time")){
				sbtime.append(tokenized.get(i)+ " ");
			}else if(label.equals("i-info")){
				sbinfo.append(tokenized.get(i)+ " ");
			} // label equals other perlu gak sih ?
		}
		
		System.out.println("==== INFORMASI EVENT ====");
		System.out.println("NAMA EVENT \t\t: "		+sbname.toString());
		System.out.println("LOKASI EVENT \t\t: "	+sbplace.toString());
		System.out.println("WAKTU EVENT \t\t: "		+sbtime.toString());
		System.out.println("INFO EVENT \t\t: "		+sbinfo.toString());
		System.out.println();
		
	}
		
	public void startConnection(boolean isLocal){
		if(!isLocal){
			String DB_USERNAME	 	= System.getenv("OPENSHIFT_MYSQL_DB_USERNAME");
			String DB_PASSWORD		= System.getenv("OPENSHIFT_MYSQL_DB_PASSWORD");
			String DB_HOST 			= System.getenv("OPENSHIFT_MYSQL_DB_HOST");
			String DB_PORT 			= System.getenv("OPENSHIFT_MYSQL_DB_PORT");
			String DB_URL 			= System.getenv("OPENSHIFT_MYSQL_DB_URL");
			String DB_NAME			= System.getenv("OPENSHIFT_APP_NAME");
			String URL 				= String.format("jdbc:mysql://%s:%s/%s", DB_HOST, DB_PORT, DB_NAME);
			startConnection(DB_USERNAME,DB_PASSWORD,URL);
		}else{
			String DB_USERNAME	 	= "admineuq7RcL";
			String DB_PASSWORD		= "NIuz5FBw59vg";
			String DB_URL 			= "mysql://127.0.0.1:3307/";
			String DB_NAME			= "mytomcatapp";
			String URL 			= "jdbc:"+DB_URL+DB_NAME;
			startConnection(URL,DB_USERNAME,DB_PASSWORD);
		}
	}
	
	private void startConnection(String URL,String DB_USERNAME, String DB_PASSWORD){
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	

		System.out.println("[INFO] Getting environment variables");
		System.out.println("DB_USERNAME \t: "+ DB_USERNAME);
		System.out.println("DB_PASSWORD \t: "+ DB_PASSWORD);
		System.out.println("URL \t\t:"+URL);

		try{
			connection 	= (Connection) DriverManager.getConnection(URL,DB_USERNAME,DB_PASSWORD);
		}catch(SQLException e){
			e.printStackTrace();
		}
	}
	
	public void CloseConnection(){
		try{
			if(resultset!=null){
				resultset.close();
			}
			if(statement!=null){
				statement.close();
			}
			if(connection!=null){
				connection.close();
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
