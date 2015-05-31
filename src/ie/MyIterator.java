package ie;

import ie.model.Raw_tweet;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import cc.mallet.types.Instance;

public class MyIterator implements Iterator<Instance>{
	private Connection connection = null;
	private Statement statement = null;
	private PreparedStatement preparedstatement = null;
	private ResultSet resultset = null;
	ArrayList<Raw_tweet> tweets;
	Iterator<Raw_tweet> internal_iterator;
	Twokenize tokenizer = new Twokenize();
	
	public MyIterator(boolean isLocal) {
		startConnection(isLocal);
		getData();
		CloseConnection();
	}
	
	
	@Override
	public boolean hasNext() {
		return internal_iterator.hasNext();
	}

	@Override
	public Instance next() {
		// TODO Auto-generated method stub
		Raw_tweet rtweet = internal_iterator.next();
		List<String> data = tokenizer.tokenizeRawTweetText(rtweet.getTweet());
		return new Instance(data, null, rtweet.getTwitter_tweet_id().toString(), "");
	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub
		
	}
	
	
	/**
	 * Private functions 
	 */
	
	private void getData(){
		try{
			
			tweets = new ArrayList<>();
			preparedstatement = connection.prepareStatement("SELECT tweet,twitter_tweet_id from raw_tweet where label = 0 limit 20");
			resultset = preparedstatement.executeQuery();
			while(resultset.next()){
				Raw_tweet tmp = new Raw_tweet(resultset.getLong("twitter_tweet_id"), resultset.getString("tweet"));
				tweets.add(tmp);
			}

			
			internal_iterator = tweets.iterator();
		}catch(SQLException e){
			e.printStackTrace();
		}
	}
	
	private void startConnection(boolean isLocal){
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
	
	private void CloseConnection(){
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
