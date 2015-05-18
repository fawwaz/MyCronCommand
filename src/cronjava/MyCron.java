package cronjava;


import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.ResponseList;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.api.TimelinesResources;
import twitter4j.conf.ConfigurationBuilder;

public class MyCron {
	
	private Connection connection = null;
	private Statement statement = null;
	private PreparedStatement preparedstatement = null;
	private ResultSet resultset = null;
	
	public static void main(String[] args) {
		
		// For Storing to database mysql, instead
		/*
		MyCron cron = new MyCron();
		cron.startConnection();
		cron.getTweet();
		cron.CloseConnection();
		*/
	}
	
	private void getTweet(){
		System.out.println("[INFO] Connecting to twitter");
		ConfigurationBuilder conf = new ConfigurationBuilder();
		conf.setDebugEnabled(true)
			.setOAuthConsumerKey("dmDMgfbO9cA0aH3uiNvEcgAPu")
			.setOAuthConsumerSecret("vdGlY077SK4Cw1y26oNxcfmBwe0oyFv1Xj17wKai72wZwBZsd6")
			.setOAuthAccessToken("470084145-mFLzF9yv4wlfQuRWqUcARJ9Wpfjq4fQBG3Z52MNQ")
			.setOAuthAccessTokenSecret("fkULaBtZKHfd7IUIYhmwYWgH0Fjm34QHygCeT5EoYgjlN");
		
		TwitterFactory tf = new TwitterFactory(conf.build());
		Twitter twitter = tf.getInstance();
		
		Query query = new Query("@infobdgevent");
		query.setCount(40);
		QueryResult result;
		try {
			result = twitter.search(query);
			System.out.println("[INFO] Connected to twitter ");
			System.out.println("[INFO] Starting to find tweet mentioning @infobdgevent");
			for (Status status : result.getTweets()) {
				if(!status.getText().startsWith("RT")){ // Make sure not duplicate data
					if(!isExist(status.getId())){
						insertIntoDB(status);
					}
				}else{
					System.out.println("[WARNING] STARTED With RT : "+status.getText());
				}
			}
			
		} catch (TwitterException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		Query query_2 = new Query("#eventbdg");
		query_2.setCount(10);
		QueryResult result_2;
		try {
			result_2 = twitter.search(query);
			System.out.println("[INFO] Connected to twitter");
			System.out.println("[INFO] Starting to find tweet with hashtag #eventbdg");
			for (Status status : result_2.getTweets()) {
				if(!status.getText().startsWith("RT")){ // Make sure not duplicate data
					if(!isExist(status.getId())){
						insertIntoDB(status);
					}
				}else{
					System.out.println("[WARNING] STARTED With RT : "+status.getText());
				}
			}
			
		} catch (TwitterException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Query query_3 = new Query("#bdgevent");
		query_3.setCount(10);
		QueryResult result_3;
		try {
			result_3 = twitter.search(query);
			System.out.println("[INFO] Connected to twitter");
			System.out.println("[INFO] Starting to find tweet with hashtag #bdgevent");
			for (Status status : result_3.getTweets()) {
				if(!status.getText().startsWith("RT")){ // Make sure not duplicate data
					if(!isExist(status.getId())){
						insertIntoDB(status);
					}
				}else{
					System.out.println("[WARNING] STARTED With RT : "+status.getText());
				}
			}
			
		} catch (TwitterException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		/* Getting @infobdgevent timeline */
		try {
			ResponseList<Status> statuses = twitter.getUserTimeline("infobdgevent");
			System.out.println("[INFO] Getting @infobdgevent timeline");
			for (int i = 0; i < statuses.size(); i++) {
				if(!isExist(statuses.get(i).getId())){
					insertIntoDB(statuses.get(i));
				}
			}
		} catch (TwitterException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			ResponseList<Status> statuses = twitter.getUserTimeline("bdgevent");
			System.out.println("[INFO] Getting @bdgevent timeline");
			for (int i = 0; i < statuses.size(); i++) {
				if(!isExist(statuses.get(i).getId())){
					insertIntoDB(statuses.get(i));
				}
			}
		} catch (TwitterException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	private void startConnection(){
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		
		String DB_USERNAME	 	= System.getenv("OPENSHIFT_MYSQL_DB_USERNAME");
		String DB_PASSWORD		= System.getenv("OPENSHIFT_MYSQL_DB_PASSWORD");
		String DB_HOST 			= System.getenv("OPENSHIFT_MYSQL_DB_HOST");
		String DB_PORT 			= System.getenv("OPENSHIFT_MYSQL_DB_PORT");
		String DB_URL 			= System.getenv("OPENSHIFT_MYSQL_DB_URL");
		String DB_NAME			= System.getenv("OPENSHIFT_APP_NAME");
		String URL 				= String.format("jdbc:mysql://%s:%s/%s", DB_HOST, DB_PORT, DB_NAME);
		
		/*
		String DB_USERNAME	 	= "root";
		String DB_PASSWORD		= "";
		String DB_URL 			= "mysql://localhost/";
		String DB_NAME			= "mytomcatapp";
		String URL 				= "jdbc:"+DB_URL+DB_NAME;
		*/
		
		/*
		String DB_USERNAME	 	= "admineuq7RcL";
		String DB_PASSWORD		= "NIuz5FBw59vg";
		String DB_URL 			= "mysql://127.0.0.1:3307/";
		String DB_NAME			= "mytomcatapp";
		String URL 				= "jdbc:"+DB_URL+DB_NAME;
		/**/
		
		System.out.println("[INFO] Getting environment variables");
		System.out.println("DB_USERNAME \t: "+ DB_USERNAME);
		System.out.println("DB_PASSWORD \t: "+ DB_PASSWORD);
		//System.out.println("DB_HOST \t: "+DB_HOST);
		//System.out.println("DB_PORT \t: "+DB_PORT);
		System.out.println("DB_URL	\t: "+ DB_URL);
		System.out.println("DB_NAME \t: "+ DB_NAME);
		System.out.println("URL \t\t:"+URL);
		
		try {
			connection 	= (Connection) DriverManager.getConnection(URL,DB_USERNAME,DB_PASSWORD);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}
	
	private void insertIntoDB(Status status){
		try {
			preparedstatement = connection.prepareStatement("insert into mytomcatapp.raw_tweet values (default, ?, ?, ?, ?, ?, ?)");
			preparedstatement.setLong(1, status.getUser().getId());
			preparedstatement.setString(2, status.getUser().getName());
			preparedstatement.setLong(3, status.getId());
			preparedstatement.setString(4, status.getText());
			preparedstatement.setTimestamp(5, new Timestamp(status.getCreatedAt().getTime()));
			preparedstatement.setInt(6, 0);
			preparedstatement.executeUpdate();
			
			System.out.println("[SUCCESS] Inserted to database : " + status.getText());
			/*
			if(!status.getText().startsWith("RT")){
				System.out.println("[SUCCESS] Inserted to database : "+status.getText());
			}else {
				System.out.println("[WARNING!] "+status.getText());
			}
			*/
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println("[FAILED] Failed to insert tweet"+ status.getText());
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
	
	private String FilterNonCharacterString(String str){
		String retval="";
		boolean noncharflagfounded=false;
		int startnonchar=-1,endnonchar=-1;
		
		for (int i = 0; i < str.length(); i++) {
			if(i==0){
				// khusus karakter pertama
				Character c = str.charAt(i);
				if(!Character.isLetterOrDigit(c)){
					retval = retval + "<SYMBOL>";
				}else{
					retval = retval + str.charAt(i);
				}
			}else{
				Character c 	= str.charAt(i);
				Character bc 	= str.charAt(i-1);
				if(isPermitable(bc) && !isPermitable(c)){
					retval = retval + "<SYMBOL>";
				}else if(isPermitable(bc) && isPermitable(c)){
					retval = retval + str.charAt(i);
				}else if(!isPermitable(bc) && isPermitable(c)){
					retval = retval + str.charAt(i);
				}else if(!isPermitable(bc) && !isPermitable(c)){
					// ignore
				}
			}
		}
		
		
		/*
		for (int i = 0; i < str.length(); i++) {
			Character c = str.charAt(i);
			if(!Character.isLetterOrDigit(c)){
				if(!noncharflagfounded){
					startnonchar = i;
					noncharflagfounded=true;
				}else{
					endnonchar  = i;
				}
			}
		}
		
		// Cek dulu apakah character yang di replace hanya 1 buah karakter.
		// Kalau endnonchar nilainya masih nilai default (-1) berarti hanya 1 karakter
		// Ada 3 kemungkinan 
		if((startnonchar == -1) && (endnonchar == -1)){
			// Kalau gak ada karakter aneh-aneh sama sekali
			retval = str;
		}else if((startnonchar != -1) && (endnonchar == -1)){
			// kalau ada karakter aneh-aneh tapi cuma satu karakter.
			for (int i = 0; i < str.length(); i++) {
				if(i==startnonchar){
					retval = retval + "<SYMBOL>";
				}else{
					retval = retval + str.charAt(i);
				}
			}
		}else{
			// Kalau karakter aneh-anehnya lebih dari satu karakter
			for (int i = 0; i < str.length(); i++) {
				if(i==startnonchar){
					retval = retval + "<SYMBOL>";
				}else if((startnonchar<i) && (i<=endnonchar)){
					// do nothing
				}else{
					retval = retval + str.charAt(i);
				}
			}
		}
		*/
		return retval;
	}
	
	private boolean isPermitable(Character c){
		if(Character.isLetterOrDigit(c) || (c.equals('\\')) || (c.equals('/')) || (c.equals('_')) ){
			return true;
		}else{
			return false;
		}
		
	}
	
	private boolean isExist(Long idTweet){
		try {
			preparedstatement = connection.prepareStatement("select twitter_tweet_id from mytomcatapp.raw_tweet where twitter_tweet_id=?");
			preparedstatement.setLong(1, idTweet);
			resultset = preparedstatement.executeQuery();		
			if (resultset.next()) {
				return true;
			}
			return false;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
	
}
