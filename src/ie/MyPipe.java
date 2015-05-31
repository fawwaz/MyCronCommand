package ie;

import ie.model.Raw_tweet;
import ie.model.Tb_katadasar;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import cc.mallet.pipe.Pipe;
import cc.mallet.pipe.PrintInputAndTarget;
import cc.mallet.types.Alphabet;
import cc.mallet.types.FeatureSequence;
import cc.mallet.types.FeatureVector;
import cc.mallet.types.FeatureVectorSequence;
import cc.mallet.types.Instance;
import cc.mallet.types.LabelAlphabet;
import cc.mallet.types.LabelSequence;

public class MyPipe extends Pipe{
	
	private static final long serialVersionUID = 1L;
	private static Connection connection = null;
	private static Statement statement = null;
	private static PreparedStatement preparedstatement = null;
	private static ResultSet resultset = null;
	Map<String,String> tb_katadasar;
	ArrayList<String> gazetteer;
	
	MyPipe(boolean isLocal){
		super (new Alphabet(), new LabelAlphabet());
		startConnection(isLocal);
		GetTBKatadasar();
		getGazeteer();
		// retrieve array kamus katadasar dan 
		CloseConnection();
	}
	
	
	/**
	 * Pipe utama main 
	 */
	@Override
	public Instance pipe(Instance carrier) {
		//Setup lokal variabel
		List<String> prev_tokenized = (List<String>) carrier.getData(); // retrieved from previous process
		Alphabet features = getDataAlphabet();
		ArrayList<String> tokenized = new ArrayList<>();
		
		// convert ke arraylist doang
		tokenized.addAll(prev_tokenized);
		
		// buat feature vector
		FeatureVector[] fvs = new FeatureVector[tokenized.size()];
		
		System.out.println(tokenized.size());
		System.out.println("PREV tokenisize"+prev_tokenized.size());
		// Main pipe
		for (int i = 0; i < tokenized.size(); i++) {
			String token = tokenized.get(i);
			
			ArrayList<Integer> featureindices = new ArrayList<>();
			int featureindex = features.lookupIndex(token);
			if(featureindex>=0){
				featureindices.add(featureindex);
			}
			
			
			if(getPOSTag(token)!=null){
				featureindices.add(features.lookupIndex(getPOSTag(token)));
			}
			
			if(token.matches("\\d+")){
				featureindices.add(features.lookupIndex("ISNUMBER"));
			}else if(token.matches("\\p{P}")){
				featureindices.add(features.lookupIndex("PUNCTUATION"));
			}else if(token.matches("(di|@|d|ke|k)")){
				featureindices.add(features.lookupIndex("ISPLACEDIRECTIVE"));
			}else if(token.matches("http://t\\.co/\\w+")){
				featureindices.add(features.lookupIndex("ISURL"));
			}else if(token.matches("@\\w+")){
				featureindices.add(features.lookupIndex("ISMENTION"));
			}else if(token.matches("#\\w+")){
				featureindices.add(features.lookupIndex("ISHASHTAG"));
			}else if(token.matches(Twokenize.varian_bulan)){
				featureindices.add(features.lookupIndex("ISMONTHNAME"));
			}else if(isGazetteer(token)){
				featureindices.add(features.lookupIndex("ISGAZETTEER"));
				System.out.println("YES IT IS GAZETEER :" +token);
			}else{
				// ini nge cek dulu tokenya l-1 dan l+1 sehingga harus i>1 atau i<n-1
				if(i>0 && i<tokenized.size()-1){
					
					String tokenbefore 	= tokenized.get(i-1);
					String tokenafter 	= tokenized.get(i+1);
					if(tokenbefore.matches("\\d+")&& token.matches("[/\\-]") && tokenafter.matches("\\d+")){
						featureindices.add(features.lookupIndex("DATESEPARATOR"));
					}

					if(getPOSTag(tokenbefore)!=null){
						featureindices.add(features.lookupIndex("Prev"+getPOSTag(tokenbefore)));
					}

					if(getPOSTag(tokenafter)!=null){
						featureindices.add(features.lookupIndex("After"+getPOSTag(tokenafter)));
					}
					
				}
			}
			
			int[] featureIndicesArr = new int[featureindices.size()];
			for (int index = 0; index < featureindices.size(); index++) {
				featureIndicesArr[index] = featureindices.get(index);
			}
			
			fvs[i] = new FeatureVector(features, featureIndicesArr);
		}
		
		
		
		carrier.setData(new FeatureVectorSequence(fvs));
		carrier.setTarget(new LabelSequence(getTargetAlphabet()));
		PrintInputAndTarget pipe = new PrintInputAndTarget();
		pipe.pipe(carrier);
		
		return carrier;
	}
	
	/**
	 * Private Functions
	 */
	private void GetTBKatadasar(){
		try{
			preparedstatement = connection.prepareStatement("SELECT * from tb_katadasar");
			resultset = preparedstatement.executeQuery();
			tb_katadasar = new HashMap<>();
			while(resultset.next()){
				tb_katadasar.put(resultset.getString("katadasar"),resultset.getString("tipe_katadasar"));
			}
		}catch(SQLException e){
			e.printStackTrace();
		}
	}

	private void getGazeteer(){
		try{
			preparedstatement = connection.prepareStatement("SELECT * from gazetteer");
			resultset = preparedstatement.executeQuery();
			gazetteer = new ArrayList<>();
			while(resultset.next()){
				gazetteer.add(resultset.getString("location"));
			}
		}catch(SQLException e){
			e.printStackTrace();
		}
	}
	
	private String getPOSTag(String token){
		return tb_katadasar.get(token);
	}
	
	private boolean isGazetteer(String token){
		return gazetteer.contains(token);
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
	
	private static void CloseConnection(){
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
