package ie.model;

public class Raw_tweet {
	public Long twitter_tweet_id;
	public String tweet;
	
	public Raw_tweet(Long twitter_tweet_id, String tweet) {
		this.twitter_tweet_id = twitter_tweet_id;
		this.tweet = tweet;
	}
	public Long getTwitter_tweet_id() {
		return twitter_tweet_id;
	}
	public void setTwitter_tweet_id(Long twitter_tweet_id) {
		this.twitter_tweet_id = twitter_tweet_id;
	}
	public String getTweet() {
		return tweet;
	}
	public void setTweet(String tweet) {
		this.tweet = tweet;
	}
	
}
