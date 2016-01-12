package DataCollection;

/*
 * Package to collect all the Tweets for a particular search word/string
 * Example Search String: paris terrorist attack
 */

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.logging.Logger;
import java.util.Map;
import twitter4j.*;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;


public class TweetsCollection {
    private final Logger logger = Logger.getLogger(TweetsCollection.class.getName());

    public static void main(String[] args) {
        new TweetsCollection().publish();
    }

    private void publish() {
       	        ConfigurationBuilder cb = new ConfigurationBuilder();
    	        cb.setOAuthConsumerKey("H50j94MxwxyI1uMnbEpQP8NBK");
    	        cb.setOAuthConsumerSecret("3SmOcJ5o30HskrOwBC7QtxJqkvOrZij4taBL7vAbNTM8gt2qjf");
    	        cb.setOAuthAccessToken("2393022937-raQQTQyhEsa4EgI0eWmy5gMUXhNfL040xb3ZYd6");
    	        cb.setOAuthAccessTokenSecret("UBqTdwrkIC2P12NTO4Ad1Tx6rXaiV9XtoG3lGBl6ib7vg");

    	        //cb.setUseSSL(true);


    	        Twitter twitter = new TwitterFactory(cb.build()).getInstance();
    	        try {

    	            AccessToken accessToken = null;
    	            while(accessToken == null){
    	                logger.fine("Open the following URL and grant access to your account:");
    	                accessToken = twitter.getOAuthAccessToken();
    	            }
    	            System.out.println(accessToken);
    	            Map<String, RateLimitStatus> rateLimitStatus = twitter.getRateLimitStatus("search");
    	            RateLimitStatus searchTweetsRateLimit = rateLimitStatus.get("/search/tweets");


    	            Query q = new Query("paris terrorist attack victims");	// Search for tweets that contain these two words
    	            q.setCount(100);							
    	            q.getResultType();						// Get all tweets
    	            q.since("2015-11-12");
    	            q.until("2015-11-29");
    	            
    	            q.setLang("en");							// English language tweets, please

    	            QueryResult r = twitter.search(q);			// Make the call

    	            BufferedWriter bw = new BufferedWriter(new FileWriter(new File("collectedTweetsForTopic.txt")));


    	            String line="";
    	            for (Status s: r.getTweets())				// Loop through all the tweets...
    	            {

    	                String tweet = s.getText();
    	                tweet = tweet.replaceAll("(\\r|\\n)","");

    	                line = line+s.getUser().getScreenName()+"|"
    	                        +s.getId()+"|"
    	                        +s.getCreatedAt()+"|"
    	                        +s.getGeoLocation()+"|"
    	                        +s.getInReplyToScreenName()+"|"
    	                        +s.getInReplyToUserId()+"|"
    	                        +s.getRetweetCount()+"|"
    	                        +s.getSource()+"|"
    	                        +tweet+"\n";
    	                bw.write(line);
    	               
    	            }
    	            bw.close();
    	        } catch (TwitterException e) {
    	            e.printStackTrace();
    	        } catch (IOException e) {
    	            e.printStackTrace();
    	        }
    	    }
    	}

