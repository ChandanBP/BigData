package DataCollection;

import java.io.*;
import java.util.logging.Logger;
import java.util.ArrayList;
import java.util.Map;

import twitter4j.*;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;


public class FindHistoryRetweets implements Serializable{
    private final Logger logger = Logger.getLogger(FindHistoryRetweets.class.getName());

    public static void main(String[] args) {
        new FindHistoryRetweets().publish();
    }

    private void publish() {

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setOAuthConsumerKey("H50j94MxwxyI1uMnbEpQP8NBK");
        cb.setOAuthConsumerSecret("3SmOcJ5o30HskrOwBC7QtxJqkvOrZij4taBL7vAbNTM8gt2qjf");
        cb.setOAuthAccessToken("2393022937-raQQTQyhEsa4EgI0eWmy5gMUXhNfL040xb3ZYd6");
        cb.setOAuthAccessTokenSecret("UBqTdwrkIC2P12NTO4Ad1Tx6rXaiV9XtoG3lGBl6ib7vg");

        Twitter twitter = new TwitterFactory(cb.build()).getInstance();
        
        String userList = "";
        
        String retweetUList[] = userList.split(",");
        String fileName= "historyRetweets47.txt";
        String user = "sterlingvoth";
      //System.out.println("number of users: "+retweetUList.length);
      try {
      BufferedWriter bw = new BufferedWriter(new FileWriter(new File(fileName)));
        //for(int i=1;i<retweetUList.length;i++){
      	  //System.out.println(retweetUList[i]);
      	  

      	  int numberOfTweets = 1000;
            long lastID = Long.MAX_VALUE;
            Paging pg = new Paging(1, 10);
            String line=null;
            ArrayList<Status> tweets = new ArrayList<Status>();
            while (tweets.size () < numberOfTweets) {
              
                tweets.addAll(twitter.getUserTimeline(user,pg));
                //System.out.println("Gathered " + tweets.size() + " tweets");
                 for (Status t: tweets) 
                  {if(t.getId() < lastID) lastID = t.getId();
                  String tweeted = t.getText();
                  
                  if(tweeted.contains("RT @")){
                  String[] owner = tweeted.split(":");
                  line = user+"|"+owner[0];
                  //System.out.println(line);
                  bw.write(line);
                  bw.newLine();
                  }
                  }
              }
            pg.setMaxId(lastID-1);
       // }
            System.out.println("Completed");
        bw.close();   
            }
              catch (Exception te) {
                System.out.println("Couldn't connect: " + te);
              }; 
    }
}


