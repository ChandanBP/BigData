package DataCollection;

import java.io.*;
import java.util.logging.Logger;
import java.util.Map;

import twitter4j.*;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;


public class FindFriendsFollowers implements Serializable{
    private final Logger logger = Logger.getLogger(FindFriendsFollowers.class.getName());

    public static void main(String[] args) {
        new FindFriendsFollowers().publish();
    }

    private void publish() {

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setOAuthConsumerKey("H50j94MxwxyI1uMnbEpQP8NBK");
        cb.setOAuthConsumerSecret("3SmOcJ5o30HskrOwBC7QtxJqkvOrZij4taBL7vAbNTM8gt2qjf");
        cb.setOAuthAccessToken("2393022937-raQQTQyhEsa4EgI0eWmy5gMUXhNfL040xb3ZYd6");
        cb.setOAuthAccessTokenSecret("UBqTdwrkIC2P12NTO4Ad1Tx6rXaiV9XtoG3lGBl6ib7vg");

       // cb.setUseSSL(true);


        Twitter twitter = new TwitterFactory(cb.build()).getInstance();
        try {

            AccessToken accessToken = null;
            while(accessToken == null){
                logger.fine("Open the following URL and grant access to your account:");
                accessToken = twitter.getOAuthAccessToken();
            }

            //BufferedWriter bw = new BufferedWriter(new FileWriter(new File("followersList.txt")));

            IDs followerIDs = twitter.getFollowersIDs("sterlingvoth",-1);
            long[] ids = followerIDs.getIDs();
            for (long id : ids) {
                User user = twitter.showUser(id);
                System.out.println("sterlingvoth,"+user.getScreenName());
                //line = user.getScreenName()+",";
              //  bw.write(line);
            }

        } catch (TwitterException e) {
            e.printStackTrace();
        } //catch (IOException e) {
            //e.printStackTrace();
       // }
    }
}

