package DataAnalysis;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.joda.time.Instant;
import scala.Tuple2;
import scala.Tuple5;
import twitter4j.*;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class TweetsAnalysis{

    static Session session;
    static SparkConf conf;
    static JavaSparkContext sc;
    static HashMap<String,String>map = new HashMap<String, String>();
    static String owner;
    static String retweetUserList;
    static String retweetText;
    static JavaPairRDD<String,Set<String>>userFriends;
    static ConfigurationBuilder cb = new ConfigurationBuilder();
    static Twitter twitter;
    static CassandraConnector connector;
    static AccessToken accessToken;
    static Broadcast<List<Tuple2<String,String>>>cacheData;
    static Broadcast<List<Tuple2<String,String>>>cacheChildParentCount;


    public static void findOwner(){

        JavaRDD<String> tweetFile;
        String file = "collectedTweetsForTopic.txt";
        tweetFile = sc.textFile(file);
        JavaPairRDD<String,Long>userTweetCount = tweetFile.mapToPair(new PairFunction<String, String, Long>() {

            public Tuple2<String, Long> call(String s) throws Exception {

                Tuple2<String,Long>tuple=null;
                String input[] = s.split("\\|");
                String text = input[8];
                String user;
                long count = Integer.parseInt(input[6]);

                int fromIndex;
                int toIndex;

                if(count>0){

                    fromIndex = text.indexOf("RT");
                    toIndex = text.indexOf(":",fromIndex);

                    if(fromIndex!=-1&&toIndex!=-1){

                        fromIndex = text.indexOf("@",fromIndex)+1;
                        toIndex-=1;
                        user = text.substring(fromIndex,toIndex);
                        user = user+"|"+text;
                        tuple = new Tuple2<String, Long>(user,new Long(count));
                    }
                    else{
                        tuple = new Tuple2<String, Long>("",new Long(0));
                    }
                }
                else{
                    tuple = new Tuple2<String, Long>("",new Long(0));
                }
                return tuple;
            }
        });

        JavaPairRDD<String,Long>distinctUser = userTweetCount.distinct(); 
        JavaPairRDD<Long,String>swap = distinctUser.mapToPair(new PairFunction<Tuple2<String, Long>, Long, String>() {

            public Tuple2<Long, String> call(Tuple2<String, Long> stringLongTuple2) throws Exception {

                if(stringLongTuple2!=null){
                    return new Tuple2<Long, String>(stringLongTuple2._2(),stringLongTuple2._1());
                }
                return null;
            }
        }) ;

        JavaPairRDD<Long,String>sorted = swap.sortByKey(false);
        List<Tuple2<Long,String>>maxRetweets = sorted.take(1);

try{
	BufferedWriter bw = new BufferedWriter(new FileWriter(new File("AnalysisOutput.txt")));
	String line;
	line = "Printing details of Tweet with Highest Retweets:";
	 bw.write(line);
     bw.newLine();
	for (Tuple2<Long, String> retweet : maxRetweets) {
            if(retweet!=null){
                String input[] = retweet._2().split("\\|");
                owner = input[0];
                retweetText = input[1];
                line = "User "+input[0]+" has highest retweet count="+retweet._1()+" whose text is "+input[1];
                System.out.println(line);
                bw.write(line);
                bw.newLine();
            }
        }
	bw.close();
}catch(Exception e){
	System.out.println("failed to write to file in findOwner"+e);
}
    }

   public static void sortUsers(){

       String file = "collectedTweetsForTopic.txt";
       JavaRDD<String> data = sc.textFile(file);

       JavaPairRDD<String,String>timeStampAsKey = data.mapToPair(new PairFunction<String, String, String>() {

           public Tuple2<String, String> call(String s) throws Exception {

               Tuple2<String,String>tuple;
               String input[] = s.split("\\|");

               String user = input[0];
               String id = input[1];
               String timeStamp = input[2];
               String tweet = input[8];
             //  UserNode userNode;

               if(tweet.contains(retweetText)){
                   if(map.get(id)==null){

                       map.put(id,user);
                       tuple = new Tuple2<String, String>(timeStamp,s);
                       return tuple;
                   }
               }

               tuple = new Tuple2<String, String>("","");
               return tuple;
           }
       });

       JavaPairRDD<String,String>uniqueTimeStamp = timeStampAsKey.distinct();

       for (Tuple2<String, String> stringTuple2 : uniqueTimeStamp.collect()) {

           if(stringTuple2!=null) {
               String input[] = stringTuple2._2().split("\\|");
               if(!stringTuple2._2().equals("")){
                   String query = "insert into TweetAnalysis.SortedData(id,name,retweetcount,time) values ('"+input[1]+"','"+input[0]+"',"+Long.parseLong(input[6])+",'"+input[2]+"')";
                   session.execute(query);
               }
           }
       }
   }

    public static void loadTables(){

        SparkContextJavaFunctions function =  CassandraJavaUtil.javaFunctions(sc);

        JavaRDD<CassandraRow>javaRDD = function.cassandraTable("tweetanalysis","sorteddata");

        JavaPairRDD<String,String>timeStampData = javaRDD.mapToPair(new PairFunction<CassandraRow, String, String>() {
            public Tuple2<String, String> call(CassandraRow cassandraRow) throws Exception {

                Tuple2<String,String>tuple;
                String key = cassandraRow.getString("time");
                String value = cassandraRow.getString("id")+","+cassandraRow.getString("name")+","+cassandraRow.getLong("retweetcount");
                tuple = new Tuple2<String, String>(key,value);
                return tuple;
            }
        });

        JavaPairRDD<String,String>sortedData = timeStampData.sortByKey();
        cacheData =  sc.broadcast(sortedData.collect());
        //System.out.println(cacheData.value());
       List<Tuple2<String,String>>reTweets = cacheData.getValue();
       String retweetUsers;
       retweetUserList = owner;
       try{
           BufferedWriter bw = new BufferedWriter(new FileWriter(new File("AnalysisOutput.txt"),true));
           String line;
           bw.newLine();
           bw.newLine();
           line="Printed the order in which retweets occured based on tweet timestamp:";
           bw.write(line);
           bw.newLine();
       for (Tuple2<String, String> tweets : reTweets) {
           if(tweets!=null){
        	   String input[] = tweets._2().split("\\,");
        	   retweetUsers = input[1];
        	   retweetUserList = retweetUserList+","+retweetUsers;
               line = tweets._1+"|"+retweetUsers;
              bw.write(line);
              bw.newLine();
           }
       }

       bw.close();
   }catch(Exception e){
	   System.out.println("failed to write to file in Load Tables "+e);
   }
       System.out.println("reTweet User List: "+retweetUserList);
    }

    public static void connect(){
        connector = CassandraConnector.apply(conf);
        session = connector.openSession();
        //session.execute("CREATE KEYSPACE TweetAnalysis WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");

        session.execute("DROP TABLE IF EXISTS TweetAnalysis.SortedData");
        session.execute("CREATE TABLE TweetAnalysis.SortedData (id TEXT PRIMARY KEY, name TEXT, retweetCount bigint,time TEXT)");
        session.close();
    }

    public static void assignParent(){
        final List<Tuple2<String,String>> sortedData = cacheData.value();


        JavaPairRDD<String,String>pairRDD = sc.parallelize(sortedData).mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {

            public Tuple2<String, String> call(Tuple2<String, String> keyValue) throws Exception {

                Tuple2<String,String>parentChild = null;
                String user;
                String value[] = keyValue._2().split(",");

                user = value[1];


                Set<String>parents = new HashSet<String>();
                String parent;

                int index = 0;
                for (Tuple2<String, String> tuple2 : sortedData) {
                    parent = tuple2._2().split(",")[1];
                    if(!parent.equals(user)){
                        index++;
                        parents.add(parent);
                    }
                    else{
                        break;
                    }
                }


                String query = "select * from tweetanalysis.frienfollower where user = '"+user+"'";
                ResultSet resultSet =  session.execute(query);
                Iterator<Row> ite = resultSet.iterator();
                Row row;
                String s,p="",plist="";
                String input[];

                long numParents=0;
                while (ite.hasNext()){
                    row = ite.next();
                    s = row.getString("friends");
                    if(s.equals("")){
                        return new Tuple2<String, String>("","");
                    }
                    input = s.split("\\|");

                    for (String s1 : input) {

                           if(!parents.add(s1)){
                                p = s1;
                                plist = s1+","+plist;
                                numParents++;
                            }
                    }
                }

                if(numParents==0){
                    parentChild = new Tuple2<String, String>(user,owner);
                }
                else if(numParents==1){

                    parentChild = new Tuple2<String, String>(user,p);
                }
                else{
                    // Tie breaking. Get parent with highest history retweet count.
                    parentChild = new Tuple2<String, String>(user,plist);
                }

                return parentChild;
            }
        });

        final List<Tuple2<String,String>> historyRetweets = cacheChildParentCount.value();
        JavaPairRDD<String,String>javaPairRDD =  sc.parallelizePairs(historyRetweets);
        JavaPairRDD<String,Tuple2<Iterable<String>,Iterable<String>>>finalRDD = pairRDD.cogroup(javaPairRDD);

        JavaPairRDD<String,String>degree = finalRDD.mapToPair(new PairFunction<Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>>, String, String>() {

            public Tuple2<String, String> call(Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>> stringTuple) throws Exception {

                Tuple2<String, String> assigningParent = null;
                String user = stringTuple._1();
                String usersList[] = retweetUserList.split(",");

                Iterator<String> sp1 = stringTuple._2()._1().iterator();
                Iterator<String> sp2 = stringTuple._2()._2().iterator();

                String input1[];
                String input2[];
                int index;
                String finalParent=owner,possibleParents="";
                int retweetCount=0;

                while(sp1.hasNext()){
                    input1 = sp1.next().split(",");
                    if(input1.length == 1) {
                        assigningParent = new Tuple2<String, String>(user, input1[0]);
                    }
                    else if(input1.length>1){
                        for(int j=0;j<usersList.length;j++){
                            if(usersList[j].equals(user))
                                break;
                            else
                                possibleParents=possibleParents+usersList[j];
                        }
                                        while (sp2.hasNext()) {
                                             input2 = sp2.next().split(",");
                                            for(int i=0;i<input1.length;i++) {
                                                if (input2[0].equals(input1[i]) && Integer.parseInt(input2[1]) > retweetCount) {
                                                    finalParent = input2[0];
                                                }
                                                else if(finalParent.equals(owner) && possibleParents.contains(input1[i])) {
                                                    finalParent = input1[i];
                                                }
                                            }
                                        }

                        assigningParent = new  Tuple2<String, String>(user,finalParent);
                               }

                }
                return assigningParent;
            }
        });

        JavaPairRDD<String,String>reverseRDD = degree.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
            public Tuple2<String, String> call(Tuple2<String, String> stringStringTuple2) throws Exception {

                if (stringStringTuple2!=null) {
                    return new Tuple2<String, String>(stringStringTuple2._2(), stringStringTuple2._1());
                }
                return new Tuple2<String, String>("", "");
            }
        });


        JavaPairRDD<String,Iterable<String>>parentChildren = reverseRDD.groupByKey();

        JavaPairRDD<Long,String>parentChildCount = parentChildren.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, Long,String>() {
            public Tuple2<Long,String> call(Tuple2<String, Iterable<String>> stringIterableTuple2) throws Exception {

                Iterator<String>ite = stringIterableTuple2._2().iterator();
                long count = 0;

                if(stringIterableTuple2._1().equals("")){
                    count = -1;
                }
                else {
                    while (ite.hasNext()){
                        count++;
                        ite.next();
                    }
                }

                return new Tuple2<Long,String>(new Long(count),stringIterableTuple2._1());
            }
        });

        JavaPairRDD<Long,String>sort = parentChildCount.sortByKey(false);
        List<Tuple2<Long,String>>informationController = sort.take(1);

        String infoController = "";
        long childCount=0;
        for (Tuple2<Long, String> tuple2 : informationController) {
            if(tuple2!=null) {
                infoController = tuple2._2();
                childCount =  tuple2._1();
                  }
        }


        String cUser="", cParent="";


        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(new File("AnalysisOutput.txt"), true));
            String line;
            line = "'User' --> 'Parent User' List:";
            bw.newLine();
            bw.newLine();
            bw.write(line);
            bw.newLine();

            long childIndex=-1;
            List<Tuple2<Tuple2<String,String>,Long>>listWithIndex =  degree.zipWithIndex().collect();
            HashMap<String,Long>userIndexMap = new HashMap<String, Long>();

             if(!listWithIndex.isEmpty()){

                Iterator<Tuple2<Tuple2<String,String>,Long>>ite =  listWithIndex.iterator();

                Tuple2<Tuple2<String,String>,Long>tuple;
                String user;
                String parent;
                while(ite.hasNext()){

                    tuple = ite.next();
                    user = tuple._1()._1();
                    parent = tuple._1()._2();

                    if(user.equals(infoController)){
                        childIndex = tuple._2();
                        cUser = user;
                        cParent = parent;
                                            }
                    line = user + " --> " + parent;
                    userIndexMap.put(user,tuple._2());
                    bw.write(line);
                    bw.newLine();

                }
            }


            line = "The User from whom  maximum RE-Tweets occurred is: [" + infoController + "] with re-tweet user count: " + childCount;
            bw.newLine();
            bw.write(line);
            bw.newLine();
            line = "User controlling the flow of information is: ["+cParent+"] who is the parent of ["+cUser+"] , the user with maximum RE-Tweet users";
            bw.newLine();
            bw.write(line);
            String topMostParent = "";
            Tuple2<Tuple2<String,String>,Long>path;
            String user,parent="";
            long parentIndex = childIndex;
            bw.newLine();
            bw.newLine();
            line = "Printing the path in which maximum information flow occurred:";
            bw.write(line);
            bw.newLine();
            while(!parent.equals(owner)){

                path = listWithIndex.get((int)parentIndex);
                user = path._1()._1();
                parent = path._1()._2();
                line = user + " <- "+parent;
                bw.write(line);
                bw.newLine();
                if(userIndexMap.get(parent)!=null){
                    parentIndex = userIndexMap.get(parent);
                }
            }
            bw.close();
        }catch(Exception e){
            System.out.println("Exception while writing to file");
        }
    }



    public static void setHistoryRetweets(){

        JavaRDD<String>historyReweetRDD = sc.textFile("historyRetweets.txt");

        JavaPairRDD<String,Long>emitOne = historyReweetRDD.mapToPair(new PairFunction<String, String, Long>() {

            public Tuple2<String, Long> call(String s) throws Exception {

                String input1[] = s.split("\\|");
                //String input2[] = input1[1].split("@");

                String child = input1[0];
                String parent = input1[1];

                Tuple2<String,Long>tuple = new Tuple2<String, Long>(child+","+parent,new Long(1));

                return tuple;
            }
        });


        JavaPairRDD<String,Long>emitCount = emitOne.reduceByKey(new Function2<Long, Long, Long>() {
            public Long call(Long aLong, Long aLong2) throws Exception {
                return aLong+aLong2;
            }
        });

        JavaPairRDD<String,String>keyValue = emitCount.mapToPair(new PairFunction<Tuple2<String, Long>, String, String>() {

            public Tuple2<String, String> call(Tuple2<String, Long> stringLongTuple2) throws Exception {

                Tuple2 tuple;
                String input[] = stringLongTuple2._1().split(",");
                String child = input[0];
                String parent = input[1];

                return new Tuple2<String, String>(child,parent+","+stringLongTuple2._2());
            }
        });

//        for (Tuple2<String, String> tuple2 : keyValue.collect()) {
//            System.out.println(tuple2._1()+","+tuple2._2());
//        }
        cacheChildParentCount = sc.broadcast(keyValue.collect());

    }
    public static void timelineAnalysis(){

        String minTime="", maxTime="", line="";
        String tweetId, tweetTime, tweetText;
        Date minDate = null,maxDate = null,newDate = null,time = null,afterAddingMins;
        long newdiff, maxDiff, minuteDiff;
        long newdiffMinutes,newdiffHours,newdiffDays,interval = 0;
        int keyCounter = 0;
        long curTimeInMs,ONE_MINUTE_IN_MILLIS=60000;
        SimpleDateFormat format = new SimpleDateFormat("YYYY-MM-DD HH:mm:ss");
        String[] timeInterval;
        String dte;

        String query1 = "select min(tweettime) as mintime,max(tweettime) as maxtime from tweetanalysis.tweettimelinedata";
        ResultSet resultSet =  session.execute(query1);
        Row row;
        Iterator<Row> ite = resultSet.iterator();
        while (ite.hasNext()){

            row = ite.next();
            minTime = row.getString("mintime");
            maxTime = row.getString("maxtime");
        }

        Calendar c = Calendar.getInstance();
        Calendar c1 = Calendar.getInstance();
        Calendar c2 = Calendar.getInstance();
        String[] minTimeString, maxTimeString, timeString;

        // Minimum Date
        minTimeString = minTime.split("-");
        c.set(Calendar.YEAR,Integer.parseInt(minTimeString[0]));
        c.set(Calendar.MONTH, Integer.parseInt(minTimeString[1]));
        String getTime[]= minTimeString[2].split(" ");
        String getTime1[] = getTime[1].split(":");
        c.set(Calendar.DATE, Integer.parseInt(getTime[0]));
        c.set(Calendar.HOUR,Integer.parseInt(getTime1[0]));
        c.set(Calendar.MINUTE,Integer.parseInt(getTime1[1]));
        c.set(Calendar.SECOND,Integer.parseInt(getTime1[2]));
        minDate=new Date(c.getTimeInMillis());

        //Maximum Date
        maxTimeString = maxTime.split("-");
        c1.set(Calendar.YEAR,Integer.parseInt(maxTimeString[0]));
        c1.set(Calendar.MONTH, Integer.parseInt(maxTimeString[1]));
        String getTime2[]= maxTimeString[2].split(" ");
        String getTime3[] = getTime2[1].split(":");
        c1.set(Calendar.DATE, Integer.parseInt(getTime2[0]));
        c1.set(Calendar.HOUR,Integer.parseInt(getTime3[0]));
        c1.set(Calendar.MINUTE,Integer.parseInt(getTime3[1]));
        c1.set(Calendar.SECOND,Integer.parseInt(getTime3[2]));

        maxDate=new Date(c1.getTimeInMillis());
       try {
           BufferedWriter bw = new BufferedWriter(new FileWriter(new File("SampledTimelineData.txt")));

        String query2 = "select tweetid,tweettime,tweettext from tweetanalysis.tweettimelinedata";
        ResultSet rs =  session.execute(query2);
        Row row1;

        Iterator<Row> ite1 = rs.iterator();
        while (ite1.hasNext()){
            row1 = ite1.next();
            tweetId = row1.getString("tweetid");
            tweetTime = row1.getString("tweettime");
            tweetText = row1.getString("tweettext");
            System.out.println("checking for: "+tweetId+"  "+tweetTime);
            keyCounter = 0;
               newDate = minDate;
               //time = format.parse(tweetTime);
            timeString = tweetTime.split("-");
            c2.set(Calendar.YEAR,Integer.parseInt(timeString[0]));
            c2.set(Calendar.MONTH, Integer.parseInt(timeString[1]));
            String getTime4[]= timeString[2].split(" ");
            String getTime5[] = getTime4[1].split(":");
            c2.set(Calendar.DATE, Integer.parseInt(getTime4[0]));
            c2.set(Calendar.HOUR,Integer.parseInt(getTime5[0]));
            c2.set(Calendar.MINUTE,Integer.parseInt(getTime5[1]));
            c2.set(Calendar.SECOND,Integer.parseInt(getTime5[2]));

             time=new Date(c2.getTimeInMillis());
            newdiff =  time.getTime() - newDate.getTime();
               maxDiff = maxDate.getTime() - newDate.getTime();
               newdiffMinutes = (newdiff / (60 * 1000)) % 60;
               newdiffHours = (newdiff / (60 * 60 * 1000)) % 24;
               newdiffDays = (newdiff / (24 * 60 * 60 * 1000));

               if(newdiffHours > 0 || newdiffDays > 0){
                   interval = 6;
               }
            else
                   interval = newdiffMinutes;

               while(interval > 5 && maxDiff > 0){
                   curTimeInMs = newDate.getTime();
                   afterAddingMins = new Date(curTimeInMs + (5 * ONE_MINUTE_IN_MILLIS));
                   newDate = afterAddingMins;
                   newdiff =  time.getTime() - newDate.getTime();;
                   maxDiff = maxDate.getTime() - newDate.getTime();
                   newdiffMinutes = (newdiff / (60 * 1000)) % 60;
                   newdiffHours = (newdiff / (60 * 60 * 1000)) % 24;
                   newdiffDays = (newdiff / (24 * 60 * 60 * 1000));
                   if(newdiffHours > 0 || newdiffDays > 0 ){
                         interval = 6;
                   }
                   else
                       interval = newdiffMinutes;

                   keyCounter = keyCounter + 1;

               }

            dte = format.format(newDate);
            timeInterval = dte.split(" ");
            line = keyCounter+"|"+timeInterval[1]+"|"+tweetId+"|"+tweetTime+"|"+tweetText;
            bw.write(line);
            bw.newLine();
           }

        bw.close();

       }catch (Exception e){
           System.out.println("Exception occured: "+e);
       }

        JavaRDD<String>timeLineDataRDD = sc.textFile("SampledTimelineData.txt");

        JavaRDD<Tuple5<String,String,String,String,String>> tweetinfo = timeLineDataRDD.map(
                new Function<String,Tuple5<String,String,String,String,String>>(){
                    public Tuple5<String,String,String,String,String> call (String line) throws Exception{
                        String[] parts =line.split("\\u007C");
                        return new Tuple5<String,String,String,String,String>(parts[0],parts[1],parts[2],parts[3],parts[4]);

                    }
                }
        );
        JavaPairRDD<String,String> tweetpair = tweetinfo.mapToPair(
                new PairFunction<Tuple5<String,String,String,String,String>,String,String>(){
                    public Tuple2<String,String> call(Tuple5<String,String,String,String,String> tweets){
                        String text1=tweets._1()+"|"+tweets._2();
                        return new Tuple2<String,String>(text1,tweets._5());
                    }
                }
        );
        JavaPairRDD<String,Integer> grouptweet = tweetpair.mapToPair(
                new PairFunction<Tuple2<String,String>,String,Integer>(){
                    public Tuple2<String,Integer> call(Tuple2<String,String> tweets){
                        String text1=tweets._1()+"|"+tweets._2();
                        return new Tuple2<String,Integer>(text1,1);
                    }
                }
        );
        JavaPairRDD<String,Integer> counts =grouptweet.reduceByKey(
                new Function2<Integer,Integer,Integer>(){
                    public Integer call(Integer a, Integer b) { return a + b; }
                }
        );
        JavaPairRDD<String,Tuple2<String,Integer>> tweetgroup = counts.mapToPair(
                new PairFunction<Tuple2<String,Integer>,String,Tuple2<String,Integer>>(){
                    public Tuple2<String,Tuple2<String,Integer>> call(Tuple2<String,Integer> tweets){
                        String[] parts=tweets._1().split("\\u007C");
                        String text= parts[1];
                        return new Tuple2<String,Tuple2<String,Integer>>(text,new Tuple2<String,Integer>(parts[2],tweets._2()));
                    }
                }
        );


        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(new File("AnalysisOutput.txt"), true));
            String writeline;
            bw.newLine();
            bw.newLine();
            writeline = "Printing the Time Line for Tweets:";
            bw.write(writeline);
            bw.newLine();
            for (Tuple2<String, Iterable<Tuple2<String, Integer>>> tuple2 : tweetgroup.groupByKey().sortByKey().collect()) {
                writeline = "@Interval: " + tuple2._1();
                bw.write(writeline);
                bw.newLine();
                Iterator<Tuple2<String, Integer>> sp1 = tuple2._2().iterator();
                while (sp1.hasNext()) {
                    writeline = "    " + sp1.next();
                    bw.write(writeline);
                    bw.newLine();
                }
            }
            bw.close();
        }catch(Exception e){
           System.out.println("Exception while writing to file");
        }

    }

    public static void main(String args[]){

        conf = new SparkConf().setAppName("TwitterDataAnalysis").setMaster("local[2]");
        conf.set("spark.cassandra.connection.host","127.0.0.1");
        conf.set("spark.driver.allowMultipleContexts", "true");

        sc = new JavaSparkContext(conf);


        // Find the user with the highest retweet count
        findOwner();

        //Create database connection
         connect();

        // Sort the users with retweet text of owner according to the timestamp
        session = connector.openSession();
        sortUsers();


        // Load the cassandra table and store in RDD
        loadTables();


        setHistoryRetweets();
        assignParent();
        timelineAnalysis();

        TweetsSentiment stAnalysis = new TweetsSentiment();
        stAnalysis.sentimentAnalysis();

        session.close();
    }

}

