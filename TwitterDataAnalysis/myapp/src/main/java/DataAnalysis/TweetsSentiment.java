package DataAnalysis;

        import org.apache.spark.SparkConf;
        import org.apache.spark.api.java.*;
        import org.apache.spark.api.java.function.*;
        import org.apache.log4j.Logger;
        import java.io.*;
        import java.util.*;
        import scala.Tuple2;
        import scala.Tuple3;
        import scala.Tuple4;
        import scala.Tuple5;
class Record implements Serializable {
    String id;
    String tweet;
    public Record(String newid,String newtweet){
        id=newid;
        tweet=newtweet;
    }
    // constructor , getters and setters
}
class StopWords implements Serializable{
    private List<String> stopWords;
    private static StopWords _singleton;
    private StopWords(){
        this.stopWords = new ArrayList<String>();
        BufferedReader rd = null;
        try
        {
           // rd = new BufferedReader(
                  //  new InputStreamReader(
                           // this.getClass().getResourceAsStream("/stop-words.txt")));
            rd = new BufferedReader(new FileReader("/files/BagOfWords/stop-words.txt"));
            String line = null;
            while ((line = rd.readLine()) != null)
                this.stopWords.add(line);
        }
        catch (IOException ex)
        {
            Logger.getLogger(this.getClass())
                    .error("IO error while initializing", ex);
        }
        finally
        {
            try {
                if (rd != null) rd.close();
            } catch (IOException ex) {}
        }
    }
    private static StopWords get()
    {
        if (_singleton == null)
            _singleton = new StopWords();
        return _singleton;
    }

    public static List<String> getWords()
    {
        return get().stopWords;
    }
}
class PositiveWords implements Serializable{
    public static final long serialVersionUID = 42L;
    private Set<String> posWords;
    private static PositiveWords _singleton;
    private PositiveWords(){
        this.posWords= new HashSet<String>();
        BufferedReader rd =null;
        try{
          //  rd = new BufferedReader(
            //        new InputStreamReader(
              //              this.getClass().getResourceAsStream("pos-words.txt")));

            rd = new BufferedReader(new FileReader("/files/BagOfWords/pos-words.txt"));
            String line;
            while ((line = rd.readLine()) != null)
                this.posWords.add(line);
        }
        catch (IOException ex)
        {
            Logger.getLogger(this.getClass())
                    .error("IO error while initializing", ex);
        }
        finally
        {
            try {
                if (rd != null) rd.close();
            } catch (IOException ex) {}
        }

    }
    private static PositiveWords get()
    {
        if (_singleton == null)
            _singleton = new PositiveWords();
        return _singleton;
    }

    public static Set<String> getWords()
    {
        return get().posWords;
    }
}
class NegativeWords implements Serializable{
    public static final long serialVersionUID = 42L;
    private Set<String> negWords;
    private static NegativeWords _singleton;
    private NegativeWords()
    {
        this.negWords = new HashSet<String>();
        BufferedReader rd = null;
        try
        {
           // rd = new BufferedReader(
                  //  new InputStreamReader(
                            //this.getClass().getResourceAsStream("neg-words.txt")));
            rd = new BufferedReader(new FileReader("/files/BagOfWords/neg-words.txt"));
            String line;
            while ((line = rd.readLine()) != null)
                this.negWords.add(line);
        }
        catch (IOException ex)
        {
            Logger.getLogger(this.getClass())
                    .error("IO error while initializing", ex);
        }
        finally
        {
            try {
                if (rd != null) rd.close();
            } catch (IOException ex) {}
        }
    }

    private static NegativeWords get()
    {
        if (_singleton == null)
            _singleton = new NegativeWords();
        return _singleton;
    }

    public static Set<String> getWords()
    {
        return get().negWords;
    }
}
public class TweetsSentiment {
    static SparkConf conf;
    static JavaSparkContext sc;
    public static void sentimentAnalysis() {
        //SparkConf conf = new SparkConf().setAppName("twitter analysis");
        //JavaSparkContext context =new JavaSparkContext(conf);
        conf = new SparkConf().setAppName("TwitterDataAnalysis").setMaster("local[2]");
        conf.set("spark.cassandra.connection.host","127.0.0.1");
        conf.set("spark.driver.allowMultipleContexts", "true");
        sc = new JavaSparkContext(conf);
        String logFile = "collectTweets.txt" ;
        JavaRDD<String> file = sc.textFile(logFile);
        JavaRDD<Record> rdd_records = file.map(
                new Function<String,Record>(){
                    public Record call(String line) throws Exception{
                        String[] parts =line.split("\\u007C");
                        Record sd=new Record(parts[1],parts[8]);
                        return sd;
                    }
                }
        );
        JavaPairRDD<String,String> tweetinfo = rdd_records.mapToPair(
                new PairFunction<Record,String,String>(){
                    public Tuple2<String,String> call(Record record){
                        return new Tuple2<String,String>(record.id,record.tweet.split("https")[0]);
                    }
                }
        );
        JavaRDD<Tuple2<String,String>> tweetsFiltered = tweetinfo.map(
                new Function<Tuple2<String,String>,Tuple2<String,String>>(){
                    public Tuple2<String,String> call(Tuple2<String,String> tweets){
                        String text = tweets._2();
                        text = text.replaceAll("[^a-zA-Z\\s]", "").trim().toLowerCase();
                        return new Tuple2<String,String>(tweets._1(),text);
                    }
                }
        );
        tweetsFiltered = tweetsFiltered.map(
                new Function<Tuple2<String, String>, Tuple2<String, String>>(){
                    public Tuple2<String, String> call(Tuple2<String, String> tweets)
                    {
                        String text = tweets._2();
                        List<String> stopWords = StopWords.getWords();
                        for (String word : stopWords)
                        {
                            text = text.replaceAll("\\b" + word + "\\b", "");
                        }
                        return new Tuple2<String, String>(tweets._1(), text);
                    }
                }
        );
        JavaPairRDD<Tuple2<String,String>,Float> positiveTweets = tweetsFiltered.mapToPair(
                new PairFunction<Tuple2<String,String>,Tuple2<String,String>,Float>(){
                    public Tuple2<Tuple2<String,String>,Float> call(Tuple2<String,String> tweets){
                        String text = tweets._2();
                        Set<String> posWords = PositiveWords.getWords();
                        String[] words = text.split(" ");
                        int numWords = words.length;
                        int numPosWords = 0;
                        for (String word : words)
                        {
                            if (posWords.contains(word))
                                numPosWords++;
                        }
                        return new Tuple2<Tuple2<String,String>,Float>(
                                new Tuple2<String,String>(tweets._1(),tweets._2()),(float) numPosWords/numWords);
                    }
                }
        );
        JavaPairRDD<Tuple2<String,String>,Float> negativeTweets = tweetsFiltered.mapToPair(
                new PairFunction<Tuple2<String,String>,Tuple2<String,String>,Float>(){
                    public Tuple2<Tuple2<String,String>,Float> call(Tuple2<String,String> tweets){
                        String text = tweets._2();
                        Set<String> negWords = NegativeWords.getWords();
                        String[] words = text.split(" ");
                        int numWords = words.length;
                        int numPosWords = 0;
                        for (String word : words)
                        {
                            if (negWords.contains(word))
                                numPosWords++;
                        }
                        return new Tuple2<Tuple2<String,String>,Float>(new Tuple2<String,String>(tweets._1(),tweets._2()),(float) numPosWords/ numWords);
                    }
                }
        );
        JavaPairRDD<Tuple2<String,String>,Tuple2<Float,Float>> joined = positiveTweets.join(negativeTweets);
        JavaRDD<Tuple4<String, String, Float, Float>> scoredTweets =
                joined.map(new Function<Tuple2<Tuple2<String, String>,
                        Tuple2<Float, Float>>,
                        Tuple4<String, String, Float, Float>>() {
                    public Tuple4<String, String, Float, Float> call(
                            Tuple2<Tuple2<String, String>, Tuple2<Float, Float>> tweets)
                    {
                        return new Tuple4<String, String, Float, Float>(
                                tweets._1()._1(),
                                tweets._1()._2(),
                                tweets._2()._1(),
                                tweets._2()._2());
                    }
                });
        JavaRDD<Tuple5<String,String,Float,Float,String>> result =
                scoredTweets.map(
                        new Function<Tuple4<String,String,Float,Float>,Tuple5<String,String,Float,Float,String>>(){
                            public Tuple5<String,String,Float,Float,String> call(Tuple4<String,String,Float,Float> tweets){
                                String score;
                                if (tweets._3() > tweets._4())
                                    score = "positive";
                                else if (tweets._3() == tweets._4())
                                    score = "neutral";
                                else	score = "negative";
                                return new Tuple5<String, String, Float, Float, String>(
                                        tweets._1(),
                                        tweets._2(),
                                        tweets._3(),
                                        tweets._4(),
                                        score);
                            }
                        }
                ).distinct();
        try{
            BufferedWriter bw = new BufferedWriter(new FileWriter(new File("SentimentAnalysisOutput.txt")));
            String line;
            bw.newLine();

            line="Print the sentiment of each tweet: TweetID   TweetText:::::Polarity";
            bw.write(line);
            bw.newLine();
            bw.newLine();
            for (Tuple5<String, String, Float, Float, String> tweets : result.collect()) {
                if(tweets!=null){
                    line = tweets._1()+"    "+tweets._2()+":::::"+tweets._5();
                    bw.write(line);
                    bw.newLine();
                }
            }

            bw.close();
        }catch(Exception e){
            System.out.println("failed to write to file in Load Tables "+e);
        }
    }
}