package DataCollection;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;


public class TopicModelling implements Serializable{

    public static void main(String args[]){

        // To find top two topics
        String file = "topicComposition.txt";
        SparkConf conf = new SparkConf().setAppName("TwitterDataAnalysis").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile(file);


        JavaPairRDD<Double,Integer> pairRDD = lines.mapToPair(new PairFunction<String, Double, Integer>() {

            public Tuple2<Double, Integer> call(String s) throws Exception {

                int index=-1;
                double val;

                String input[];
                input = s.split("\\s+");

                double max = Double.MIN_VALUE;
                for (int i = 2; i <input.length ; i++) {
                    val = Double.parseDouble(input[i])*100;

                    if (val>max){
                        max = val;
                        index = i;
                    }

                }
                return new Tuple2<Double, Integer>(max,index-2);
            }
        });


        JavaPairRDD<Double,Integer>sorted = pairRDD.sortByKey(false);

        final List<Tuple2<Double,Integer>> filterSort = sorted.take(2);

        for (Tuple2<Double, Integer> tuple2 : filterSort) {
            System.out.println(tuple2._1()+" "+tuple2._2());
        }

        // Get the keywords for the top topic
        String keysFile = "topicKeys.txt";

        JavaRDD<String> keyFileLines = sc.textFile(keysFile);

        JavaPairRDD<Integer,String>keyContents = keyFileLines.mapToPair(new PairFunction<String, Integer, String>() {

            public Tuple2<Integer, String> call(String s) throws Exception {

                String input[] = s.split("\\s+");
                int key = Integer.parseInt(input[0]);
                Tuple2<Integer,String>tuple = null;

                for (Tuple2<Double, Integer> tuple2 : filterSort) {

                    if(tuple2._2() == key){
                        tuple =  new Tuple2<Integer, String>(key,s);
                    }
                }

                return tuple;
            }
        });

        for (Tuple2<Integer, String> stringTuple2 : keyContents.collect()) {
            if(stringTuple2!=null) {
                System.out.println(stringTuple2._1() + " " + stringTuple2._2());
            }
        }

    }
}

