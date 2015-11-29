import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TopTen {

	public static class Map
	extends Mapper<LongWritable, Text, Text, FloatWritable>{


		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, FloatWritable>.Context context)
				throws IOException, InterruptedException {
			 String line = value.toString();
			 String input[] = line.split("\\^");
			 float rating = Float.parseFloat(input[3]);
			 Text bID = new Text(input[2]);
			 context.write(bID, new FloatWritable(rating));
		}
	}
	
	public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable>{
	
TreeMap<FloatWritable, Text>map = new TreeMap<FloatWritable, Text>();
		@Override
		protected void reduce(Text key, Iterable<FloatWritable> val,
				Reducer<Text, FloatWritable, Text, FloatWritable>.Context context)
				throws IOException, InterruptedException {
			
			float sum = 0;
			int count = 0;
			float avg=0;
			
			Iterator<FloatWritable>it = val.iterator();
			
			while(it.hasNext()){
				sum =  sum + it.next().get();
				++count;
			}
			
			if(count!=0){
				avg = (float)sum/count;
			}

			map.put(new FloatWritable(avg), new Text(key));
			if(map.size()>10){
				map.remove(map.firstKey());
			}
			super.reduce(key, val, context);
		}
		
		@Override
		protected void cleanup(
				Reducer<Text, FloatWritable, Text, FloatWritable>.Context context)
				throws IOException, InterruptedException {
			
			Text key;
			FloatWritable value;
			Float f;
			String s;
			for(java.util.Map.Entry<FloatWritable, Text>entry:map.entrySet()){
				value = entry.getKey();
				key = entry.getValue();
				f = value.get();
				s = Float.toString(f);
				context.write(new Text(key), new FloatWritable(value.get()));
			super.cleanup(context);
		}
	}
}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		// get all args
		if (args.length != 2) {
			System.err.println("Usage: TopTen <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "topten"); 
		job.setJarByClass(TopTen.class); 
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);


		// set output key type 
		job.setOutputKeyClass(Text.class);
		// set output value type 
		job.setOutputValueClass(FloatWritable.class);
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		//Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
