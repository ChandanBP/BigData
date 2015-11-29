import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ListBusiness {

	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		
		HashMap<Text, Text>map = new HashMap<Text, Text>();

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			
			String line = value.toString();
			String[] mydata = line.split(",");
			String addr = mydata[1];
			if(addr.startsWith(" PA")){
				String[] input = line.split("\\^");
				map.put(new Text(input[0]), new Text(""));
		  }
		}
		
		@Override
		protected void cleanup(
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
		
            for(java.util.Map.Entry<Text, Text>entry:map.entrySet()){
            	context.write(new Text(entry.getKey().toString()), new Text(entry.getValue().toString()));
            }
			super.cleanup(context);
		}
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			Iterator<Text>it = values.iterator();
			while(it.hasNext()){
				context.write(key, it.next());
			}

			super.reduce(key, values, context);
		}
		
	}
	
	public static void main(String[] args) throws IOException, IllegalArgumentException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		// get all args
		if (args.length != 2) {
			System.err.println("Usage: ListBusiness <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "listbusiness"); 
		job.setJarByClass(ListBusiness.class); 
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);


		// set output key type 
		job.setOutputKeyClass(Text.class);
		// set output value type 
		job.setOutputValueClass(Text.class);
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		//Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}
