import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class BusinessDetails {
	
	public static class Map1 extends Mapper<LongWritable, Text, Text, FloatWritable>{
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, FloatWritable>.Context context)
				throws IOException, InterruptedException {
			
			String pathName = ((FileSplit)context.getInputSplit()).getPath().toString();
			String line = value.toString();
			 String input[] = line.split("\\^");
			 float rating = Float.parseFloat(input[3]);
			 Text bID = new Text(input[2]);
			 context.write(bID, new FloatWritable(rating));
		}
	}
	
	public static class Reduce1 extends Reducer<Text, FloatWritable, Text, FloatWritable>{
		
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
			for(java.util.Map.Entry<FloatWritable, Text>entry:map.entrySet()){
				value = entry.getKey();
				key = entry.getValue();
				context.write(new Text(key+","), new FloatWritable(value.get()));
			super.cleanup(context);
		}
	}
}
	
	@SuppressWarnings("deprecation")
	public static class Map3 extends Mapper<LongWritable, Text, Text, Text>{
		
		HashMap<Text, Text>map = new HashMap<Text, Text>();
		
		@Override
		protected void setup(
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

           Configuration conf = context.getConfiguration();
           String myfilepath = conf.get("myFile");
           Path part=new Path(myfilepath);
           FileSystem fs = FileSystem.get(conf);
           FileStatus[] fss = fs.listStatus(part);
           for (FileStatus status : fss) {
		        Path pt = status.getPath();
		        
		        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
		        String line;
		        line=br.readLine();
		        String input[];
	        	String bID;
	        	String ratings;
		        
		        
		        while (line != null){
		            
		        	input = line.split(",");
	        		   bID = input[0];
	        		   ratings = input[1].trim();
	        		   map.put(new Text(bID), new Text(ratings));
	        		   line = br.readLine();
		        }
		       br.close();
		    }
           
			super.setup(context);
		}
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
		
			String line = value.toString();
			String input[] = line.split("\\^");
			String ratings;
			
			Object object = map.get(new Text(input[0]));
			if(object!=null){
				ratings = map.get(new Text(input[0])).toString();
				context.write(new Text(input[0]), new Text(input[1]+" "+input[2]+" "+ratings));
			}
		}
		
	}
	
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		
		if(args.length!=4){
			System.err.println("Usage: BusinessDetails <in1> <out1> <in2> <out2>");
			System.exit(2);
		}
		
		// Job1 Details
		Configuration conf1 = new Configuration();
		Job job1 = new Job(conf1, "topten"); 

		job1.setJarByClass(BusinessDetails.class); 
		job1.setMapperClass(Map1.class);
		job1.setReducerClass(Reduce1.class);
		job1.setNumReduceTasks(1);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(FloatWritable.class);

		Path inp1 = new Path(args[0]);
		FileInputFormat.addInputPath(job1, inp1);
		Path op1 = new Path(args[1]);
		FileOutputFormat.setOutputPath(job1, op1);
		
		job1.waitForCompletion(true);
		
		// Job2 Details
		Configuration conf2 = new Configuration();
		conf2.set("myFile", "/cbp140230/reviewop/part-r-00000");
		Job job2 = new Job(conf2, "details"); 

		job2.setJarByClass(BusinessDetails.class); 
		job2.setNumReduceTasks(1);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		//job2.addCacheFile(new URI(args[1]+"/part-r-00000"));
		
		Path inp3 = new Path(args[2]);
		Path op2 = new Path(args[3]);
		
		MultipleInputs.addInputPath(job2, inp3, TextInputFormat.class,Map3.class);
		FileOutputFormat.setOutputPath(job2, op2);
		
		System.exit(job2.waitForCompletion(true)?0:1);
		
	}

}
