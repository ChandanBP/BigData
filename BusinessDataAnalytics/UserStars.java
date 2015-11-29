import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class UserStars {

	public static class Map1 extends Mapper<LongWritable, Text, Text, Text>{
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			String input[] = line.split(",");
			
			String address = input[0];
			if(address.contains("Stanford")){
				
				String input1[] = line.split("\\^");
				String bID = input1[0];
				String fullAddress = input1[1];
				
				context.write(new Text(bID), new Text(fullAddress));
			}
			
			//super.map(key, value, context);
		}
	}
	
	public static class Reduce1 extends Reducer<Text, Text, Text, Text>{
		
		HashMap<Text, Text>map = new HashMap<Text, Text>();
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
		
			Iterator<Text>it = values.iterator();
			while(it.hasNext()){
				//context.write(key, it.next());
				
				map.put(new Text(key.toString()), new Text(it.next().toString()));
			}
			super.reduce(key, values, context);
		}
		
		@Override
		protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			for (Map.Entry<Text, Text>entry:map.entrySet()) {
				context.write(entry.getKey(), entry.getValue());
			}
			super.cleanup(context);
		}
		
	}
	
	public static class Map2 extends Mapper<LongWritable, Text, Text, Text>{
		
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
		        	String addr;
			        
		        	while (line != null){
			        	input = line.split("\\s+");
		        		bID = input[0];
		        		addr = input[1];
		        		map.put(new Text(bID), new Text(addr));
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

			String input[] = value.toString().split("\\^");
			
			String uID = input[1];
			String bID = input[2];
			String ratings = input[3];
			
			Object object = map.get(new Text(bID));
			if(object!=null){
				context.write(new Text(uID), new Text(ratings));
			}
			
		}
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		
		// Job1 Details
		Configuration conf1 = new Configuration();
		Job job1 = new Job(conf1, "topten"); 

		job1.setJarByClass(UserStars.class); 
		job1.setMapperClass(Map1.class);
		job1.setReducerClass(Reduce1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		Path inp1 = new Path(args[0]);
		FileInputFormat.addInputPath(job1, inp1);
		Path op1 = new Path(args[1]);
		FileOutputFormat.setOutputPath(job1, op1);
				
		job1.waitForCompletion(true);
		
		Configuration conf2 = new Configuration();
		conf2.set("myFile", "/cbp140230/busyop/part-r-00000");
		Job job2 = new Job(conf2, "details"); 

		job2.setJarByClass(UserStars.class); 
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		
		Path inp2 = new Path(args[2]);
		Path op2 = new Path(args[3]);
		
		MultipleInputs.addInputPath(job2, inp2, TextInputFormat.class,Map2.class);
		FileOutputFormat.setOutputPath(job2, op2);
		
		System.exit(job2.waitForCompletion(true)?0:1);
		
		
	}
}
