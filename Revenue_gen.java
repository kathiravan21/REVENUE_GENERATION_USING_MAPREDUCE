
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;


public class WordCount {
	
	public static class Map extends Mapper<LongWritable,Text,Text,FloatWritable>{

//		Text x=new Text();
//		IntWritable r=new IntWritable();
		public void map(LongWritable key, Text value,
				Context context)
				throws IOException,InterruptedException {
			
			String line = value.toString();
			
			String s[]=line.split(",");
			//to omit the first row in the dataset
			if(!(s[1].equals("Company Name")))
			context.write(new Text(s[1]), new FloatWritable(Float.parseFloat(s[2])));
            
			
	
			
		}
		
		
	}
	public static class Reduce extends Reducer<Text,FloatWritable,Text,FloatWritable>{

		public void reduce(Text key, Iterable<FloatWritable> values,
				Context context)
				throws IOException,InterruptedException {
			long sum=0;
			// TODO Auto-generated method stub
			for(FloatWritable x: values)
			{
				sum+=x.get();
			}
			context.write(key, new FloatWritable(sum));
			
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		JobConf conf1 = new JobConf(WordCount.class);
		Configuration conf= new Configuration();
		
		
		conf1.setJobName("mywc");
		Job job = new Job(conf,"mywc");
		
		job.setJarByClass(WordCount.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		//conf.setMapperClass(Map.class);
		//conf.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		

		Path outputPath = new Path(args[1]);
			
	        //Configuring the input/output path from the filesystem into the job
	        
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			//deleting the output path automatically from hdfs so that we don't have delete it explicitly
			
		outputPath.getFileSystem(conf).delete(outputPath);
			
			//exiting the job only if the flag value becomes false
			
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
