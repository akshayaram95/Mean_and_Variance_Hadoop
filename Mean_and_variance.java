
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Mean_and_variance {
	
	public static class Map extends Mapper<LongWritable, Text, Text, PairWritable>{
		private PairWritable pairMap = new PairWritable();
		private Text word = new Text(); // type of output key
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			word.set("Number");
			pairMap.set(Long.parseLong(value.toString()), (long)1,(long) 0);
			context.write(word,pairMap);
		}
	}
	
	public static class CombineClass extends Reducer<Text, PairWritable, Text, PairWritable>{
		private PairWritable value = new PairWritable();

		public void reduce(Text key, Iterable<PairWritable> values, Context context) throws IOException, InterruptedException{
			long sum = 0;
			long count = 0; 
			long product_sum=0;// initialize the sum for each keyword
			for(PairWritable numberPair:values){
				sum+=numberPair.getStar();
				count+=numberPair.getCount();
				product_sum=product_sum+(numberPair.getStar()*numberPair.getStar());
			}
			value.set(sum, count,product_sum);
			context.write(key, value);
		}
	}
		
	public static class Reduce extends Reducer<Text,PairWritable,Text,Text> {
		private Text word = new Text();
		private DoubleWritable mean_result = new DoubleWritable();
		private DoubleWritable variance_result = new DoubleWritable();
		private Text solution = new Text();
		
		public void reduce(Text key, Iterable<PairWritable> values, Context context) throws IOException, InterruptedException{
			long sum = 0;
			long count = 0;
			long product_sum=0;
			for (PairWritable pair:values){
				sum += pair.getStar();
				count += pair.getCount();
				product_sum+=pair.getProduct_Sum();
			}
			word.set("");
			mean_result.set((double) (sum/count));
			long mean=sum/count;
			variance_result.set((double)((product_sum/count)-(mean*mean)));
			solution.set(mean_result.toString().concat("\t").concat(variance_result.toString()));
			context.write(solution, word);
		}
	}
	
	public static void main(String[] args) throws Exception {
	  	Configuration conf = new Configuration();
	  	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	  	if (otherArgs.length != 2) {
	  	System.err.println("Usage: Mean_and_variance <in> <out>");
	  	System.exit(2);
	  	}
	  	Job job = new Job(conf, "Mean_and_variance");
	  	job.setJarByClass(Mean_and_variance.class);
	  	job.setMapperClass(Map.class);
	  	job.setCombinerClass(CombineClass.class);
	  	job.setReducerClass(Reduce.class);
	  	job.setOutputKeyClass(Text.class);
	  	job.setOutputValueClass(Text.class);
	  	job.setMapOutputValueClass(PairWritable.class);
	  	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	  	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	  	System.exit(job.waitForCompletion(true) ? 0 : 1);
  	}
}