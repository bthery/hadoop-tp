
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Counter;

public class MostUsedTags {
	
	public static class MostUsedTagsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		enum CounterType {
			EmptyLine
		};

		private final Text text = new Text();
		private final static IntWritable intwrit = new IntWritable();

		private HashMap<String, Integer> tagsMap;
		private Counter counter;

		@Override
		protected void setup(Context context)
              throws IOException, InterruptedException
		{
			// Initialize hashmap
			tagsMap = new HashMap<String, Integer>();
			counter = context.getCounter(CounterType.EmptyLine);
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException
		{
			// Loop through all hashmap entries and write them
			for (String tag : tagsMap.keySet()) {
				text.set(tag);
				intwrit.set(tagsMap.get(tag));
				context.write(text,  intwrit);
			}
		}
       
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException
		{
			// Extract user tags list: it is the 9th field of the line.
			String[] tags = value.toString().split("\t")[8].split(",");
			for (String encodedTag : tags) {
				if (!encodedTag.isEmpty()) {
					String tag = java.net.URLDecoder.decode(encodedTag);
					// System.out.println(tag);
					Integer prevCount = 0;
					if (tagsMap.containsKey(tag)) {
						prevCount = tagsMap.get(tag);
					}
					tagsMap.put(tag, prevCount + 1);
				} else {
					// Line was empty
					counter.increment(1);
				}
			}
		}

	}

	// Cannot re-use the MostUsedTagsReducer as-is as the input types doesn't match the output types (long vs int)
	public static class MostUsedTagsCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable intwrit = new IntWritable();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException
		{
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			intwrit.set(sum);
			context.write(key, intwrit);
		}

	}

	public static class MostUsedTagsReducer extends Reducer<Text, IntWritable, Text, LongWritable> {

		private LongWritable longwrit = new LongWritable();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException
		{			
			Configuration conf = context.getConfiguration();
			int tagsNumber = conf.getInt("tagsNumber", 1);
			
			// tag count
			long sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			longwrit.set(sum);
			context.write(key, longwrit);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		// Get K tags value and store it in configuration
		conf.setInt("tagsNumber", Integer.parseInt(otherArgs[0]));
		
		Job job = Job.getInstance(conf, "most used tags per country");
		// [BT] Add 3 reducers
		job.setNumReduceTasks(3);
		job.setJarByClass(MostUsedTags.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setMapperClass(MostUsedTagsMapper.class);
		// [BT] Add combiner
		//job.setCombinerClass(MostUsedTagsCombiner.class);
		job.setReducerClass(MostUsedTagsReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}