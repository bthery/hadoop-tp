// Mastère Big Data 2019/2020 - TP Hadoop
//
// Benjamin Thery - benjamin.thery@grenoble-inp.org
//
// Word count with in-mapper combiner and 'empty lines' counter
// Changed code sections are prefixed with "// [BT]" comments

import java.io.IOException;
// [BT] Import hashmap module for in-mapper combiner
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
// [BT] Import Hadoop counter module
import org.apache.hadoop.mapreduce.Counter;

public class WordCountWithInMapperCombinerWithCounter {

	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		// [BT] New counter type
		enum CounterType {
			EmptyLine
		};

		private final Text text = new Text();
		//private final static IntWritable ONE = new IntWritable(1);
		private final static IntWritable intwrit = new IntWritable();
		private HashMap<String, Integer> wordMap;
		private Counter counter;

		// [BT] setup: Called before first map on input split
		@Override
		protected void setup(Context context)
              throws IOException, InterruptedException {
			// [BT] Initialize hashmap for in-mapper combiner
			wordMap = new HashMap<String, Integer>();
			// [BT] Get counter from context using the type defined above
			counter = context.getCounter(CounterType.EmptyLine);
		}
		
		// [BT] cleanup: Called after last map on input split
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			// [BT] Mapping is done
			// [BT] Loop through all hashmap entries and write them
			for (String word : wordMap.keySet()) {
				text.set(word);
				intwrit.set(wordMap.get(word));
				context.write(text,  intwrit);
			}
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			for (String word : value.toString().replaceAll("[^0-9a-zA-Z ]", "").toLowerCase().split("\\s+")) {
				if (!word.isEmpty()) {
					// [BT] Do not write result now
					// [BT] instead increment counter for this word in the hashmap
					Integer prevCount = 0;
					if (wordMap.containsKey(word)) {
						prevCount = wordMap.get(word);
					}
					wordMap.put(word, prevCount + 1);
				} else {
					// [BT] Line was empty, increment our counter
					counter.increment(1);
				}
			}
		}

	}

	// [BT] Combiner class (not used as we're adding a in-mapper combiner in this exercise)
	// [BT] Cannot re-use the WordCountReducer as-is as the input types doesn't match the output types (long vs int)
	public static class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable intwrit = new IntWritable();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
				InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			intwrit.set(sum);
			context.write(key, intwrit);
		}

	}

	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, LongWritable> {

		private LongWritable longwrit = new LongWritable();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
				InterruptedException {
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
		Job job = Job.getInstance(conf, "word count");
		// [BT] Add 3 reducers
		job.setNumReduceTasks(3);
		// [BT] Set main class
		job.setJarByClass(WordCountWithInMapperCombinerWithCounter.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setMapperClass(WordCountMapper.class);
		// [BT] Add combiner
		// [BT] We're adding an in-mapper combiner, so we can comment this out
		//job.setCombinerClass(WordCountCombiner.class);
		job.setReducerClass(WordCountReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
