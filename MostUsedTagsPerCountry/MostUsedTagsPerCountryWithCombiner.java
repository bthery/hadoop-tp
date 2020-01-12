// Mastère Big Data 2019/2020 - TP Hadoop
//
// Benjamin Thery - benjamin.thery@grenoble-inp.org
//
// Afficher les K tags les plus utilisés par pays

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
import com.google.common.collect.MinMaxPriorityQueue;

public class MostUsedTagsPerCountryWithCombiner {

	public static class MostUsedTagsMapper extends Mapper<LongWritable, Text, Text, StringAndIntWritable> {

		enum CounterType {
			CountryNotFound,
			PhotoWithNoTags
		};

		private final Text countryText = new Text();
		private final Text tagsText = new Text();

		private Counter countryNotFoundCounter;
		private Counter photoWithNoTagCounter;

		@Override
		protected void setup(Context context)
              throws IOException, InterruptedException
		{
			// Initialize counter for entries with unknown country
			countryNotFoundCounter = context.getCounter(CounterType.CountryNotFound);
			// Initialize counter for entries with no tags
			photoWithNoTagCounter = context.getCounter(CounterType.PhotoWithNoTags);
		}

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException
		{
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException
		{
			// Extract user tags list: it is the 9th field of the line.
			String tags = java.net.URLDecoder.decode(value.toString()).split("\t")[8];
			if (tags.length() == 0) {
				// This photo has no tags, skip it
				photoWithNoTagCounter.increment(1);
				return;
			}

			// Get country at longitude/latitude: respectively 10th and 11th fields
			try {
				double longitude = Double.parseDouble(value.toString().split("\t")[10]);
				double latitude = Double.parseDouble(value.toString().split("\t")[11]);

				Country country = Country.getCountryAt(latitude, longitude);
				if (country != null) {
					countryText.set(country.toString());
				} else {
					System.out.println("No country found at coordinates lat:" + latitude + " long:" + longitude);
					countryNotFoundCounter.increment(1);
					// Tags will be counted in a dummy "Unknown" country
					countryText.set("Unknown");
				}
			} catch (NumberFormatException e) {
				System.out.println("Failed to get coordinates");
				countryNotFoundCounter.increment(1);
				// Tags will be counted in a dummy "Unknown" country
				countryText.set("Unknown");
			}

			// Write to output one entry per tag: (country, (tag, 1))
			for (String tag : tags.split(",")) {
				if (tag.length() > 0) {
					context.write(countryText, new StringAndIntWritable(tag, 1));
				}
			}
		}
	}

	public static class MostUsedTagsCombiner extends Reducer<Text, StringAndIntWritable, Text, StringAndIntWritable> {

		@Override
		protected void reduce(Text key, Iterable<StringAndIntWritable> values, Context context)
				throws IOException, InterruptedException
		{
			// Fill hashmap that stores the number of occurences for each tag
			HashMap<String, Integer> tagsMap = new HashMap<String, Integer>();
			for (StringAndIntWritable value : values) {
				String tag = value.strValue;
				Integer prevCount = 0;
				if (tagsMap.containsKey(tag)) {
					prevCount = tagsMap.get(tag);
				}
				tagsMap.put(tag, prevCount + value.intValue);
			}

			// Write one entry for each tag associated with the country: (Country, (tag, count))
			for (String tag : tagsMap.keySet()) {
				context.write(key, new StringAndIntWritable(tag, tagsMap.get(tag)));
			}
		}
	}

	public static class MostUsedTagsReducer extends Reducer<Text, StringAndIntWritable, Text, Text> {

		private final Text countryTagsCountText = new Text();

		@Override
		protected void reduce(Text key, Iterable<StringAndIntWritable> values, Context context)
				throws IOException, InterruptedException
		{
			// Fill hashmap that stores the number of occurences for each tag
			HashMap<String, Integer> tagsMap = new HashMap<String, Integer>();
			for (StringAndIntWritable value : values) {
				String tag = value.strValue;
				Integer prevCount = 0;
				if (tagsMap.containsKey(tag)) {
					prevCount = tagsMap.get(tag);
				}
				tagsMap.put(tag, prevCount + value.intValue);
			}

			// Get number of tags to return and display type from configuration
			Configuration conf = context.getConfiguration();
			int tagsNumber = conf.getInt("tagsNumber", 1);
			Boolean prettyPrint = conf.getBoolean("prettyPrint", false);
			System.out.println(prettyPrint);

			// Go through all hashmap entries and add them to the priority queue
			// only the K entries with the highest counter will be kept
			MinMaxPriorityQueue<StringAndIntWritable> mostUsedTagsQueue = MinMaxPriorityQueue
				.maximumSize(tagsNumber)
				.create();

			for (String tag : tagsMap.keySet()) {
				mostUsedTagsQueue.add(new StringAndIntWritable(tag, tagsMap.get(tag)));
			}

			// Build string of K most used tags and write it to output
			StringAndIntWritable si;
			if (prettyPrint == false) {
				// Raw output : one (Country, tag) entry per tag
				do {
					si = mostUsedTagsQueue.pollFirst();
					if (si != null) {
						countryTagsCountText.set(si.strValue);
						context.write(key, countryTagsCountText);
					}
				} while (si != null);
			} else {
				// Pretty output : a single entry per country with the K tags and their count
				String countryTagsStr = "";
				do {
					si = mostUsedTagsQueue.pollFirst();
					if (si != null) {
						if (countryTagsStr.length() > 0)
							countryTagsStr += ", ";
						countryTagsStr += si.strValue;
						countryTagsStr += "(" + si.intValue + ")";
					}
				} while (si != null);
				countryTagsCountText.set(countryTagsStr);
				context.write(key, countryTagsCountText);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		// Get K tags value and store it in configuration
		conf.setInt("tagsNumber", Integer.parseInt(otherArgs[0]));
		// Get display type and store it in configuration
		if (otherArgs.length > 3)
			conf.setBoolean("prettyPrint", Boolean.parseBoolean(otherArgs[3]));

		Job job = Job.getInstance(conf, "most used tags per country");
		// [BT] Add 3 reducers
		job.setNumReduceTasks(3);
		job.setJarByClass(MostUsedTagsPerCountryWithCombiner.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StringAndIntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(MostUsedTagsMapper.class);
		// [BT] Add combiner
		job.setCombinerClass(MostUsedTagsCombiner.class);
		job.setReducerClass(MostUsedTagsReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
