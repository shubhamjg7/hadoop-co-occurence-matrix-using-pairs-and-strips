package edu.rmit.cosc2367.s3806186.Assignment2;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class WordPairRelativeFrequencyStrips {
	private static final Logger LOG = Logger.getLogger(WordPairFrequencyPairs.class);

	// Map method
	public static class WordPairRelativeFrequencyStripsMapper extends Mapper<Object, Text, Text, MapWritable> {
		private Text word = new Text();
		private MapWritable neighbourMap = new MapWritable();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			LOG.info("Mapper task of s3806186");
			
			// Get current line being processed in mapper
			String strValue = value.toString();

			// Remove punctuation/digits and convert to lower case
			strValue = strValue.replaceAll("\\p{Punct}|\\d", "").toLowerCase();

			// Get words separated by space
			String[] words = strValue.toString().split("\\s+");

			// If more than 1 word
			if (words.length > 1) {
				// For each word
				for (int i = 0; i < words.length; i++) {
					if(words[i].equals("")) {
						continue;
					}
					word.set(words[i]);
					neighbourMap.clear();

					// For other words in the current line
					for (int j = 0; j < words.length; j++) {
						if (i != j) {
							if(words[j].equals("")) {
								continue;
							}
							// Set word and neighbour to context
							Text neighbour = new Text(words[j]);
							if (neighbourMap.containsKey(neighbour)) {
								IntWritable sum = (IntWritable) neighbourMap.get(neighbour);
								sum.set(sum.get() + 1);
							} else {
								neighbourMap.put(neighbour, new IntWritable(1));
							}
						}
					}
					context.write(word, neighbourMap);
				}
			}
		}
	}

	// Reducer method
	public static class WordPairRelativeFrequencyStripsReducer extends Reducer<Text, MapWritable, Text, MapWritable> {

		@Override
		public void reduce(Text key, Iterable<MapWritable> values, Context context)
				throws IOException, InterruptedException {
			LOG.info("Reducer task of s3806186");
			
			MapWritable result = new MapWritable();
			// Cache for total count of current word
			double totalCount = 0;

			// For all values i.e. our list of neighbours and their counts
			for (MapWritable value : values) {

				// Get all neighbour names
				Set<Writable> neighbours = value.keySet();

				// For each neighbour
				for (Writable neighbour : neighbours) {
					// Get old count which needs to be updated
					IntWritable currentCount = (IntWritable) value.get(neighbour);
					
					totalCount = totalCount + currentCount.get(); // Update total count
					
					// If some count already present
					if (result.containsKey(neighbour)) {
						// Update existing count
						IntWritable count = (IntWritable) result.get(neighbour);
						count.set(count.get() + currentCount.get());
					} else {
						// Add first count for this neighbour
						result.put(neighbour, currentCount);
					}
				}
			}

			Set<Writable> resultKeys = result.keySet();

			MapWritable finResult = new MapWritable();
			for (Writable resultKey : resultKeys) {
				double count = (double) ((IntWritable) result.get(resultKey)).get();
				finResult.put(resultKey, new DoubleWritable(count/totalCount)); // Use final total count to perform the division
			}

			context.write(key, finResult); // Write final aggregated result for the given key

		}
	}

	public static void main(String[] args) throws Exception {

		LOG.setLevel(Config.logLevel); // Set log level

		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}
		
		Job job = Job.getInstance(conf, "Word pair count using pairs approach.");
		job.setJarByClass(WordPairRelativeFrequencyStrips.class);
		job.setMapperClass(WordPairRelativeFrequencyStripsMapper.class); // Setting mapper
		job.setReducerClass(WordPairRelativeFrequencyStripsReducer.class); // Setting reducer
		job.setOutputKeyClass(Text.class); // Setting op key type
		job.setOutputValueClass(MapWritable.class); // Setting op value type
		FileInputFormat.addInputPath(job, new Path(args[0])); // Use passed input path
		FileOutputFormat.setOutputPath(job, new Path(args[1])); // Use passsed output path

		LOG.info("INPUT PATH: " + args[0]);
		LOG.info("OUTPUT PATH: " + args[1]);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
