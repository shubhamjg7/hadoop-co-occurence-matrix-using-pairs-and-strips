package edu.rmit.cosc2367.s3806186.Assignment2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class WordPairFrequencyPairs {
	private static final Logger LOG = Logger.getLogger(WordPairFrequencyPairs.class);

	// Map method
	public static class WordPairFrequencyPairsMapper extends Mapper<Object, Text, WordPair, IntWritable> {
		private WordPair wordPair = new WordPair();
		private IntWritable one = new IntWritable(1);

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
			if(words.length > 1) {
				// For each word
				for(int i = 0; i < words.length; i++) {
					// For other words in the current line
					for(int j = 0; j < words.length; j++) {
						if(i != j ) { 
							if(!words[i].equals("") && !words[j].equals("")) {
								// Proceeding as neither word is blank character
								// Set word and neighbour to context
								wordPair.setWord(words[i]);
								wordPair.setNeighbour(words[j]);
								context.write(wordPair, one);
							}

						}
					}
				}
			}
		}
	}

	// Reducer method
	public static class WordPairFrequencyPairsReducer extends Reducer<WordPair, IntWritable, WordPair, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		public void reduce(WordPair key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			LOG.info("Reducer task of s3806186");
			
			int sum = 0; 
			for (IntWritable val : values) {
				sum += val.get(); // Aggregate key values into sum
			}

			result.set(sum);
			context.write(key, result); // Write final aggregated result for the given key
		}
	}

	public static void main(String[] args) throws Exception {

		LOG.setLevel(Config.logLevel); // Set log level

		Configuration conf = new Configuration();

		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}

		Job job = Job.getInstance(conf, "Word pair count using pairs approach.");
		job.setJarByClass(WordPairFrequencyPairs.class);
		job.setMapperClass(WordPairFrequencyPairsMapper.class); // Setting mapper
		job.setCombinerClass(WordPairFrequencyPairsReducer.class); // Setting combiner
		job.setReducerClass(WordPairFrequencyPairsReducer.class); // Setting reducer
		job.setOutputKeyClass(WordPair.class); // Setting op key type
		job.setOutputValueClass(IntWritable.class); // Setting op value type
		FileInputFormat.addInputPath(job, new Path(args[0])); // Use passed input path
		FileOutputFormat.setOutputPath(job, new Path(args[1])); // Use passsed output path

		LOG.info("INPUT PATH: " + args[0]);
		LOG.info("OUTPUT PATH: " + args[1]);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
