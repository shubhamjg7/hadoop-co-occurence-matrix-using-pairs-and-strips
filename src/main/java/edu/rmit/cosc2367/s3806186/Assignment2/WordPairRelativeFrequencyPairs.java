package edu.rmit.cosc2367.s3806186.Assignment2;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class WordPairRelativeFrequencyPairs {
	private static final Logger LOG = Logger.getLogger(WordPairRelativeFrequencyPairs.class);

	// Map method
	public static class WordPairRelativeFrequencyPairsMapper extends Mapper<Object, Text, WordPair, DoubleWritable> {
		private WordPair wordPair = new WordPair();
		private DoubleWritable one = new DoubleWritable(1);

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
					if(!words[i].equals("")) { 
					wordPair.setWord(words[i]);

					// For other words in the current line
					for (int j = 0; j < words.length; j++) {
						if (i != j) {
							// Set word and neighbour to context
							if(!words[j].equals("")) {
								wordPair.setNeighbour(words[j]);
								context.write(wordPair, one);
							}
						}
					}
					
					// Also write * as neighbour to count total number of words
					wordPair.setNeighbour("*");
					context.write(wordPair, new DoubleWritable(words.length - 1));
					}
				}
			}
		}
	}

	// Partitioner class
	public static class WordPairRelativeFrequencyPairsPartitioner extends Partitioner<WordPair, DoubleWritable> {

		// Partition method
		public int getPartition(WordPair key, DoubleWritable value, int numReduceTasks) {
			LOG.info("The partitioner task of Shubham Gupta, s3806186");

			// If we have only one reduce task
			if (numReduceTasks == 0) {
				return 0;
			}

			// Partition words based on first char so each word is sent to same reducer
			String word = key.getWord().toString();
			int firstChar = word.charAt(0);
			return firstChar % numReduceTasks;
		}
	}
	
	// Comparator class
	public static class WordPairRelativeFrequencyPairsComparator extends WritableComparator {
		public WordPairRelativeFrequencyPairsComparator() {
			super(WordPair.class, true);
		}

		@Override
		public int compare(WritableComparable arg0, WritableComparable arg1) {
			LOG.info("Comparator task of s3806186");
			
			// Compare word pairs so that we have words with asterisk as numbers at top
			WordPair wp1 = (WordPair) arg0;
			WordPair wp2 = (WordPair) arg1;
			
			return wp1.compareTo(wp2);
		}
	}

	// Reducer method
	public static class WordPairRelativeFrequencyPairsReducer
			extends Reducer<WordPair, DoubleWritable, WordPair, DoubleWritable> {
		private Text totalFlag = new Text("*");
		private Text cacheWord = new Text("_B_L_A_N_K_");
		private DoubleWritable countTotal = new DoubleWritable();
		
		@Override
		public void reduce(WordPair key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			LOG.info("Reducer task of s3806186");
			
			// If counting for asterisk
			if (key.getNeighbour().equals(totalFlag)) {
				if (key.getWord().equals(cacheWord)) {
					// Update existing count
					countTotal.set(countTotal.get() + countTotalValues(values));
				} else {
					// Reset count
					countTotal.set(0);

					// Set new current word
					cacheWord.set(key.getWord());
					countTotal.set(countTotalValues(values));
				}
				// Else counting relative frequency with other words
			} else {
				double countForNeighbour = countTotalValues(values);
				DoubleWritable countRelative = new DoubleWritable();
				countRelative.set(countForNeighbour / countTotal.get());
				context.write(key, countRelative);
			}
		}

		private double countTotalValues(Iterable<DoubleWritable> counts) {
			double total = 0;
			for (DoubleWritable count : counts) {
				total = total + count.get();
			}
			return total;
		}
	}

	public static void main(String[] args) throws Exception {

		LOG.setLevel(Config.logLevel); // Set log level
		
		 

		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}
		
		Job job = Job.getInstance(conf, "Word pair relative count using pairs approach.");
		job.setJarByClass(WordPairRelativeFrequencyPairs.class);
		job.setMapperClass(WordPairRelativeFrequencyPairsMapper.class); // Setting mapper
		job.setPartitionerClass(WordPairRelativeFrequencyPairsPartitioner.class); // Setting partitioner
		job.setNumReduceTasks(3); // Setting number of reduce tasks
		job.setGroupingComparatorClass(WordPairRelativeFrequencyPairsComparator.class);
		job.setReducerClass(WordPairRelativeFrequencyPairsReducer.class); // Setting reducer
		job.setOutputKeyClass(WordPair.class); // Setting op key type
		job.setOutputValueClass(DoubleWritable.class); // Setting op value type
		FileInputFormat.addInputPath(job, new Path(args[0])); // Use passed input path
		FileOutputFormat.setOutputPath(job, new Path(args[1])); // Use passsed output path

		LOG.info("INPUT PATH: " + args[0]);
		LOG.info("OUTPUT PATH: " + args[1]);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
