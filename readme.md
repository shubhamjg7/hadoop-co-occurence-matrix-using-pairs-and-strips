# To copy standalone jar from HDFS to EMR FS:

`hadoop fs -copyToLocal /user/s3806186/Assignment2-0.0.1-SNAPSHOT.jar .`

# Map-reduce program for “pairs approach” and “strips approach” to compute the co-occurrence matrix where the word-pair frequency is maintained.

Using pairs:
`hadoop jar Assignment2-0.0.1-SNAPSHOT.jar edu.rmit.cosc2367.s3806186.Assignment2.WordPairFrequencyPairs /user/s3806186/input/ /user/s3806186/output1 ./`

Using strips:
`hadoop jar Assignment2-0.0.1-SNAPSHOT.jar edu.rmit.cosc2367.s3806186.Assignment2.WordPairFrequencyStrips /user/s3806186/input/ /user/s3806186/output2 ./`

# Map-reduce program for “pairs approach” and “strips approach” to compute the co-occurrence matrix where the word-pair relative frequency is maintained.

Using relative pairs:
`hadoop jar Assignment2-0.0.1-SNAPSHOT.jar edu.rmit.cosc2367.s3806186.Assignment2.WordPairRelativeFrequencyPairs /user/s3806186/input/ /user/s3806186/output3 ./`

Using relative strips:
`hadoop jar Assignment2-0.0.1-SNAPSHOT.jar edu.rmit.cosc2367.s3806186.Assignment2.WordPairRelativeFrequencyStrips /user/s3806186/input/ /user/s3806186/output4 ./`