package pack;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import twitter4j.Status;

public class Tutorial {
  public static void main(String[] args) throws Exception {

    // Checkpoint directory 
    String checkpointDir = TutorialHelper.getCheckpointDirectory();

    // Configuring Twitter credentials 
    String apiKey = "fQjLCXNks6K3PlZ8BjuVGWIwl";
    String apiSecret = "fvM9LbWcbBZRYMDgAASVbJd18egJY5HLeBP0Ji4F2ikGUoJcpd";
    String accessToken = "220282104-nR3XeBHtgHfPt3pvI15T2YkTXChGk5U9iJU3EWzQ";
    String accessTokenSecret = "vfHg2jG4LhKvTR8wa9slDXgp9JMrM5dlA8ZcKTESf62Hc";
    TutorialHelper.configureTwitterCredentials(apiKey, apiSecret, accessToken, accessTokenSecret);

    // Your code goes here
	JavaStreamingContext ssc = new JavaStreamingContext(new SparkConf(), new Duration(1000));
	JavaDStream<Status> tweets = TwitterUtils.createStream(ssc);
	
	JavaDStream<String> statuses = tweets.map(
		new Function<Status, String>() {
			public String call(Status status) { return status.getText(); }
		}
	);
	statuses.print();
	
	ssc.checkpoint(checkpointDir);
	
	ssc.start();
	ssc.awaitTermination();
  }
}

