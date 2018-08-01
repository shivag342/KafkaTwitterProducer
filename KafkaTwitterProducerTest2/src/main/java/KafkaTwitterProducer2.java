import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.gson.Gson;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

public class KafkaTwitterProducer2 {

	public static void main(String[] args) throws Exception {
		
		LinkedBlockingQueue<Status> queue = new LinkedBlockingQueue<Status>(1000);
		
		
		
		String consumerKey = "xxxxxxxxxxxxxxxxxxxxxxxxx";
		String consumerSecret = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
		String accessToken = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
		String accessTokenSecret = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
		String topicName = "MITweets";
		String[] keyWords = {"FalloutMachineImpossible","MachineImpossible","TomCruise"};
		
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(false).setOAuthConsumerKey(consumerKey).setOAuthConsumerSecret(consumerSecret).setOAuthAccessToken(accessToken).setOAuthAccessTokenSecret(accessTokenSecret);
		
		TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
		
		StatusListener listener = new StatusListener() {
			public void onStatus(Status status) {
				queue.offer(status);
                System.out.println(status.getUser().getName() + " : " + status.getText()+ "  Tweeted AT: " + status.getCreatedAt());
            }
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
            public void onException(Exception ex) {
                ex.printStackTrace();
            }
            public void onScrubGeo(long arg0, long arg1) {
                // TODO Auto-generated method stub
                
            }
            public void onStallWarning(StallWarning arg0) {
                // TODO Auto-generated method stub
                
            } 
			
		};
		
		twitterStream.addListener(listener);
		
		FilterQuery query = new FilterQuery().track(keyWords);
		twitterStream.filter(query);
		
		Thread.sleep(40000);
		
		// add kafka producer config settings
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
	      /*props.put("acks", "all");
	      props.put("retries", 0);
	      props.put("batch.size", 16384);
	      props.put("linger.ms", 1);
	      props.put("buffer.memory", 33554432);*/
		props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
		
		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		int i =0;
		int j=0;
		
		while(i <30) {
			Status ret = queue.poll();
			
			if(ret == null) {
				Thread.sleep(100);
				i++;
				
			} else {
				Gson gsonObj = new Gson();
				HashMap<String, String> keyvalue = new HashMap<String, String>();
				keyvalue.put("Twitter_Name", ret.getUser().getName().toString());
				keyvalue.put("Twitter_Text",ret.getText());
				keyvalue.put("Tweeted_AT",ret.getCreatedAt().toString());
				// convert map to JSON String
		        String jsonStr = gsonObj.toJson(keyvalue);
		        System.out.println("JsonTweet : "+jsonStr);
				//String tweetdetails= ret.getUser().getName() + " : " + ret.getText()+ "  Tweeted AT: " + ret.getCreatedAt();
				producer.send(new ProducerRecord<String, String>(
						topicName,Integer.toString(j++),jsonStr
						));
				/*for(HashtagEntity hashtage : ret.getHashtagEntities()) {
					System.out.println("Hashtag: "+ hashtage.getText());
					producer.send(new ProducerRecord<String, String>(
							topicName,Integer.toString(j++),hashtage.getText()
							));
				}*/
			}
		}
		producer.close();
		Thread.sleep(5000);
		twitterStream.shutdown();
	}
}
