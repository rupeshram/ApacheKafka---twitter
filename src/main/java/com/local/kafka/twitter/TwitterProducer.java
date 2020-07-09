package com.local.kafka.twitter;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;



import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.local.kafka.twitter.TwitterProducer;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;



public class TwitterProducer{

	public static void main(String[] args) throws InterruptedException, JsonProcessingException {
	final Logger logger =LoggerFactory.getLogger(TwitterProducer.class);
		
		
		
		//Create properties for producer
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "YOUR KAFKA IP HERE:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
		properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");
		properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
		properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
		properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024));
		//#Create the producer instance
		KafkaProducer<String, String> producer=new KafkaProducer<String, String>(properties);
		
		BlockingQueue<String> queue = new LinkedBlockingQueue<String>(1000);		
		twitterClient twc =new twitterClient(queue, "your consumer key", "consumer secret", "api token", "api token secret");
		
		int id_gen = 0;
		try {
				while(twc.isActive()) {
					
					String jMess=queue.take();
					String key="id1_"+Integer.toString(id_gen);
					
					
					
					Gson gs =new Gson();
					
					Message Inputmsg = gs.fromJson(jMess, Message.class);
					String message=Inputmsg.text;
					SimpleDateFormat dtf=new SimpleDateFormat("EEE MMM d HH:mm:ss zzzz yyyy");
					
					 
					 ObjectMapper objectMapper = new ObjectMapper();
					 objectMapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
					 Message outMess = new Message(key,Inputmsg.id_str,dtf.parse(Inputmsg.created_at).toString(), message,Inputmsg.source);
					 String finMessage=objectMapper.writeValueAsString(outMess);
					
					 logger.info("Tweet: " +finMessage);
					
				
					
					//Setting the producer record
					ProducerRecord<String, String> prodRec=new ProducerRecord<String, String>("twitter",key, finMessage);
					logger.info("Key:" +key);
					producer.send(prodRec,new Callback() {				
						public void onCompletion(RecordMetadata metadata, Exception exception) {
							// TODO Auto-generated method stub
							if(exception == null) {
								logger.info("Message successfully added. \n" +					   
							   "Topic " + metadata.topic() + "\n"+
							   "Partition: " + metadata.partition() + "\n"+ 
							   "Offset: " + metadata.offset());			
								
							}
							else {
								logger.info(exception.getMessage());
							}
						}
					}); 			
					producer.flush();	
				
					id_gen++;					
				}
		}catch(InterruptedException ex)	{
			logger.info("Exception occurred: " + ex.getMessage());
			twc.disconnect();
		}catch(ParseException par)	{
			logger.info("Exception occurred: " + par.getMessage());		
		}	
		finally {
			twc.disconnect();
			producer.close();
		}	
		
	}			
		

	public static class twitterClient{	
		
		private Client twClient;
		
		public twitterClient(BlockingQueue queue, String consumerKey,String consumerSecret, String token, String secret) {			
			
			//Authenticate
			Authentication auth =new OAuth1(consumerKey, consumerSecret, token, secret);
			
			//Create a hbc client
			ClientBuilder builder =new ClientBuilder()
					.hosts(Constants.STREAM_HOST)
					.authentication(auth)
					.endpoint(new StatusesFilterEndpoint().trackTerms(Lists.newArrayList("Sunny")))
					.processor(new StringDelimitedProcessor(queue));
					
			
			Client twClient=builder.build();
			
			this.twClient=twClient;
			
			twClient.connect();
			
		}
		
		private boolean isActive() {
			if(!twClient.isDone()){
				return true;
			}else
			{
				return false;
			}
		}
		
		private void disconnect() {
			twClient.stop();
		}
	}
	
	
	public static class Message{
		private String id;
		private String id_str;
		private String created_at;
	    private String text;
	    private String source;    
	    
	    
       
		public  Message(String id,String id_str,String created_at,String text,String source){
	    	this.created_at=created_at;
	    	this.text=text;
	    	this.id=id;
	    	this.id_str=id_str;	    	
	    	this.source=source;
	    
	   }
	    
	}
	
	
}
