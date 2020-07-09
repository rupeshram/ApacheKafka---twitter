package com.local.kafka.twitter;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Produces;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.GenericEntity;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.glassfish.jersey.client.ClientConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class TwitterConsumerSolr {

	public static void main(String[] args) {
				
		
		final Logger logger =LoggerFactory.getLogger(TwitterConsumerSolr.class);
		Response resp;
		String json;
		
		
		Properties properties=new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"YOUR KAFKA IP HERE:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "twitter_app_2");
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "20");
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
	
		
		
		
		//Create consumer instance
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String,String>(properties);
		
		consumer.subscribe(Arrays.asList("twitter"));
		
		
		solrRESTClient solrClient=new solrRESTClient();    
		
		
	
		
		while(true){
			ConsumerRecords<String, String> records=  consumer.poll(Duration.ofMillis(100));
			
			logger.info("Received :" + records.count() );
			
		
			
			for (ConsumerRecord record : records) {
				logger.info("Key: "+ record.key() + "\n" +
			                "Value:"+ record.value().toString() + "\n" +
			             "Topic:" + record.topic() + "\n" +
						 "Partition: " + record.partition() + "\n" ); 
											
				logger.info("Processing:" + record.value()) ;
				Gson gson = null;
				String finalJSON="";
				gson= new GsonBuilder().setPrettyPrinting().create();				
							
				
				finalJSON= "{"
				            + "\"add\":"
				            + "{"
				            + "\"doc\":"
				            + record.value()
				            +"}"
				            +"}";
				
				json = gson.toJson(finalJSON);
				
				resp=solrClient.getResponse(solrClient.getTarget(),json);	
				logger.info(json);
				logger.info(resp.toString());
				
			}
			
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			consumer.commitSync();
			logger.info("Commited to offset :" + records.count() );
		}
				
	}
	
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public static class solrRESTClient{
		
		private WebTarget target;
		private Response resp;
		
		public solrRESTClient(){
			Client client = ClientBuilder.newClient();
			WebTarget webTarget = client.target("YOUR SOLR CLOUD REST INDEX COMMIT API here");			
            this.target= webTarget;       
			
		}
		
		 @POST
		 @Produces("application/json")	
		public Response getResponse(WebTarget target,String json) {		
			
			 
			Response response =  target.request(MediaType.APPLICATION_JSON).post(Entity.json(json));
			
			return response;
		}
		
		public WebTarget getTarget() {
			return this.target;
		}
		
		
		
	}
	
	
	public static class Message{
		
		private String id;
		private String id_str;
		private String created_at;
	    private String text;
	    private String source;          
	    
	}

}
