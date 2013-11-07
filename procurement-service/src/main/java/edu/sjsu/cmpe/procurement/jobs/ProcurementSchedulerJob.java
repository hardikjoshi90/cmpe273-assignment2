package edu.sjsu.cmpe.procurement.jobs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.text.ParseException;
import java.util.ArrayList;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.fusesource.stomp.jms.StompJmsConnectionFactory;
import org.fusesource.stomp.jms.StompJmsDestination;
import org.fusesource.stomp.jms.message.StompJmsMessage;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.spinscale.dropwizard.jobs.Job;
import de.spinscale.dropwizard.jobs.annotations.Every;
import edu.sjsu.cmpe.procurement.ProcurementService;

/**
 * This job will run at every 5 second.
 */
@Every("300s")
public class ProcurementSchedulerJob extends Job {
    private final Logger log = LoggerFactory.getLogger(getClass());
    JSONParser parser = new JSONParser();
    JSONArray responseMsg;
    String []msgFormat;

    @Override
    public void doJob() {
    ProcurementSchedulerJob job = new ProcurementSchedulerJob();
    try {
    	String lostBooks = job.Consumer();
    	if(lostBooks != null)
    	{
    		job.HttpClientPost(lostBooks);
    	}
		String []availableBooks;
		availableBooks = job.HttpClientGet();
		job.PubSub(availableBooks);
	} catch (JMSException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	String strResponse = ProcurementService.jerseyClient.resource("http://ip.jsontest.com/").get(String.class);
	log.debug("Response from jsontest.com: {}", strResponse);
    }
    
    public String Consumer() throws JMSException
    {
    	String user = env("APOLLO_USER", "admin");
    	String password = env("APOLLO_PASSWORD", "password");
    	String host = env("APOLLO_HOST", "54.215.210.214");
    	int port = Integer.parseInt(env("APOLLO_PORT", "61613"));
    	String queue = "/queue/33387.book.orders";
    	String destination = queue;
    	ArrayList<String> msgQueue = new ArrayList<String>();

    	StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
    	factory.setBrokerURI("tcp://" + host + ":" + port);

    	Connection connection = factory.createConnection(user, password);
    	connection.start();
    	Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    	Destination dest = new StompJmsDestination(destination);

    	MessageConsumer consumer = session.createConsumer(dest);
    	System.out.println("Waiting for messages from " + queue + "...");
    	while(true) {
    	    Message msg = consumer.receive(500);
    	    if(msg == null)
    	    {
    	    	break;
    	    }
    	    if( msg instanceof  TextMessage ) {
    		String body = ((TextMessage) msg).getText();
    		msgQueue.add(body.split(":")[1]);
    		System.out.println("Received message = " + body);
    	    } else if (msg instanceof StompJmsMessage) {
    		StompJmsMessage smsg = ((StompJmsMessage) msg);
    		String body = smsg.getFrame().contentAsString();
    		System.out.println("Received message = " + body);

    	    } else {
    		System.out.println("Unexpected message type: "+msg.getClass());
    	    }
    	}
    	connection.close();
    	String lostBooks = msgQueue.toString();
    	System.out.println("Returning LostBooks:"+lostBooks);
    	return lostBooks;
        }

        private static String env(String key, String defaultValue) {
    	String rc = System.getenv(key);
    	if( rc== null ) {
    	    return defaultValue;
    	}
    	return rc;
        }
        
        public void HttpClientPost(String lostBooks)
        {
        	try {
        		 
        		DefaultHttpClient httpClient = new DefaultHttpClient();
        		HttpPost postRequest = new HttpPost(
        			"http://54.215.210.214:9000/orders");
         
        		StringEntity input = new StringEntity("{\"id\":\"33387\",\"order_book_isbns\":"+lostBooks+"}");
        		input.setContentType("application/json");
        		postRequest.setEntity(input);
         
        		HttpResponse response = httpClient.execute(postRequest);
         
        		if (response.getStatusLine().getStatusCode() != 200) {
        			throw new RuntimeException("Failed : HTTP error code : "
        				+ response.getStatusLine().getStatusCode());
        		}
         
        		BufferedReader br = new BufferedReader(
                                new InputStreamReader((response.getEntity().getContent())));
         
        		String output;
        		System.out.println("Output from Server .... \n");
        		while ((output = br.readLine()) != null) {
        			System.out.println(output);
        		}
         
        		httpClient.getConnectionManager().shutdown();
         
        	  } catch (MalformedURLException e) {
         
        		e.printStackTrace();
         
        	  } catch (IOException e) {
         
        		e.printStackTrace();
         
        	  }
         
        }
        
        public String[] HttpClientGet()
        {
        	try {
        		
        		 
        		DefaultHttpClient httpClient = new DefaultHttpClient();
        		HttpGet getRequest = new HttpGet(
        			"http://54.215.210.214:9000/orders/33387");
        		getRequest.addHeader("accept", "application/json");
         
        		HttpResponse response = httpClient.execute(getRequest);
         
        		if (response.getStatusLine().getStatusCode() != 200) {
        			throw new RuntimeException("Failed : HTTP error code : "
        			   + response.getStatusLine().getStatusCode());
        		}
        		String ent = response.getEntity().toString();
        		System.out.println(ent);
         
        		BufferedReader br = new BufferedReader(
                                 new InputStreamReader((response.getEntity().getContent())));
         
        		String output;
        		System.out.println("Output from Server .... \n");
        		while ((output = br.readLine()) != null) {
        			System.out.println(output);
        			Object obj = parser.parse(output);
					JSONObject jsonObject = (JSONObject) obj;
					responseMsg = (JSONArray) jsonObject.get("shipped_books");
					msgFormat = new String[responseMsg.size()];
					for(int i=0;i<responseMsg.size();i++)
					{
						JSONObject books = (JSONObject) responseMsg.get(i);
						msgFormat[i]= books.get("isbn").toString() +":\""+ books.get("title").toString() +"\":\""+books.get("category").toString()+"\":\""+books.get("coverimage").toString()+"\"";
						System.out.println(msgFormat[i]);
					}
        		}
         
        		
					
					
        		httpClient.getConnectionManager().shutdown();
        	}
        	  catch (ClientProtocolException e) {
         
        		e.printStackTrace();
         
        	  } catch (IOException e) {
         
        		e.printStackTrace();
        	  } catch (org.json.simple.parser.ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        	return msgFormat;
        }
        
        public void PubSub(String []availableBooks) throws JMSException
        {
        	String user = env("APOLLO_USER", "admin");
			String password = env("APOLLO_PASSWORD", "password");
			String host = env("APOLLO_HOST", "54.215.210.214");
			int port = Integer.parseInt(env("APOLLO_PORT", "61613"));
			String destination = "/topic/33387.book";

			StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
			factory.setBrokerURI("tcp://" + host + ":" + port);

			Connection connection = factory.createConnection(user, password);
			connection.start();
			Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			
			for(int i=0;i<availableBooks.length;i++)
			{
				JSONObject books = (JSONObject) responseMsg.get(i);
				Destination dest = new StompJmsDestination(destination+"."+books.get("category").toString());
				MessageProducer producer = session.createProducer(dest);
				producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

				String data = availableBooks[i];
				TextMessage msg = session.createTextMessage(data);
				msg.setLongProperty("id", System.currentTimeMillis());
				producer.send(msg);
			}
			
			
			
			connection.close();
			

			/**
			 * Notify all Listeners to shut down. if you don't signal them, they
			 * will be running forever.
			 */
		} 
}

     

    
