package edu.sjsu.cmpe.library.api.resources;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.fusesource.stomp.jms.StompJmsConnectionFactory;
import org.fusesource.stomp.jms.StompJmsDestination;

import edu.sjsu.cmpe.library.config.LibraryServiceConfiguration;

public class point2pointProducer {
	
	private String apolloUser;
    private String apolloPassword;
    private String apolloHost;
    private int apolloPort;
    private String stompQueue;
    private final LibraryServiceConfiguration configuration;
    
    public point2pointProducer(LibraryServiceConfiguration config) {
            this.configuration = config;
            apolloUser = configuration.getApolloUser();
            apolloPassword = configuration.getApolloPassword();
            apolloHost = configuration.getApolloHost();
            apolloPort = configuration.getApolloPort();
            stompQueue = configuration.getStompQueueName();
            
    }
    
    public void producer(String msgData) throws JMSException
    {
    	StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
        factory.setBrokerURI("tcp://" + apolloHost + ":" + apolloPort);
        System.out.println(factory.getBrokerURI());

        Connection connection = factory.createConnection(apolloUser, apolloPassword);
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination dest = new StompJmsDestination(stompQueue);
        MessageProducer producer = session.createProducer(dest);
        System.out.println("Sending messages to " + stompQueue + "...");

        TextMessage msg = session.createTextMessage(msgData);
        msg.setLongProperty("id", System.currentTimeMillis());
        System.out.println(msg.getText());
        producer.send(msg);
        //producer.send(session.createTextMessage("SHUTDOWN"));
        connection.close();
    }

}
