package edu.sjsu.cmpe.library.api.resources;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Savepoint;
import java.util.ArrayList;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.fusesource.stomp.jms.StompJmsConnectionFactory;
import org.fusesource.stomp.jms.StompJmsDestination;
import org.fusesource.stomp.jms.message.StompJmsMessage;

import ch.qos.logback.core.status.Status;
import edu.sjsu.cmpe.library.config.LibraryServiceConfiguration;
import edu.sjsu.cmpe.library.domain.Book;
import edu.sjsu.cmpe.library.repository.BookRepositoryInterface;

public class pubsubListener {
	
	 private String apolloUser;
     private String apolloPassword;
     private String apolloHost;
     private int apolloPort;
     private String stompTopic;
     private final LibraryServiceConfiguration configuration;
     private BookRepositoryInterface bookRepository;
     
     long isbn;
     String bookTitle;
     String bookCategory;
     String bookCoverImage;
     Status status;
     Book addBook = new Book();
     
     public pubsubListener(LibraryServiceConfiguration config, BookRepositoryInterface bookRepository) 
     {
         this.configuration = config;
         this.bookRepository = bookRepository;
         apolloUser = configuration.getApolloUser();
         apolloPassword = configuration.getApolloPassword();
         apolloHost = configuration.getApolloHost();
         apolloPort = configuration.getApolloPort();
         stompTopic = configuration.getStompTopicName();
     }
     
     public Runnable listener() throws JMSException, MalformedURLException
     {
    	 
         
         
         
    	 
    	 StompJmsConnectionFactory topicfactory = new StompJmsConnectionFactory();
     	topicfactory.setBrokerURI("tcp://" + apolloHost + ":" + apolloPort);
     	ArrayList<String> newBooks = new ArrayList<String>();
     	Connection topicconnection = topicfactory.createConnection(apolloUser, apolloPassword);
     	topicconnection.start();
     	Session topicsession = topicconnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
     	Destination topicdest = new StompJmsDestination(stompTopic);

     	MessageConsumer topicconsumer = topicsession.createConsumer(topicdest);
     	System.currentTimeMillis();
     	System.out.println("Waiting for messages...");
     	while(true) {
     	    Message topicmsg = topicconsumer.receive();
     	    
     	    if(topicmsg == null)
     	    	break;
     	    if( topicmsg instanceof  TextMessage ) {
     		String body = ((TextMessage) topicmsg).getText();
     		
 			
     		System.out.println("Received message Text = " + body);
     		
     		System.out.println("******New Book"+newBooks);
     		isbn = Long.parseLong(body.split(":")[0]);
 			bookTitle = body.split(":")[1].toString();
 			bookCategory = body.split(":")[2].toString();
 			bookCoverImage = body.split(":\"")[3].toString();
 			bookCoverImage =bookCoverImage.substring(0,bookCoverImage.length()-1);
 			
 			addBook = bookRepository.getBookByISBN(isbn);
 			if(addBook==null)
 			{
 				Book book = new Book();
 				book.setTitle(bookTitle);
 				book.setIsbn(isbn);
 				
 				book.setCategory(bookCategory);
 				book.setCoverimage(new URL(bookCoverImage));
 				//addBook.setStatus(addBook.getStatus());
 				bookRepository.saveBook(book);
 				System.out.println(book.getIsbn());
 			}
 			else
 			{
 				addBook.setStatus(edu.sjsu.cmpe.library.domain.Book.Status.available);
 				bookRepository.saveBook(addBook);
 			}

     		
     		
     	    } else if (topicmsg instanceof StompJmsMessage) {
     		StompJmsMessage smsg = ((StompJmsMessage) topicmsg);
     		String body = smsg.getFrame().contentAsString();
     		System.out.println("Received message Stomp = " + body);
     	
     	    } else {
     		System.out.println("Unexpected message type: "+topicmsg.getClass());
     	    }
     	}
     	
     	topicconnection.close();
		return topicsession;
     }
         
 }
