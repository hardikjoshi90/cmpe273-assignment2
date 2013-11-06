package edu.sjsu.cmpe.library.api.resources;

import java.net.MalformedURLException;
import java.net.URL;
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
    	 long isbn;
         String bookTitle;
         String bookCategory;
         String bookCoverImage;
         
         
    	 
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
     	    Message topicmsg = topicconsumer.receive(500);
     	    
     	    if(topicmsg == null)
     	    	break;
     	    if( topicmsg instanceof  TextMessage ) {
     		String body = ((TextMessage) topicmsg).getText();
     		newBooks.add(body);
     		if( "SHUTDOWN".equals(body)) {
     		    break;
     		}
     		System.out.println("Received message = " + body);

     	    } else if (topicmsg instanceof StompJmsMessage) {
     		StompJmsMessage smsg = ((StompJmsMessage) topicmsg);
     		String body = smsg.getFrame().contentAsString();
     		if ("SHUTDOWN".equals(body)) {
     		    break;
     		}
     		System.out.println("Received message = " + body);

     	    } else {
     		System.out.println("Unexpected message type: "+topicmsg.getClass());
     	    }
     	}
     	topicconnection.close();
     	System.out.println(newBooks);
     	if(!newBooks.isEmpty())
     	{
     		for(String book:newBooks)
     		{
     			Book addBook = new Book();
     			isbn = Long.parseLong(book.split(":")[0]);
     			bookTitle = book.split(":")[1].toString();
     			bookCategory = book.split(":")[2].toString();
     			bookCoverImage = book.split(":")[3].toString();
     			
     			addBook = bookRepository.getBookByISBN(isbn);
     			if(addBook.equals(""))
     			{
     				addBook.setIsbn(isbn);
     				addBook.setTitle(bookTitle);
     				addBook.setCategory(bookCategory);
     				addBook.setCoverimage(new URL(bookCoverImage));
     				bookRepository.saveBook(addBook);
     			}
     			
     		}
     	}
		return topicsession;
     }
         
 }
