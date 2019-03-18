package com.test.kafka;

import java.util.Scanner;
import com.test.kafka.KafkaSampleProducer.*;
/**
 * This functions takes count of message as input and sends to kafka broker
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	KafkaSampleProducer kafkaSampleProducer =new KafkaSampleProducer();
        System.out.println( "Starting to send data data in kafka" );
        Scanner user = new Scanner( System.in ); 
        
        int count = 0;
        System.out.println("Please enter number of messages to be sent: ");
        count = user.nextInt();
        
        int userInterval = 0;
        String userTopic = null;
        String userBroker = null;
        
        System.out.println("Please enter the interval in which data to be sent: ");
        userInterval = user.nextInt();
        
        System.out.println("Please enter topic in which data to be sent: ");
        userTopic = user.nextLine();
        
        System.out.println("Please enter broker in which data to be sent: ");
        userBroker = user.nextLine().trim();
        
        try 
        {
			kafkaSampleProducer.kafkaDataCreator(count, userInterval,  userTopic,  userBroker);
		} 
        catch (Exception e) 
        {
			System.out.println("Exception has occured. ");
			e.printStackTrace();
		}        
    }
}
