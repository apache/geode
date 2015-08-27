/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;

/**
 * This class provides 'do-nothing' implementations of all of the methods of
 * interface ClientServerObserver. See the documentation for class
 * ClientServerObserverHolder for details.
 * 
 * @author Yogesh Mahajan
 * @since 5.1
 */
public class ClientServerObserverAdapter implements ClientServerObserver
{
  /**
   * This callback is called when now primary Ep is identified.
   */
  public void afterPrimaryIdentificationFromBackup(ServerLocation primaryEndpoint)
  {
  }

  /**
   * This callback is called just before interest registartion
   */
  public void beforeInterestRegistration()
  {
  }

  /**
   * This callback is called just after interest registartion
   */
  public void afterInterestRegistration()
  {
  }

  /**
   * This callback is called just before primary identification
   */
  public void beforePrimaryIdentificationFromBackup()
  {
  }

  /**
   * This callback is called just before Interest Recovery by DSM thread happens
   */
  public void beforeInterestRecovery()
  {

  }

  public void beforeFailoverByCacheClientUpdater(ServerLocation epFailed)
  {
  }
  /**
   * Invoked before sending an instantiator message to server
   * 
   * @param eventId
   */
  public void beforeSendingToServer(EventID eventId){
    
  }
  /**
   * Invoked after sending an instantiator message to server 
   * 
   * @param eventId
   */
  public void afterReceivingFromServer(EventID eventId){
    
  }
  
  /**
   * This callback is called just before sending client ack to the primary servrer.
   */
  public void beforeSendingClientAck(){
    
  }  

  /**
   * Invoked after Message is created
   *
   * @param msg
   */
  public void afterMessageCreation(Message msg){
  
  }
  
  /**
   * Invoked after Queue Destroy Message has been sent
   */
  public void afterQueueDestroyMessage(){
    
  }
  
  /**
   * Invoked after a primary is recovered from a backup or new connection. 
   */
  public void afterPrimaryRecovered(ServerLocation location) {
  }
}
