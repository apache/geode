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
 * This interface is used by testing/debugging code to be notified of different
 * client/server events.
 * See the documentation for class ClientServerObserverHolder for details.
 * 
 * @author Yogesh Mahajan
 * @since 5.1
 *  
 */
public interface ClientServerObserver
{
  /**
   * This callback is called when now primary Ep is identified.
   */
  public void afterPrimaryIdentificationFromBackup(ServerLocation location);

  /**
   * This callback is called just before interest registartion
   */
  public void beforeInterestRegistration();

  /**
   * This callback is called just after interest registartion
   */
  public void afterInterestRegistration();

  /**
   * This callback is called just before primary identification
   */
  public void beforePrimaryIdentificationFromBackup();

  /**
   * This callback is called just before Interest Recovery by DSM thread happens
   */
  public void beforeInterestRecovery();
  
  /**
   * Invoked by CacheClientUpdater just before invoking endpointDied for
   * fail over
   * @param location ServerLocation which has failed
   */
  public void beforeFailoverByCacheClientUpdater(ServerLocation location);
  /**
   * Invoked before sending an instantiator message to server
   * 
   * @param eventId
   */
  public void beforeSendingToServer(EventID eventId);
  /**
   * Invoked after sending an instantiator message to server 
   * 
   * @param eventId
   */
  public void afterReceivingFromServer(EventID eventId);

  /**
   * This callback is called just before sending client ack to the primary servrer.
   */
   public void beforeSendingClientAck();  

   /**
    * Invoked after Message is created
    *
    * @param msg
    */
   public void afterMessageCreation(Message msg);
   
   /**
    * Invoked after Queue Destroy Message has been sent
    */
   public void afterQueueDestroyMessage();
   
   /**
    * Invoked after a primary is recovered from a backup or new connection. 
    */
   public void afterPrimaryRecovered(ServerLocation location);
   
}
