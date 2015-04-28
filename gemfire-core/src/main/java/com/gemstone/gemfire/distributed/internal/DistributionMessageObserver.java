/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal;


/**
 * This class is a test hook to intercept DistributionMessages in the
 * VM receiving the message.
 * 
 * @author dsmith
 *
 */
public abstract class DistributionMessageObserver {
  
  private static DistributionMessageObserver instance;
  
  /**
   * Set the instance of the observer. Setting to null will clear the observer.
   * @param instance
   * @return the old observer, or null if there was no old observer.
   */
  public static final DistributionMessageObserver setInstance(DistributionMessageObserver instance) {
    DistributionMessageObserver oldInstance = DistributionMessageObserver.instance;
    DistributionMessageObserver.instance = instance;
    return oldInstance;
  }
  
  public static final DistributionMessageObserver getInstance() {
    return instance;
  }
  
  /**
   * Called before a the process method of the DistributionMessage is called
   * @param dm the distribution manager that received the message
   * @param message The message itself
   */
  public void beforeProcessMessage(DistributionManager dm,
      DistributionMessage message) {
    
  }

  /**
   * Called after the process method of the DistributionMessage is called
   * @param dm the distribution manager that received the message
   * @param message The message itself
   */
  public void afterProcessMessage(DistributionManager dm,
      DistributionMessage message) {
    
  }

  /**
   * Called just before a message is distributed.
   * @param dm the distribution manager that's sending the messsage
   * @param msg the message itself
   */
  public void beforeSendMessage(DistributionManager dm,
      DistributionMessage msg) {
    
  }
}