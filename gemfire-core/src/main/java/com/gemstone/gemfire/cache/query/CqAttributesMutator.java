/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.cache.query;

/**
 * This interface is used to modify the listeners that are associated with a CQ. 
 * Each CqQuery has an CqAttributesMutator interface which supports modification 
 * of certain CQ attributes after the CQ has been created. 
 * 
 * @author anil 
 * @since 5.5
 */
public interface CqAttributesMutator {
  
  /**
   * Adds a CQ listener to the end of the list of CQ listeners on this CqQuery.
   * @param aListener the user defined CQ listener to add to the CqQuery.
   * @throws IllegalArgumentException if <code>aListener</code> is null
   */
  public void addCqListener(CqListener aListener);
  
  /**
   * Removes given CQ listener from the list of CQ listeners on this CqQuery.
   * Does nothing if the specified listener has not been added.
   * If the specified listener has been added then will
   * be called on it; otherwise does nothing.
   * @param aListener the CQ listener to remove from the CqQuery.
   * @throws IllegalArgumentException if <code>aListener</code> is null
   */
  public void removeCqListener(CqListener aListener);
  
  /**
   * Adds the given set CqListner on this CQ. If the CQ already has CqListeners, this 
   * removes those old CQs and initializes with the newListeners.
   * @param newListeners a possibly null or empty array of listeners to add 
   * to this CqQuery.
   * @throws IllegalArgumentException if the <code>newListeners</code> array 
   * has a null element
   */
  public void initCqListeners(CqListener[] newListeners);
  
}
