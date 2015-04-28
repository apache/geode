/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.query;

/** 
 * This interface holds all attribute values for a CQ and provides methods for 
 * retrieving all attribute settings. This interface can be modified only by 
 * the CqAttributesFactory class (before CQ creation) and the CqAttributesMutator 
 * interface (after CQ creation).
 * 
 * For compatibility rules and default values, see {@link CqAttributesFactory}.
 *
 * @author Anil
 * @since 5.5
 */

public interface CqAttributes {
    
  /**
   * Get the CqListeners set with the CQ.
   * Returns all the Listener associated with this CQ.
   * @see CqListener
   * @return CQListener[] array of CqListner
   */
  public CqListener[] getCqListeners();
  
  /**
   * Get the CqListener set with the CQ.
   * Returns the CqListener associated with the CQ. 
   * If there are more than one CqListener throws IllegalStateException. 
   * @see CqListener
   * @return CqListener Object, returns null if there is no CqListener.      
   */
  public CqListener getCqListener();
   
}
