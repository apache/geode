/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.tier.sockets;

import com.gemstone.gemfire.internal.cache.EnumListenerEvent;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientUpdateMessageImpl.CqNameToOp;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;

import java.util.HashMap;

/**
 * Interface <code>ClientUpdateMessage</code> is a message representing a cache
 * operation that is sent from a server to an interested client.
 *
 * @author Barry Oglesby
 *
 * @since 5.5
 */
public interface ClientUpdateMessage extends ClientMessage {

  /**
   * Returns the region name that was updated.
   * 
   * @return the region name that was updated
   */
  public String getRegionName();

  /**
   * Set the region name that was updated.
   */
  public void setRegionName(String regionName);

  /**
   * Returns the new value.
   * 
   * @return the new value
   */
  public Object getValue();

  /**
   * Returns the new value.
   * 
   * @return the new value
   */
  public boolean valueIsObject();

  /**
   * Returns the key that was updated.
   * 
   * @return the key that was updated
   */
  public Object getKeyOfInterest();
  
  /**
   * Returns the operation
   * @return the operation performed by this message
   */
  public EnumListenerEvent getOperation();
  

  /**
   * Returns the membership Id of the originator of the event.
   * 
   * @return the membership Id of the originator of the event.
   */
  public ClientProxyMembershipID getMembershipId();
  
  /**
   * Returns whether the message satisfies cqs.
   *
   * @return boolean true if cq info is present.
   */
  public boolean hasCqs();
  
  
  public CqNameToOp getClientCq(ClientProxyMembershipID clientId);
  
  /**
   * Returns whether the message satisfies CQs for the given clientId.
   * @param clientId ClientProxyMembershipID 
   * @return boolean true if CQ info is present for the given proxy.
   */
  public boolean hasCqs(ClientProxyMembershipID clientId); 
  
  /**
   * Returns the version of the region entry affected by this operation
   * on the server
   */
  public VersionTag getVersionTag();

  /**
   * Returns true if the message satisfies the interest list registered
   * by the given client.
   * @param clientId ClientProxyMembershipID
   * @return true if the given client is interested in this message
   */
  public boolean isClientInterested(ClientProxyMembershipID clientId);
    
  /**
   * Returns whether the message is due to a result of net load.
   *
   * @return boolean true if the event is due to net load.
   */
  public boolean isNetLoad(); 

  /**
   * Sets the isNetLoad flag.
   *
   * @param isNetLoad boolean true if the event is due to net load.
   */
  public void setIsNetLoad(boolean isNetLoad); 

  /**
   * Returns whether this <code>ClientUpdateMessage</code>'s operation is
   * AFTER_CREATE.
   *
   * @return whether this <code>ClientUpdateMessage</code>'s operation is
   *         AFTER_CREATE
   */
  public boolean isCreate();

  /**
   * Returns whether this <code>ClientUpdateMessage</code>'s operation is
   * AFTER_UPDATE.
   *
   * @return whether this <code>ClientUpdateMessage</code>'s operation is
   *         AFTER_UPDATE
   */
  public boolean isUpdate();

  /**
   * Returns whether this <code>ClientUpdateMessage</code>'s operation is
   * AFTER_DESTROY.
   *
   * @return whether this <code>ClientUpdateMessage</code>'s operation is
   *         AFTER_DESTROY
   */
  public boolean isDestroy();

  /**
   * Returns whether this <code>ClientUpdateMessage</code>'s operation is
   * AFTER_INVALIDATE.
   *
   * @return whether this <code>ClientUpdateMessage</code>'s operation is
   *         AFTER_INVALIDATE
   */
  public boolean isInvalidate();

  /**
   * Returns whether this <code>ClientUpdateMessage</code>'s operation is
   * AFTER_REGION_DESTROY.
   *
   * @return whether this <code>ClientUpdateMessage</code>'s operation is
   *         AFTER_REGION_DESTROY
   */
  public boolean isDestroyRegion();

  /**
   * Returns whether this <code>ClientUpdateMessage</code>'s operation is
   * AFTER_REGION_CLEAR.
   *
   * @return whether this <code>ClientUpdateMessage</code>'s operation is
   *         AFTER_REGION_CLEAR
   */
  public boolean isClearRegion();
  
  /**
   * @return whether this message can be sent without going through an
   * authorization check
   */
  public boolean needsNoAuthorizationCheck();
     
}
