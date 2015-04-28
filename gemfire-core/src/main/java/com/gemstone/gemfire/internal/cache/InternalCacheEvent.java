/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.CacheEvent;
import com.gemstone.gemfire.internal.cache.FilterRoutingInfo.FilterInfo;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;

/**
 * A CacheEvent, but the isGenerateCallbacks() is hidden from public consumption
 * @author jpenney
 *
 */
public interface InternalCacheEvent extends CacheEvent
{
  /**
   * Answers true if this event should generate user callbacks.
   * @return true if this event will generate user callbacks
   */
  public boolean isGenerateCallbacks();
  
  /**
   * Answers true if this event is from a client
   * @deprecated as of 5.7 use {@link #hasClientOrigin} instead.
   */
  @Deprecated
  public boolean isBridgeEvent();
  /**
   * Answers true if this event is from a client
   * @since 5.7
   */
  public boolean hasClientOrigin();
  
  /**
   * returns the ID associated with this event
   */
  public EventID getEventId();
  
  /**
   * Returns the Operation type.
   * @return eventType
   */
  public EnumListenerEvent getEventType();

  /**
   * sets the event type
   * @param operation the operation performed by this event
   */
  public void setEventType(EnumListenerEvent operation);
  
  /**
   * returns the bridge context for the event, if any
   */
  public ClientProxyMembershipID getContext();
  
  /**
   * set the client routing information for this event
   * @param info TODO
   */
  public void setLocalFilterInfo(FilterInfo info);
  
  /**
   * get the local routing information for this event
   */
  public FilterInfo getLocalFilterInfo();
  
  /**
   * get the version tag for the event
   */
  public VersionTag getVersionTag();

}
