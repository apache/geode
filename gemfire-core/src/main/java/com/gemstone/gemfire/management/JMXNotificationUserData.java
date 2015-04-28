/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management;

/**
 * This interface acts as UserData section of JMX notifications of type "system.alert".
 * It contains some additional information apart from the Notification details.
 * 
 * @author rishim
 * @since  8.0
 */
public interface JMXNotificationUserData { 
  /**
   * The level at which this alert is issued.
   */
  public static final String ALERT_LEVEL = "AlertLevel"; 
  
  /**
   * The member of the distributed system that issued the alert, or
   * null if the issuer is no longer a member of the distributed system.
   * This constant is defined in com.gemstone.gemfire.management.UserData
   */
  
  public static final String MEMBER = "Member"; 
  
  /** 
   * The thread causing the alert
   */
  
  public static final String THREAD = "Thread";

}
