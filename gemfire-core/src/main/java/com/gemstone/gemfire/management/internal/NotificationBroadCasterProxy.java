/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management.internal;

import javax.management.Notification;
import javax.management.NotificationBroadcaster;

/**
 * This interface is to extend the functionality of NotificationBroadcaster.
 * It enables proxies to send notifications on their own as 
 * proxy implementations have to implement the sendNotification method.
 * 
 * 
 * @author rishim
 *
 */

public interface NotificationBroadCasterProxy extends NotificationBroadcaster{
  /**
   * send the notification to registered clients
   * @param notification
   */
	public void sendNotification(Notification notification);
}
