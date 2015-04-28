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

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * This class actually distribute the notification with the help of the actual
 * broadcaster proxy.
 * 
 * @author rishim
 * 
 */

public class NotificationHubClient {

  private static final Logger logger = LogService.getLogger();
  
  /**
   * proxy factory
   */
	private MBeanProxyFactory proxyFactory;

	protected NotificationHubClient(MBeanProxyFactory proxyFactory) {
		this.proxyFactory = proxyFactory;
	}

	/**
	 * send the notification to actual client
	 * on the Managing node VM
	 * 
	 * it does not throw any exception. it will capture all
	 * exception and log a warning
	 * @param event
	 */
	public void sendNotification(EntryEvent<NotificationKey, Notification> event) {

		NotificationBroadCasterProxy notifBroadCaster;
		try {

      notifBroadCaster = proxyFactory.findProxy(event.getKey().getObjectName(),
          NotificationBroadCasterProxy.class);
			// Will return null if the Bean is filtered out.
			if (notifBroadCaster != null) {
				notifBroadCaster.sendNotification(event.getNewValue());
			}

		} catch (Exception e) {
		  if (logger.isDebugEnabled()) {
		    logger.debug(" NOTIFICATION Not Done {}", e.getMessage(), e);
		  }		  
      logger.warn(e.getMessage(), e);
    }

	}

}
