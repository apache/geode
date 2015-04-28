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
import javax.management.ObjectName;

import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.RegionEvent;

/**
 * This listener will be attached to each notification region
 * corresponding to a member 
 * @author rishim
 *
 */
public class NotificationCacheListener implements
		CacheListener<NotificationKey, Notification> {
  
  /**
   * For the 
   */
	private NotificationHubClient notifClient;
	
  private volatile boolean  readyForEvents;

	public NotificationCacheListener(MBeanProxyFactory proxyHelper) {

		notifClient = new NotificationHubClient(proxyHelper);
		this.readyForEvents = false;

	}

	@Override
	public void afterCreate(EntryEvent<NotificationKey, Notification> event) {
	  if(!readyForEvents){
      return;
    }
		notifClient.sendNotification(event);

	}

	@Override
	public void afterDestroy(EntryEvent<NotificationKey, Notification> event) {
		// TODO Auto-generated method stub

	}

	@Override
	public void afterInvalidate(EntryEvent<NotificationKey, Notification> event) {
		// TODO Auto-generated method stub

	}

	@Override
	public void afterRegionClear(RegionEvent<NotificationKey, Notification> event) {
		// TODO Auto-generated method stub

	}

	@Override
	public void afterRegionCreate(RegionEvent<NotificationKey, Notification> event) {
		// TODO Auto-generated method stub

	}

	@Override
	public void afterRegionDestroy(RegionEvent<NotificationKey, Notification> event) {
		// TODO Auto-generated method stub

	}

	@Override
	public void afterRegionInvalidate(
			RegionEvent<NotificationKey, Notification> event) {
		// TODO Auto-generated method stub

	}

	@Override
	public void afterRegionLive(RegionEvent<NotificationKey, Notification> event) {
		// TODO Auto-generated method stub

	}

	@Override
	public void afterUpdate(EntryEvent<NotificationKey, Notification> event) {
    if(!readyForEvents){
      return;
    }
		notifClient.sendNotification(event);

	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}
  public void markReady(){
    readyForEvents = true;
  }

}
