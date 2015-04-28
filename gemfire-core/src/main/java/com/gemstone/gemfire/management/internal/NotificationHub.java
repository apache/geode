/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management.internal;

import java.util.HashMap;
import java.util.Map;

import javax.management.InstanceNotFoundException;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanServer;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.management.ManagementException;

/**
 * This class acts as a central point hub for collecting all notifications
 * originated from VM and sending across to Managing Node
 * 
 * @author rishim
 * 
 */
public class NotificationHub {

  /**
   * logger
   */
  private LogWriterI18n logger;

  /**
   * This is a single window to manipulate region resources for management
   */
  protected ManagementResourceRepo repo;

  /**
   * Platform MBean Server
   */
  private MBeanServer mbeanServer = MBeanJMXAdapter.mbeanServer;
  
  private Map<ObjectName , NotificationHubListener> listenerObjectMap;
 
  
  /** Member Name **/
  private String memberSource;

  /**
   * public constructor
   * 
   * @param repo
   *          Resource repo for this member
   */
  public NotificationHub(ManagementResourceRepo repo) {
    this.repo = repo;
    logger = InternalDistributedSystem.getLoggerI18n();
    this.listenerObjectMap = new HashMap<ObjectName, NotificationHubListener>();
    memberSource = MBeanJMXAdapter.getMemberNameOrId(InternalDistributedSystem
        .getConnectedInstance().getDistributedMember());
   

  }

  /**
   * Adds a NotificationHubListener
   * 
   * @param objectName
   */
  public void addHubNotificationListener(String memberName,
      ObjectName objectName) {

    try {
      synchronized (listenerObjectMap) {
        NotificationHubListener listener = listenerObjectMap.get(objectName);
        if (listener == null) {
          listener = new NotificationHubListener(objectName);
          listener.incNumCounter();
          mbeanServer.addNotificationListener(objectName, listener, null, null);
          listenerObjectMap.put(objectName, listener);
        } else {
          listener.incNumCounter();
        }
      }

    } catch (InstanceNotFoundException e) {
      throw new ManagementException(e);
    }
  }

  /**
   * Removes a NotificationHubListener
   * 
   * @param objectName
   */
  public void removeHubNotificationListener(String memberName, ObjectName objectName) {
    try {
      synchronized (listenerObjectMap) {
        if (listenerObjectMap.get(objectName) != null) {
          NotificationHubListener listener = listenerObjectMap.get(objectName);
          if (listener.decNumCounter() == 0) {
            listenerObjectMap.remove(objectName);
            // The MBean might have been un registered if the resource is
            // removed from cache.
            // The below method is to ensure clean up of user defined MBeans
            mbeanServer.removeNotificationListener(objectName, listener);
          }
        }
      }
    } catch (ListenerNotFoundException e) {
      // No op
    } catch (InstanceNotFoundException e) {
      // No op
    }
  }
  
  /**
   * This method is basically to cleanup resources which might cause leaks if
   * the same VM is used again for cache creation.
   */
  public void cleanUpListeners() {
    synchronized (listenerObjectMap) {
      for (ObjectName objectName : listenerObjectMap.keySet()) {

        NotificationHubListener listener = listenerObjectMap.get(objectName);

        if (listener != null) {
          try {
            mbeanServer.removeNotificationListener(objectName, listener);
          } catch (ListenerNotFoundException e) {
            // Do nothing. Already have been un-registered ( For listeners which
            // are on other MBeans apart from MemberMXBean)
          } catch (InstanceNotFoundException e) {
            // Do nothing. Already have been un-registered ( For listeners which
            // are on other MBeans apart from MemberMXBean)
          }
        }
      }
    }

    listenerObjectMap.clear();
  }

  public Map<ObjectName, NotificationHubListener> getListenerObjectMap() {
    return this.listenerObjectMap;
  }

  /**
   * This class is the managed node counterpart to listen to notifications from
   * MBeans for which it is resistered
   * 
   * @author rishim
   * 
   */
  public class NotificationHubListener implements NotificationListener {
    /**
     * MBean for which this listener is added
     */
    private ObjectName name;
    
    /**
     * Counter to indicate how many listener are attached to this MBean
     */
    private int numCounter = 0;


    protected NotificationHubListener(ObjectName name) {
      this.name = name;
    }   

    public int incNumCounter() {
       return ++numCounter;
    }

    public int decNumCounter() {
       return --numCounter;
    }
    
    public int getNumCounter(){
      return this.numCounter;
    }
    
    @Override
    public void handleNotification(Notification notification, Object handback) {
      NotificationKey key = new NotificationKey(name);
      notification.setUserData(memberSource);
      repo.putEntryInLocalNotificationRegion(key, notification);
    }

  }

}