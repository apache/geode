/*========================================================================= 
 * (c)Copyright 2002-2011, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006 
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.connection.internal;

import java.util.ArrayList;
import java.util.Date;

import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import com.gemstone.gemfire.admin.jmx.internal.AdminDistributedSystemJmxImpl;
import com.gemstone.gemfire.management.internal.beans.ResourceNotification;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.ConnectionFailureException;
import com.gemstone.gemfire.mgmt.DataBrowser.utils.LogUtil;

public class JMXNotificationListener implements NotificationListener, Runnable {

  private volatile boolean _stop;
  protected ArrayList< Notification >      _notifications;
  protected Thread         _notifier;
  private JMXDiscoveryImpl discovery;

  JMXNotificationListener(JMXDiscoveryImpl dscvry) {
    this.discovery = dscvry;
    _notifications = new ArrayList<Notification>();
    _stop = false;
    _notifier = new Thread(this, "JMXNotificationListener");
    _notifier
        .setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
          public void uncaughtException(Thread t, Throwable e) {
            LogUtil.error("Uncaught Exception " + t.getId(), e);
          }
        });
  }

  protected void addNotification(Notification notification) {
    synchronized (this._notifications) {
      _notifications.add(notification);
    }

    LogUtil.fine("\tAdded a new Notification :" + notification.getType());
  }

  protected int getNotificationsListSize() {
    synchronized (this._notifications) {
      return _notifications.size();
    }
  }

  protected Notification getNextNotification() {
    Notification ntfyRet = null;
    synchronized (this._notifications) {
      if (_notifications.size() > 0) {
        ntfyRet = (Notification) _notifications.remove(0);
      }
    }

    return ntfyRet;
  }

  protected void processNotification(Notification notification)
      throws Exception {
    String type = notification.getType();

    if (type.equals(AdminDistributedSystemJmxImpl.NOTIF_MEMBER_JOINED)) {
      discovery.registerDSMember((ObjectName) notification.getSource());
    } else if (type.equals(AdminDistributedSystemJmxImpl.NOTIF_MEMBER_LEFT)) {
      ObjectName mem = (ObjectName) notification.getSource();
      String id = JMXDiscoveryImpl.getMemberID(mem);
      discovery.cleanupMember(id, false);

    } else if (type.equals(AdminDistributedSystemJmxImpl.NOTIF_MEMBER_CRASHED)) {
      ObjectName mem = (ObjectName) notification.getSource();
      String id = JMXDiscoveryImpl.getMemberID(mem);
      discovery.cleanupMember(id, true);
    }

  }
  
  protected void processMBeanNotification(Notification notification)
      throws Exception {
    String type = notification.getType();
    if (type.equals(ResourceNotification.CACHE_SERVER_STOPPED)) {
      discovery.cleanupMember((String)notification.getSource(), false);
    } else if (type.equals(ResourceNotification.CACHE_MEMBER_DEPARTED)) {
      discovery.cleanupMember((String)notification.getSource(), true);
    }
    ObjectName source = discovery.getDistributedSystemMXBeanProxy().fetchMemberObjectName((String)notification.getSource());
    if (type.equals(ResourceNotification.CACHE_MEMBER_JOINED)) {
      discovery.registerDSMemberMbean(source);
    } else  if (type.equals(ResourceNotification.CACHE_SERVER_STARTED)) {
      discovery.registerDSMemberMbean(source);
    } 

  }

  public void handleNotification(Notification notification, Object handback) {
    String type = notification.getType();
    Date time = new Date(notification.getTimeStamp());
    String memID = notification.getMessage();
    long uniqueNum = notification.getSequenceNumber();

//    LogUtil.info("Received a Notification for " + "Type = " + type + "   "
  //      + "Time = " + time.toString() + "\n\t" + "MemID = " + memID + "   "
    //    + "Unique# = " + uniqueNum);

    addNotification(notification);
    synchronized (_notifier) {
      _notifier.notify();
    }
  }

  public void run() {
    // Enter waiting
    while (true && !_stop) {
      boolean interrupted = Thread.interrupted();

      try {
        if (!interrupted) {
          // Sit in a loop and process all notifications
          Notification notification;
          while ((notification = getNextNotification()) != null && !_stop) {
            //processNotification(notification);
            processMBeanNotification(notification);
          }

          if (!_stop) {
            // Enter wait loop
            synchronized (_notifier) {
              _notifier.wait();
            }
          }
        }
      } catch (InterruptedException exp1) {
        // Clear the status via Exception, since we're waiting
        interrupted = true;
        LogUtil
            .fine("DSNotificaionListener: Thread's interrupt status post last interrupted "
                + interrupted);

      } catch (Exception exp2) {
        // TODO: Need a mechanism to stop the execution.
        LogUtil.error("Exception in JMX notification listener", exp2);

      } finally {
        if (interrupted) {
          _stop = true;

          LogUtil
              .warning("JMXNotificaionListener: Thread got an interrupt. Should stop ");
        }
      }
    }

    LogUtil.fine("JMXNotificaionListener: Thread out of run loop: Stop");
  }

  public void stop() {
    synchronized (_notifier) {
      if (discovery.getConnection() != null) {
        try {
          discovery.getConnection().removeNotificationListener(
              discovery.getAdminDistributedSystem(), this);
        } catch (Exception e) {
          LogUtil
              .warning("Exception while removing JMX connection notifier", e);
        }
      }
      _stop = true;
      _notifier.notify(); // Should run the loop & exit
    }
  }

  // TODO - MGH Is this correct? If an exception is thrown by addNotificationListener
  // the thread does not start? Why are multiple threads started? Why does the is the Thread 
  // created in the ctor of this class a named thread while this one is not? 
  public void start() throws ConnectionFailureException {
    if (_stop) {
      _notifier = new Thread(this);
      _stop = false;
    }

    try {
      discovery.getConnection().addNotificationListener(discovery.getAdminDistributedSystem(), this, null, null);
    } catch (Exception ex) {
      String message = "Unable to listen for GemFire system membership changes. Reason : "+ex.getMessage();
      throw new ConnectionFailureException(message, ex);
    }

    _notifier.start();
  }
}
