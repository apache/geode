/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.admin.jmx.internal;

import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.management.InstanceNotFoundException;
import javax.management.JMException;
import javax.management.JMRuntimeException;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.MBeanServerNotification;
import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.timer.TimerMBean;

import org.apache.commons.modeler.ManagedBean;
import org.apache.commons.modeler.Registry;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;

import org.apache.geode.SystemFailure;
import org.apache.geode.admin.RuntimeAdminException;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.logging.LogService;

/**
 * Common support for MBeans and {@link ManagedResource}s. Static loading of this class creates the
 * MBeanServer and Modeler Registry.
 *
 * @since GemFire 3.5
 *
 */
public class MBeanUtil {

  private static final Logger logger = LogService.getLogger();

  /** The default MBeanServer domain name is "GemFire" */
  private static final String DEFAULT_DOMAIN = "GemFire";

  /** MBean Name for refreshTimer */
  private static String REFRESH_TIMER_NAME = DEFAULT_DOMAIN + ":type=RefreshTimer";

  /* indicates whether the mbeanServer, registry & refreshTimer are started */
  private static boolean isStarted;

  /** The Commons-Modeler configuration registry for our managed beans */
  private static Registry registry;

  /** The <code>MBeanServer</code> for this application */
  private static MBeanServer mbeanServer;

  /** MBean name of the Timer which handles refresh notifications */
  private static ObjectName refreshTimerObjectName;

  /** Actual TimerMBean responsible for refresh notifications */
  private static TimerMBean refreshTimer;

  /**
   * Map of ObjectNames to current timerNotificationIds
   * <p>
   * map: key=ObjectName, value=map: key=RefreshNotificationType, value=timerNotificationId
   */
  private static Map<NotificationListener, Map<RefreshNotificationType, Integer>> refreshClients =
      new HashMap<NotificationListener, Map<RefreshNotificationType, Integer>>();

  /** key=ObjectName, value=ManagedResource */
  private static final Map<ObjectName, ManagedResource> managedResources =
      new HashMap<ObjectName, ManagedResource>();

  static {
    try {
      refreshTimerObjectName = ObjectName.getInstance(REFRESH_TIMER_NAME);
    } catch (Exception e) {
      logStackTrace(Level.ERROR, e);
    }
  }

  /**
   * Initializes Mbean Server, Registry, Refresh Timer & registers Server Notification Listener.
   *
   * @return reference to the mbeanServer
   */
  static MBeanServer start() {
    if (!isStarted) {
      mbeanServer = createMBeanServer();
      registry = createRegistry();

      registerServerNotificationListener();
      createRefreshTimer();
      isStarted = true;
    }

    return mbeanServer;
  }

  /**
   * Stops Registry, Refresh Timer. Releases Mbean Server after.
   */
  static void stop() {
    if (isStarted) {
      stopRefreshTimer();

      registry.stop();
      registry = null;
      releaseMBeanServer();// makes mbeanServer null
      isStarted = false;
    }
  }

  /**
   * Create and configure (if necessary) and return the <code>MBeanServer</code> with which we will
   * be registering our <code>ModelMBean</code> implementations.
   *
   * @see javax.management.MBeanServer
   */
  static synchronized MBeanServer createMBeanServer() {
    if (mbeanServer == null) {
      mbeanServer = MBeanServerFactory.createMBeanServer(DEFAULT_DOMAIN);
    }
    return mbeanServer;
  }

  /**
   * Create and configure (if necessary) and return the Commons-Modeler registry of managed object
   * descriptions.
   *
   * @see org.apache.commons.modeler.Registry
   */
  static synchronized Registry createRegistry() {
    if (registry == null) {
      try {
        registry = Registry.getRegistry(null, null);
        if (mbeanServer == null) {
          throw new IllegalStateException(
              "MBean Server is not initialized yet.");
        }
        registry.setMBeanServer(mbeanServer);

        String mbeansResource = getOSPath("/org/apache/geode/admin/jmx/mbeans-descriptors.xml");

        URL url = ClassPathLoader.getLatest().getResource(MBeanUtil.class, mbeansResource);
        raiseOnFailure(url != null, String.format("Failed to find %s",
            new Object[] {mbeansResource}));
        registry.loadMetadata(url);

        // simple test to make sure the xml was actually loaded and is valid...
        String[] test = registry.findManagedBeans();
        raiseOnFailure(test != null && test.length > 0,
            String.format("Failed to load metadata from %s",
                new Object[] {mbeansResource}));
      } catch (Exception e) {
        logStackTrace(Level.WARN, e);
        throw new RuntimeAdminException(
            "Failed to get MBean Registry", e);
      }
    }
    return registry;
  }

  /**
   * Creates and registers a <code>ModelMBean</code> for the specified <code>ManagedResource</code>.
   * State changing callbacks into the <code>ManagedResource</code> will also be made.
   *
   * @param resource the ManagedResource to create a managing MBean for
   *
   * @return The object name of the newly-created MBean
   *
   * @see ManagedResource#setModelMBean
   */
  static ObjectName createMBean(ManagedResource resource) {
    return createMBean(resource, lookupManagedBean(resource));
  }

  /**
   * Creates and registers a <code>ModelMBean</code> for the specified <code>ManagedResource</code>.
   * State changing callbacks into the <code>ManagedResource</code> will also be made.
   *
   * @param resource the ManagedResource to create a managing MBean for
   * @param managed the ManagedBean definition to create the MBean with
   * @see ManagedResource#setModelMBean
   */
  static ObjectName createMBean(ManagedResource resource, ManagedBean managed) {

    try {
      DynamicManagedBean mb = new DynamicManagedBean(managed);
      resource.setModelMBean(mb.createMBean(resource));

      // create the ObjectName and register the MBean...
      final ObjectName objName;
      try {
        objName = ObjectName.getInstance(resource.getMBeanName());
      } catch (MalformedObjectNameException e) {
        throw new MalformedObjectNameException(String.format("%s in '%s'",
            new Object[] {e.getMessage(), resource.getMBeanName()}));
      }

      synchronized (MBeanUtil.class) {
        // Only register a bean once. Otherwise, you risk race
        // conditions with things like the RMI connector accessing it.

        if (mbeanServer != null && !mbeanServer.isRegistered(objName)) {
          mbeanServer.registerMBean(resource.getModelMBean(), objName);
          synchronized (managedResources) {
            managedResources.put(objName, resource);
          }
        }
      }
      return objName;
    } catch (java.lang.Exception e) {
      throw new RuntimeAdminException(
          String.format("Failed to create MBean representation for resource %s.",
              new Object[] {resource.getMBeanName()}),
          e);
    }
  }

  /**
   * Ensures that an MBean is registered for the specified <code>ManagedResource</code>. If an MBean
   * cannot be found in the <code>MBeanServer</code>, then this creates and registers a
   * <code>ModelMBean</code>. State changing callbacks into the <code>ManagedResource</code> will
   * also be made.
   *
   * @param resource the ManagedResource to create a managing MBean for
   *
   * @return The object name of the MBean that manages the ManagedResource
   *
   * @see ManagedResource#setModelMBean
   */
  static ObjectName ensureMBeanIsRegistered(ManagedResource resource) {
    try {
      ObjectName objName = ObjectName.getInstance(resource.getMBeanName());
      synchronized (MBeanUtil.class) {
        if (mbeanServer != null && !mbeanServer.isRegistered(objName)) {
          return createMBean(resource);
        }
      }
      raiseOnFailure(mbeanServer.isRegistered(objName),
          String.format("Could not find a MBean registered with ObjectName: %s.",
              new Object[] {objName.toString()}));
      return objName;
    } catch (java.lang.Exception e) {
      throw new RuntimeAdminException(e);
    }
  }

  /**
   * Retrieves the <code>ManagedBean</code> configuration from the Registry for the specified
   * <code>ManagedResource</code>
   *
   * @param resource the ManagedResource to find the configuration for
   */
  static ManagedBean lookupManagedBean(ManagedResource resource) {
    // find the registry defn for our MBean...
    ManagedBean managed = null;
    if (registry != null) {
      managed = registry.findManagedBean(resource.getManagedResourceType().getClassTypeName());
    } else {
      throw new IllegalArgumentException(
          "ManagedBean is null");
    }

    if (managed == null) {
      throw new IllegalArgumentException(
          "ManagedBean is null");
    }

    // customize the defn...
    managed.setClassName("org.apache.geode.admin.jmx.internal.MX4JModelMBean");

    return managed;
  }

  /**
   * Registers a refresh notification for the specified client MBean. Specifying zero for the
   * refreshInterval disables notification for the refresh client. Note: this does not currently
   * support remote connections.
   *
   * @param client client to listen for refresh notifications
   * @param userData userData to register with the Notification
   * @param type refresh notification type the client will use
   * @param refreshInterval the seconds between refreshes
   */
  static void registerRefreshNotification(NotificationListener client, Object userData,
      RefreshNotificationType type, int refreshInterval) {
    if (client == null) {
      throw new IllegalArgumentException(
          "NotificationListener is required");
    }
    if (type == null) {
      throw new IllegalArgumentException(
          "RefreshNotificationType is required");
    }
    if (refreshTimerObjectName == null || refreshTimer == null) {
      throw new IllegalStateException(
          "RefreshTimer has not been properly initialized.");
    }

    try {
      // get the notifications for the specified client...
      Map<RefreshNotificationType, Integer> notifications = null;
      synchronized (refreshClients) {
        notifications = (Map<RefreshNotificationType, Integer>) refreshClients.get(client);
      }

      if (notifications == null) {
        // If refreshInterval is being set to zero and notifications is removed return
        if (refreshInterval <= 0) {
          return;
        }

        // never registered before, so add client...
        notifications = new HashMap<RefreshNotificationType, Integer>();
        synchronized (refreshClients) {
          refreshClients.put(client, notifications);
        }
        validateRefreshTimer();
        try {
          // register client as a listener with MBeanServer...
          mbeanServer.addNotificationListener(refreshTimerObjectName, // timer to listen to
              client, // the NotificationListener object
              null, // optional NotificationFilter TODO: convert to using
              new Object() // not used but null throws IllegalArgumentException
          );
        } catch (InstanceNotFoundException e) {
          // should not happen since we already checked refreshTimerObjectName
          logStackTrace(Level.WARN, e,
              "Could not find registered RefreshTimer instance.");
        }
      }

      // TODO: change to manipulating timer indirectly thru mserver...

      // check for pre-existing refresh notification entry...
      Integer timerNotificationId = (Integer) notifications.get(type);
      if (timerNotificationId != null) {
        try {
          // found one, so let's remove it...
          refreshTimer.removeNotification(timerNotificationId);
        } catch (InstanceNotFoundException e) {
          // that's ok cause we just wanted to remove it anyway
        } finally {
          // null out the map entry for that notification type...
          notifications.put(type, null);
        }
      }

      if (refreshInterval > 0) {
        // add notification to the refresh timer...
        timerNotificationId = refreshTimer.addNotification(type.getType(), // type
            type.getMessage(), // message = "refresh"
            userData, // userData
            new Date(System.currentTimeMillis() + refreshInterval * 1000), // first occurrence
            refreshInterval * 1000); // period to repeat

        // put an entry into the map for the listener...
        notifications.put(type, timerNotificationId);
      } else {
        // do nothing! refreshInterval must be over 0 to do anything...
      }
    } catch (java.lang.RuntimeException e) {
      logStackTrace(Level.WARN, e);
      throw e;
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error. We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (java.lang.Error e) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above). However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      logStackTrace(Level.ERROR, e);
      throw e;
    }
  }

  /**
   * Verifies a refresh notification for the specified client MBean. If notification is not
   * registered, then returns a false
   *
   * @param client client to listen for refresh notifications
   * @param type refresh notification type the client will use
   *
   * @return isRegistered boolean indicating if a notification is registered
   */
  static boolean isRefreshNotificationRegistered(NotificationListener client,
      RefreshNotificationType type) {
    boolean isRegistered = false;

    // get the notifications for the specified client...
    Map<RefreshNotificationType, Integer> notifications = null;
    synchronized (refreshClients) {
      notifications = (Map<RefreshNotificationType, Integer>) refreshClients.get(client);
    }

    // never registered before if null ...
    if (notifications != null) {
      // check for pre-existing refresh notification entry...
      Integer timerNotificationId = notifications.get(type);
      if (timerNotificationId != null) {
        isRegistered = true;
      }
    }

    return isRegistered;
  }

  /**
   * Validates refreshTimer has been registered without problems and attempts to re-register if
   * there is a problem.
   */
  static void validateRefreshTimer() {
    if (refreshTimerObjectName == null || refreshTimer == null) {
      createRefreshTimer();
    }

    raiseOnFailure(refreshTimer != null, "Failed to validate Refresh Timer");

    if (mbeanServer != null && !mbeanServer.isRegistered(refreshTimerObjectName)) {
      try {
        mbeanServer.registerMBean(refreshTimer, refreshTimerObjectName);
      } catch (JMException e) {
        logStackTrace(Level.WARN, e);
      } catch (JMRuntimeException e) {
        logStackTrace(Level.WARN, e);
      }
    }
  }

  /**
   * Initializes the timer for sending refresh notifications.
   */
  static void createRefreshTimer() {
    try {
      refreshTimer = new javax.management.timer.Timer();
      mbeanServer.registerMBean(refreshTimer, refreshTimerObjectName);

      refreshTimer.start();
    } catch (JMException e) {
      logStackTrace(Level.WARN, e,
          "Failed to create/register/start refresh timer.");
    } catch (JMRuntimeException e) {
      logStackTrace(Level.WARN, e,
          "Failed to create/register/start refresh timer.");
    } catch (Exception e) {
      logStackTrace(Level.WARN, e,
          "Failed to create/register/start refresh timer.");
    }
  }

  /**
   * Initializes the timer for sending refresh notifications.
   */
  static void stopRefreshTimer() {
    try {
      if (refreshTimer != null && mbeanServer != null) {
        mbeanServer.unregisterMBean(refreshTimerObjectName);

        refreshTimer.stop();
      }
    } catch (JMException e) {
      logStackTrace(Level.WARN, e);
    } catch (JMRuntimeException e) {
      logStackTrace(Level.WARN, e);
    } catch (Exception e) {
      logStackTrace(Level.DEBUG, e, "Failed to stop refresh timer for MBeanUtil");
    }
  }

  /**
   * Return a String that been modified to be compliant as a property of an ObjectName.
   * <p>
   * The property name of an ObjectName may not contain any of the following characters: <b><i>: , =
   * * ?</i></b>
   * <p>
   * This method will replace the above non-compliant characters with a dash: <b><i>-</i></b>
   * <p>
   * If value is empty, this method will return the string "nothing".
   * <p>
   * Note: this is <code>public</code> because certain tests call this from outside of the package.
   * TODO: clean this up
   *
   * @param value the potentially non-compliant ObjectName property
   * @return the value modified to be compliant as an ObjectName property
   */
  public static String makeCompliantMBeanNameProperty(String value) {
    value = value.replace(':', '-');
    value = value.replace(',', '-');
    value = value.replace('=', '-');
    value = value.replace('*', '-');
    value = value.replace('?', '-');
    if (value.length() < 1) {
      value = "nothing";
    }
    return value;
  }

  /**
   * Unregisters all GemFire MBeans and then releases the MBeanServer for garbage collection.
   */
  static void releaseMBeanServer() {
    try {
      // unregister all GemFire mbeans...
      Iterator iter = mbeanServer.queryNames(null, null).iterator();
      while (iter.hasNext()) {
        ObjectName name = (ObjectName) iter.next();
        if (name.getDomain().startsWith(DEFAULT_DOMAIN)) {
          unregisterMBean(name);
        }
      }

      // last, release the mbean server...
      MBeanServerFactory.releaseMBeanServer(mbeanServer);
      mbeanServer = null;
    } catch (JMRuntimeException e) {
      logStackTrace(Level.WARN, e);
    }
    /*
     * See #42391. Cleaning up the static maps which might be still holding references to
     * ManagedResources
     */
    synchronized (MBeanUtil.managedResources) {
      MBeanUtil.managedResources.clear();
    }
    synchronized (refreshClients) {
      refreshClients.clear();
    }
    /*
     * See #42391. Cleaning up the static maps which might be still holding references to
     * ManagedResources
     */
    synchronized (MBeanUtil.managedResources) {
      MBeanUtil.managedResources.clear();
    }
    synchronized (refreshClients) {
      refreshClients.clear();
    }
  }

  /**
   * Returns true if a MBean with given ObjectName is registered.
   *
   * @param objectName ObjectName to use for checking if MBean is registered
   * @return true if MBeanServer is not null & MBean with given ObjectName is registered with the
   *         MBeanServer
   */
  static boolean isRegistered(ObjectName objectName) {
    return mbeanServer != null && mbeanServer.isRegistered(objectName);
  }

  /**
   * Unregisters the identified MBean if it's registered.
   */
  static void unregisterMBean(ObjectName objectName) {
    try {
      if (mbeanServer != null && mbeanServer.isRegistered(objectName)) {
        mbeanServer.unregisterMBean(objectName);
      }
    } catch (MBeanRegistrationException e) {
      logStackTrace(Level.WARN, null,
          String.format("Failed while unregistering MBean with ObjectName : %s",
              new Object[] {objectName}));
    } catch (InstanceNotFoundException e) {
      logStackTrace(Level.WARN, null,
          String.format("While unregistering, could not find MBean with ObjectName : %s",
              new Object[] {objectName}));
    } catch (JMRuntimeException e) {
      logStackTrace(Level.WARN, null,
          String.format("Could not un-register MBean with ObjectName : %s",
              new Object[] {objectName}));
    }
  }

  static void unregisterMBean(ManagedResource resource) {
    if (resource != null) {
      unregisterMBean(resource.getObjectName());

      // call cleanup on managedResource here and not rely on listener
      // since it is possible that notification listener not deliver
      // all notifications of un-registration. If resource is
      // cleaned here, another call from the listener should be as good as a no-op
      cleanupResource(resource);
    }
  }

  // cleanup resource
  private static void cleanupResource(ManagedResource resource) {
    synchronized (MBeanUtil.managedResources) {
      MBeanUtil.managedResources.remove(resource.getObjectName());
    }
    resource.cleanupResource();

    // get the notifications for the specified client...
    Map<RefreshNotificationType, Integer> notifications = null;
    synchronized (refreshClients) {
      notifications = (Map<RefreshNotificationType, Integer>) refreshClients.remove(resource);
    }

    // never registered before if null ...
    // Also as of current, there is ever only 1 Notification type per
    // MBean, so we do need need a while loop here
    if (notifications != null) {

      // Fix for findbugs reported inefficiency with keySet().
      Set<Map.Entry<RefreshNotificationType, Integer>> entries = notifications.entrySet();

      for (Map.Entry<RefreshNotificationType, Integer> e : entries) {
        Integer timerNotificationId = e.getValue();
        if (null != timerNotificationId) {
          try {
            // found one, so let's remove it...
            refreshTimer.removeNotification(timerNotificationId);
          } catch (InstanceNotFoundException xptn) {
            // that's ok cause we just wanted to remove it anyway
            logStackTrace(Level.DEBUG, xptn);
          }
        }
      }

      try {
        if (mbeanServer != null && mbeanServer.isRegistered(refreshTimerObjectName)) {
          // remove client as a listener with MBeanServer...
          mbeanServer.removeNotificationListener(refreshTimerObjectName, // timer to listen to
              (NotificationListener) resource // the NotificationListener object
          );
        }
      } catch (ListenerNotFoundException xptn) {
        // should not happen since we already checked refreshTimerObjectName
        logStackTrace(Level.WARN, null, xptn.getMessage());
      } catch (InstanceNotFoundException xptn) {
        // should not happen since we already checked refreshTimerObjectName
        logStackTrace(Level.WARN, null,
            String.format("While unregistering, could not find MBean with ObjectName : %s",
                new Object[] {refreshTimerObjectName}));
      }
    }
  }

  // ----- borrowed the following from admin.internal.RemoteCommand -----
  /** Translates the path between Windows and UNIX. */
  static String getOSPath(String path) {
    if (pathIsWindows(path)) {
      return path.replace('/', '\\');
    } else {
      return path.replace('\\', '/');
    }
  }

  /** Returns true if the path is on Windows. */
  static boolean pathIsWindows(String path) {
    if (path != null && path.length() > 1) {
      return (Character.isLetter(path.charAt(0)) && path.charAt(1) == ':')
          || (path.startsWith("//") || path.startsWith("\\\\"));
    }
    return false;
  }

  static void registerServerNotificationListener() {
    if (mbeanServer == null) {
      return;
    }
    try {
      // the MBeanServerDelegate name is spec'ed as the following...
      ObjectName delegate = ObjectName.getInstance("JMImplementation:type=MBeanServerDelegate");
      mbeanServer.addNotificationListener(delegate, new NotificationListener() {
        public void handleNotification(Notification notification, Object handback) {
          MBeanServerNotification serverNotification = (MBeanServerNotification) notification;
          if (MBeanServerNotification.UNREGISTRATION_NOTIFICATION
              .equals(serverNotification.getType())) {
            ObjectName objectName = serverNotification.getMBeanName();
            synchronized (MBeanUtil.managedResources) {
              Object entry = MBeanUtil.managedResources.get(objectName);
              if (entry == null)
                return;
              if (!(entry instanceof ManagedResource)) {
                throw new ClassCastException(String.format("%s is not a ManagedResource",
                    new Object[] {entry.getClass().getName()}));
              }
              ManagedResource resource = (ManagedResource) entry;
              {
                // call cleanup on managedResource
                cleanupResource(resource);
              }
            }
          }
        }
      }, null, null);
    } catch (JMException e) {
      logStackTrace(Level.WARN, e,
          "Failed to register ServerNotificationListener.");
    } catch (JMRuntimeException e) {
      logStackTrace(Level.WARN, e,
          "Failed to register ServerNotificationListener.");
    }
  }

  /**
   * Logs the stack trace for the given Throwable if logger is initialized else prints the stack
   * trace using System.out.
   *
   * @param level severity level to log at
   * @param throwable Throwable to log stack trace for
   */
  public static void logStackTrace(Level level, Throwable throwable) {
    logStackTrace(level, throwable, null);
  }

  /**
   * Logs the stack trace for the given Throwable if logger is initialized else prints the stack
   * trace using System.out.
   *
   * @param level severity level to log at
   * @param throwable Throwable to log stack trace for
   * @param message user friendly error message to show
   */
  public static void logStackTrace(Level level, Throwable throwable, String message) {
    logger.log(level, message, throwable);
  }

  /**
   * Raises RuntimeAdminException with given 'message' if given 'condition' is false.
   *
   * @param condition condition to evaluate
   * @param message failure message
   */
  private static void raiseOnFailure(boolean condition, String message) {
    if (!condition) {
      throw new RuntimeAdminException(message);
    }
  }
}
