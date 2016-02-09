/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.management.InstanceNotFoundException;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanServer;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.admin.Alert;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.AlertAppender;
import com.gemstone.gemfire.management.internal.AlertDetails;
import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;
import com.gemstone.gemfire.management.internal.ManagementConstants;
import com.gemstone.gemfire.management.internal.NotificationHub;
import com.gemstone.gemfire.management.internal.NotificationHub.NotificationHubListener;
import com.gemstone.gemfire.management.internal.SystemManagementService;
import com.gemstone.gemfire.management.internal.beans.MemberMBean;
import com.gemstone.gemfire.management.internal.beans.SequenceNumber;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

/**
 * Distributed System tests
 * 
 * a) For all the notifications 
 * 
 *  i) gemfire.distributedsystem.member.joined
 * 
 *  ii) gemfire.distributedsystem.member.left
 * 
 *  iii) gemfire.distributedsystem.member.suspect
 * 
 *  iv ) All notifications emitted by member mbeans
 * 
 *  vi) Alerts
 * 
 * b) Concurrently modify proxy list by removing member and accessing the
 * distributed system MBean
 * 
 * c) Aggregate Operations like shutDownAll
 * 
 * d) Member level operations like fetchJVMMetrics()
 * 
 * e ) Statistics
 * 
 * 
 * @author rishim
 * 
 */
public class DistributedSystemDUnitTest extends ManagementTestBase {

  private static final Logger logger = LogService.getLogger();
  
  private static final long serialVersionUID = 1L;

 
  private static final int MAX_WAIT = 10 * 1000;
  
  private static MBeanServer mbeanServer = MBeanJMXAdapter.mbeanServer;
  
  static List<Notification> notifList = new ArrayList<>();
  
  static Map<ObjectName , NotificationListener> notificationListenerMap = new HashMap<ObjectName , NotificationListener>();
  
  static final String WARNING_LEVEL_MESSAGE = "Warninglevel Alert Message";
  
  static final String SEVERE_LEVEL_MESSAGE =  "Severelevel Alert Message";

  
  public DistributedSystemDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    

  }

  /**
   * Tests each and every operations that is defined on the MemberMXBean
   * 
   * @throws Exception
   */
  public void testDistributedSystemAggregate() throws Exception {
    VM managingNode = getManagingNode();
    createManagementCache(managingNode);
    startManagingNode(managingNode);
    addNotificationListener(managingNode);

    for (VM vm : getManagedNodeList()) {
      createCache(vm);
    }
    
    checkAggregate(managingNode);
    for (VM vm : getManagedNodeList()) {
      closeCache(vm);
    }

    closeCache(managingNode);

  }
   
  /**
   * Tests each and every operations that is defined on the MemberMXBean
   * 
   * @throws Exception
   */
  public void testAlertManagedNodeFirst() throws Exception {

    for (VM vm : getManagedNodeList()) {
      createCache(vm);
      warnLevelAlert(vm);
      severeLevelAlert(vm);
    }

    VM managingNode = getManagingNode();

    createManagementCache(managingNode);
    startManagingNode(managingNode);
    addAlertListener(managingNode);
    checkAlertCount(managingNode, 0, 0);

    final DistributedMember managingMember = getMember(managingNode);

    // Before we start we need to ensure that the initial (implicit) SEVERE alert has propagated everywhere.
    for (VM vm : getManagedNodeList()) {
      ensureLoggerState(vm, managingMember, Alert.SEVERE);
    }

    setAlertLevel(managingNode, AlertDetails.getAlertLevelAsString(Alert.WARNING));

    for (VM vm : getManagedNodeList()) {
      ensureLoggerState(vm, managingMember, Alert.WARNING);
      warnLevelAlert(vm);
      severeLevelAlert(vm);
    }

    checkAlertCount(managingNode, 3, 3);
    resetAlertCounts(managingNode);

    setAlertLevel(managingNode, AlertDetails.getAlertLevelAsString(Alert.SEVERE));

    for (VM vm : getManagedNodeList()) {
      ensureLoggerState(vm, managingMember, Alert.SEVERE);
      warnLevelAlert(vm);
      severeLevelAlert(vm);
    }

    checkAlertCount(managingNode, 3, 0);
    resetAlertCounts(managingNode);
    
    for (VM vm : getManagedNodeList()) {
      closeCache(vm);
    }

    closeCache(managingNode);
  }
  
  @SuppressWarnings("serial")
  public void ensureLoggerState(VM vm1, final DistributedMember member,
      final int alertLevel) throws Exception {
    {
      vm1.invoke(new SerializableCallable("Ensure Logger State") {

        public Object call() throws Exception {
          
          Wait.waitForCriterion(new WaitCriterion() {
            public String description() {
              return "Waiting for all alert Listener to register with managed node";
            }

            public boolean done() {

              if (AlertAppender.getInstance().hasAlertListener(member, alertLevel)) {
                return true;
              }
              return false;
            }

          }, MAX_WAIT, 500, true);

          return null;
        }
      });

    }
  }
  
  /**
   * Tests each and every operations that is defined on the MemberMXBean
   * 
   * @throws Exception
   */
  public void testShutdownAll() throws Exception {
    final Host host = Host.getHost(0);
    VM managedNode1 = host.getVM(0);
    VM managedNode2 = host.getVM(1);
    VM managedNode3 = host.getVM(2);

    VM managingNode = host.getVM(3);

    // Managing Node is created first
    createManagementCache(managingNode);
    startManagingNode(managingNode);
    
    createCache(managedNode1);
    createCache(managedNode2);
    createCache(managedNode3);
    shutDownAll(managingNode);
    closeCache(managingNode);
  }
  
  public void testNavigationAPIS() throws Exception{
    
    final Host host = Host.getHost(0); 
    
    createManagementCache(managingNode);
    startManagingNode(managingNode);
    
    for(VM vm : managedNodeList){
      createCache(vm);
    }
    
    checkNavigationAPIs(managingNode);    
  }
  
  public void testNotificationHub() throws Exception {
    this.initManagement(false);

    class NotificationHubTestListener implements NotificationListener {
      @Override
      public synchronized void handleNotification(Notification notification, Object handback) {
        logger.info("Notification received {}", notification);
        notifList.add(notification);
      }
    }

    managingNode
        .invoke(new SerializableRunnable("Add Listener to MemberMXBean") {

          public void run() {
            Cache cache = getCache();
            ManagementService service = getManagementService();
            final DistributedSystemMXBean bean = service.getDistributedSystemMXBean();
            
            Wait.waitForCriterion(new WaitCriterion() {
              public String description() {
                return "Waiting for all members to send their initial Data";
              }

              public boolean done() {
                if (bean.listMemberObjectNames().length == 5) {// including locator 
                  return true;
                } else {
                  return false;
                }
              }
            }, MAX_WAIT, 500, true);
            for (ObjectName objectName : bean.listMemberObjectNames()) {
              NotificationHubTestListener listener = new NotificationHubTestListener();
              try {
                mbeanServer.addNotificationListener(objectName, listener, null,
                    null);
                notificationListenerMap.put(objectName, listener);
              } catch (InstanceNotFoundException e) {
                LogWriterUtils.getLogWriter().error(e);
              }
            }
          }
        });

    // Check in all VMS

    for (VM vm : managedNodeList) {
      vm.invoke(new SerializableRunnable("Check Hub Listener num count") {

        public void run() {
          Cache cache = getCache();
          SystemManagementService service = (SystemManagementService) getManagementService();
          NotificationHub hub = service.getNotificationHub();
          Map<ObjectName, NotificationHubListener> listenerObjectMap = hub
              .getListenerObjectMap();
          assertEquals(1, listenerObjectMap.keySet().size());
          ObjectName memberMBeanName = MBeanJMXAdapter.getMemberMBeanName(cache
              .getDistributedSystem().getDistributedMember());

          NotificationHubListener listener = listenerObjectMap
              .get(memberMBeanName);

          /*
           * Counter of listener should be 2 . One for default Listener which is
           * added for each member mbean by distributed system mbean One for the
           * added listener in test
           */
          assertEquals(2, listener.getNumCounter());

          // Raise some notifications

          NotificationBroadcasterSupport memberLevelNotifEmitter = (MemberMBean) service
              .getMemberMXBean();

          String memberSource = MBeanJMXAdapter.getMemberNameOrId(cache
              .getDistributedSystem().getDistributedMember());

          // Only a dummy notification , no actual region is creates
          Notification notification = new Notification(
              JMXNotificationType.REGION_CREATED, memberSource, SequenceNumber
                  .next(), System.currentTimeMillis(),
                  ManagementConstants.REGION_CREATED_PREFIX + "/test");
          memberLevelNotifEmitter.sendNotification(notification);

        }
      });
    }

    managingNode.invoke(new SerializableRunnable(
        "Check notifications && Remove Listeners") {

      public void run() {

        Wait.waitForCriterion(new WaitCriterion() {
          public String description() {
            return "Waiting for all Notifications to reach the Managing Node";
          }

          public boolean done() {
            if (notifList.size() == 3) {
              return true;
            } else {
              return false;
            }
          }
        }, MAX_WAIT, 500, true);

        notifList.clear();

        Iterator<ObjectName> it = notificationListenerMap.keySet().iterator();
        while (it.hasNext()) {
          ObjectName objectName = it.next();
          NotificationListener listener = notificationListenerMap
              .get(objectName);
          try {
            mbeanServer.removeNotificationListener(objectName, listener);
          } catch (ListenerNotFoundException e) {
            LogWriterUtils.getLogWriter().error(e);
          } catch (InstanceNotFoundException e) {
            LogWriterUtils.getLogWriter().error(e);
          }
        }

      }
    });

    // Check in all VMS again

    for (VM vm : managedNodeList) {
      vm.invoke(new SerializableRunnable("Check Hub Listener num count Again") {

        public void run() {
          Cache cache = getCache();
          SystemManagementService service = (SystemManagementService) getManagementService();
          NotificationHub hub = service.getNotificationHub();
          Map<ObjectName, NotificationHubListener> listenerObjectMap = hub
              .getListenerObjectMap();

          assertEquals(1, listenerObjectMap.keySet().size());

          ObjectName memberMBeanName = MBeanJMXAdapter.getMemberMBeanName(cache
              .getDistributedSystem().getDistributedMember());

          NotificationHubListener listener = listenerObjectMap
              .get(memberMBeanName);

          /*
           * Counter of listener should be 1 for the default Listener which is
           * added for each member mbean by distributed system mbean.
           */
          assertEquals(1, listener.getNumCounter());

        }
      });
    }

    managingNode
    .invoke(new SerializableRunnable("Remove Listener from MemberMXBean") {

      public void run() {
        Cache cache = getCache();
        ManagementService service = getManagementService();
        final DistributedSystemMXBean bean = service.getDistributedSystemMXBean();
        
        Wait.waitForCriterion(new WaitCriterion() {
          public String description() {
            return "Waiting for all members to send their initial Data";
          }

          public boolean done() {
            if (bean.listMemberObjectNames().length == 5) {// including locator 
              return true;
            } else {
              return false;
            }

          }

        }, MAX_WAIT, 500, true);
        for (ObjectName objectName : bean.listMemberObjectNames()) {
          NotificationHubTestListener listener = new NotificationHubTestListener();
          try {
            mbeanServer.removeNotificationListener(objectName, listener);
          } catch (InstanceNotFoundException e) {
            LogWriterUtils.getLogWriter().error(e);
          } catch (ListenerNotFoundException e) {
            // TODO: apparently there is never a notification listener on any these mbeans at this point 
            // fix this test so it doesn't hit these unexpected exceptions -- getLogWriter().error(e);
          }
        }
      }
    });
    
    for (VM vm : managedNodeList) {
      vm.invoke(new SerializableRunnable("Check Hub Listeners clean up") {

        public void run() {
          Cache cache = getCache();
          SystemManagementService service = (SystemManagementService) getManagementService();
          NotificationHub hub = service.getNotificationHub();
          hub.cleanUpListeners();
          assertEquals(0, hub.getListenerObjectMap().size());

          Iterator<ObjectName> it = notificationListenerMap.keySet().iterator();
          while (it.hasNext()) {
            ObjectName objectName = it.next();
            NotificationListener listener = notificationListenerMap
                .get(objectName);
            try {
              mbeanServer.removeNotificationListener(objectName, listener);
              fail("Found Listeners inspite of clearing them");
            } catch (ListenerNotFoundException e) {
              // Expected Exception Do nothing
            } catch (InstanceNotFoundException e) {
              LogWriterUtils.getLogWriter().error(e);
            }
          }
        }
      });
    }
  }
  
  /**
   * Tests each and every operations that is defined on the MemberMXBean
   * 
   * @throws Exception
   */
  public void testAlert() throws Exception {
    VM managingNode = getManagingNode();
   
    createManagementCache(managingNode);
    startManagingNode(managingNode);
    addAlertListener(managingNode);
    resetAlertCounts(managingNode);
    
    final DistributedMember managingMember = getMember(managingNode);
    
    
    
    warnLevelAlert(managingNode);
    severeLevelAlert(managingNode);
    checkAlertCount(managingNode, 1, 0);
    resetAlertCounts(managingNode);
    
    for (VM vm : getManagedNodeList()) {
      
      createCache(vm);
      // Default is severe ,So only Severe level alert is expected
      
      ensureLoggerState(vm, managingMember, Alert.SEVERE);
      
      warnLevelAlert(vm);
      severeLevelAlert(vm);
      
    }
    checkAlertCount(managingNode, 3, 0);
    resetAlertCounts(managingNode);
    setAlertLevel(managingNode, AlertDetails.getAlertLevelAsString(Alert.WARNING));

    
    for (VM vm : getManagedNodeList()) {
      // warning and severe alerts both are to be checked
      ensureLoggerState(vm, managingMember, Alert.WARNING);
      warnLevelAlert(vm);
      severeLevelAlert(vm);
    }

    checkAlertCount(managingNode, 3, 3);
    
    resetAlertCounts(managingNode);
    
    setAlertLevel(managingNode, AlertDetails.getAlertLevelAsString(Alert.OFF));
    
    for (VM vm : getManagedNodeList()) {
      ensureLoggerState(vm, managingMember, Alert.OFF);
      warnLevelAlert(vm);
      severeLevelAlert(vm);
    }
    checkAlertCount(managingNode, 0, 0);
    resetAlertCounts(managingNode);
    
    for (VM vm : getManagedNodeList()) {
      closeCache(vm);
    }

    closeCache(managingNode);

  }
  
  @SuppressWarnings("serial")
  public void checkAlertCount(VM vm1, final int expectedSevereAlertCount,
      final int expectedWarningAlertCount) throws Exception {
    {
      vm1.invoke(new SerializableCallable("Check Alert Count") {

        public Object call() throws Exception {
          final AlertNotifListener nt = AlertNotifListener.getInstance();
          Wait.waitForCriterion(new WaitCriterion() {
            public String description() {
              return "Waiting for all alerts to reach the Managing Node";
            }
            public boolean done() {
              if (expectedSevereAlertCount == nt.getseverAlertCount()
                  && expectedWarningAlertCount == nt.getWarnigAlertCount()) {
                return true;
              } else {
                return false;
              }

            }

          }, MAX_WAIT, 500, true);

          return null;
        }
      });

    }
  }
  


  
  @SuppressWarnings("serial")
  public void setAlertLevel(VM vm1, final String alertLevel) throws Exception {
    {
      vm1.invoke(new SerializableCallable("Set Alert level") {

        public Object call() throws Exception {
          GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
          ManagementService service = getManagementService();
          DistributedSystemMXBean bean = service.getDistributedSystemMXBean();
          assertNotNull(bean);
          bean.changeAlertLevel(alertLevel);

          return null;
        }
      });

    }
  }
  
  @SuppressWarnings("serial")
  public void warnLevelAlert(VM vm1) throws Exception {
    {
      vm1.invoke(new SerializableCallable("Warning level Alerts") {

        public Object call() throws Exception {
          final IgnoredException warnEx = IgnoredException.addIgnoredException(WARNING_LEVEL_MESSAGE);
          logger.warn(WARNING_LEVEL_MESSAGE);
          warnEx.remove();
          return null;
        }
      });

    }
  }
  
  
  @SuppressWarnings("serial")
  public void resetAlertCounts(VM vm1) throws Exception {
    {
      vm1.invoke(new SerializableCallable("Reset Alert Count") {

        public Object call() throws Exception {
          AlertNotifListener nt =  AlertNotifListener.getInstance();
          nt.resetCount();
          return null;
        }
      });

    }
  }

  @SuppressWarnings("serial")
  public void severeLevelAlert(VM vm1) throws Exception {
    {
      vm1.invoke(new SerializableCallable("Severe Level Alert") {

        public Object call() throws Exception {
          // add expected exception strings         
          
          final IgnoredException severeEx = IgnoredException.addIgnoredException(SEVERE_LEVEL_MESSAGE);
          logger.fatal(SEVERE_LEVEL_MESSAGE);
          severeEx.remove();
          return null;
        }
      });

    }
  }
  
  @SuppressWarnings("serial")
  public void addAlertListener(VM vm1) throws Exception {
    {
      vm1.invoke(new SerializableCallable("Add Alert Listener") {

        public Object call() throws Exception {
          GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
          ManagementService service = getManagementService();
          DistributedSystemMXBean bean = service.getDistributedSystemMXBean();
          AlertNotifListener nt =  AlertNotifListener.getInstance();
          nt.resetCount();
          
          NotificationFilter notificationFilter = new NotificationFilter() {
            @Override
            public boolean isNotificationEnabled(Notification notification) {
              return notification.getType().equals(JMXNotificationType.SYSTEM_ALERT);
            }

          };
          
          mbeanServer.addNotificationListener(MBeanJMXAdapter
              .getDistributedSystemName(), nt, notificationFilter, null);

          return null;
        }
      });

    }
  }
  
  /**
   * Check aggregate related functions and attributes
   * @param vm1
   * @throws Exception
   */
  @SuppressWarnings("serial")
  public void checkAggregate(VM vm1) throws Exception {
    {
      vm1.invoke(new SerializableCallable("Chech Aggregate Attributes") {

        public Object call() throws Exception {
          GemFireCacheImpl cache = GemFireCacheImpl.getInstance();

          ManagementService service = getManagementService();

          final DistributedSystemMXBean bean = service.getDistributedSystemMXBean();
          assertNotNull(service.getDistributedSystemMXBean());
          
          Wait.waitForCriterion(new WaitCriterion() {
            public String description() {
              return "Waiting All members to intitialize DistributedSystemMBean expect 5 but found " + bean.getMemberCount();
            }
            public boolean done() {
              // including locator
              if (bean.getMemberCount() == 5) {
                return true;
              } else {
                return false;
              }

            }

          }, MAX_WAIT, 500, true);



          final Set<DistributedMember> otherMemberSet = cache
              .getDistributionManager().getOtherNormalDistributionManagerIds();
          Iterator<DistributedMember> memberIt = otherMemberSet.iterator();
          while (memberIt.hasNext()) {
            DistributedMember member = memberIt.next();
            LogWriterUtils.getLogWriter().info(
                "JVM Metrics For Member " + member.getId() + ":"
                    + bean.showJVMMetrics(member.getId()));
            LogWriterUtils.getLogWriter().info(
                "OS Metrics For Member " + member.getId() + ":"
                    + bean.showOSMetrics(member.getId()));
          }

          return null;
        }
      });

    }
  }

  @SuppressWarnings("serial")
  public void addNotificationListener(VM vm1) throws Exception {
    {
      vm1.invoke(new SerializableCallable("Add Notification Listener") {

        public Object call() throws Exception {
          GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
          ManagementService service = getManagementService();
          DistributedSystemMXBean bean = service.getDistributedSystemMXBean();
          assertNotNull(bean);
          TestDistributedSystemNotif nt = new TestDistributedSystemNotif();
          mbeanServer.addNotificationListener(MBeanJMXAdapter
              .getDistributedSystemName(), nt, null, null);

          return null;
        }
      });

    }
  }

 

  @SuppressWarnings("serial")
  public void shutDownAll(VM vm1) throws Exception {
    {
      vm1.invoke(new SerializableCallable("Shut Down All") {

        public Object call() throws Exception {
          GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
          ManagementService service = getManagementService();
          DistributedSystemMXBean bean = service.getDistributedSystemMXBean();
          assertNotNull(service.getDistributedSystemMXBean());
          bean.shutDownAllMembers();
          Wait.pause(2000);
          assertEquals(
              cache.getDistributedSystem().getAllOtherMembers().size(), 1);
          return null;
        }
      });

    }
  }
  

  
  @SuppressWarnings("serial")
  public void checkNavigationAPIs(VM vm1) throws Exception {
    {
      vm1.invoke(new SerializableCallable("Check Navigation APIS") {

        public Object call() throws Exception {
          GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
          ManagementService service = getManagementService();
          final DistributedSystemMXBean bean = service.getDistributedSystemMXBean();
          
          assertNotNull(service.getDistributedSystemMXBean());
          
          waitForAllMembers(4);
          
          for(int i =0; i< bean.listMemberObjectNames().length ; i++){
            LogWriterUtils.getLogWriter().info(
                "ObjectNames Of the Mmeber" + bean.listMemberObjectNames()[i] );
          }

          
          ObjectName thisMemberName = MBeanJMXAdapter
              .getMemberMBeanName(InternalDistributedSystem
                  .getConnectedInstance().getDistributedMember().getId());

          ObjectName memberName = bean
              .fetchMemberObjectName(InternalDistributedSystem
                  .getConnectedInstance().getDistributedMember().getId());
          assertEquals(thisMemberName, memberName);
          
          return null;
        }
      });

    }
  }


  /**
   * Notification handler
   * 
   * @author rishim
   * 
   */
  private static class TestDistributedSystemNotif implements
      NotificationListener {

    @Override
    public void handleNotification(Notification notification, Object handback) {
      assertNotNull(notification);      
    }

  }
  
  /**
   * Notification handler
   * 
   * @author rishim
   * 
   */
  private static class AlertNotifListener implements NotificationListener {
    
    private static AlertNotifListener listener = new AlertNotifListener();
    
    public static AlertNotifListener getInstance(){
      return listener;
    }

    private int warnigAlertCount = 0;

    private int severAlertCount = 0;

    @Override
    public synchronized void handleNotification(Notification notification, Object handback) {
      assertNotNull(notification);
      logger.info("Notification received {}", notification);
      Map<String,String> notifUserData = (Map<String,String>)notification.getUserData();
      if (notifUserData.get(JMXNotificationUserData.ALERT_LEVEL).equalsIgnoreCase("warning")) {
        assertEquals(WARNING_LEVEL_MESSAGE,notification.getMessage());
        ++warnigAlertCount;
      }
      if (notifUserData.get(JMXNotificationUserData.ALERT_LEVEL).equalsIgnoreCase("severe")) {
        assertEquals(SEVERE_LEVEL_MESSAGE,notification.getMessage());
        ++severAlertCount;
      }
    }

    public void resetCount() {
      warnigAlertCount = 0;

      severAlertCount = 0;
    }

    public int getWarnigAlertCount() {
      return warnigAlertCount;
    }

    public int getseverAlertCount() {
      return severAlertCount;
    }

  }

}
