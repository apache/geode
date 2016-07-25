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
package com.gemstone.gemfire.admin.jmx.internal;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.admin.*;
import com.gemstone.gemfire.admin.internal.AdminDistributedSystemImpl;
import com.gemstone.gemfire.admin.internal.CacheServerConfigImpl;
import com.gemstone.gemfire.admin.internal.DistributionLocatorImpl;
import com.gemstone.gemfire.cache.persistence.PersistentID;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.admin.Alert;
import com.gemstone.gemfire.internal.admin.*;
import com.gemstone.gemfire.internal.admin.remote.UpdateAlertDefinitionMessage;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import org.apache.logging.log4j.Logger;

import javax.management.*;
import javax.management.modelmbean.ModelMBean;
import javax.management.openmbean.*;
import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Provides MBean support for managing a GemFire distributed system.
 * <p>
 * TODO: refactor to implement DistributedSystem and delegate to instance of
 * DistributedSystemImpl. Wrap all delegate calls w/ e.printStackTrace() since 
 * the HttpAdaptor devours them (what to do w/ template methods then?)
 *
 * @since GemFire     3.5
 */
public class AdminDistributedSystemJmxImpl 
              extends AdminDistributedSystemImpl
              implements ManagedResource, DistributedSystemConfig, StatAlertsAggregator {

  private static final Logger logger = LogService.getLogger();
  
  private Properties mailProps;

  // The file name where the StatAlertDefinitions would be serialized
  private String statAlertDefnSerFile = System.getProperty("user.dir");
  
  /** 
   * Simple counter incrementing on each notification.  This this currently 
   * resets at every restart of Agent
   */
  private final AtomicInteger notificationSequenceNumber = new AtomicInteger();

  /**
   * Variable to indicate if there are no Rmi clients connected.
   */
  private volatile boolean isRmiClientCountZero;

  /**
   * Variable to indicate if Statistics Alert definitions could be persisted 
   * across runs/sessions.
   */
  private volatile boolean canPersistStatAlertDefs = true;

  /** Cache Listener to listen to Cache & Region create/destroy events */
  private CacheAndRegionListenerImpl cacheRegionListener;

  // -------------------------------------------------------------------------
  //   Constructor(s)
  // -------------------------------------------------------------------------
  
  /**
   * Constructs new DistributedSystemJmxImpl and registers an MBean to represent
   * it.
   * 
   * @param config
   *          configuration defining the JMX agent.
   */
  public AdminDistributedSystemJmxImpl(AgentConfigImpl config)
      throws com.gemstone.gemfire.admin.AdminException {
    super(config);
    this.mbeanName = "GemFire:type=AdminDistributedSystem,id="
        + MBeanUtil.makeCompliantMBeanNameProperty(getId());
    this.objectName = MBeanUtil.createMBean(this);
    isEmailNotificationEnabled = config.isEmailNotificationEnabled();
    if (isEmailNotificationEnabled) {
      initMailProps(config);
    }
    // Init file name for StatAlertDefns
    initStateSaveFile(config);
    Assert.assertTrue(this.objectName != null);

    cacheRegionListener = new CacheAndRegionListenerImpl(this);
  }

  private void initMailProps(AgentConfigImpl config) {
    mailProps = new Properties();
    mailProps.put(MailManager.PROPERTY_MAIL_FROM, 
                    config.getEmailNotificationFrom());
    mailProps.put(MailManager.PROPERTY_MAIL_HOST, 
                    config.getEmailNotificationHost());
    mailProps.put(MailManager.PROPERTY_MAIL_TO_LIST, 
                    config.getEmailNotificationToList());
  }

  private void initStateSaveFile(AgentConfigImpl agentConfig) {
    // Init file name for StatAlertDefns
    AgentConfigImpl impl = (AgentConfigImpl) agentConfig;
    File propFile = impl.getPropertyFile();

    if (propFile!=null) {
     if (propFile.isDirectory()) {
       statAlertDefnSerFile = propFile.getAbsolutePath();
     } else if (propFile.getParentFile()!=null) {
       statAlertDefnSerFile = propFile.getParentFile().getAbsolutePath();
     }
    }

    statAlertDefnSerFile = statAlertDefnSerFile + File.separator + agentConfig.getStateSaveFile();
  }
  
  // -------------------------------------------------------------------------
  //   MBean operations
  // -------------------------------------------------------------------------
  
  /**
   * Registers the MBeans for monitoring the health of GemFire 
   *
   * @see com.gemstone.gemfire.admin.internal.AdminDistributedSystemImpl#getGemFireHealth
   */
  public ObjectName monitorGemFireHealth() throws MalformedObjectNameException {
    GemFireHealthJmxImpl health = (GemFireHealthJmxImpl) getGemFireHealth();
    health.ensureMBeansAreRegistered();
    return health.getObjectName();
  }

  /**
   * Creates a new DistributionLocator for this system and registers an MBean
   * for managing it.
   * <p>
   * If the Locator already exists, then this will simply register an MBean
   * for it.
   *
   * @param host              the host name or IP address of the locator
   * @param port              the port the locator service listens on
   * @param workingDirectory  directory path for the locator and its log
   * @param productDirectory  directory path to the GemFire product to use 
   */
  public ObjectName createDistributionLocator(String host,
                                              int port,
                                              String workingDirectory,
                                              String productDirectory) 
                                       throws MalformedObjectNameException {
    return createDistributionLocator(
        host, port, workingDirectory, productDirectory, getRemoteCommand());
  }
  
  /**
   * Creates a new DistributionLocator for this system and registers an MBean
   * for managing it.
   * <p>
   * If the Locator already exists, then this will simply register an MBean
   * for it.
   *
   * @param host              the host name or IP address of the locator
   * @param port              the port the locator service listens on
   * @param workingDirectory  directory path for the locator and its log
   * @param productDirectory  directory path to the GemFire product to use 
   * @param remoteCommand     formatted remote command to control remotely
   */
  public ObjectName createDistributionLocator(String host,
                                              int port,
                                              String workingDirectory,
                                              String productDirectory,
                                              String remoteCommand) 
                                       throws MalformedObjectNameException {
    try {
      DistributionLocatorJmxImpl locator =
        (DistributionLocatorJmxImpl) addDistributionLocator();
      
      DistributionLocatorConfig config = locator.getConfig();
      config.setHost(host);
      config.setPort(port);
      config.setWorkingDirectory(workingDirectory);
      config.setProductDirectory(productDirectory);
      config.setRemoteCommand(remoteCommand);

      return new ObjectName(locator.getMBeanName());
    } catch (RuntimeException e) {
      logger.warn(e.getMessage(), e);
      throw e;
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Error e) { 
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      logger.error(e.getMessage(), e);
      throw e;
    }
  }
  
  // -------------------------------------------------------------------------
  //   Template methods overriden from superclass...
  // -------------------------------------------------------------------------
  
  /** Override createSystemMember by instantiating SystemMemberJmxImpl */
  @Override
  protected SystemMember createSystemMember(ApplicationVM app)
  throws com.gemstone.gemfire.admin.AdminException {
    return new SystemMemberJmxImpl(this, app);
  }

  /**
   * Constructs & returns a SystemMember instance using the corresponding
   * InternalDistributedMember object.
   * 
   * @param member
   *          InternalDistributedMember instance for which a SystemMember
   *          instance is to be constructed.
   * @return constructed SystemMember instance
   * @throws com.gemstone.gemfire.admin.AdminException
   *           if construction of SystemMember instance fails
   *
   * @since GemFire 6.5
   */
  protected SystemMember createSystemMember(InternalDistributedMember member)
      throws com.gemstone.gemfire.admin.AdminException {
    return new SystemMemberJmxImpl(this, member);
  }
  

  @Override
  protected CacheServer createCacheServer(ApplicationVM member) 
    throws AdminException {

    return new CacheServerJmxImpl(this, member);
  }

  @Override
  protected CacheServer createCacheServer(CacheServerConfigImpl config) 
    throws AdminException {

    return new CacheServerJmxImpl(this, config);
  }

  /** Override createGemFireHealth by instantiating GemFireHealthJmxImpl */
  @Override
  protected GemFireHealth createGemFireHealth(GfManagerAgent system) 
    throws com.gemstone.gemfire.admin.AdminException {
    if (system == null) {
      throw new IllegalStateException(LocalizedStrings.AdminDistributedSystemJmxImpl_GFMANAGERAGENT_MUST_NOT_BE_NULL.toLocalizedString());
    }
    return new GemFireHealthJmxImpl(system, this);
  }
  
  /** Template-method for creating a DistributionLocatorImpl instance. */
  @Override
  protected DistributionLocatorImpl 
    createDistributionLocatorImpl(DistributionLocatorConfig config) {
    return new DistributionLocatorJmxImpl(config, this);
  }

  // -------------------------------------------------------------------------
  //   Internal Admin listeners and JMX Notifications
  // -------------------------------------------------------------------------
  
  /** Notification type for indicating system member joined */
  public static final String NOTIF_MEMBER_JOINED =
      DistributionConfig.GEMFIRE_PREFIX + "distributedsystem.member.joined";
  /** Notification type for indicating system member left */
  public static final String NOTIF_MEMBER_LEFT =
      DistributionConfig.GEMFIRE_PREFIX + "distributedsystem.member.left";
  /** Notification type for indicating system member crashed */
  public static final String NOTIF_MEMBER_CRASHED =
      DistributionConfig.GEMFIRE_PREFIX + "distributedsystem.member.crashed";
  /** Notification type for sending GemFire alerts as JMX notifications */
  public static final String NOTIF_ALERT =
      DistributionConfig.GEMFIRE_PREFIX + "distributedsystem.alert";
  /** Notification type for sending GemFire StatAlerts as JMX notifications */
  public static final String NOTIF_STAT_ALERT =
      DistributionConfig.GEMFIRE_PREFIX + "distributedsystem.statalert";
  /** Notification type for indicating abnormal disconnection from the distributed system */
  public static final String NOTIF_ADMIN_SYSTEM_DISCONNECT =
      DistributionConfig.GEMFIRE_PREFIX + "distributedsystem.disconnect";
  
  
  private static final String EML_SUBJ_PRFX_GFE_ALERT = "[GemFire Alert] ";
  private static final String EML_SUBJ_PRFX_GFE_NOTFY = "[GemFire Notification] ";
  private static final String EML_SUBJ_ITEM_GFE_DS = "Distributed System: ";

  // --------- com.gemstone.gemfire.internal.admin.JoinLeaveListener ---------
  /** 
   * Listener callback for when a member has joined this DistributedSystem.
   * <p>
   * React by creating an MBean for managing the SystemMember and then fire
   * a Notification with the internal Id of the member VM.
   *
   * @param source  the distributed system that fired nodeJoined
   * @param joined  the VM that joined
   * @see com.gemstone.gemfire.internal.admin.JoinLeaveListener#nodeJoined
   */
  @Override
  public void nodeJoined(GfManagerAgent source, GemFireVM joined) {
    try {
      super.nodeJoined(source, joined);
      
      /* super.nodeJoined results in creation of a new SystemMember which 
       * registers itself as an MBean, so now we try to find it...
       */
      SystemMember member = findSystemMember(joined);
      
      if(null == member) {
        if (logger.isDebugEnabled()) {
          logger.debug("AdminDistributedSystemJmxImpl.nodeJoined(), Could not find SystemMember for VM {}", joined);
        }
        return;
      }
      
      try {
        if (logger.isDebugEnabled()) {
          logger.debug("Processing node joined for: {}", member);
          logger.debug("RemoteGemFireVM.nodeJoined(), setting alerts manager *************");
        }
        setAlertsManager(joined);
        
        this.modelMBean.sendNotification(new Notification(
            NOTIF_MEMBER_JOINED,
            ((ManagedResource)member).getObjectName(), // Pass the ObjName of the Source Member
            notificationSequenceNumber.addAndGet(1),
            joined.getId().toString()));

//      String mess = "Gemfire AlertNotification: System Member Joined, System member Id: " + joined.getId().toString();
//      sendEmail("Gemfire AlertNotification: Member Joined, ID: " + joined.getId().toString(), mess);
        if (isEmailNotificationEnabled) {
          String mess = LocalizedStrings.AdminDistributedSystemJmxImpl_MEMBER_JOINED_THE_DISTRIBUTED_SYSTEM_MEMBER_ID_0.toLocalizedString(new Object[] {joined.getId().toString()} );
        	sendEmail(
        	    EML_SUBJ_PRFX_GFE_NOTFY + EML_SUBJ_ITEM_GFE_DS + getName() + " <" 
        	    + LocalizedStrings.AdminDistributedSystemJmxImpl_MEMBER_JOINED.toLocalizedString() +">", 
        	    mess);
        }
      } catch (javax.management.MBeanException e) {
        logger.warn(e.getMessage(), e);
      }
    } catch (RuntimeException e) {
      logger.warn(e.getMessage(), e);
      throw e;
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Error e) { 
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      logger.error(e.getMessage(), e);
      throw e;
    }
  }
  
  /** 
   * Listener callback for when a member has left this DistributedSystem.
   * <p>
   * React by removing the member's MBean.
   * Also fire a Notification with the internal Id of the member VM.
   *
   * @param source  the distributed system that fired nodeLeft
   * @param left    the VM that left
   * @see com.gemstone.gemfire.internal.admin.JoinLeaveListener#nodeLeft
   */
  @Override
  public void nodeLeft(GfManagerAgent source, GemFireVM left) {
    try {
      SystemMember member = findSystemMember(left, false);
      super.nodeLeft(source, left);
      if (logger.isDebugEnabled()) {
        logger.debug("Processing node left for: {}", member);
      }
      try {
        this.modelMBean.sendNotification(new Notification(
            NOTIF_MEMBER_LEFT,
            ((ManagedResource)member).getObjectName(), // Pass the ObjName of the Source Member
            notificationSequenceNumber.addAndGet(1),
            left.getId().toString()));

//        String mess = "Gemfire AlertNotification: System Member Left the system, System member Id: " + left.getId().toString();
//        sendEmail("Gemfire AlertNotification: Member Left, ID: " + left.getId().toString(), mess);
        if (isEmailNotificationEnabled) {
          String mess = LocalizedStrings.AdminDistributedSystemJmxImpl_MEMBER_LEFT_THE_DISTRIBUTED_SYSTEM_MEMBER_ID_0.toLocalizedString(new Object[] {left.getId().toString()} );
        	sendEmail( 
        	    EML_SUBJ_PRFX_GFE_NOTFY + EML_SUBJ_ITEM_GFE_DS + getName() + " <" 
        	    + LocalizedStrings.AdminDistributedSystemJmxImpl_MEMBER_LEFT.toLocalizedString() +">", 
        	    mess);
        }
      } catch (javax.management.MBeanException e) { 
        logger.warn(e.getMessage(), e);
      }
      
      SystemMemberType memberType = member.getType();  
      if (/* member != null && */ memberType.isApplication() || memberType.isCacheVm()) {
        // automatically unregister the MBean...
        MBeanUtil.unregisterMBean((ManagedResource) member);
      }
    } catch (RuntimeException e) {
      logger.warn(e.getMessage(), e);
      throw e;
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Error e) { 
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      logger.error(e.getMessage(), e);
      throw e;
    }
  }
  
  /** 
   * Listener callback for when a member of this DistributedSystem has crashed.
   * <p>
   * Also fires a Notification with the internal Id of the member VM.
   *
   * @param source  the distributed system that fired nodeCrashed
   * @param crashed the VM that crashed
   * @see com.gemstone.gemfire.internal.admin.JoinLeaveListener#nodeCrashed
   */
  @Override
  public void nodeCrashed(GfManagerAgent source, GemFireVM crashed) {
    try {
      // SystemMember application has left...
      SystemMember member = findSystemMember(crashed, false);
      super.nodeCrashed(source, crashed);
      if (logger.isDebugEnabled()) {
        logger.debug("Processing node crash for: {}", member);
      }
      
      try {
        this.modelMBean.sendNotification(new Notification(
            NOTIF_MEMBER_CRASHED,
            ((ManagedResource)member).getObjectName(), // Pass the ObjName of the Source Member
            notificationSequenceNumber.addAndGet(1),
            crashed.getId().toString()));
        
//        String mess = "Gemfire AlertNotification: System Member Crashed, System member Id: " + crashed.getId().toString();
//        sendEmail("Gemfire AlertNotification: Member Crashed, ID: " + crashed.getId().toString(), mess);
        if (isEmailNotificationEnabled) {
          String mess = LocalizedStrings.AdminDistributedSystemJmxImpl_MEMBER_CRASHED_IN_THE_DISTRIBUTED_SYSTEM_MEMBER_ID_0.toLocalizedString(new Object[] {crashed.getId().toString()} );
        	sendEmail(
        	    EML_SUBJ_PRFX_GFE_ALERT + EML_SUBJ_ITEM_GFE_DS + getName() + " <" 
        	    + LocalizedStrings.AdminDistributedSystemJmxImpl_MEMBER_CRASHED.toLocalizedString() +">", 
        	    mess);
        }
      } catch (javax.management.MBeanException e) {
        logger.warn(e.getMessage(), e);
      }

      SystemMemberType memberType = member.getType();
      if (/* member != null && */ memberType.isApplication() || memberType.isCacheVm()) {
        // automatically unregister the MBean...
        MBeanUtil.unregisterMBean((ManagedResource) member);
      }
    } catch (RuntimeException e) {
      logger.warn(e.getMessage(), e);
      throw e;
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Error e) { 
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      logger.error(e.getMessage(), e);
      throw e;
    }
  }

  // ----------- com.gemstone.gemfire.internal.admin.AlertListener -----------
  /** 
   * Listener callback for when a SystemMember of this DistributedSystem has 
   * crashed.
   * <p>
   * Fires a Notification with the information from the alert.
   *
   * @param alert the gemfire alert to broadcast as a notification
   * @see com.gemstone.gemfire.internal.admin.AlertListener#alert
   */
  @Override
  public void alert(Alert alert) {
    try {
      super.alert(alert);
      try {
        String strAlert = alert.toString();
        this.modelMBean.sendNotification(new Notification(
            NOTIF_ALERT,
            this.mbeanName,
            notificationSequenceNumber.addAndGet(1),
            strAlert));
        
//        String mess = "Gemfire AlertNotification: System Alert :" + alert.toString();
//        sendEmail("Gemfire AlertNotification: System Alert", mess);
        if (isEmailNotificationEnabled) {
          String mess = LocalizedStrings.AdminDistributedSystemJmxImpl_SYSTEM_ALERT_FROM_DISTRIBUTED_SYSTEM_0.toLocalizedString(strAlert);
        	sendEmail( EML_SUBJ_PRFX_GFE_ALERT + EML_SUBJ_ITEM_GFE_DS + getName() + " <System Alert>", mess);
        }
      } catch (javax.management.MBeanException e) {
        logger.warn(e.getMessage(), e);
      }
    } catch (RuntimeException e) {
      logger.warn(e.getMessage(), e);
      throw e;
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Error e) { 
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      logger.error(e.getMessage(), e);
      throw e;
    }
  }
  
  @Override
  public void onDisconnect(InternalDistributedSystem sys) {
    if (logger.isDebugEnabled()) {
      this.logger.debug("Calling AdminDistributedSystemJmxImpl#onDisconnect");
    }
  	try {
  		super.onDisconnect(sys);
  		
  		try {
  			this.modelMBean.sendNotification(new Notification(
  			        NOTIF_ADMIN_SYSTEM_DISCONNECT,
  			        this.mbeanName,
  			        notificationSequenceNumber.addAndGet(1),
  			        null));
  		} catch (MBeanException e) {
  		  logger.warn(e.getMessage(), e);	
  		}
  	} catch (RuntimeException e) {
  	  logger.warn(e.getMessage(), e);
  	  throw e;
  	} catch (VirtualMachineError err) {
  	  SystemFailure.initiateFailure(err);
  	  // If this ever returns, rethrow the error. We're poisoned
  	  // now, so don't let this thread continue.
  	  throw err;
  	} catch (Error e) {
  	  // Whenever you catch Error or Throwable, you must also
  	  // catch VirtualMachineError (see above). However, there is
  	  // _still_ a possibility that you are dealing with a cascading
  	  // error condition, so you also need to check to see if the JVM
  	  // is still usable:
  	  SystemFailure.checkFailure();
  	  logger.error(e.getMessage(), e);
  	  throw e;
  	}
  	if (logger.isDebugEnabled()) {
  	  this.logger.debug("Completed AdminDistributedSystemJmxImpl#onDisconnect");
  	}
  }
  
  // -------------------------------------------------------------------------
  //   ManagedResource implementation
  // -------------------------------------------------------------------------
  
  /** The name of the MBean that will manage this resource */
  private String mbeanName;

  /** The remotable ObjectName that the  MBean is registered under */
  final private ObjectName objectName;
  
  /** The ModelMBean that is configured to manage this resource */
  private ModelMBean modelMBean;
  
	public String getMBeanName() {
		return this.mbeanName;
	}
  
	public ModelMBean getModelMBean() {
		return this.modelMBean;
	}
	public void setModelMBean(ModelMBean modelMBean) {
		this.modelMBean = modelMBean;
	}

  public ObjectName getObjectName() {
    return this.objectName;
  }

  public ManagedResourceType getManagedResourceType() {
    return ManagedResourceType.DISTRIBUTED_SYSTEM;
  }
  
  // -------------------------------------------------------------------------
  //   Error traps added to overridden methods...
  // -------------------------------------------------------------------------
  
  @Override
  public boolean isRunning() {
    try {
      return super.isRunning();
    } catch (java.lang.RuntimeException e) {
      logger.warn(e.getMessage(), e);
      throw e;
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (java.lang.Error e) { 
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      logger.error(e.getMessage(), e);
      throw e;
    }
  }

  @Override
  public void start() throws AdminException {
    try {
      super.start();
    } catch (java.lang.RuntimeException e) {
      logger.warn(e.getMessage(), e);
      throw e;
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (java.lang.Error e) { 
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      logger.error(e.getMessage(), e);
      throw e;
    }
  }
  
  @Override
  public void stop() throws AdminException {
    try {
      super.stop();
    } catch (java.lang.RuntimeException e) {
      logger.warn(e.getMessage(), e);
      throw e;
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (java.lang.Error e) { 
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      logger.error(e.getMessage(), e);
      throw e;
    }
  }

  @Override
  public boolean waitToBeConnected(long timeout)
    throws InterruptedException {

    if (Thread.interrupted()) throw new InterruptedException();
    try {
      return super.waitToBeConnected(timeout);
    } catch (java.lang.RuntimeException e) {
      logger.warn(e.getMessage(), e);
      throw e;
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (java.lang.Error e) { 
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      logger.error(e.getMessage(), e);
      throw e;
    }
  }

  @Override
  public String displayMergedLogs() {
    try {
      return super.displayMergedLogs();
    } catch (java.lang.RuntimeException e) {
      logger.warn(e.getMessage(), e);
      throw e;
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (java.lang.Error e) { 
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      logger.error(e.getMessage(), e);
      throw e;
    }
  }

  public ObjectName manageDistributionLocator()
    throws MalformedObjectNameException {

    try {
      return new ObjectName(((ManagedResource) addDistributionLocator()).getMBeanName());
    }
//     catch (AdminException e) { logger.warn(e.getMessage(), e); throw e; }
    catch (RuntimeException e) {
      logger.warn(e.getMessage(), e);
      throw e;
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Error e) { 
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      logger.error(e.getMessage(), e);
      throw e;
    }
  }

  public ObjectName[] manageDistributionLocators() 
  throws MalformedObjectNameException {
    try {
      DistributionLocator[] locs = getDistributionLocators();
      ObjectName[] onames = new javax.management.ObjectName[locs.length];
      for (int i = 0; i < locs.length; i++) {
        DistributionLocatorJmxImpl loc = (DistributionLocatorJmxImpl) locs[i];
        onames[i] = new ObjectName(loc.getMBeanName());
      }
      return onames;
    }
    //catch (AdminException e) { logger.warn(e.getMessage(), e); throw e; }
    catch (RuntimeException e) { 
      logger.warn(e.getMessage(), e); 
      throw e; 
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Error e) { 
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      logger.error(e.getMessage(), e); 
      throw e; 
    }
  }
    
  /**
   * @deprecated as of 5.7 use {@link #manageCacheVm} instead.
   */
  @Deprecated
  public ObjectName manageCacheServer()
  throws AdminException, MalformedObjectNameException {
    return manageCacheVm();
  }
  public ObjectName manageCacheVm()
  throws AdminException, MalformedObjectNameException {
    try {
      return new ObjectName(((ManagedResource) addCacheVm()).getMBeanName());
    } catch (AdminException e) { 
      logger.warn(e.getMessage(), e); 
      throw e; 
    } catch (RuntimeException e) { 
      logger.warn(e.getMessage(), e); 
      throw e; 
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Error e) { 
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      logger.warn(e.getMessage(), e);
      throw e; 
    }
  }

  /**
   * @deprecated as of 5.7 use {@link #manageCacheVms} instead.
   */
  @Deprecated
  public ObjectName[] manageCacheServers()
  throws AdminException, MalformedObjectNameException {
    return manageCacheVms();
  }

  public ObjectName[] manageCacheVms()
  throws AdminException, MalformedObjectNameException {
    try {
      CacheVm[] mgrs = getCacheVms();
      ObjectName[] onames = new javax.management.ObjectName[mgrs.length];
      for (int i = 0; i < mgrs.length; i++) {
        CacheServerJmxImpl mgr = (CacheServerJmxImpl) mgrs[i];
        onames[i] = new ObjectName(mgr.getMBeanName());
      }
      return onames;
    } catch (AdminException e) { 
      logger.warn(e.getMessage(), e); 
      throw e; 
    } catch (RuntimeException e) { 
      logger.warn(e.getMessage(), e); 
      throw e; 
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Error e) { 
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      logger.error(e.getMessage(), e);
      throw e; 
    }
  }

  public ObjectName[] manageSystemMemberApplications()
  throws AdminException, MalformedObjectNameException {
    try {
      SystemMember[] apps = getSystemMemberApplications();
      ObjectName[] onames = new javax.management.ObjectName[apps.length];
      for (int i = 0; i < apps.length; i++) {
        SystemMemberJmxImpl app = (SystemMemberJmxImpl) apps[i];
        onames[i] = new ObjectName(app.getMBeanName());
      }
      return onames;
    } catch (AdminException e) { 
      logger.warn(e.getMessage(), e); 
      throw e; 
    } catch (RuntimeException e) { 
      logger.warn(e.getMessage(), e); 
      throw e; 
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Error e) { 
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      logger.error(e.getMessage(), e);
      throw e; 
    }
  }
  
  /**
   * Return the ObjectName for the SystemMemberMBean representing the
   * specified distributed member or null if the member is not found.
   *
   * @param distributedMember the distributed member to manage
   * @return the ObjectName for the SystemMemberMBean
   */
  public ObjectName manageSystemMember(DistributedMember distributedMember)
  throws AdminException, MalformedObjectNameException {
    try {
      SystemMember member = lookupSystemMember(distributedMember);
      if (member == null) return null;
      SystemMemberJmxImpl jmx = (SystemMemberJmxImpl) member;
      ObjectName oname = new ObjectName(jmx.getMBeanName());
      return oname;
    } catch (AdminException e) { 
      logger.warn(e.getMessage(), e); 
      throw e; 
    } catch (RuntimeException e) { 
      logger.warn(e.getMessage(), e); 
      throw e; 
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Error e) { 
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      logger.error(e.getMessage(), e);
      throw e; 
    }
  }
  
  @Override
  public void connect(InternalLogWriter logWriter) {
    try {
      // LOG: passes the AdminDistributedSystemImpl LogWriterLogger into GfManagerAgentConfig
      super.connect(logWriter);
      
      // Load existing StatAlert Definitions
      readAlertDefinitionsAsSerializedObjects();

      /* Add Cache Listener to listen to Cache & Region create/destroy events */
      if (logger.isDebugEnabled()) {
        logger.debug("Adding CacheAndRegionListener .... ");
      }
      addCacheListener(cacheRegionListener);
    } catch (RuntimeException e) { 
      logger.warn(e.getMessage(), e);
      throw e; 
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Error e) { 
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      logger.error(e.getMessage(), e);
      throw e; 
    }
  }
  
  @Override
  public void disconnect() {
    try {
      super.disconnect();
      
      // Save existing StatAlert Definitions
      saveAlertDefinitionsAsSerializedObjects();

      /* Remove Cache Listener to listen to Cache & Region create/destroy events */
      if (logger.isDebugEnabled()) {
        logger.debug("Removing CacheAndRegionListener .... ");
      }
      removeCacheListener(cacheRegionListener);      
    } catch (RuntimeException e) { 
      logger.warn(e.getMessage(), e);
      throw e;
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, re-throw the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Error e) { 
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      logger.error(e.getMessage(), e);
      throw e;
    }
  }

  public void cleanupResource() {
    disconnect();
  }
  
  /**
   * @return the isRmiClientCountZero
   * @since GemFire 6.0
   */
  public boolean isRmiClientCountZero() {
    return isRmiClientCountZero;
  }

  /**
   * @param isRmiClientCountZero the isJmxClientCountZero to set
   * @since GemFire 6.0
   */
  void setRmiClientCountZero(boolean isRmiClientCountZero) {
    this.isRmiClientCountZero = isRmiClientCountZero;
    
    if (isRmiClientCountZero) {
      logger.info(LocalizedStrings.AdminDistributedSystemJmxImpl_JMX_CLIENT_COUNT_HAS_DROPPED_TO_ZERO);
    }
  }


  ///////////////////////  Configuration  ///////////////////////

  public String getEntityConfigXMLFile() {
    return this.getConfig().getEntityConfigXMLFile();
  }

  public void setEntityConfigXMLFile(String xmlFile) {
    this.getConfig().setEntityConfigXMLFile(xmlFile);
  }

  public String getSystemId() {
    return this.getConfig().getSystemId();
  }

  public void setSystemId(String systemId) {
    this.getConfig().setSystemId(systemId);
  }

  @Override
  public String getSystemName() {
    return this.getConfig().getSystemName();
  }

  public void setSystemName(final String name) {
    this.getConfig().setSystemName(name);
  }
    
  @Override
  public boolean getDisableTcp() {
    return this.getConfig().getDisableTcp();
  }

  public void setEnableNetworkPartitionDetection(boolean newValue) {
    getConfig().setEnableNetworkPartitionDetection(newValue);
  }
  public boolean getEnableNetworkPartitionDetection() {
    return getConfig().getEnableNetworkPartitionDetection();
  }
  public int getMemberTimeout() {
   return getConfig().getMemberTimeout();
  }
  public void setMemberTimeout(int value) {
    getConfig().setMemberTimeout(value);
  }
  
  @Override
  public String getMcastAddress() {
    return this.getConfig().getMcastAddress();
  }

  public void setMcastAddress(String mcastAddress) {
    this.getConfig().setMcastAddress(mcastAddress);
  }

  @Override
  public int getMcastPort() {
    return this.getConfig().getMcastPort();
  }
  
  public void setMcastPort(int mcastPort) {
    this.getConfig().setMcastPort(mcastPort);
  }

  public int getAckWaitThreshold() {
    return getConfig().getAckWaitThreshold();
  }
  
  public void setAckWaitThreshold(int seconds) {
    getConfig().setAckWaitThreshold(seconds);
  }

  public int getAckSevereAlertThreshold() {
    return getConfig().getAckSevereAlertThreshold();
  }
  
  public void setAckSevereAlertThreshold(int seconds) {
    getConfig().setAckSevereAlertThreshold(seconds);
  }

  @Override
  public String getLocators() {
    return this.getConfig().getLocators();
  }

  @Override
  public void setLocators(String locators) {
    this.getConfig().setLocators(locators);
  }
  
  /* Note that the getter & setter for membership port range are referred from 
   * the super class AdminDistributedSystemImpl */

  public String getBindAddress() {
    return this.getConfig().getBindAddress();
  }

  public void setBindAddress(String bindAddress) {
    this.getConfig().setBindAddress(bindAddress);
  }
  
  public String getServerBindAddress() {
    return this.getConfig().getServerBindAddress();
  }

  public void setServerBindAddress(String bindAddress) {
    this.getConfig().setServerBindAddress(bindAddress);
  }

  @Override
  public String getRemoteCommand() {
    return this.getConfig().getRemoteCommand();
  }

  @Override
  public void setRemoteCommand(String command) {
    this.getConfig().setRemoteCommand(command);
  }

  public boolean isSSLEnabled() {
    return this.getConfig().isSSLEnabled();
  }

  public void setSSLEnabled(boolean enabled) {
    this.getConfig().setSSLEnabled(enabled);
  }

  public String getSSLProtocols() {
    return this.getConfig().getSSLProtocols();
  }

  public void setSSLProtocols(String protocols) {
    this.getConfig().setSSLProtocols(protocols);
  }

  public String getSSLCiphers() {
    return this.getConfig().getSSLCiphers();
  }

  public void setSSLCiphers(String ciphers) {
    this.getConfig().setSSLCiphers(ciphers);
  }

  public boolean isSSLAuthenticationRequired() {
    return this.getConfig().isSSLAuthenticationRequired();
  }

  public void setSSLAuthenticationRequired(boolean authRequired) {
    this.getConfig().setSSLAuthenticationRequired(authRequired);
  }
  
  public Properties getSSLProperties() {
    return this.getConfig().getSSLProperties();
  }

  public void setSSLProperties(Properties sslProperties) {
    this.getConfig().setSSLProperties(sslProperties);
  }

  public void addSSLProperty(String key, String value) {
    this.getConfig().addSSLProperty(key, value);
  }

  public void removeSSLProperty(String key) {
    this.getConfig().removeSSLProperty(key);
  }
  
  public String getLogFile() {
    return this.getConfig().getLogFile();
  }

  public void setLogFile(String logFile) {
    this.getConfig().setLogFile(logFile);
  }

  public String getLogLevel() {
    return this.getConfig().getLogLevel();
  }

  public void setLogLevel(String logLevel) {
    this.getConfig().setLogLevel(logLevel);
  }

  public int getLogDiskSpaceLimit() {
    return this.getConfig().getLogDiskSpaceLimit();
  }

  public void setLogDiskSpaceLimit(int limit) {
    this.getConfig().setLogDiskSpaceLimit(limit);
  }

  public int getLogFileSizeLimit() {
    return this.getConfig().getLogFileSizeLimit();
  }

  public void setLogFileSizeLimit(int limit) {
    this.getConfig().setLogFileSizeLimit(limit);
  }

  public void setDisableTcp(boolean flag) {
    this.getConfig().setDisableTcp(flag);
  }

  public int getRefreshInterval() {
    return this.getConfig().getRefreshInterval();
  }

  /*
   * The interval (in seconds) between auto-polling for updating
   * AdminDistributedSystem constituents including SystemMember,
   * SystemMemberCache and StatisticResource. This applies only to the default
   * interval set when the resource is created. Changes to this interval will
   * not get propagated to existing resources but will apply to all new
   * resources
   */
  public void setRefreshInterval(int interval) {
    this.getConfig().setRefreshInterval(interval);
  }

  public CacheServerConfig[] getCacheServerConfigs() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  public CacheServerConfig createCacheServerConfig() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  public void removeCacheServerConfig(CacheServerConfig config) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  public CacheVmConfig[] getCacheVmConfigs() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  public CacheVmConfig createCacheVmConfig() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  public void removeCacheVmConfig(CacheVmConfig config) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  public DistributionLocatorConfig[] getDistributionLocatorConfigs() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }
  
  public DistributionLocatorConfig createDistributionLocatorConfig() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  public void removeDistributionLocatorConfig(DistributionLocatorConfig config) 
{
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  public void addListener(ConfigListener listener) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  public void removeListener(ConfigListener listener) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  public void validate() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }
  

  private static final String[] PERSISTENT_ID_FIELDS = new String[] {"host", "directory", "uuid"};
  private static final String[] PERSISTENT_ID_DESCRIPTIONS = new String[] {"The host that was persisting the missing files", "The directory where the files were persisted", "The unique id for the persistent files"};
  private final CompositeType PERSISTENT_ID_TYPE;
  private final TabularType PERSISTENT_ID_TABLE_TYPE;
  
  { 
    try {
      PERSISTENT_ID_TYPE = new CompositeType(PersistentID.class.getCanonicalName(), "A single member's a set of persistent files for a region", PERSISTENT_ID_FIELDS, PERSISTENT_ID_DESCRIPTIONS, new OpenType[] {SimpleType.STRING, SimpleType.STRING, SimpleType.STRING});
      PERSISTENT_ID_TABLE_TYPE = new TabularType("TABLE_" + PERSISTENT_ID_TYPE.getTypeName(), "A table of persistent member ids", PERSISTENT_ID_TYPE, PERSISTENT_ID_FIELDS);
    } catch (OpenDataException e) {
      throw new ExceptionInInitializerError(e);
    }
  }
  
  public TabularData getMissingPersistentMembersJMX() throws AdminException {
    
    try {
      Set<PersistentID> members = super.getMissingPersistentMembers();
      TabularData results = new TabularDataSupport(PERSISTENT_ID_TABLE_TYPE);
      int index = 0;
      for(PersistentID id : members) {
        CompositeData idData = new CompositeDataSupport(PERSISTENT_ID_TYPE, PERSISTENT_ID_FIELDS, new Object[] {id.getHost().toString(), id.getDirectory(), id.getUUID().toString()});
        results.put(idData);
        index++;
      }
      return results;
    } catch( OpenDataException e) {
      logger.warn("Exception occurred while getting missing persistent members.", e);
      throw new AdminException(e);
    }
  }

  public void revokePersistentMember(String host,
      String directory) throws AdminException, UnknownHostException {
    super.revokePersistentMember(InetAddress.getByName(host), directory);
  }
  
  public void revokePersistentMember(String uuid) throws AdminException, UnknownHostException {
    super.revokePersistentMember(UUID.fromString(uuid));
  }

  /* ********************************************************************* */
  /* ************** Implementing StatAlertsAggregator interface ********** */
  /* ********************************************************************* */
  
  /* Map to store admin stat alert definitions */
  private final Map ALERT_DEFINITIONS = new Hashtable();
  
  /* Refresh interval for all stat alerts managers in seconds */
  private int refreshIntervalForStatAlerts = 20;

  /* 
   * This map contains list of stat alerts as a value for alert def ID as a key  
   */
  private final HashMap alertsStore = new HashMap();
  
  //TODO: yet to set the timer task
//  private SystemTimer systemwideAlertNotificationScheduler = new SystemTimer();
  
  private MailManager mailManager = null;
  private final boolean isEmailNotificationEnabled;
  
  /**
   * Convenience method to retrieve admin stat alert definition.
   * 
   * @param alertDefinitionId id of a stat alert definition 
   * @return StatAlertDefinition reference to an instance of StatAlertDefinition
   * @since GemFire 5.7
   */
  private StatAlertDefinition getAlertDefinition(int alertDefinitionId) {
    synchronized(ALERT_DEFINITIONS) {
      return (StatAlertDefinition)ALERT_DEFINITIONS.get(Integer.valueOf(alertDefinitionId));
    }
  }
  
/*  private void setAlertDefinition(StatAlertDefinition alertDefinition) {
    ALERT_DEFINITIONS.put(Integer.valueOf(alertDefinition.getId()), alertDefinition);
  }*/

  /** 
   * This method can be used to get an alert definition. 
   * 
   * @param alertDefinition StatAlertDefinition to retrieve
   * @return StatAlertDefinition 
   * @since GemFire 5.7
   */
  public StatAlertDefinition getAlertDefinition(StatAlertDefinition alertDefinition) {
    return getAlertDefinition(alertDefinition.getId());
  }
  
  /**
   * This method is used to write existing StatAlertDefinitions 
   * to a file
   */
  protected void readAlertDefinitionsAsSerializedObjects() {
    StatAlertDefinition[] defns = new StatAlertDefinition[0];

    File serFile = null;
    FileInputStream foStr = null;
    DataInputStream ooStr = null;

    try {
      serFile = new File(statAlertDefnSerFile);
      
      if (!canWriteToFile(serFile)) {/* can not write a file */
        canPersistStatAlertDefs = false;
      }
      if (!serFile.exists()) {/* file does not exist */
        return;
      }
      
      if (logger.isDebugEnabled()) {
        logger.debug("AdminDistributedSystemJmxImpl.readAlertDefinitionsAsSerializedObjects: File: {}", serFile.getPath());
      }

      foStr = new FileInputStream(serFile);
      ooStr = new DataInputStream(foStr);
      defns = (StatAlertDefinition[])DataSerializer.readObjectArray(ooStr);
    } catch (ClassNotFoundException cnfEx) {
      logger.error(LocalizedMessage.create(
          LocalizedStrings.AdminDistributedSystem_ENCOUNTERED_A_0_WHILE_LOADING_STATALERTDEFINITIONS_1,
          new Object[] {cnfEx.getClass().getName(), statAlertDefnSerFile}), cnfEx);
      canPersistStatAlertDefs = false;
    } catch (IOException ex) {
      logger.error(LocalizedMessage.create(
          LocalizedStrings.AdminDistributedSystem_ENCOUNTERED_A_0_WHILE_LOADING_STATALERTDEFINITIONS_1_LOADING_ABORTED,
          new Object[] {ex.getClass().getName(), statAlertDefnSerFile}), ex);
      canPersistStatAlertDefs = false;
    } finally {
      if (foStr!=null) {
        try {
          foStr.close();
        } catch (IOException ex) {
          ;
        }
      }
      if (ooStr!=null) { 
        try {
          ooStr.close();
        } catch (IOException ex) {
          ;
        }
      }
    }
    
    for (int i=0; i<defns.length; i++) {
      updateAlertDefinition(defns[i]);
    }
  }

  /**
   * This method is used to write existing StatAlertDefinitions 
   * to a file
   */
  public void saveAlertDefinitionsAsSerializedObjects() {
    File serFile = null;
    FileOutputStream foStr = null;
    DataOutputStream ooStr = null;
    try {
      serFile = new File(statAlertDefnSerFile);
      
  
      if (logger.isDebugEnabled()) {
        logger.debug("AdminDistributedSystemJmxImpl.saveAlertDefinitionsAsSerializedObjects: File: {}", serFile.getPath());
      }
      
      if (!canWriteToFile(serFile)) {
        return;
      }
      
      foStr = new FileOutputStream(serFile);
      ooStr = new DataOutputStream(foStr);
      
      int numOfAlerts = 0;
      StatAlertDefinition[] defs = null;

      synchronized (ALERT_DEFINITIONS) {
        numOfAlerts = ALERT_DEFINITIONS.size();
        defs = new StatAlertDefinition[numOfAlerts];
        
        int i = 0;
        for (Iterator iter=ALERT_DEFINITIONS.keySet().iterator(); iter.hasNext() ;) {
          Integer key = (Integer) iter.next();
          StatAlertDefinition readDefn = (StatAlertDefinition) ALERT_DEFINITIONS.get(key);
          defs[i] = readDefn;
          i++; 
        }
      }

      DataSerializer.writeObjectArray(defs, ooStr);
    } catch (IOException ex) {
      logger.error(LocalizedMessage.create(
          LocalizedStrings.AdminDistributedSystem_ENCOUNTERED_A_0_WHILE_SAVING_STATALERTDEFINITIONS_1,
          new Object[] {ex.getClass().getName(), statAlertDefnSerFile}), ex);
    } finally {
      if (foStr!=null) 
        try {
          foStr.close();
        } catch (IOException ex) {
          ;
        }
      if (ooStr!=null) 
        try {
          ooStr.close();
        } catch (IOException ex) {
          ;
        }
    }
  }

  /**
   * Checks if the given file is writable.
   * 
   * @param file
   *          file to check write permissions for
   * @return true if file is writable, false otherwise
   */
  private boolean canWriteToFile(File file) {
    boolean fileIsWritable = true;
    // Fix for BUG40360 : When the user does not have write permissions for
    // saving the stat-alert definitions, then appropriate warning message is 
    // logged and the operation is aborted. In case the file doesn't exist, then 
    // it attempts to create a file. If the attempt fails then it logs 
    // appropriate warning and the operation is aborted. File.canWrite check for
    // a directory sometimes fails on Windows platform. Hence file creation is 
    // necessary.
    if (file.exists()) {
      if (!file.canWrite()) {
        logger.warn(LocalizedMessage.create(
            LocalizedStrings.AdminDistributedSystemJmxImpl_READONLY_STAT_ALERT_DEF_FILE_0,
            new Object[] { file }));
        fileIsWritable = false;
      }
    } else {
      try {
        file.createNewFile();
      } catch (IOException e) {
        logger.warn(LocalizedMessage.create(
            LocalizedStrings.AdminDistributedSystemJmxImpl_FAILED_TO_CREATE_STAT_ALERT_DEF_FILE_0,
            new Object[] { file }), e);
        fileIsWritable = false;
      } finally {
        // Since we had created this file only for testing purpose, delete the
        // same.
        if ((file.exists() && !file.delete()) && logger.isDebugEnabled()) {
          logger.debug("Could not delete file :'{}' which is created for checking permissions.", file.getAbsolutePath());
        }
      }
    }
    return fileIsWritable;
  }

  /** 
   * This method can be used to update alert definition for the Stat mentioned.
   * This method should update the collection maintained at the aggregator and 
   * should notify members for the newly added alert definitions.
   * A new alert definition will be created if matching one not found.
   * 
   * @param alertDefinition alertDefinition to be updated 
   * @since GemFire 5.7
   */  
  public void updateAlertDefinition(StatAlertDefinition alertDefinition) {
    if (logger.isDebugEnabled()) {
      logger.debug("Entered AdminDistributedSystemJmxImpl.updateAlertDefinition(StatAlertDefinition) *****");
    }
    /*
     * What to update in the alert definition? There should be another argument 
     * or arguments in a map.
     * 1. Need to update the list/map of alert definitions across members.   
     */
    synchronized (ALERT_DEFINITIONS) {
      ALERT_DEFINITIONS.put(Integer.valueOf(alertDefinition.getId()), alertDefinition);
      
      if (logger.isDebugEnabled()) {
        logger.debug("AdminDistributedSystemJmxImpl.updateAlertDefinition : alertDefinition :: id={} :: {}", alertDefinition.getId(), alertDefinition.getStringRepresentation());
      }
      
      /* TODO: add code to retry on failure */
      notifyMembersForAlertDefinitionChange(alertDefinition);
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Exiting AdminDistributedSystemJmxImpl.updateAlertDefinition(StatAlertDefinition) *****");
    }
  }

  /** 
   * This method can be used to remove alert definition for the Stat 
   * mentioned. 
   * This method should update the collection maintained at the aggregator and 
   * should notify members for the newly added alert definitions. 
   * 
   * @param defId id of the alert definition to be removed
   * @since GemFire 5.7
   */  
  public void removeAlertDefinition(Integer defId) {
    if (logger.isDebugEnabled()) {
      logger.debug("Entered AdminDistributedSystemJmxImpl.removeAlertDefinition id *****");
    }
    /*
     * alert passed to be deleted from the list/map of alerts on JMX MBean 
     * & all Member MBeans
     */
    synchronized (ALERT_DEFINITIONS) {
      StatAlertDefinition alertDefinition = (StatAlertDefinition)ALERT_DEFINITIONS.get(defId);
  	  if (alertDefinition != null) {
    		ALERT_DEFINITIONS.remove(defId);
    		synchronized (alertsStore) {
    		  alertsStore.remove(defId);
    		}
    		/* TODO: add code to retry on failure */
    		notifyMembersForAlertDefinitionRemoval(alertDefinition);
  	  }      
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Exiting AdminDistributedSystemJmxImpl.removeAlertDefinition() *****");
    }
  }

  /** 
   * Convenience method to check whether an alert definition is created.
   * 
   * @param alertDefinition alert definition to check whether already created
   * @return true if the alert definition is already created, false 
   *         otherwise 
   * @since GemFire 5.7
   */
  public boolean isAlertDefinitionCreated(StatAlertDefinition alertDefinition) {
    /*
     * Need to maintain a map of stat against the StatAlertDefinitions.
     * check in that map whether the alert definition is there for the given 
     * alert
     * 
     * TODO: optimize to use Map.containsKey - DONE
     */
    synchronized(ALERT_DEFINITIONS) {
      return ALERT_DEFINITIONS.containsKey(Integer.valueOf(alertDefinition.getId()));
    }
  }

  /**
   * Returns the refresh interval for the Stats in seconds.
   * 
   * @return refresh interval for the Stats(in seconds)
   * @since GemFire 5.7
   */
  public synchronized int getRefreshIntervalForStatAlerts() {
    /*
     * state to store the refresh interval set by the user/GFMon client 
     */
    return refreshIntervalForStatAlerts;
  }

  /**
   * This method is used to set the refresh interval for the Stats in 
   * seconds 
   * 
   * @param refreshIntervalForStatAlerts refresh interval for the Stats(in seconds)
   * @since GemFire 5.7
   */
  public synchronized void setRefreshIntervalForStatAlerts(int refreshIntervalForStatAlerts) {
    /*
     * change the state refresh interval here. 
     */
    this.refreshIntervalForStatAlerts = refreshIntervalForStatAlerts;
    notifyMembersForRefreshIntervalChange(this.refreshIntervalForStatAlerts*1000l);
  }

  /**
   * Returns whether Statistics Alert definitions could be persisted across 
   * runs/sessions
   * 
   * @return value of canPersistStatAlertDefs.
   * @since GemFire 6.5
   */
  public boolean canPersistStatAlertDefs() {
    return canPersistStatAlertDefs;
  }

  /**
   * An intermediate method to notify all members for change in refresh interval.
   * 
   * @param newInterval refresh interval to be set for members(in milliseconds)
   */
  private void notifyMembersForRefreshIntervalChange(long newInterval) {
    GfManagerAgent  agent = getGfManagerAgent();
    ApplicationVM[] VMs   = agent.listApplications();    
    //TODO: is there any other way to get all VMs?
    
    for (int i = 0; i < VMs.length; i++) {
      VMs[i].setRefreshInterval(newInterval);
    }
  }
  
  /**
   * An intermediate method to notify all members for change in stat alert 
   * definition.
   * 
   * @param alertDef stat alert definition that got changed
   */  
  private void notifyMembersForAlertDefinitionChange(StatAlertDefinition alertDef) {
    if (logger.isDebugEnabled()) {
      logger.debug("Entered AdminDistributedSystemJmxImpl.notifyMembersForAlertDefinitionChange(StatAlertDefinition) *****");
    }
    GfManagerAgent        agent     = getGfManagerAgent();
    StatAlertDefinition[] alertDefs = new StatAlertDefinition[] {alertDef};
    ApplicationVM[]       VMs       = agent.listApplications();
    
    for (int i = 0; i < VMs.length; i++) {
      VMs[i].updateAlertDefinitions(alertDefs, 
          UpdateAlertDefinitionMessage.UPDATE_ALERT_DEFINITION);
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Exiting AdminDistributedSystemJmxImpl.notifyMembersForAlertDefinitionChange(StatAlertDefinition) "
    		    + VMs.length+" members notified.*****");
    }
  }

  /**
   * An intermediate method to notify all members for removal of stat alert 
   * definition.
   * 
   * @param alertDef stat alert definition to be removed
   */
  private void notifyMembersForAlertDefinitionRemoval(StatAlertDefinition alertDef) {
    GfManagerAgent        agent     = getGfManagerAgent();
    StatAlertDefinition[] alertDefs = new StatAlertDefinition[] {alertDef};
    ApplicationVM[]       VMs       = agent.listApplications();
    
    for (int i = 0; i < VMs.length; i++) {
      VMs[i].updateAlertDefinitions(alertDefs, 
          UpdateAlertDefinitionMessage.REMOVE_ALERT_DEFINITION);
    }
  }

  /**
   * This method can be used to set the AlertsManager for the newly joined     
   * member VM.
   * 
   * @param memberVM Member VM to set AlertsManager for
   * @since GemFire 5.7
   */
  public synchronized void setAlertsManager(GemFireVM memberVM) {
    /*
     * 1. Who'll call this method? Who gets notified when a member joins? 
     *    I think that's AdminDistributedSystemJmxImpl.nodeCreated()
     * 2. Is the argument GemFireVM correct? Need to modify this interface to 
     *    add method to set an interface. Need to see how it can be passed to 
     *    the RemoteGemFireVM implementation. Also need to check whetherother 
     *    implementors (like DistributedSystemHealthMonitor) of GemFireVM even 
     *    need to have the AlertsManager
     * 3. Would the alerts manager be set by aggregator or a JMXAgent i.e. AdminDistributedSystemJmxImpl
     * 4. Setting the list of available alert definitions & refresh interval at 
     *    this moment only would be better/easier.
     * 5. Need to know Alerts Manager creation/construction. Need to decide how 
     *    the object would be set & sent across to the Agent VM.
     */
    if (logger.isDebugEnabled()) {
      logger.debug("Entered AdminDistributedSystemJmxImpl.setAlertsManager(GemFireVM) *****");
    }
    
    // creating an array of stat alert definition objects
    StatAlertDefinition[] alertDefs = new StatAlertDefinition[0];
    
    Collection alertDefsCollection = null;
    synchronized(ALERT_DEFINITIONS) {
      alertDefsCollection = ALERT_DEFINITIONS.values();
    }
    
    alertDefs = (StatAlertDefinition[])alertDefsCollection.toArray(alertDefs);
    
    memberVM.setAlertsManager(alertDefs, getRefreshIntervalForStatAlerts()*1000l, true);
    
    if (logger.isDebugEnabled()) {
      logger.debug("Exiting AdminDistributedSystemJmxImpl.setAlertsManager(GemFireVM) *****");
    }
  }

  /** 
   * This method can be used to retrieve all available stat alert definitions. 
   * Returns empty array if there are no stat alert definitions defined.
   * 
   * @return An array of all available StatAlertDefinition objects 
   * @since GemFire 5.7
   */
  public StatAlertDefinition[] getAllStatAlertDefinitions() {
    if (logger.isDebugEnabled()) {
      logger.debug("Entered AdminDistributedSystemJmxImpl.getAllStatAlertDefinitions() *****");
    }
    
    Collection alertDefs = null;
    synchronized(ALERT_DEFINITIONS) {
      alertDefs = ALERT_DEFINITIONS.values();
    }
    
    StatAlertDefinition[] alertDefsArr = null;
    
    if (alertDefs != null) {
      alertDefsArr = new StatAlertDefinition[alertDefs.size()];
      alertDefsArr = (StatAlertDefinition[])alertDefs.toArray(alertDefsArr);
    } else {
      alertDefsArr = new StatAlertDefinition[0];
    }
    
    if (logger.isDebugEnabled()) {
      logger.debug("Exiting AdminDistributedSystemJmxImpl.getAllStatAlertDefinitions() *****");
    }
    
    return alertDefsArr;
  }


  /**
   * This method can be used to process the notifications sent by the 
   * member(s). Actual aggregation of stats can occur here. The array contains 
   * alert objects with alert def. ID & value. AlertHelper class can be used to 
   * retrieve the corresponding alert definition.
   * 
   * @param alerts array of Alert class(contains alert def. ID & value)
   * @param remoteVM Remote Member VM that sent Stat Alerts for processing the
   *                 notifications to the clients
   */  
  public void processNotifications(StatAlert[] alerts, GemFireVM remoteVM) {
    if (logger.isDebugEnabled()) {
      logger.debug("Entered AdminDistributedSystemJmxImpl.processNotifications(StatAlert[{}], GemFireVM) *************", alerts.length);
    }
    
    /* 
     * Notifications can not be processed if the remote VM is not available.
     * NOTE: Should this method get the required GemFireVM information instead 
     * of its reference so that even if the member leaves we still have the 
     * information collected earlier to process the notification?
     */
    if (remoteVM == null) {
      if (logger.isDebugEnabled()) {
        logger.debug("Could not process stat alert notifications as given GemFireVM is null.");
      }
      return;
    }
    
    /*
     * 1. The implementation idea is yet not clear.
     * 2. The StatAlert array would received directly or from a request object. 
     */
    ArrayList notificationObjects = new ArrayList();
    
    String memberId = remoteVM.getId().getId();

    final boolean isSystemWide = false;
    
    StatAlert alert = null;
    Integer   defId = null;
//    Number[]  values = null;
    for (int i = 0; i < alerts.length; i++) {
      alert = alerts[i];
      
//      defId = Integer.valueOf(alert.getDefinitionId());
      if (getAlertDefinition(alert.getDefinitionId())==null)
        continue; // Ignore any removed AlertDefns 
//      values = alert.getValues();
      
//      StatAlertDefinition statAlertDef = (StatAlertDefinition)ALERT_DEFINITIONS.get(defId);
      
      /*
       * 1. check if it's system-wide.
       * 2. if system-wide keep, it in a collection (that should get cleared on 
       *    timeout). Process all alerts when notifications from all members are 
       *    received. Need to check if the member leaves meanwhile. 
       * 
       * 1. Check if function evaluation is required?
       * 2. If it's not required, the notification should directly be sent to 
       *    clients.
       */
      
      //if (statAlertDef.getFunctionId() != 0) {
        /*
         * StatAlert with alert definitions having functions
         * assigned will get evaluated at manager side only.
         * 
         * Is combination of systemwide alerts with function valid? It should 
         * be & hence such evaluation should be skipped on manager side. Or is 
         * it to be evaluated at manager as well as aggragator?
         */
      //}
      
      //TODO: is this object required? Or earlier canbe resused?
      StatAlertNotification alertNotification = new StatAlertNotification(alert, memberId);
      
      /*
       * variable isSystemWide is created only for convienience, there
       * should be an indication for the same in the alert definition. 
       * Currently there is no systemWide definition
       * 
       * Evaluating system wide alerts:
       *   1. It'll take time for aggregator to gather alerts from all members.
       *      Member might keep joining & leaving in between. The member for whom 
       *      the stat-alert value was taken might have left & new ones might 
       *      have joined leave until all the calculations are complete. 
       *      A disclaimer to be put that these are not exact values.
       *   2. How would the aggregator know that it has received alerts from all 
       *      the managers? Is the concept of system-wide alerts valid? 
       *      System-wide stats might be!
       *      
       */
      if (!isSystemWide) {
        notificationObjects.add(alertNotification);
        continue;
      }
      HashSet accumulatedAlertValues;
      synchronized (alertsStore) {
        accumulatedAlertValues = (HashSet)alertsStore.get(defId);
        
        if (accumulatedAlertValues == null) {
          accumulatedAlertValues = new HashSet();
          alertsStore.put(defId, accumulatedAlertValues);
        }
      }
      synchronized (accumulatedAlertValues) {
        accumulatedAlertValues.add(alertNotification);
      }
    } //for ends
    
    if (!notificationObjects.isEmpty()) {
      /* TODO: should ths method send & forget or be fail-safe? */
      sendNotifications(notificationObjects, getObjectName());
    }
    
    if (logger.isDebugEnabled()) {
      logger.debug("Exiting AdminDistributedSystemJmxImpl.processNotifications(StatAlert[], GemFireVM) *************");
    }
  }

  private byte[] convertNotificationsDataToByteArray(ArrayList notificationObjects) {
    if (logger.isDebugEnabled()) {
      logger.debug("AdminDistributedSystemJmxImpl#convertNotificationsDataToByteArray: {} notifications", notificationObjects.size());
    }

    byte[] arr = null;

    try {
      ByteArrayOutputStream byteArrStr = new ByteArrayOutputStream();
      DataOutputStream str = new DataOutputStream(byteArrStr);
      DataSerializer.writeArrayList(notificationObjects, str);
      str.flush();
      arr = byteArrStr.toByteArray();
    } catch(IOException ex) {
      logger.warn(LocalizedMessage.create(
          LocalizedStrings.AdminDistributedSystem_ENCOUNTERED_AN_IOEXCEPTION_0,
          ex.getLocalizedMessage()));
    }

    return arr;
  }

  /**
   * An intermediate method to send notifications to the clients.
   * 
   * @param notificationObjects list of StatAlertNotification objects
   */
  private void sendNotifications(ArrayList notificationObjects, 
      ObjectName objName) {
    try {
      if (logger.isDebugEnabled()) {
        logger.debug("AdminDistributedSystemJmxImpl#sendNotifications: sending {} notifications", notificationObjects.size());
      }
      
      byte[] notifBytes = convertNotificationsDataToByteArray(notificationObjects);
      if (notifBytes!=null) {
        Notification notif = new Notification(NOTIF_STAT_ALERT,  
            objName, // Pass the StatNotifications
            notificationSequenceNumber.addAndGet(1), "StatAlert Notifications");
        notif.setUserData(notifBytes);
        this.modelMBean.sendNotification(notif);
      } // IOException handled and logged in convertNotificationsDataToByteArray

      StringBuffer buf = new StringBuffer();
      for (int i = 0; i < notificationObjects.size(); i ++) {
        StatAlertNotification not = (StatAlertNotification)
            notificationObjects.get(i);
        buf.append(not.toString(getAlertDefinition(not.getDefinitionId())));
      }
//      sendEmail("Gemfire AlertNotification on Member:" + objName, buf.toString());
      if (isEmailNotificationEnabled) {
        String mess = LocalizedStrings.AdminDistributedSystemJmxImpl_STATISTICS_ALERT_FROM_DISTRIBUTED_SYSTEM_MEMBER_0_STATISTICS_1.toLocalizedString(new Object[] {objName.getCanonicalName(), buf.toString()} );
        sendEmail(
            EML_SUBJ_PRFX_GFE_ALERT + EML_SUBJ_ITEM_GFE_DS + getName() 
            + " <"+LocalizedStrings.AdminDistributedSystemJmxImpl_STATISTICS_ALERT_FOR_MEMBER.toLocalizedString()+">", 
            mess);
      }
    } catch (javax.management.MBeanException e) {
      logger.error(e.getMessage(), e);
    } catch (RuntimeException e) {
      logger.error(e.getMessage(), e);
      throw e;
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error. We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Error e) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above). However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      logger.error(e.getMessage(), e);
      throw e;
    }
  }
  
  /**
   * Sends an email to the configured recipients using configured email server.
   * The given message will be the email body. NOTE: the check whether email
   * notfication is enabled or not should done using
   * {@link #isEmailNotificationEnabled} before calling this method.
   * 
   * @param subject
   *          subject of the email
   * @param message
   *          message body of the email
   */
  private void sendEmail(String subject, String message) {
	  if (mailManager == null) {
	    mailManager = new MailManager(mailProps);
	  }
    this.mailManager.sendEmail(subject, message);
  }

  public void processSystemwideNotifications() {
  }

  public String getId() {
    String myId = super.getId();
    return MBeanUtil.makeCompliantMBeanNameProperty(myId);
  }

  /**
   * This method is used to process ClientMembership events sent for
   * BridgeMembership by bridge servers to all admin members.
   * 
   * @param senderId
   *          id of the member that sent the ClientMembership changes for
   *          processing (could be null)
   * @param clientId
   *          id of a client for which the notification was sent
   * @param clientHost
   *          host on which the client is/was running
   * @param eventType
   *          denotes whether the client Joined/Left/Crashed should be one of
   *          ClientMembershipMessage#JOINED, ClientMembershipMessage#LEFT,
   *          ClientMembershipMessage#CRASHED
   */
  @Override
  public void processClientMembership(String senderId, String clientId,
      String clientHost, int eventType) {
    logger.info(LocalizedMessage.create(LocalizedStrings.AdminDistributedSystemJmxImpl_PROCESSING_CLIENT_MEMBERSHIP_EVENT_0_FROM_1_FOR_2_RUNNING_ON_3, new String[] {ClientMembershipMessage.getEventTypeString(eventType), senderId, clientId, clientHost}));
    try {
      SystemMemberJmx systemMemberJmx = null;
      CacheVm[] cacheVms = getCacheVms();
      
      for (CacheVm cacheVm : cacheVms) {
        if (cacheVm.getId().equals(senderId) && 
            cacheVm instanceof CacheServerJmxImpl) {
          systemMemberJmx = (CacheServerJmxImpl) cacheVm;
          break;
        }
      }
      
      if (systemMemberJmx == null) {
        SystemMember[] appVms = getSystemMemberApplications();
        
        for (SystemMember appVm : appVms) {
          if (appVm.getId().equals(senderId) && 
              appVm instanceof SystemMemberJmxImpl) {
            systemMemberJmx = (SystemMemberJmxImpl) appVm;
            break;
          }
        }
        
      }

      if (systemMemberJmx != null) {
        systemMemberJmx.handleClientMembership(clientId, eventType);
      }
    } catch (AdminException e) {
      logger.error(LocalizedMessage.create(
          LocalizedStrings.AdminDistributedSystemJmxImpl_FAILED_TO_PROCESS_CLIENT_MEMBERSHIP_FROM_0_FOR_1,
              new Object[] { senderId, clientId }), e);
      return;
    } catch (RuntimeOperationsException e) {
      logger.warn(e.getMessage(), e);//failed to send notification
    }
  }

  public String getAlertLevelAsString() {
    return super.getAlertLevelAsString();
  }
  
  public void setAlertLevelAsString(String level) {
    String newLevel = level != null ? level.trim() : null;
    try {
      super.setAlertLevelAsString(newLevel);
    } catch (IllegalArgumentException e) {
      throw new RuntimeMBeanException(e, e.getMessage());
    }
  }

  /**
   * Finds the member as per details in the given event object and passes on
   * this event for handling the cache creation.
   * 
   * @param event
   *          event object corresponding to the creation of the cache
   */
  public void handleCacheCreateEvent(SystemMemberCacheEvent event) {
    String memberId = event.getMemberId();
    SystemMemberJmx systemMemberJmx = (SystemMemberJmx) findCacheOrAppVmById(memberId);
    
    if (systemMemberJmx != null) {
      systemMemberJmx.handleCacheCreate(event);
    }
    
  }

  /**
   * Finds the member as per details in the given event object and passes on
   * this event for handling the cache closure.
   * 
   * @param event
   *          event object corresponding to the closure of the cache
   */
  public void handleCacheCloseEvent(SystemMemberCacheEvent event) {
    String memberId = event.getMemberId();
    SystemMemberJmx systemMemberJmx = (SystemMemberJmx) findCacheOrAppVmById(memberId);
    
    if (systemMemberJmx != null) {
      systemMemberJmx.handleCacheClose(event);
    }
    
  }

  /**
   * Finds the member as per details in the given event object and passes on
   * this event for handling the region creation.
   * 
   * @param event
   *          event object corresponding to the creation of a region
   */
  public void handleRegionCreateEvent(SystemMemberRegionEvent event) {
    String memberId = event.getMemberId();
    SystemMemberJmx systemMemberJmx = (SystemMemberJmx) findCacheOrAppVmById(memberId);
    
    if (systemMemberJmx != null) {
      systemMemberJmx.handleRegionCreate(event);
    }
  }  
  
  /**
   * Finds the member as per details in the given event object and passes on 
   * this event for handling the region loss.
   * 
   * @param event
   *          event object corresponding to the loss of a region
   */
  public void handleRegionLossEvent(SystemMemberRegionEvent event) {
    String memberId = event.getMemberId();
    SystemMemberJmx systemMemberJmx = (SystemMemberJmx) findCacheOrAppVmById(memberId);
    
    if (systemMemberJmx != null) {
      systemMemberJmx.handleRegionLoss(event);
    }
  }

  @Override
  public void setDisableAutoReconnect(boolean newValue) {
    getConfig().setDisableAutoReconnect(newValue);
  }

  @Override
  public boolean getDisableAutoReconnect() {
    return getConfig().getDisableAutoReconnect();
  }  
  
}

class ProcessSystemwideStatAlertsNotification extends TimerTask {
  private StatAlertsAggregator aggregator;
  
  ProcessSystemwideStatAlertsNotification(StatAlertsAggregator aggregator) {
    this.aggregator = aggregator;
  }
  
  @Override
  public void run() {
    aggregator.processSystemwideNotifications();
  }
  
}

/**
 * Implementation of SystemMemberCacheListener used for listening to events:
 * <ol> <li> Cache Created </li>
 * <li> Cache Closed </li>
 * <li> Region Created </li>
 * <li> Region Loss </li>
 * </ol>
 * 
 */
class CacheAndRegionListenerImpl implements SystemMemberCacheListener {
  private AdminDistributedSystemJmxImpl adminDS;

  /**
   * Csontructor to create CacheAndRegionListenerImpl
   * 
   * @param adminDSResource
   *          instance of AdminDistributedSystemJmxImpl
   */
  CacheAndRegionListenerImpl(AdminDistributedSystemJmxImpl adminDSResource) {
    this.adminDS = adminDSResource;
  }
  
  /**
   * See SystemMemberCacheListener#afterCacheClose(SystemMemberCacheEvent)
   */
  public void afterCacheClose(SystemMemberCacheEvent event) {
    adminDS.handleCacheCloseEvent(event);
  }
  
  /**
   * See SystemMemberCacheListener#afterCacheCreate(SystemMemberCacheEvent)
   */
  public void afterCacheCreate(SystemMemberCacheEvent event) {
    adminDS.handleCacheCreateEvent(event);
  }
  
  /**
   * See SystemMemberCacheListener#afterRegionCreate(SystemMemberCacheEvent)
   */
  public void afterRegionCreate(SystemMemberRegionEvent event) {
    adminDS.handleRegionCreateEvent(event);    
  }
  
  /**
   * See SystemMemberCacheListener#afterRegionLoss(SystemMemberCacheEvent)
   */
  public void afterRegionLoss(SystemMemberRegionEvent event) {
    adminDS.handleRegionLossEvent(event);
  }  
}

