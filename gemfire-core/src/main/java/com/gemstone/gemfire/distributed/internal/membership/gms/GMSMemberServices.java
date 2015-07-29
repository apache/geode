package com.gemstone.gemfire.distributed.internal.membership.gms;

import java.util.Timer;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.distributed.internal.DMStats;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.distributed.internal.membership.DistributedMembershipListener;
import com.gemstone.gemfire.distributed.internal.membership.MembershipManager;
import com.gemstone.gemfire.distributed.internal.membership.NetView;
import com.gemstone.gemfire.distributed.internal.membership.gms.membership.GMSJoinLeave;
import com.gemstone.gemfire.distributed.internal.membership.gms.messenger.JGroupsMessenger;
import com.gemstone.gemfire.distributed.internal.membership.gms.mgr.GMSMembershipManager;
import com.gemstone.gemfire.distributed.internal.membership.gms.auth.GMSAuthenticator;
import com.gemstone.gemfire.distributed.internal.membership.gms.fd.GMSHealthMonitor;
import com.gemstone.gemfire.distributed.internal.membership.gms.interfaces.Authenticator;
import com.gemstone.gemfire.distributed.internal.membership.gms.interfaces.HealthMonitor;
import com.gemstone.gemfire.distributed.internal.membership.gms.interfaces.JoinLeave;
import com.gemstone.gemfire.distributed.internal.membership.gms.interfaces.Locator;
import com.gemstone.gemfire.distributed.internal.membership.gms.interfaces.Manager;
import com.gemstone.gemfire.distributed.internal.membership.gms.interfaces.Messenger;
import com.gemstone.gemfire.internal.admin.remote.RemoteTransportConfig;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.LoggingThreadGroup;
import com.gemstone.gemfire.security.AuthenticationFailedException;

public class GMSMemberServices {

  private static final Logger logger = LogService.getLogger();

  private static final ThreadGroup threadGroup = LoggingThreadGroup.createThreadGroup("Membership", logger); 
  
  private static InternalLogWriter staticSecurityLogWriter;

  final private Manager manager;
  final private JoinLeave joinLeave;
  private Locator locator;
  final private HealthMonitor healthMon;
  final private Messenger messenger;
  final private Authenticator auth;
  final private ServiceConfig config;
  final private DMStats stats;
  final private Stopper cancelCriterion;

  private InternalLogWriter securityLogWriter;
  
  private Timer timer = new Timer("Membership Timer", true);
  
  

  /**
   * A common logger for membership classes
   */
  public static Logger getLogger() {
    return logger;
  }

  /**
   * The thread group for all membership threads
   */
  public static ThreadGroup getThreadGroup() {
    return threadGroup;
  }
  
  /**
   * a timer used for membership tasks
   */
  public Timer getTimer() {
    return this.timer;
  }
  


  public GMSMemberServices(
      DistributedMembershipListener listener, DistributionConfig config,
      RemoteTransportConfig transport, DMStats stats) {
    this.cancelCriterion = new Stopper();
    this.stats = stats;
    this.config = new ServiceConfig(transport, config);
    this.manager = new GMSMembershipManager(listener);
    this.joinLeave = new GMSJoinLeave();
    this.healthMon = new GMSHealthMonitor();
    this.messenger = new JGroupsMessenger();
    this.auth = new GMSAuthenticator();
  }
  
  protected void init() {
    // InternalDistributedSystem establishes this log writer at boot time
    // TODO fix this so that IDS doesn't know about Services
    securityLogWriter = staticSecurityLogWriter;
    staticSecurityLogWriter = null;
    this.auth.init(this);
    this.messenger.init(this);
    this.manager.init(this);
    this.joinLeave.init(this);
    this.healthMon.init(this);
    InternalLocator l = (InternalLocator)com.gemstone.gemfire.distributed.Locator.getLocator();
    if (l != null) {
      l.getLocatorHandler().setMembershipManager((MembershipManager)this.manager);
      this.locator = (Locator)l.getLocatorHandler();
    }
  }
  
  protected void start() {
    boolean started = false;
    try {
      logger.info("Membership: starting Authenticator");
      this.auth.start();
      logger.info("Membership: starting Messenger");
      this.messenger.start();
      logger.info("Membership: starting JoinLeave");
      this.joinLeave.start();
      logger.info("Membership: starting HealthMonitor");
      this.healthMon.start();
      logger.info("Membership: starting Manager");
      this.manager.start();
      started = true;
    } catch (RuntimeException e) {
      logger.fatal("Unexpected exception while booting membership services", e);
      throw e;
    } finally {
      if (!started) {
        this.manager.stop();
        this.healthMon.stop();
        this.joinLeave.stop();
        this.messenger.stop();
        this.auth.stop();
      }
    }
    this.auth.started();
    this.messenger.started();
    this.joinLeave.started();
    this.healthMon.started();
    this.manager.started();
    
    this.manager.joinDistributedSystem();
  }
  
  public void emergencyClose() {
  }
  
  public void stop() {
    logger.info("Membership: stopping services");
    this.joinLeave.stop();
    this.healthMon.stop();
    this.auth.stop();
    this.messenger.stop();
    this.manager.stop();
    this.timer.cancel();
  }
  
  public static void setSecurityLogWriter(InternalLogWriter writer) {
    staticSecurityLogWriter = writer;
  }
  
  public LogWriter getSecurityLogWriter() {
    return this.securityLogWriter;
  }
  
  public Authenticator getAuthenticator() {
    return auth;
  }

  public void installView(NetView v) {
    try {
      auth.installView(v);
    } catch (AuthenticationFailedException e) {
      return;
    }
    if (locator != null) {
      locator.installView(v);
    }
    healthMon.installView(v);
    messenger.installView(v);
    manager.installView(v);
  }

  public Manager getManager() {
    return manager;
  }

  public Locator getLocator() {
    return locator;
  }

  public void setLocator(Locator locator) {
    this.locator = locator;
  }

  public JoinLeave getJoinLeave() {
    return joinLeave;
  }

  public HealthMonitor getHealthMonitor() {
    return healthMon;
  }

  public ServiceConfig getConfig() {
    return this.config;
  }
  
  public Messenger getMessenger() {
    return this.messenger;
  }
  
  public DMStats getStatistics() {
    return this.stats;
  }
  
  public Stopper getCancelCriterion() {
    return this.cancelCriterion;
  }
  
  
  
  
  public static class Stopper extends CancelCriterion {
    volatile String reasonForStopping = null;
    
    public void cancel(String reason) {
      this.reasonForStopping = reason;
    }

    @Override
    public String cancelInProgress() {
      return reasonForStopping;
    }
    
    public boolean isCancelInProgress() {
      return cancelInProgress() != null;
    }

    @Override
    public RuntimeException generateCancelledException(Throwable e) {
      String reason = cancelInProgress();
      if (reason == null) {
        return null;
      }
      else {
        return new ServicesStoppedException(reasonForStopping, e);
      }
    }
    
  }
  
  public static class ServicesStoppedException extends CancelException {
    private static final long serialVersionUID = 2134474966059876801L;

    public ServicesStoppedException(String message, Throwable cause) {
      super(message, cause);
    }
  }

}
