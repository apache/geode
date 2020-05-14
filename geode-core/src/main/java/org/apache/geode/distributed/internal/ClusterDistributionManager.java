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

package org.apache.geode.distributed.internal;


import java.io.NotSerializableException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelCriterion;
import org.apache.geode.CancelException;
import org.apache.geode.ForcedDisconnectException;
import org.apache.geode.IncompatibleSystemException;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.InternalGemFireException;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.SystemConnectException;
import org.apache.geode.SystemFailure;
import org.apache.geode.ToDataException;
import org.apache.geode.admin.GemFireHealthConfig;
import org.apache.geode.alerting.internal.api.AlertingService;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.Role;
import org.apache.geode.distributed.internal.locks.ElderState;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.api.MemberData;
import org.apache.geode.distributed.internal.membership.api.MemberDisconnectedException;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifierFactory;
import org.apache.geode.distributed.internal.membership.api.Membership;
import org.apache.geode.distributed.internal.membership.api.MembershipLocator;
import org.apache.geode.distributed.internal.membership.api.MembershipView;
import org.apache.geode.distributed.internal.membership.api.Message;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.admin.remote.AdminConsoleDisconnectMessage;
import org.apache.geode.internal.admin.remote.RemoteGfManagerAgent;
import org.apache.geode.internal.admin.remote.RemoteTransportConfig;
import org.apache.geode.internal.cache.InitialImageOperation;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;
import org.apache.geode.internal.sequencelog.MembershipLogger;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.internal.tcp.ConnectionTable;
import org.apache.geode.internal.tcp.ReenteredConnectException;
import org.apache.geode.logging.internal.OSProcess;
import org.apache.geode.logging.internal.executors.LoggingThread;
import org.apache.geode.logging.internal.executors.LoggingUncaughtExceptionHandler;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * The <code>DistributionManager</code> uses a {@link Membership} to distribute
 * {@link DistributionMessage messages}. It also reports on who is currently in the distributed
 * system and tracks the elder member for the distributed lock service. You may also register a
 * membership listener with the DistributionManager to receive notification of changes in
 * membership.
 *
 * <P>
 *
 * Code that wishes to send a {@link DistributionMessage} must get the
 * <code>DistributionManager</code> and invoke {@link #putOutgoing}.
 *
 * <P>
 *
 * @see DistributionMessage#process
 * @see IgnoredByManager
 */
public class ClusterDistributionManager implements DistributionManager {

  private static final Logger logger = LogService.getLogger();

  private static final boolean DEBUG_NO_ACKNOWLEDGEMENTS =
      Boolean.getBoolean("DistributionManager.DEBUG_NO_ACKNOWLEDGEMENTS");

  /**
   * Maximum number of interrupt attempts to stop a thread
   */
  private static final int MAX_STOP_ATTEMPTS = 10;



  private static final boolean SYNC_EVENTS = Boolean.getBoolean("DistributionManager.syncEvents");


  /** The DM type for regular distribution managers */
  public static final int NORMAL_DM_TYPE = MemberIdentifier.NORMAL_DM_TYPE;

  /** The DM type for locator distribution managers */
  public static final int LOCATOR_DM_TYPE = MemberIdentifier.LOCATOR_DM_TYPE;

  /** The DM type for Console (admin-only) distribution managers */
  public static final int ADMIN_ONLY_DM_TYPE = MemberIdentifier.ADMIN_ONLY_DM_TYPE;

  /** The DM type for stand-alone members */
  public static final int LONER_DM_TYPE = MemberIdentifier.LONER_DM_TYPE;



  /** Is this node running an AdminDistributedSystem? */
  @MakeNotStatic
  private static volatile boolean isDedicatedAdminVM = false;

  @MakeNotStatic
  private static final ThreadLocal<Boolean> isStartupThread = new ThreadLocal<>();

  ///////////////////// Instance Fields //////////////////////

  private final Stopper stopper = new Stopper(this);


  /** The id of this distribution manager */
  private final InternalDistributedMember localAddress;

  /**
   * The distribution manager type of this dm; set in its constructor.
   */
  private final int dmType;

  /**
   * The <code>MembershipListener</code>s that are registered on this manager.
   */
  private final ConcurrentMap<MembershipListener, Boolean> membershipListeners;
  private final ClusterElderManager clusterElderManager = new ClusterElderManager(this);
  private Distribution distribution;
  private ClusterOperationExecutors executors;

  /**
   * Membership failure listeners - for testing
   */
  private List<MembershipTestHook> membershipTestHooks;


  /**
   * The <code>MembershipListener</code>s that are registered on this manager for ALL members.
   *
   * @since GemFire 5.7
   */
  private volatile Set<MembershipListener> allMembershipListeners = Collections.emptySet();

  /**
   * A lock to hold while adding and removing all membership listeners.
   *
   * @since GemFire 5.7
   */
  private final Object allMembershipListenersLock = new Object();

  /** A queue of MemberEvent instances */
  private final BlockingQueue<MemberEvent> membershipEventQueue = new LinkedBlockingQueue<>();

  /** Used to invoke registered membership listeners in the background. */
  private Thread memberEventThread;


  /** A brief description of this DistributionManager */
  protected final String description;

  /** Statistics about distribution */
  protected final DistributionStats stats;

  /** Did an exception occur in one of the DM threads? */
  private boolean exceptionInThreads;

  private volatile boolean shutdownMsgSent = false;

  /** Set to true when this manager is being shutdown */
  private volatile boolean closeInProgress = false;

  private volatile boolean receivedStartupResponse = false;

  private volatile String rejectionMessage = null;

  /**
   * Map of all locator members of the distributed system. The value is a collection of locator
   * strings that are hosted in that member. All accesses to this field must be synchronized on
   * {@link #membersLock}.
   */
  private Map<InternalDistributedMember, Collection<String>> hostedLocatorsAll =
      Collections.emptyMap();

  /**
   * Map of all locator members of the distributed system which have the shared configuration. The
   * value is a collection of locator strings that are hosted in that member. All accesses to this
   * field must be synchronized on {@link #membersLock}.
   */
  private Map<InternalDistributedMember, Collection<String>> hostedLocatorsWithSharedConfiguration =
      Collections.emptyMap();

  /** a map keyed on InternalDistributedMember, to direct channels to other systems */
  // protected final Map channelMap = CFactory.createCM();

  private volatile boolean readyForMessages = false;

  /**
   * Set to true once this DM is ready to send messages. Note that it is always ready to send the
   * startup message.
   */
  private volatile boolean readyToSendMsgs = false;
  private final Object readyToSendMsgsLock = new Object();

  /** Is this distribution manager closed? */
  private volatile boolean closed = false;

  /**
   * The distributed system to which this distribution manager is connected.
   */
  private InternalDistributedSystem system;

  /** The remote transport configuration for this dm */
  private RemoteTransportConfig transport;

  /**
   * The administration agent associated with this distribution manager.
   */
  private volatile RemoteGfManagerAgent agent;

  /**
   * TODO why does the distribution manager arbitrate GII operations? That should be a Cache
   * function
   */
  private final Semaphore parallelGIIs = new Semaphore(InitialImageOperation.MAX_PARALLEL_GIIS);

  /**
   * Map of InetAddress to HashSets of InetAddress, to define equivalences between network interface
   * cards and hosts.
   */
  private final HashMap<InetAddress, Set<InetAddress>> equivalentHosts = new HashMap<>();

  private int distributedSystemId;


  private final Map<InternalDistributedMember, String> redundancyZones =
      Collections.synchronizedMap(new HashMap<>());

  private boolean enforceUniqueZone = false;

  /**
   * root cause of forcibly shutting down the distribution manager
   */
  private volatile Throwable rootCause = null;

  /**
   * @see #closeInProgress
   */
  private final Object shutdownMutex = new Object();

  private final AlertingService alertingService;

  private Object membersLock = new Object();

  ////////////////////// Static Methods //////////////////////

  /**
   * Creates a new distribution manager and discovers the other members of the distributed system.
   * Note that it does not check to see whether or not this VM already has a distribution manager.
   *
   * @param system The distributed system to which this distribution manager will send messages.
   */
  static ClusterDistributionManager create(InternalDistributedSystem system,
      final MembershipLocator<InternalDistributedMember> membershipLocator) {

    ClusterDistributionManager distributionManager = null;
    boolean beforeJoined = true;

    try {

      int vmKind;

      if (Boolean.getBoolean(InternalLocator.FORCE_LOCATOR_DM_TYPE)) {
        // if this DM is starting for a locator, set it to be a locator DM
        vmKind = LOCATOR_DM_TYPE;

      } else if (isDedicatedAdminVM()) {
        vmKind = ADMIN_ONLY_DM_TYPE;

      } else {
        vmKind = NORMAL_DM_TYPE;
      }

      RemoteTransportConfig transport = new RemoteTransportConfig(system.getConfig(), vmKind);
      transport.setIsReconnectingDS(system.isReconnectingDS());
      transport.setOldDSMembershipInfo(system.oldDSMembershipInfo());

      long start = System.currentTimeMillis();

      distributionManager =
          new ClusterDistributionManager(system, transport, system.getAlertingService(),
              membershipLocator);
      distributionManager.assertDistributionManagerType();

      beforeJoined = false; // we have now joined the system

      {
        InternalDistributedMember id = distributionManager.getDistributionManagerId();
        if (!"".equals(id.getName())) {
          for (InternalDistributedMember m : distributionManager
              .getViewMembers()) {
            if (m.equals(id)) {
              // I'm counting on the members returned by getViewMembers being ordered such that
              // members that joined before us will precede us AND members that join after us
              // will succeed us.
              // SO once we find ourselves break out of this loop.
              break;
            }
            if (id.getName().equals(m.getName())) {
              if (distributionManager.getDistribution().verifyMember(m,
                  "member is using the name of " + id)) {
                throw new IncompatibleSystemException("Member " + id
                    + " could not join this distributed system because the existing member " + m
                    + " used the same name. Set the \"name\" gemfire property to a unique value.");
              }
            }
          }
        }
        distributionManager.addNewMember(id); // add ourselves
      }

      // Send out a StartupMessage to the other members.
      StartupOperation op = new StartupOperation(distributionManager, transport);

      try {
        if (!distributionManager.sendStartupMessage(op)) {
          // Well we didn't hear back from anyone else. We assume that
          // we're the first one.
          if (distributionManager.getOtherDistributionManagerIds().size() == 0) {
            logger.info("Did not hear back from any other system. I am the first one.");
          } else if (transport.isMcastEnabled()) {
            // perform a multicast ping test
            if (!distributionManager.testMulticast()) {
              logger.warn(
                  "Did not receive a startup response but other members exist.  Multicast does not seem to be working.");
            }
          }
        }
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        // This is ALWAYS bad; don't consult a CancelCriterion.
        throw new InternalGemFireException(
            "Interrupted while waiting for first StartupResponseMessage",
            ex);
      } catch (IncompatibleSystemException ex) {
        logger.fatal(ex.getMessage(), ex);
        throw ex;
      } finally {
        distributionManager.readyToSendMsgs();
      }

      if (logger.isInfoEnabled()) {
        long delta = System.currentTimeMillis() - start;
        logger.info(LogMarker.DM_MARKER,
            "DistributionManager {} started on {}. There were {} other DMs. others: {} {} {}",
            distributionManager.getDistributionManagerId(), transport,
            distributionManager.getOtherDistributionManagerIds().size(),
            distributionManager.getOtherDistributionManagerIds(),
            (logger.isInfoEnabled(LogMarker.DM_MARKER) ? " (took " + delta + " ms)" : ""),
            ((distributionManager.getDMType() == ADMIN_ONLY_DM_TYPE) ? " (admin only)"
                : (distributionManager.getDMType() == LOCATOR_DM_TYPE) ? " (locator)" : ""));

        MembershipLogger.logStartup(distributionManager.getDistributionManagerId());
      }
      return distributionManager;
    } catch (RuntimeException r) {
      if (distributionManager != null) {
        if (logger.isDebugEnabled()) {
          logger.debug("cleaning up incompletely started DistributionManager due to exception", r);
        }
        distributionManager.uncleanShutdown(beforeJoined);
      }
      throw r;
    }
  }

  public OperationExecutors getExecutors() {
    return executors;
  }

  @Override
  public ThreadsMonitoring getThreadMonitoring() {
    return executors.getThreadMonitoring();
  }

  /////////////////////// Constructors ///////////////////////

  /**
   * Creates a new <code>DistributionManager</code> by initializing itself, creating the membership
   * manager and executors
   *
   * @param transport The configuration for the communications transport
   *
   */
  private ClusterDistributionManager(RemoteTransportConfig transport,
      InternalDistributedSystem system, AlertingService alertingService,
      MembershipLocator<InternalDistributedMember> locator) {

    this.system = system;
    this.transport = transport;
    this.alertingService = alertingService;

    dmType = transport.getVmKind();
    membershipListeners = new ConcurrentHashMap<>();
    distributedSystemId = system.getConfig().getDistributedSystemId();

    long statId = OSProcess.getId();
    stats = new DistributionStats(system, statId);
    DistributionStats.enableClockStats = system.getConfig().getEnableTimeStatistics();

    exceptionInThreads = false;
    boolean finishedConstructor = false;
    try {

      executors = new ClusterOperationExecutors(stats, system);

      if (!SYNC_EVENTS) {
        memberEventThread =
            new LoggingThread("DM-MemberEventInvoker", new MemberEventInvoker());
      }

      StringBuilder sb = new StringBuilder(" (took ");

      // connect to the cluster
      long start = System.currentTimeMillis();

      DMListener listener = new DMListener(this);
      distribution = DistributionImpl
          .createDistribution(this, transport, system, listener,
              this::handleIncomingDMsg, locator);

      sb.append(System.currentTimeMillis() - start);

      localAddress = distribution.getLocalMember();

      sb.append(" ms)");

      logger.info("Starting DistributionManager {}. {}",
          new Object[] {localAddress,
              (logger.isInfoEnabled(LogMarker.DM_MARKER) ? sb.toString() : "")});

      description = "Distribution manager on " + localAddress + " started at "
          + (new Date(System.currentTimeMillis())).toString();

      finishedConstructor = true;
    } finally {
      if (!finishedConstructor && executors != null) {
        askThreadsToStop(); // fix for bug 42039
      }
    }
  }

  /**
   * Creates a new distribution manager
   *
   * @param system The distributed system to which this distribution manager will send messages.
   */
  private ClusterDistributionManager(InternalDistributedSystem system,
      RemoteTransportConfig transport,
      AlertingService alertingService,
      final MembershipLocator<InternalDistributedMember> membershipLocator) {
    this(transport, system, alertingService, membershipLocator);

    boolean finishedConstructor = false;
    try {

      setIsStartupThread();

      startThreads();

      // Allow events to start being processed.
      distribution.startEventProcessing();
      for (;;) {
        getCancelCriterion().checkCancelInProgress(null);
        boolean interrupted = Thread.interrupted();
        try {
          distribution.waitForEventProcessing();
          break;
        } catch (InterruptedException e) {
          interrupted = true;
        } finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
      }

      finishedConstructor = true;
    } finally {
      if (!finishedConstructor) {
        askThreadsToStop(); // fix for bug 42039
      }
    }
  }

  /**
   * Is this VM dedicated to administration (like a GUI console or a JMX agent)? If so, then it
   * creates {@link #ADMIN_ONLY_DM_TYPE} type distribution managers.
   *
   * @since GemFire 4.0
   */
  public static boolean isDedicatedAdminVM() {
    return isDedicatedAdminVM;
  }

  public static void setIsDedicatedAdminVM(boolean isDedicatedAdminVM) {
    ClusterDistributionManager.isDedicatedAdminVM = isDedicatedAdminVM;
  }

  private static Boolean isStartupThread() {
    return isStartupThread.get();
  }

  private static void setIsStartupThread() {
    ClusterDistributionManager.isStartupThread.set(Boolean.TRUE);
  }

  //////////////////// Instance Methods /////////////////////

  /**
   * Returns true if the two members are on the same equivalent host based on overlapping IP
   * addresses collected for all NICs during exchange of startup messages.
   *
   * @param member1 First member
   * @param member2 Second member
   */
  @Override
  public boolean areOnEquivalentHost(InternalDistributedMember member1,
      InternalDistributedMember member2) {
    Set<InetAddress> equivalents1 = getEquivalents(member1.getInetAddress());
    return equivalents1.contains(member2.getInetAddress());
  }

  /**
   * Set the host equivalencies for a given host. This overrides any previous information in the
   * tables.
   *
   * @param equivs list of InetAddress's that all point at same host
   */
  void setEquivalentHosts(Set<InetAddress> equivs) {
    Iterator<InetAddress> it = equivs.iterator();
    synchronized (equivalentHosts) {
      while (it.hasNext()) {
        equivalentHosts.put(it.next(), Collections.unmodifiableSet(equivs));
      }
    }
  }


  /**
   * Return all of the InetAddress's that are equivalent to the given one (same host)
   *
   * @param in host to match up
   * @return all the addresses thus equivalent
   */
  @Override
  public Set<InetAddress> getEquivalents(InetAddress in) {
    Set<InetAddress> result;
    synchronized (equivalentHosts) {
      result = equivalentHosts.get(in);
    }
    // DS 11/25/08 - It appears that when using VPN, the distributed member
    // id is the vpn address, but that doesn't show up in the equivalents.
    if (result == null) {
      result = Collections.singleton(in);
    }
    return result;
  }

  void setRedundancyZone(InternalDistributedMember member, String redundancyZone) {
    if (redundancyZone != null && !redundancyZone.equals("")) {
      redundancyZones.put(member, redundancyZone);
    }
    if (member != getDistributionManagerId()) {
      String relationship = areInSameZone(getDistributionManagerId(), member) ? "" : "not ";
      Object[] logArgs = new Object[] {member, relationship};
      logger.info("Member {} is {} equivalent or in the same redundancy zone.",
          logArgs);
    }
  }

  /**
   * Set the flag indicating that we should enforce unique zones. If we are already enforcing unique
   * zones, keep it that way.
   */
  void setEnforceUniqueZone(boolean enforceUniqueZone) {
    this.enforceUniqueZone |= enforceUniqueZone;
  }

  @Override
  public boolean enforceUniqueZone() {
    return enforceUniqueZone;
  }

  @Override
  public String getRedundancyZone(InternalDistributedMember member) {
    return redundancyZones.get(member);
  }

  /**
   * Asserts that distributionManagerType is LOCAL, GEMFIRE, or ADMIN_ONLY. Also asserts that the
   * distributionManagerId (jgroups DistributedMember) has a VmKind that matches.
   */
  private void assertDistributionManagerType() {
    // Assert that dmType is one of the three DM types...
    int theDmType = getDMType();
    switch (theDmType) {
      case NORMAL_DM_TYPE:
      case LONER_DM_TYPE:
      case ADMIN_ONLY_DM_TYPE:
      case LOCATOR_DM_TYPE:
        break;
      default:
        Assert.assertTrue(false, "unknown distribution manager type");
    }

    // Assert InternalDistributedMember VmKind matches this DistributionManagerType...
    final InternalDistributedMember theId = getDistributionManagerId();
    final int vmKind = theId.getVmKind();
    if (theDmType != vmKind) {
      Assert.assertTrue(false,
          "InternalDistributedMember has a vmKind of " + vmKind + " instead of " + theDmType);
    }
  }

  @Override
  public int getDMType() {
    return dmType;
  }

  @Override
  public List<InternalDistributedMember> getViewMembers() {
    return distribution.getView().getMembers();
  }

  private boolean testMulticast() {
    return distribution.testMulticast();
  }

  /**
   * Need to do this outside the constructor so that the child constructor can finish.
   */
  private void startThreads() {
    system.setDM(this); // fix for bug 33362
    if (memberEventThread != null)
      memberEventThread.start();
    try {

      // And the distinguished guests today are...
      MembershipView<InternalDistributedMember> v = distribution.getView();
      logger.info("Initial (distribution manager) view, {}",
          String.valueOf(v));

      // Add them all to our view
      for (InternalDistributedMember internalDistributedMember : v.getMembers()) {
        addNewMember(internalDistributedMember);
      }

    } catch (Exception ex) {
      throw new InternalGemFireException(
          "Could not process initial view",
          ex);
    }
    try {
      executors.getWaitingThreadPool().execute(() -> {
        // call in background since it might need to send a reply
        // and we are not ready to send messages until startup is finished
        setIsStartupThread();
        readyForMessages();
      });
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error. We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Throwable t) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above). However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      logger.fatal("Uncaught exception calling readyForMessages", t);
    }
  }

  private void readyForMessages() {
    synchronized (this) {
      readyForMessages = true;
      notifyAll();
    }
    distribution.startEventProcessing();
  }

  private void waitUntilReadyForMessages() {
    if (readyForMessages)
      return;
    synchronized (this) {
      while (!readyForMessages) {
        stopper.checkCancelInProgress(null);
        boolean interrupted = Thread.interrupted();
        try {
          wait();
        } catch (InterruptedException e) {
          interrupted = true;
          stopper.checkCancelInProgress(e);
        } finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
      }
    } // synchronized
  }

  /**
   * Call when the DM is ready to send messages.
   */
  private void readyToSendMsgs() {
    synchronized (readyToSendMsgsLock) {
      readyToSendMsgs = true;
      readyToSendMsgsLock.notifyAll();
    }
  }

  /**
   * Return when DM is ready to send out messages.
   *
   * @param msg the messsage that is currently being sent
   */
  private void waitUntilReadyToSendMsgs(DistributionMessage msg) {
    if (readyToSendMsgs) {
      return;
    }
    // another process may have been started in the same view, so we need
    // to be responsive to startup messages and be able to send responses
    if (msg instanceof AdminMessageType) {
      return;
    }
    if (isStartupThread() == Boolean.TRUE) {
      // let the startup thread send messages
      // the only case I know of that does this is if we happen to log a
      // message during startup and an alert listener has registered.
      return;
    }

    synchronized (readyToSendMsgsLock) {
      while (!readyToSendMsgs) {
        stopper.checkCancelInProgress(null);
        boolean interrupted = Thread.interrupted();
        try {
          readyToSendMsgsLock.wait();
        } catch (InterruptedException e) {
          interrupted = true;
          stopper.checkCancelInProgress(e);
        } finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
      } // for
    } // synchronized
  }


  @Override
  public void forceUDPMessagingForCurrentThread() {
    distribution.forceUDPMessagingForCurrentThread();
  }


  @Override
  public void releaseUDPMessagingForCurrentThread() {
    distribution.releaseUDPMessagingForCurrentThread();
  }

  /**
   * Did an exception occur in one of the threads launched by this distribution manager?
   */
  @Override
  public boolean exceptionInThreads() {
    return exceptionInThreads
        || LoggingUncaughtExceptionHandler.getUncaughtExceptionsCount() > 0;
  }

  /**
   * Clears the boolean that determines whether or not an exception occurred in one of the worker
   * threads. This method should be used for testing purposes only!
   */
  @Override
  public void clearExceptionInThreads() {
    exceptionInThreads = false;
    LoggingUncaughtExceptionHandler.clearUncaughtExceptionsCount();
  }

  /**
   * Returns the current "cache time" in milliseconds since the epoch. The "cache time" takes into
   * account skew among the local clocks on the various machines involved in the cache.
   */
  @Override
  public long cacheTimeMillis() {
    return system.getClock().cacheTimeMillis();
  }


  @Override
  public DistributedMember getMemberWithName(String name) {
    for (DistributedMember id : getViewMembers()) {
      if (Objects.equals(id.getName(), name)) {
        return id;
      }
    }
    if (Objects.equals(localAddress.getName(), name)) {
      return localAddress;
    }
    return null;
  }

  /**
   * Returns the id of this distribution manager.
   */
  @Override
  public InternalDistributedMember getDistributionManagerId() {
    return localAddress;
  }

  /**
   * Returns an unmodifiable set containing the identities of all of the known (non-admin-only)
   * distribution managers.
   */
  @Override
  public Set<InternalDistributedMember> getDistributionManagerIds() {
    return distribution.getMembersNotShuttingDown();
  }

  /**
   * Adds the entry in {@link #hostedLocatorsAll} for a member with one or more hosted locators. The
   * value is a collection of host[port] strings. If a bind-address was used for a locator then the
   * form is bind-addr[port].
   *
   * @since GemFire 6.6.3
   */
  @Override
  public void addHostedLocators(InternalDistributedMember member, Collection<String> locators,
      boolean isSharedConfigurationEnabled) {
    synchronized (membersLock) {
      if (locators == null || locators.isEmpty()) {
        throw new IllegalArgumentException("Cannot use empty collection of locators");
      }
      if (hostedLocatorsAll.isEmpty()) {
        hostedLocatorsAll = new HashMap<>();
      }
      Map<InternalDistributedMember, Collection<String>> tmp =
          new HashMap<>(hostedLocatorsAll);
      tmp.remove(member);
      tmp.put(member, locators);
      tmp = Collections.unmodifiableMap(tmp);
      hostedLocatorsAll = tmp;

      if (isSharedConfigurationEnabled) {
        if (hostedLocatorsWithSharedConfiguration.isEmpty()) {
          hostedLocatorsWithSharedConfiguration = new HashMap<>();
        }
        tmp = new HashMap<>(hostedLocatorsWithSharedConfiguration);
        tmp.remove(member);
        tmp.put(member, locators);
        tmp = Collections.unmodifiableMap(tmp);
        hostedLocatorsWithSharedConfiguration = tmp;
      }

    }
  }


  private void removeHostedLocators(InternalDistributedMember member) {
    synchronized (membersLock) {
      if (hostedLocatorsAll.containsKey(member)) {
        Map<InternalDistributedMember, Collection<String>> tmp =
            new HashMap<>(hostedLocatorsAll);
        tmp.remove(member);
        if (tmp.isEmpty()) {
          tmp = Collections.emptyMap();
        } else {
          tmp = Collections.unmodifiableMap(tmp);
        }
        hostedLocatorsAll = tmp;
      }
      if (hostedLocatorsWithSharedConfiguration.containsKey(member)) {
        Map<InternalDistributedMember, Collection<String>> tmp =
            new HashMap<>(
                hostedLocatorsWithSharedConfiguration);
        tmp.remove(member);
        if (tmp.isEmpty()) {
          tmp = Collections.emptyMap();
        } else {
          tmp = Collections.unmodifiableMap(tmp);
        }
        hostedLocatorsWithSharedConfiguration = tmp;
      }
    }
  }



  /**
   * Gets the value in {@link #hostedLocatorsAll} for a member with one or more hosted locators. The
   * value is a collection of host[port] strings. If a bind-address was used for a locator then the
   * form is bind-addr[port].
   *
   * @since GemFire 6.6.3
   */
  @Override
  public Collection<String> getHostedLocators(InternalDistributedMember member) {
    synchronized (membersLock) {
      return hostedLocatorsAll.get(member);
    }
  }

  /**
   * Returns a copy of the map of all members hosting locators. The key is the member, and the value
   * is a collection of host[port] strings. If a bind-address was used for a locator then the form
   * is bind-addr[port].
   *
   * The member is the vm that hosts one or more locator, if another locator starts up linking to
   * this locator, it will put that member in this map as well, and this member will in the map on
   * the other locato vm as well.
   *
   * The keyset of the map are the locator vms in this cluster.
   *
   * the value is a collection of strings in case one vm can have multiple locators ????
   *
   * @since GemFire 6.6.3
   */
  @Override
  public Map<InternalDistributedMember, Collection<String>> getAllHostedLocators() {
    synchronized (membersLock) {
      return hostedLocatorsAll;
    }
  }

  /**
   * Returns a copy of the map of all members hosting locators with shared configuration. The key is
   * the member, and the value is a collection of host[port] strings. If a bind-address was used for
   * a locator then the form is bind-addr[port].
   *
   * @since GemFire 8.0
   */
  @Override
  public Map<InternalDistributedMember, Collection<String>> getAllHostedLocatorsWithSharedConfiguration() {
    synchronized (membersLock) {
      return hostedLocatorsWithSharedConfiguration;
    }
  }

  /**
   * Returns an unmodifiable set containing the identities of all of the known (including admin)
   * distribution managers.
   */
  @Override
  public Set<InternalDistributedMember> getDistributionManagerIdsIncludingAdmin() {
    return new HashSet<>(getViewMembers());
  }


  /**
   * Returns a private-memory list containing the identities of all the other known distribution
   * managers not including me.
   */
  @Override
  public Set<InternalDistributedMember> getOtherDistributionManagerIds() {
    // We return a modified copy of the list, so
    // collect the old list and copy under the lock.
    Set<InternalDistributedMember> result = new HashSet<>(getDistributionManagerIds());

    InternalDistributedMember me = getDistributionManagerId();
    result.remove(me);

    // It's okay for my own id to not be in the list of all ids yet.
    return result;
  }

  @Override
  public Set<InternalDistributedMember> getOtherNormalDistributionManagerIds() {
    // We return a modified copy of the list, so
    // collect the old list and copy under the lock.
    Set<InternalDistributedMember> result = new HashSet<>(getNormalDistributionManagerIds());

    InternalDistributedMember me = getDistributionManagerId();
    result.remove(me);

    // It's okay for my own id to not be in the list of all ids yet.
    return result;
  }

  @Override
  public InternalDistributedMember getCanonicalId(DistributedMember id) {
    Distribution m = distribution;
    if (m == null) {
      return (InternalDistributedMember) id;
    }
    return m.getView().getCanonicalID(
        (InternalDistributedMember) id);
  }

  /**
   * Add a membership listener and return other DistributionManagerIds as an atomic operation
   */
  @Override
  public Set<InternalDistributedMember> addMembershipListenerAndGetDistributionManagerIds(
      MembershipListener l) {
    return distribution.doWithViewLocked(() -> {
      addMembershipListener(l);
      return distribution.getMembersNotShuttingDown();
    });
  }

  private void addNewMember(InternalDistributedMember member) {
    // This is the place to cleanup the zombieMembers
    int vmType = member.getVmKind();
    switch (vmType) {
      case ADMIN_ONLY_DM_TYPE:
        handleConsoleStartup(member);
        break;
      case LOCATOR_DM_TYPE:
      case NORMAL_DM_TYPE:
        handleManagerStartup(member);
        break;
      default:
        throw new InternalGemFireError(String.format("Unknown member type: %s",
            vmType));
    }
  }

  /**
   * Returns the identity of this <code>DistributionManager</code>
   */
  @Override
  public InternalDistributedMember getId() {
    return localAddress;
  }

  @Override
  public Set<InternalDistributedMember> putOutgoing(final DistributionMessage msg) {
    try {
      DistributionMessageObserver observer = DistributionMessageObserver.getInstance();
      if (observer != null) {
        observer.beforeSendMessage(this, msg);
      }
      return sendMessage(msg);
    } catch (NotSerializableException e) {
      throw new InternalGemFireException(e);
    }
  }

  @Override
  public String toString() {
    return description;
  }

  /**
   * Informs other members that this dm is shutting down. Stops the pusher, puller, and processor
   * threads and closes the connection to the transport layer.
   */
  protected void shutdown() {
    // Make sure only one thread initiates shutdown...
    synchronized (shutdownMutex) {
      if (closeInProgress) {
        return;
      }
      closeInProgress = true;
      this.distribution.setCloseInProgress();
    } // synchronized

    // [bruce] log shutdown at info level and with ID to balance the
    // "Starting" message. recycleConn.conf is hard to debug w/o this
    final String exceptionStatus = (exceptionInThreads()
        ? "At least one Exception occurred."
        : "");
    logger.info("Shutting down DistributionManager {}. {}",
        new Object[] {localAddress, exceptionStatus});

    final long start = System.currentTimeMillis();
    try {
      if (rootCause instanceof ForcedDisconnectException) {
        if (logger.isDebugEnabled()) {
          logger.debug(
              "inhibiting sending of shutdown message to other members due to forced-disconnect");
        }
      } else {
        // Don't block indefinitely trying to send the shutdown message, in
        // case other VMs in the system are ill-behaved. (bug 34710)
        final Runnable r = () -> {
          try {
            ConnectionTable.threadWantsSharedResources();
            sendShutdownMessage();
          } catch (final CancelException e) {
            // We were terminated.
            logger.debug("Cancelled during shutdown message", e);
          }
        };
        final Thread t =
            new LoggingThread(String.format("Shutdown Message Thread for %s",
                localAddress), false, r);
        t.start();
        boolean interrupted = Thread.interrupted();
        try {
          t.join(ClusterOperationExecutors.MAX_STOP_TIME / 4);
        } catch (final InterruptedException e) {
          interrupted = true;
          t.interrupt();
          logger.warn("Interrupted sending shutdown message to peers",
              e);
        } finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }

        if (t.isAlive()) {
          t.interrupt();
          logger.warn("Failed sending shutdown message to peers (timeout)");
        }
      }

    } finally {
      shutdownMsgSent = true; // in case sendShutdownMessage failed....
      try {
        uncleanShutdown(false);
      } finally {
        final Long delta = System.currentTimeMillis() - start;
        logger.info("DistributionManager stopped in {}ms.", delta);
      }
    }
  }

  private void askThreadsToStop() {
    executors.askThreadsToStop();

    Thread th = memberEventThread;
    if (th != null)
      th.interrupt();
  }

  private void waitForThreadsToStop(long timeInMillis) throws InterruptedException {
    long start = System.currentTimeMillis();
    executors.waitForThreadsToStop(timeInMillis);
    long remaining = timeInMillis - (System.currentTimeMillis() - start);
    if (remaining <= 0) {
      return;
    }
    Thread th = memberEventThread;
    if (th != null) {
      th.interrupt();
      th.join(remaining);
    }

  }

  /**
   * Cheap tool to kill a referenced thread
   *
   * @param t the thread to kill
   */
  private void clobberThread(Thread t) {
    if (t == null)
      return;
    if (t.isAlive()) {
      logger.warn("Forcing thread stop on < {} >", t);

      // Start by being nice.
      t.interrupt();

      // we could be more violent here...
      // t.stop();
      try {
        for (int i = 0; i < MAX_STOP_ATTEMPTS && t.isAlive(); i++) {
          t.join(ClusterOperationExecutors.STOP_PAUSE_TIME);
          t.interrupt();
        }
      } catch (InterruptedException ex) {
        logger.debug("Interrupted while attempting to terminate threads.");
        Thread.currentThread().interrupt();
        // just keep going
      }

      if (t.isAlive()) {
        logger.warn("Thread refused to die: {}", t);
      }
    }
  }

  /**
   * Wait for the ancillary queues to exit. Kills them if they are still around.
   *
   */
  private void forceThreadsToStop() {
    executors.forceThreadsToStop();
    Thread th = memberEventThread;
    if (th != null) {
      clobberThread(th);
    }
  }

  private volatile boolean shutdownInProgress = false;

  /** guard for membershipViewIdAcknowledged */
  private final Object membershipViewIdGuard = new Object();

  /** the latest view ID that has been processed by all membership listeners */
  private long membershipViewIdAcknowledged;

  @Override
  public boolean shutdownInProgress() {
    return shutdownInProgress;
  }

  /**
   * Stops the pusher, puller and processor threads and closes the connection to the transport
   * layer. This should only be used from shutdown() or from the dm initialization code
   */
  private void uncleanShutdown(boolean beforeJoined) {
    try {
      closeInProgress = true; // set here also to fix bug 36736
      removeAllHealthMonitors();
      shutdownInProgress = true;
      if (distribution != null) {
        distribution.setShutdown();
      }

      askThreadsToStop();

      // wait a moment before asking threads to terminate
      try {
        waitForThreadsToStop(1000);
      } catch (InterruptedException ie) {
        // No need to reset interrupt bit, we're really trying to quit...
      }
      forceThreadsToStop();

    } // try
    finally {
      // ABSOLUTELY ESSENTIAL that we close the distribution channel!
      try {
        // For safety, but channel close in a finally AFTER this...
        if (stats != null) {
          stats.close();
          try {
            Thread.sleep(100);
          } catch (InterruptedException ie) {
            // No need to reset interrupt bit, we're really trying to quit...
          }
        }
      } finally {
        if (distribution != null) {
          logger.info("Now closing distribution for {}",
              localAddress);
          distribution.disconnect(beforeJoined);
        }
      }
    }
  }

  @Override
  public InternalDistributedSystem getSystem() {
    return system;
  }

  @Override
  public AlertingService getAlertingService() {
    return alertingService;
  }

  /**
   * Returns the transport configuration for this distribution manager
   */
  RemoteTransportConfig getTransport() {
    return transport;
  }


  @Override
  public void addMembershipListener(MembershipListener l) {
    membershipListeners.putIfAbsent(l, Boolean.TRUE);
  }

  @Override
  public void removeMembershipListener(MembershipListener l) {
    membershipListeners.remove(l);
  }

  @Override
  public Collection<MembershipListener> getMembershipListeners() {
    return Collections.unmodifiableSet(membershipListeners.keySet());
  }

  /**
   * Adds a <code>MembershipListener</code> to this distribution manager.
   */
  private void addAllMembershipListener(MembershipListener l) {
    synchronized (allMembershipListenersLock) {
      Set<MembershipListener> newAllMembershipListeners =
          new HashSet<>(allMembershipListeners);
      newAllMembershipListeners.add(l);
      allMembershipListeners = newAllMembershipListeners;
    }
  }

  @Override
  public void removeAllMembershipListener(MembershipListener l) {
    synchronized (allMembershipListenersLock) {
      Set<MembershipListener> newAllMembershipListeners =
          new HashSet<>(allMembershipListeners);
      if (!newAllMembershipListeners.remove(l)) {
        // There seems to be a race condition in which
        // multiple departure events can be registered
        // on the same peer. We regard this as benign.
        // FIXME when membership events become sane again
        // String s = "MembershipListener was never registered";
        // throw new IllegalArgumentException(s);
      }
      allMembershipListeners = newAllMembershipListeners;
    }
  }

  private boolean shouldInhibitMembershipWarnings() {
    if (isCloseInProgress()) {
      return true;
    }
    InternalDistributedSystem ds = getSystem();
    return ds != null && ds.isDisconnecting();
  }

  /**
   * Returns true if this distribution manager has initiated shutdown
   */
  public boolean isCloseInProgress() {
    return closeInProgress;
  }

  private void handleViewInstalledEvent(ViewInstalledEvent ev) {
    synchronized (membershipViewIdGuard) {
      membershipViewIdAcknowledged = ev.getViewId();
      membershipViewIdGuard.notifyAll();
    }
  }

  /**
   * This stalls waiting for the current membership view (as seen by the membership manager) to be
   * acknowledged by all membership listeners
   */
  void waitForViewInstallation(long id) throws InterruptedException {
    if (id <= membershipViewIdAcknowledged) {
      return;
    }
    synchronized (membershipViewIdGuard) {
      while (membershipViewIdAcknowledged < id && !stopper.isCancelInProgress()) {
        if (logger.isDebugEnabled()) {
          logger.debug("waiting for view {}.  Current DM view processed by all listeners is {}", id,
              membershipViewIdAcknowledged);
        }
        membershipViewIdGuard.wait();
      }
    }
  }

  private void handleMemberEvent(MemberEvent ev) {
    ev.handleEvent(this);
  }

  /**
   * This thread processes member events as they occur.
   *
   * @see ClusterDistributionManager.MemberCrashedEvent
   * @see ClusterDistributionManager.MemberJoinedEvent
   * @see ClusterDistributionManager.MemberDepartedEvent
   *
   */
  protected class MemberEventInvoker implements Runnable {


    @Override
    @SuppressWarnings("synthetic-access")
    public void run() {
      for (;;) {
        SystemFailure.checkFailure();
        // bug 41539 - member events need to be delivered during shutdown
        // or reply processors may hang waiting for replies from
        // departed members
        // if (getCancelCriterion().isCancelInProgress()) {
        // break; // no message, just quit
        // }
        if (!system.isConnected
            && isClosed()) {
          break;
        }
        try {
          MemberEvent ev =
              membershipEventQueue.take();
          handleMemberEvent(ev);
        } catch (InterruptedException e) {
          if (shouldInhibitMembershipWarnings()) {
            if (logger.isTraceEnabled()) {
              logger.trace("MemberEventInvoker: InterruptedException during shutdown");
            }
          } else {
            logger.warn("Unexpected InterruptedException", e);
          }
          break;
        } catch (DistributedSystemDisconnectedException e) {
          break;
        } catch (CancelException e) {
          if (shouldInhibitMembershipWarnings()) {
            if (logger.isTraceEnabled()) {
              logger.trace("MemberEventInvoker: cancelled");
            }
          } else {
            logger.warn("Unexpected cancellation", e);
          }
          break;
        } catch (Exception e) {
          logger.fatal(
              "Uncaught exception processing member event",
              e);
        }
      } // for
      if (logger.isTraceEnabled()) {
        logger.trace("MemberEventInvoker on {} stopped", ClusterDistributionManager.this);
      }
    }
  }

  private void addMemberEvent(MemberEvent ev) {
    if (SYNC_EVENTS) {
      handleMemberEvent(ev);
    } else {
      stopper.checkCancelInProgress(null);
      boolean interrupted = Thread.interrupted();
      try {
        membershipEventQueue.put(ev);
      } catch (InterruptedException ex) {
        interrupted = true;
        stopper.checkCancelInProgress(ex);
        handleMemberEvent(ev); // FIXME why???
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }


  @Override
  public void close() {
    if (!closed) {
      shutdown();
      logger.info("Marking DistributionManager {} as closed.",
          localAddress);
      MembershipLogger.logShutdown(localAddress);
      closed = true;
    }
  }

  @Override
  public void throwIfDistributionStopped() {
    if (shutdownMsgSent) {
      throw new DistributedSystemDisconnectedException(
          "Message distribution has terminated",
          getRootCause());
    }
  }

  /**
   * Returns true if this distribution manager has been closed.
   */
  public boolean isClosed() {
    return closed;
  }


  @Override
  public void addAdminConsole(InternalDistributedMember theId) {
    // no-op: new members are added to the membership manager's view
  }

  @Override
  public DMStats getStats() {
    return stats;
  }

  @Override
  public DistributionConfig getConfig() {
    DistributionConfig result = null;
    InternalDistributedSystem sys = getSystem();
    if (sys != null) {
      result = system.getConfig();
    }
    return result;
  }

  @Override
  public Set<InternalDistributedMember> getAllOtherMembers() {
    Set<InternalDistributedMember> result =
        getDistributionManagerIdsIncludingAdmin();
    result.remove(getDistributionManagerId());
    return result;
  }

  @Override
  public void retainMembersWithSameOrNewerVersion(Collection<InternalDistributedMember> members,
      Version version) {
    members.removeIf(id -> id.getVersionObject().compareTo(version) < 0);
  }

  @Override
  public void removeMembersWithSameOrNewerVersion(Collection<InternalDistributedMember> members,
      Version version) {
    members.removeIf(id -> id.getVersionObject().compareTo(version) >= 0);
  }

  @Override
  public Set<InternalDistributedMember> addAllMembershipListenerAndGetAllIds(
      MembershipListener l) {
    return distribution.doWithViewLocked(() -> {
      // Don't let the members come and go while we are adding this
      // listener. This ensures that the listener (probably a
      // ReplyProcessor) gets a consistent view of the members.
      addAllMembershipListener(l);
      return distribution.getMembersNotShuttingDown();
    });
  }

  /**
   * Sends a startup message and waits for a response. Returns true if response received; false if
   * it timed out or there are no peers.
   */
  private boolean sendStartupMessage(StartupOperation startupOperation)
      throws InterruptedException {
    if (Thread.interrupted())
      throw new InterruptedException();
    receivedStartupResponse = false;
    boolean ok;

    // Be sure to add ourself to the equivalencies list!
    Set<InetAddress> equivs = StartupMessage.getMyAddresses(this);
    if (equivs == null || equivs.size() == 0) {
      // no network interface
      equivs = new HashSet<>();
      try {
        equivs.add(LocalHostUtil.getLocalHost());
      } catch (UnknownHostException e) {
        // can't even get localhost
        if (getViewMembers().size() > 1) {
          throw new SystemConnectException(
              "Unable to examine network cards and other members exist");
        }
      }
    }
    setEquivalentHosts(equivs);
    setEnforceUniqueZone(getConfig().getEnforceUniqueHost());
    String redundancyZone = getConfig().getRedundancyZone();
    if (redundancyZone != null && !redundancyZone.equals("")) {
      setEnforceUniqueZone(true);
    }
    setRedundancyZone(getDistributionManagerId(), redundancyZone);
    if (logger.isDebugEnabled()) {
      StringBuffer sb = new StringBuffer();
      sb.append("Equivalent IPs for this host: ");
      Iterator it = equivs.iterator();
      while (it.hasNext()) {
        InetAddress in = (InetAddress) it.next();
        sb.append(in.toString());
        if (it.hasNext()) {
          sb.append(", ");
        }
      } // while
      logger.debug(sb);
    }

    // we need to send this to everyone else; even admin vm
    Set<InternalDistributedMember> allOthers = new HashSet<>(getViewMembers());
    allOthers.remove(getDistributionManagerId());

    if (allOthers.isEmpty()) {
      return false; // no peers, we are alone.
    }

    try {
      ok = startupOperation.sendStartupMessage(allOthers, equivs, redundancyZone,
          enforceUniqueZone());
    } catch (Exception re) {
      throw new SystemConnectException(
          "One or more peers generated exceptions during connection attempt",
          re);
    }
    if (rejectionMessage != null) {
      throw new IncompatibleSystemException(rejectionMessage);
    }

    boolean receivedAny = receivedStartupResponse;

    if (!ok) { // someone didn't reply
      int unresponsiveCount;

      synchronized (unfinishedStartupsLock) {
        if (unfinishedStartups == null)
          unresponsiveCount = 0;
        else
          unresponsiveCount = unfinishedStartups.size();

        if (unresponsiveCount != 0) {
          if (Boolean.getBoolean("DistributionManager.requireAllStartupResponses")) {
            throw new SystemConnectException(
                String.format("No startup replies from: %s",
                    unfinishedStartups));
          }
        }
      } // synchronized


      // Bug 35887:
      // If there are other members, we must receive at least _one_ response
      if (allOthers.size() != 0) { // there exist others
        if (!receivedAny) { // and none responded
          StringBuilder sb = new StringBuilder();
          Iterator itt = allOthers.iterator();
          while (itt.hasNext()) {
            Object m = itt.next();
            sb.append(m.toString());
            if (itt.hasNext())
              sb.append(", ");
          }
          if (DEBUG_NO_ACKNOWLEDGEMENTS) {
            printStacks(allOthers, false);
          }
          throw new SystemConnectException(
              String.format(
                  "Received no connection acknowledgments from any of the %s senior cache members: %s",
                  Integer.toString(allOthers.size()), sb.toString()));
        } // and none responded
      } // there exist others

      InternalDistributedMember e = clusterElderManager.getElderId();
      if (e != null) { // an elder exists
        boolean unresponsiveElder;
        synchronized (unfinishedStartupsLock) {
          if (unfinishedStartups == null)
            unresponsiveElder = false;
          else
            unresponsiveElder = unfinishedStartups.contains(e);
        }
        if (unresponsiveElder) {
          logger.warn(
              "Forcing an elder join event since a startup response was not received from elder {}.",
              e);
          handleManagerStartup(e);
        }
      } // an elder exists
    } // someone didn't reply
    return receivedAny;
  }

  /**
   * List of InternalDistributedMember's that we have not received startup replies from. If null, we
   * have not finished sending the startup message.
   * <p>
   * Must be synchronized using {@link #unfinishedStartupsLock}
   */
  private Set<InternalDistributedMember> unfinishedStartups = null;

  /**
   * Synchronization for {@link #unfinishedStartups}
   */
  private final Object unfinishedStartupsLock = new Object();

  @Override
  public void setUnfinishedStartups(Collection<InternalDistributedMember> s) {
    synchronized (unfinishedStartupsLock) {
      Assert.assertTrue(unfinishedStartups == null, "Set unfinished startups twice");
      unfinishedStartups = new HashSet<>(s);

      // OK, I don't _quite_ trust the list to be current, so let's
      // prune it here.
      Iterator it = unfinishedStartups.iterator();
      synchronized (membersLock) {
        while (it.hasNext()) {
          InternalDistributedMember m = (InternalDistributedMember) it.next();
          if (!isCurrentMember(m)) {
            it.remove();
          }
        } // while
      } // synchronized
    }
  }

  @Override
  public void removeUnfinishedStartup(InternalDistributedMember m, boolean departed) {
    synchronized (unfinishedStartupsLock) {
      if (logger.isDebugEnabled()) {
        logger.debug("removeUnfinishedStartup for {} wtih {}", m, unfinishedStartups);
      }
      if (unfinishedStartups == null)
        return; // not yet done with startup
      if (!unfinishedStartups.remove(m))
        return;
      String msg;
      if (departed) {
        msg =
            "Stopped waiting for startup reply from <{}> because the peer departed the view.";
      } else {
        msg =
            "Stopped waiting for startup reply from <{}> because the reply was finally received.";
      }
      logger.info(msg, m);
      int numLeft = unfinishedStartups.size();
      if (numLeft != 0) {
        logger.info("Still awaiting {} response(s) from: {}.",
            new Object[] {numLeft, unfinishedStartups});
      }
    } // synchronized
  }

  /**
   * Processes the first startup response.
   *
   * @see StartupResponseMessage#process
   */
  void processStartupResponse(InternalDistributedMember sender, String theRejectionMessage) {
    removeUnfinishedStartup(sender, false);
    synchronized (this) {
      if (!receivedStartupResponse) {
        // only set the cacheTimeDelta once
        receivedStartupResponse = true;
      }
      if (theRejectionMessage != null && rejectionMessage == null) {
        // remember the first non-null rejection. This fixes bug 33266
        rejectionMessage = theRejectionMessage;
      }
    }
  }

  private String prettifyReason(String r) {
    final String str = "java.io.IOException:";
    if (r.startsWith(str)) {
      return r.substring(str.length());
    }
    return r;
  }

  /**
   * Returns true if id was removed. Returns false if it was not in the list of managers.
   */
  private boolean removeManager(InternalDistributedMember theId, boolean crashed, String p_reason) {
    String reason = p_reason;

    reason = prettifyReason(reason);
    if (logger.isDebugEnabled()) {
      logger.debug("DistributionManager: removing member <{}>; crashed {}; reason = {}", theId,
          crashed, reason);
    }
    removeHostedLocators(theId);

    redundancyZones.remove(theId);

    return true;
  }

  /**
   * Makes note of a new distribution manager that has started up in the distributed cache. Invokes
   * the appropriately listeners.
   *
   * @param theId The id of the distribution manager starting up
   *
   */
  private void handleManagerStartup(InternalDistributedMember theId) {
    // Note test is under membersLock
    if (theId.getVmKind() != ClusterDistributionManager.LOCATOR_DM_TYPE) {
      stats.incNodes(1);
    }
    addMemberEvent(new MemberJoinedEvent(theId));
  }

  @Override
  public boolean isCurrentMember(DistributedMember id) {
    return distribution.getView().contains((InternalDistributedMember) id);
  }

  /**
   * Makes note of a new console that has started up in the distributed cache.
   *
   */
  private void handleConsoleStartup(InternalDistributedMember theId) {
    for (MembershipListener listener : allMembershipListeners) {
      listener.memberJoined(this, theId);
    }
    logger.info("DMMembership: Admitting new administration member < {} >.",
        theId);
  }

  /**
   * Process an incoming distribution message. This includes scheduling it correctly based on the
   * message's nioPriority (executor type)
   */
  private void handleIncomingDMsg(Message message) {
    stats.incReceivedMessages(1L);
    stats.incReceivedBytes(message.getBytesRead());
    stats.incMessageChannelTime(message.resetTimestamp());

    if (logger.isDebugEnabled()) {
      logger.debug("Received message '{}' from <{}>", message, message.getSender());
    }
    scheduleIncomingMessage((DistributionMessage) message);
  }

  /**
   * Makes note of a console that has shut down.
   *
   * @param theId The id of the console shutting down
   * @param crashed only true if we detect this id to be gone from a javagroup view
   *
   * @see AdminConsoleDisconnectMessage#process
   */
  public void handleConsoleShutdown(InternalDistributedMember theId, boolean crashed,
      String reason) {
    removeHostedLocators(theId);
    for (MembershipListener listener : allMembershipListeners) {
      listener.memberDeparted(this, theId, crashed);
    }
    redundancyZones.remove(theId);
  }

  void shutdownMessageReceived(InternalDistributedMember theId, String reason) {
    removeHostedLocators(theId);
    distribution.shutdownMessageReceived(theId, reason);
  }

  @Override
  public void handleManagerDeparture(InternalDistributedMember theId, boolean p_crashed,
      String p_reason) {

    alertingService.removeAlertListener(theId);

    int vmType = theId.getVmKind();
    if (vmType == ADMIN_ONLY_DM_TYPE) {
      removeUnfinishedStartup(theId, true);
      handleConsoleShutdown(theId, p_crashed, p_reason);
      return;
    }

    removeUnfinishedStartup(theId, true);

    if (removeManager(theId, p_crashed, p_reason)) {
      if (theId.getVmKind() != ClusterDistributionManager.LOCATOR_DM_TYPE) {
        stats.incNodes(-1);
      }
      String msg;
      if (p_crashed && !shouldInhibitMembershipWarnings()) {
        msg =
            "Member at {} unexpectedly left the distributed cache: {}";
        addMemberEvent(new MemberCrashedEvent(theId, p_reason));
      } else {
        msg =
            "Member at {} gracefully left the distributed cache: {}";
        addMemberEvent(new MemberDepartedEvent(theId, p_reason));
      }
      logger.info(msg, new Object[] {theId, prettifyReason(p_reason)});

      executors.handleManagerDeparture(theId);
    }
  }

  private void handleManagerSuspect(InternalDistributedMember suspect,
      InternalDistributedMember whoSuspected, String reason) {
    if (!isCurrentMember(suspect)) {
      return; // fault tolerance
    }

    int vmType = suspect.getVmKind();
    if (vmType == ADMIN_ONLY_DM_TYPE) {
      return;
    }

    addMemberEvent(new MemberSuspectEvent(suspect, whoSuspected, reason));
  }

  private void handleViewInstalled(MembershipView view) {
    addMemberEvent(new ViewInstalledEvent(view));
  }

  private void handleQuorumLost(Set<InternalDistributedMember> failures,
      List<InternalDistributedMember> remaining) {
    addMemberEvent(new QuorumLostEvent(failures, remaining));
  }

  /**
   * Sends the shutdown message. Not all DistributionManagers need to do this.
   */
  private void sendShutdownMessage() {
    if (getDMType() == ADMIN_ONLY_DM_TYPE && Locator.getLocators().size() == 0) {
      // [bruce] changed above "if" to have ShutdownMessage sent by locators.
      // Otherwise the system can hang because an admin member does not trigger
      // member-left notification unless a new view is received showing the departure.
      // If two locators are simultaneously shut down this may not occur.
      return;
    }

    ShutdownMessage m = new ShutdownMessage();
    InternalDistributedMember theId = getDistributionManagerId();
    m.setDistributionManagerId(theId);
    Set<InternalDistributedMember> allOthers = new HashSet<>(getViewMembers());
    allOthers.remove(getDistributionManagerId());
    m.setRecipients(allOthers);

    // Address recipient = (Address) m.getRecipient();
    if (logger.isTraceEnabled()) {
      logger.trace("{} Sending {} to {}", getDistributionManagerId(), m,
          m.getRecipientsDescription());
    }

    try {
      // m.resetTimestamp(); // nanotimers across systems don't match
      long startTime = DistributionStats.getStatTime();
      sendViaMembershipManager(m.getRecipients(), m, this, stats);
      stats.incSentMessages(1L);
      if (DistributionStats.enableClockStats) {
        stats.incSentMessagesTime(DistributionStats.getStatTime() - startTime);
      }
    } catch (CancelException e) {
      logger.debug(String.format("CancelException caught sending shutdown: %s", e.getMessage()), e);
    } catch (Exception ex2) {
      logger.fatal("While sending shutdown message", ex2);
    } finally {
      // Even if the message wasn't sent, *lie* about it, so that
      // everyone believes that message distribution is done.
      shutdownMsgSent = true;
    }
  }

  /**
   * Actually does the work of sending a message out over the distribution channel.
   *
   * @param message the message to send
   * @return list of recipients that did not receive the message because they left the view (null if
   *         all received it or it was sent to {@link Message#ALL_RECIPIENTS}.
   * @throws NotSerializableException If <code>message</code> cannot be serialized
   */
  Set<InternalDistributedMember> sendOutgoing(DistributionMessage message)
      throws NotSerializableException {
    long startTime = DistributionStats.getStatTime();

    Set<InternalDistributedMember> result =
        sendViaMembershipManager(message.getRecipients(), message, this, stats);
    long endTime = 0L;
    if (DistributionStats.enableClockStats) {
      endTime = NanoTimer.getTime();
    }
    boolean sentToAll = message.forAll();

    if (sentToAll) {
      stats.incBroadcastMessages(1L);
      if (DistributionStats.enableClockStats) {
        stats.incBroadcastMessagesTime(endTime - startTime);
      }
    }
    stats.incSentMessages(1L);
    if (DistributionStats.enableClockStats) {
      stats.incSentMessagesTime(endTime - startTime);
      stats.incDistributeMessageTime(endTime - message.getTimestamp());
    }

    return result;
  }



  /**
   * @return recipients who did not receive the message
   * @throws NotSerializableException If <codE>message</code> cannot be serialized
   */
  private Set<InternalDistributedMember> sendMessage(DistributionMessage message)
      throws NotSerializableException {
    try {
      // Verify we're not too far into the shutdown
      stopper.checkCancelInProgress(null);

      // avoid race condition during startup
      waitUntilReadyToSendMsgs(message);

      return sendOutgoing(message);
    } catch (NotSerializableException | ToDataException | ReenteredConnectException
        | InvalidDeltaException | CancelException ex) {
      throw ex;
    } catch (Exception ex) {
      exceptionInThreads = true;
      String receiver = "NULL";
      if (message != null) {
        receiver = message.getRecipientsDescription();
      }

      logger.fatal(String.format("While pushing message <%s> to %s", message, receiver), ex);
      if (message == null || message.forAll()) {
        return null;
      }
      return new HashSet<>(message.getRecipients());
    }
  }

  /**
   * @return list of recipients who did not receive the message because they left the view (null if
   *         all received it or it was sent to {@link Message#ALL_RECIPIENTS}).
   * @throws NotSerializableException If content cannot be serialized
   */
  private Set<InternalDistributedMember> sendViaMembershipManager(
      List<InternalDistributedMember> destinations,
      DistributionMessage content, ClusterDistributionManager dm, DistributionStats stats)
      throws NotSerializableException {
    if (distribution == null) {
      logger.warn("Attempting a send to a disconnected DistributionManager");
      if (destinations.size() == 1 && destinations.get(0) == Message.ALL_RECIPIENTS)
        return null;
      return new HashSet<>(destinations);
    }

    return distribution.send(destinations, content);
  }


  /**
   * Schedule a given message appropriately, depending upon its executor kind.
   */
  private void scheduleIncomingMessage(DistributionMessage message) {
    /*
     * Potential race condition between starting up and getting other distribution manager ids -- DM
     * will only be initialized upto the point at which it called startThreads
     */
    waitUntilReadyForMessages();
    message.schedule(this);
  }

  @Override
  public InternalDistributedMember getElderId() throws DistributedSystemDisconnectedException {
    return clusterElderManager.getElderId();
  }

  @Override
  public boolean isElder() {
    return clusterElderManager.isElder();
  }

  @Override
  public boolean isLoner() {
    return false;
  }

  @Override
  public ElderState getElderState(boolean waitToBecomeElder) throws InterruptedException {
    return clusterElderManager.getElderState(waitToBecomeElder);
  }

  /**
   * Waits until elder if newElder or newElder is no longer a member
   *
   * @return true if newElder is the elder; false if it is no longer a member or we are the elder.
   */
  public boolean waitForElder(final InternalDistributedMember desiredElder)
      throws InterruptedException {

    return clusterElderManager.waitForElder(desiredElder);
  }

  /**
   * Sets the administration agent associated with this distribution manager.
   */
  public void setAgent(RemoteGfManagerAgent agent) {
    // Don't let the agent be set twice. There should be a one-to-one
    // correspondence between admin agent and distribution manager.
    if (agent != null) {
      if (this.agent != null) {
        throw new IllegalStateException(
            "There is already an Admin Agent associated with this distribution manager.");
      }

    } else {
      if (this.agent == null) {
        throw new IllegalStateException(
            "There was never an Admin Agent associated with this distribution manager.");
      }
    }
    this.agent = agent;
  }

  /**
   * Returns the agent that owns this distribution manager. (in ConsoleDistributionManager)
   */
  public RemoteGfManagerAgent getAgent() {
    return agent;
  }

  /**
   * Returns a description of the distribution configuration used for this distribution manager. (in
   * ConsoleDistributionManager)
   *
   * @return <code>null</code> if no admin {@linkplain #getAgent agent} is associated with this
   *         distribution manager
   */
  public String getDistributionConfigDescription() {
    if (agent == null) {
      return null;

    } else {
      return agent.getTransport().toString();
    }
  }

  /* -----------------------------Health Monitor------------------------------ */
  private final ConcurrentMap<InternalDistributedMember, HealthMonitor> hmMap =
      new ConcurrentHashMap<>();

  private volatile InternalCache cache;

  /**
   * Returns the health monitor for this distribution manager and owner.
   *
   * @param owner the agent that owns the returned monitor
   * @return the health monitor created by the owner; <code>null</code> if the owner has now created
   *         a monitor.
   * @since GemFire 3.5
   */
  @Override
  public HealthMonitor getHealthMonitor(InternalDistributedMember owner) {
    return hmMap.get(owner);
  }

  /**
   * Returns the health monitor for this distribution manager.
   *
   * @param owner the agent that owns the created monitor
   * @param cfg the configuration to use when creating the monitor
   * @since GemFire 3.5
   */
  @Override
  public void createHealthMonitor(InternalDistributedMember owner, GemFireHealthConfig cfg) {
    if (closeInProgress) {
      return;
    }
    {
      final HealthMonitor hm = getHealthMonitor(owner);
      if (hm != null) {
        hm.stop();
        hmMap.remove(owner);
      }
    }
    {
      HealthMonitorImpl newHm = new HealthMonitorImpl(owner, cfg, this);
      newHm.start();
      hmMap.put(owner, newHm);
    }
  }

  /**
   * Remove a monitor that was previously created.
   *
   * @param owner the agent that owns the monitor to remove
   */
  @Override
  public void removeHealthMonitor(InternalDistributedMember owner, int theId) {
    final HealthMonitor hm = getHealthMonitor(owner);
    if (hm != null && hm.getId() == theId) {
      hm.stop();
      hmMap.remove(owner);
    }
  }

  private void removeAllHealthMonitors() {
    Iterator it = hmMap.values().iterator();
    while (it.hasNext()) {
      HealthMonitor hm = (HealthMonitor) it.next();
      hm.stop();
      it.remove();
    }
  }

  @Override
  public Set<InternalDistributedMember> getAdminMemberSet() {
    return distribution.getView().getMembers().stream()
        .filter((id) -> id.getVmKind() == ADMIN_ONLY_DM_TYPE).collect(
            Collectors.toSet());
  }

  /** Returns count of members filling the specified role */
  @Override
  public int getRoleCount(Role role) {
    int count = 0;
    Set<InternalDistributedMember> mbrs = getDistributionManagerIds();
    for (InternalDistributedMember mbr : mbrs) {
      Set<Role> roles = (mbr).getRoles();
      for (Role mbrRole : roles) {
        if (mbrRole.equals(role)) {
          count++;
          break;
        }
      }
    }
    return count;
  }

  /** Returns true if at least one member is filling the specified role */
  @Override
  public boolean isRolePresent(Role role) {
    Set<InternalDistributedMember> mbrs = getDistributionManagerIds();
    for (InternalDistributedMember mbr : mbrs) {
      Set<Role> roles = mbr.getRoles();
      for (Role mbrRole : roles) {
        if ((mbrRole).equals(role)) {
          return true;
        }
      }
    }
    return false;
  }

  /** Returns a set of all roles currently in the distributed system. */
  @Override
  public Set<Role> getAllRoles() {
    Set<Role> allRoles = new HashSet<>();
    Set<InternalDistributedMember> mbrs = getDistributionManagerIds();
    for (InternalDistributedMember mbr : mbrs) {
      allRoles.addAll(mbr.getRoles());
    }
    return allRoles;
  }

  /**
   * Returns the membership manager for this distributed system. The membership manager owns the
   * membership set and handles all communications. The manager should NOT be used to bypass
   * DistributionManager to send or receive messages.
   * <p>
   * This method was added to allow hydra to obtain thread-local data for transport from one thread
   * to another.
   */
  @Override
  public Distribution getDistribution() {
    // NOTE: do not add cancellation checks here. This method is
    // used during auto-reconnect after the DS has been closed
    return distribution;
  }


  ////////////////////// Inner Classes //////////////////////



  /**
   * This is the listener implementation for responding from events from the Membership Manager.
   *
   */
  private class DMListener implements
      org.apache.geode.distributed.internal.membership.api.MembershipListener<InternalDistributedMember> {
    ClusterDistributionManager dm;

    DMListener(ClusterDistributionManager dm) {
      this.dm = dm;
    }

    @Override
    public void membershipFailure(String reason, Throwable t) {
      exceptionInThreads = true;
      rootCause = t;
      if (rootCause != null && !(rootCause instanceof ForcedDisconnectException)) {
        logger.info("cluster membership failed due to ", rootCause);
        rootCause = new ForcedDisconnectException(rootCause.getMessage());
      }
      try {
        if (membershipTestHooks != null) {
          List<MembershipTestHook> l = membershipTestHooks;
          for (final MembershipTestHook aL : l) {
            MembershipTestHook dml = aL;
            dml.beforeMembershipFailure(reason, rootCause);
          }
        }
        getSystem().disconnect(reason, true);
        if (membershipTestHooks != null) {
          List<MembershipTestHook> l = membershipTestHooks;
          for (final MembershipTestHook aL : l) {
            MembershipTestHook dml = aL;
            dml.afterMembershipFailure(reason, rootCause);
          }
        }
      } catch (RuntimeException re) {
        logger.warn("Exception caught while shutting down", re);
      }
    }

    @Override
    public void newMemberConnected(InternalDistributedMember member) {
      // Do not elect the elder here as surprise members invoke this callback
      // without holding the view lock. That can cause a race condition and
      // subsequent deadlock (#45566). Elder selection is now done when a view
      // is installed.
      try {
        dm.addNewMember(member);
      } catch (VirtualMachineError err) {
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw err;
      } catch (DistributedSystemDisconnectedException e) {
        // don't log shutdown exceptions
      } catch (Throwable t) {
        logger.info(String.format("Membership: Fault while processing view addition of %s",
            member),
            t);
      }
    }

    @Override
    public void memberDeparted(InternalDistributedMember theId, boolean crashed, String reason) {
      try {
        boolean wasAdmin = getAdminMemberSet().contains(theId);
        if (wasAdmin) {
          // Pretend we received an AdminConsoleDisconnectMessage from the console that
          // is no longer in the JavaGroup view.
          // It must have died without sending a ShutdownMessage.
          // This fixes bug 28454.
          AdminConsoleDisconnectMessage message = new AdminConsoleDisconnectMessage();
          message.setSender(theId);
          message.setCrashed(crashed);
          message.setAlertListenerExpected(true);
          message.setIgnoreAlertListenerRemovalFailure(true); // we don't know if it was a listener
                                                              // so
          // don't issue a warning
          message.setRecipient(localAddress);
          message.setReason(reason); // added for #37950
          handleIncomingDMsg(message);
        }
        dm.handleManagerDeparture(theId, crashed, reason);
      } catch (DistributedSystemDisconnectedException se) {
        // let's not get huffy about it
      }
    }

    @Override
    public void memberSuspect(InternalDistributedMember suspect,
        InternalDistributedMember whoSuspected, String reason) {
      try {
        dm.handleManagerSuspect(suspect, whoSuspected, reason);
      } catch (DistributedSystemDisconnectedException se) {
        // let's not get huffy about it
      }
    }

    @Override
    public void viewInstalled(MembershipView view) {
      try {
        dm.handleViewInstalled(view);
      } catch (DistributedSystemDisconnectedException se) {
      }
    }

    @Override
    public void quorumLost(Set<InternalDistributedMember> failures,
        List<InternalDistributedMember> remaining) {
      dm.handleQuorumLost(failures, remaining);
    }

    @Override
    public void saveConfig() {
      if (!getConfig().getDisableAutoReconnect()) {
        cache.saveCacheXmlForReconnect();
      }
    }
  }


  private abstract static class MemberEvent {

    private InternalDistributedMember id;

    MemberEvent(InternalDistributedMember id) {
      this.id = id;
    }

    public InternalDistributedMember getId() {
      return id;
    }

    void handleEvent(ClusterDistributionManager manager) {
      handleEvent(manager, manager.membershipListeners.keySet());
      handleEvent(manager, manager.allMembershipListeners);
    }

    protected abstract void handleEvent(ClusterDistributionManager manager,
        MembershipListener listener);

    private void handleEvent(ClusterDistributionManager manager,
        Set<MembershipListener> membershipListeners) {
      for (MembershipListener listener : membershipListeners) {
        try {
          handleEvent(manager, listener);
        } catch (CancelException e) {
          if (manager.shouldInhibitMembershipWarnings()) {
            if (logger.isTraceEnabled()) {
              logger.trace("MemberEventInvoker: cancelled");
            }
          } else {
            logger.warn("Unexpected cancellation", e);
          }
          break;
        } catch (VirtualMachineError err) {
          SystemFailure.initiateFailure(err);
          // If this ever returns, rethrow the error. We're poisoned
          // now, so don't let this thread continue.
          throw err;
        } catch (Throwable t) {
          // Whenever you catch Error or Throwable, you must also
          // catch VirtualMachineError (see above). However, there is
          // _still_ a possibility that you are dealing with a cascading
          // error condition, so you also need to check to see if the JVM
          // is still usable:
          SystemFailure.checkFailure();
          logger.warn(String.format("Exception while calling membership listener for event: %s",
              this),
              t);
        }
      }
    }
  }

  /**
   * This is an event reflecting that a InternalDistributedMember has joined the system.
   *
   *
   */
  private static class MemberJoinedEvent extends MemberEvent {
    MemberJoinedEvent(InternalDistributedMember id) {
      super(id);
    }

    @Override
    public String toString() {
      return "member " + getId() + " joined";
    }

    @Override
    protected void handleEvent(ClusterDistributionManager manager, MembershipListener listener) {
      listener.memberJoined(manager, getId());
    }
  }

  /**
   * This is an event reflecting that a InternalDistributedMember has left the system.
   *
   */
  private static class MemberDepartedEvent extends MemberEvent {
    String reason;

    MemberDepartedEvent(InternalDistributedMember id, String r) {
      super(id);
      reason = r;
    }

    @Override
    public String toString() {
      return "member " + getId() + " departed (" + reason + ")";
    }

    @Override
    protected void handleEvent(ClusterDistributionManager manager, MembershipListener listener) {
      listener.memberDeparted(manager, getId(), false);
    }
  }

  /**
   * This is an event reflecting that a InternalDistributedMember has left the system in an
   * unexpected way.
   *
   *
   */
  private static class MemberCrashedEvent extends MemberEvent {
    String reason;

    MemberCrashedEvent(InternalDistributedMember id, String r) {
      super(id);
      reason = r;
    }

    @Override
    public String toString() {
      return "member " + getId() + " crashed: " + reason;
    }

    @Override
    protected void handleEvent(ClusterDistributionManager manager, MembershipListener listener) {
      listener.memberDeparted(manager, getId(), true/* crashed */);
    }
  }

  /**
   * This is an event reflecting that a InternalDistributedMember may be missing but has not yet
   * left the system.
   */
  private static class MemberSuspectEvent extends MemberEvent {
    InternalDistributedMember whoSuspected;
    String reason;

    MemberSuspectEvent(InternalDistributedMember suspect, InternalDistributedMember whoSuspected,
        String reason) {
      super(suspect);
      this.whoSuspected = whoSuspected;
      this.reason = reason;
    }

    public InternalDistributedMember whoSuspected() {
      return whoSuspected;
    }

    public String getReason() {
      return reason;
    }

    @Override
    public String toString() {
      return "member " + getId() + " suspected by: " + whoSuspected + " reason: " + reason;
    }

    @Override
    protected void handleEvent(ClusterDistributionManager manager, MembershipListener listener) {
      listener.memberSuspect(manager, getId(), whoSuspected(), reason);
    }
  }

  private static class ViewInstalledEvent extends MemberEvent {
    MembershipView view;

    ViewInstalledEvent(MembershipView view) {
      super(null);
      this.view = view;
    }

    public long getViewId() {
      return view.getViewId();
    }

    @Override
    public String toString() {
      return "view installed: " + view;
    }

    @Override
    public void handleEvent(ClusterDistributionManager manager) {
      manager.handleViewInstalledEvent(this);
    }

    @Override
    protected void handleEvent(ClusterDistributionManager manager, MembershipListener listener) {
      throw new UnsupportedOperationException();
    }
  }

  private static class QuorumLostEvent extends MemberEvent {
    Set<InternalDistributedMember> failures;
    List<InternalDistributedMember> remaining;

    QuorumLostEvent(Set<InternalDistributedMember> failures,
        List<InternalDistributedMember> remaining) {
      super(null);
      this.failures = failures;
      this.remaining = remaining;
    }

    public Set<InternalDistributedMember> getFailures() {
      return failures;
    }

    public List<InternalDistributedMember> getRemaining() {
      return remaining;
    }

    @Override
    public String toString() {
      return "quorum lost.  failures=" + failures + "; remaining=" + remaining;
    }

    @Override
    protected void handleEvent(ClusterDistributionManager manager, MembershipListener listener) {
      listener.quorumLost(manager, getFailures(), getRemaining());
    }
  }


  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.distributed.internal.DM#getRootCause()
   */
  @Override
  public Throwable getRootCause() {
    return rootCause;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.distributed.internal.DM#setRootCause(java.lang.Throwable)
   */
  @Override
  public void setRootCause(Throwable t) {
    rootCause = t;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.distributed.internal.DM#getMembersOnThisHost()
   *
   * @since GemFire 5.9
   */
  @Override
  public Set<InternalDistributedMember> getMembersInThisZone() {
    return getMembersInSameZone(getDistributionManagerId());
  }

  @Override
  public Set<InternalDistributedMember> getMembersInSameZone(
      InternalDistributedMember targetMember) {
    Set<InternalDistributedMember> buddyMembers = new HashSet<>();
    if (!redundancyZones.isEmpty()) {
      synchronized (redundancyZones) {
        String targetZone = redundancyZones.get(targetMember);
        for (Map.Entry<InternalDistributedMember, String> entry : redundancyZones.entrySet()) {
          if (entry.getValue().equals(targetZone)) {
            buddyMembers.add(entry.getKey());
          }
        }
      }
    } else {
      buddyMembers.add(targetMember);
      Set<InetAddress> targetAddrs = getEquivalents(targetMember.getInetAddress());
      for (InternalDistributedMember o : getDistributionManagerIds()) {
        if (!Collections.disjoint(targetAddrs, getEquivalents(o.getInetAddress()))) {
          buddyMembers.add(o);
        }
      }
    }
    return buddyMembers;
  }

  @Override
  public boolean areInSameZone(InternalDistributedMember member1,
      InternalDistributedMember member2) {

    if (!redundancyZones.isEmpty()) {
      String zone1 = redundancyZones.get(member1);
      String zone2 = redundancyZones.get(member2);
      return zone1 != null && zone1.equals(zone2);
    } else {
      return areOnEquivalentHost(member1, member2);
    }
  }

  @Override
  public void acquireGIIPermitUninterruptibly() {
    parallelGIIs.acquireUninterruptibly();
    stats.incInitialImageRequestsInProgress(1);
  }

  @Override
  public void releaseGIIPermit() {
    stats.incInitialImageRequestsInProgress(-1);
    parallelGIIs.release();
  }

  public void setDistributedSystemId(int distributedSystemId) {
    if (distributedSystemId != -1) {
      this.distributedSystemId = distributedSystemId;
    }
  }

  @Override
  public int getDistributedSystemId() {
    return distributedSystemId;
  }

  @Override
  public void registerTestHook(MembershipTestHook mth) {
    this.getDistribution().doWithViewLocked(() -> {
      if (this.membershipTestHooks == null) {
        this.membershipTestHooks = Collections.singletonList(mth);
      } else {
        List<MembershipTestHook> l = new ArrayList<>(this.membershipTestHooks);
        l.add(mth);
        this.membershipTestHooks = l;
      }
      return null;
    });
  }

  @Override
  public void unregisterTestHook(MembershipTestHook mth) {
    this.getDistribution().doWithViewLocked(() -> {
      if (this.membershipTestHooks != null) {
        if (this.membershipTestHooks.size() == 1) {
          this.membershipTestHooks = null;
        } else {
          List<MembershipTestHook> l = new ArrayList<>(this.membershipTestHooks);
          l.remove(mth);
          this.membershipTestHooks = l;
        }
      }
      return null;
    });
  }

  /**
   * this causes the given InternalDistributedMembers to log thread dumps. If useNative is true we
   * attempt to use OSProcess native code for the dumps. This goes to stdout instead of the
   * system.log files.
   */
  public void printStacks(Collection<InternalDistributedMember> ids, boolean useNative) {
    Set<InternalDistributedMember> requiresMessage = new HashSet<>();
    if (ids.contains(localAddress)) {
      OSProcess.printStacks(0, useNative);
    }
    if (useNative) {
      requiresMessage.addAll(ids);
      ids.remove(localAddress);
    } else {
      for (InternalDistributedMember mbr : ids) {
        if (mbr.getProcessId() > 0
            && mbr.getInetAddress().equals(localAddress.getInetAddress())) {
          if (!mbr.equals(localAddress)) {
            if (!OSProcess.printStacks(mbr.getProcessId(), false)) {
              requiresMessage.add(mbr);
            }
          }
        } else {
          requiresMessage.add(mbr);
        }
      }
    }
    if (requiresMessage.size() > 0) {
      HighPriorityAckedMessage msg = new HighPriorityAckedMessage();
      msg.dumpStacks(requiresMessage, useNative, false);
    }
  }

  @Override
  public Set<DistributedMember> getGroupMembers(String group) {
    HashSet<DistributedMember> result = null;
    for (DistributedMember m : getDistributionManagerIdsIncludingAdmin()) {
      if (m.getGroups().contains(group)) {
        if (result == null) {
          result = new HashSet<>();
        }
        result.add(m);
      }
    }
    if (result == null) {
      return Collections.emptySet();
    } else {
      return result;
    }
  }

  @Override
  public Set<InternalDistributedMember> getNormalDistributionManagerIds() {
    return distribution.getMembersNotShuttingDown().stream()
        .filter((id) -> id.getVmKind() != LOCATOR_DM_TYPE).collect(
            Collectors.toSet());
  }

  /** test method to get the member IDs of all locators in the distributed system */
  public Set<InternalDistributedMember> getLocatorDistributionManagerIds() {
    return distribution.getMembersNotShuttingDown().stream()
        .filter((id) -> id.getVmKind() == LOCATOR_DM_TYPE).collect(
            Collectors.toSet());
  }

  @Override
  public void setCache(InternalCache instance) {
    cache = instance;
  }

  @Override
  public InternalCache getCache() {
    return cache;
  }

  @Override
  public InternalCache getExistingCache() {
    InternalCache result = cache;
    if (result == null) {
      throw new CacheClosedException(
          "A cache has not yet been created.");
    }
    result.getCancelCriterion().checkCancelInProgress(null);
    if (result.isClosed()) {
      throw result.getCacheClosedException(
          "The cache has been closed.", null);
    }
    return result;
  }


  private static class Stopper extends CancelCriterion {
    private ClusterDistributionManager dm;

    Stopper(ClusterDistributionManager dm) {
      this.dm = dm;
    }

    @Override
    public String cancelInProgress() {
      checkFailure();

      // remove call to validateDM() to fix bug 38356

      if (dm.shutdownMsgSent) {
        return String.format("%s: Message distribution has terminated",
            dm.toString());
      }
      if (dm.rootCause != null) {
        return dm.toString() + ": " + dm.rootCause.getMessage();
      }

      // Nope.
      return null;
    }

    @Override
    public RuntimeException generateCancelledException(Throwable e) {
      String reason = cancelInProgress();
      if (reason == null) {
        return null;
      }
      Throwable rc = dm.rootCause; // volatile read
      if (rc == null) {
        // No root cause, specify the one given and be done with it.
        return new DistributedSystemDisconnectedException(reason, e);
      }

      if (e == null) {
        if (rc instanceof MemberDisconnectedException) {
          rc = new ForcedDisconnectException(rc.getMessage());
        }
        // Caller did not specify any root cause, so just use our own.
        return new DistributedSystemDisconnectedException(reason, rc);
      }

      // Attempt to stick rootCause at tail end of the exception chain.
      Throwable nt = e;
      while (nt.getCause() != null) {
        nt = nt.getCause();
      }
      if (nt == rc) {
        // Root cause already in place; we're done
        return new DistributedSystemDisconnectedException(reason, e);
      }

      try {
        nt.initCause(rc);
        return new DistributedSystemDisconnectedException(reason, e);
      } catch (IllegalStateException e2) {
        // Bug 39496 (Jrockit related) Give up. The following
        // error is not entirely sane but gives the correct general picture.
        return new DistributedSystemDisconnectedException(reason, rc);
      }
    }
  }

  @Override
  public CancelCriterion getCancelCriterion() {
    return stopper;
  }

  static class ClusterDistributionManagerIDFactory
      implements MemberIdentifierFactory<InternalDistributedMember> {
    @Immutable
    private static final Comparator<InternalDistributedMember> idComparator =
        InternalDistributedMember::compareTo;

    @Override
    public InternalDistributedMember create(MemberData memberInfo) {
      return new InternalDistributedMember(memberInfo);
    }

    @Override
    public Comparator<InternalDistributedMember> getComparator() {
      return idComparator;
    }
  }

}
