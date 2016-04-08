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
package com.gemstone.gemfire.distributed.internal;

import java.io.Externalizable;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.ForcedDisconnectException;
import com.gemstone.gemfire.IncompatibleSystemException;
import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.InternalGemFireException;
import com.gemstone.gemfire.InvalidDeltaException;
import com.gemstone.gemfire.SystemConnectException;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.ToDataException;
import com.gemstone.gemfire.admin.GemFireHealthConfig;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.Role;
import com.gemstone.gemfire.distributed.internal.locks.ElderState;
import com.gemstone.gemfire.distributed.internal.membership.DistributedMembershipListener;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.MemberFactory;
import com.gemstone.gemfire.distributed.internal.membership.MembershipManager;
import com.gemstone.gemfire.distributed.internal.membership.NetView;
import com.gemstone.gemfire.i18n.StringId;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.OSProcess;
import com.gemstone.gemfire.internal.SetUtils;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.admin.remote.AdminConsoleDisconnectMessage;
import com.gemstone.gemfire.internal.admin.remote.RemoteGfManagerAgent;
import com.gemstone.gemfire.internal.admin.remote.RemoteTransportConfig;
import com.gemstone.gemfire.internal.cache.InitialImageOperation;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.LoggingThreadGroup;
import com.gemstone.gemfire.internal.logging.log4j.AlertAppender;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;
import com.gemstone.gemfire.internal.sequencelog.MembershipLogger;
import com.gemstone.gemfire.internal.tcp.Connection;
import com.gemstone.gemfire.internal.tcp.ConnectionTable;
import com.gemstone.gemfire.internal.tcp.ReenteredConnectException;
import com.gemstone.gemfire.internal.util.concurrent.StoppableReentrantLock;

/**
 * The <code>DistributionManager</code> uses a {@link
 * MembershipManager} to distribute {@link DistributionMessage messages}
 * queued in {@link MQueue}s.
 *
 * <P>
 *
 * Code that wishes to send a {@link DistributionMessage} must get
 * the <code>DistributionManager</code> and invoke {@link
 * #putOutgoing}.
 *
 * <P>
 *
 * <code>DistributionManager</code> is not intended to be
 * serialized.  It is <code>Externalizable</code> only to prevent it
 * from being copy shared.  See {@link #writeExternal}.
 *
 * <P>
 *
 * Prior to GemFire 4.0, <code>DistributionManager</code> was an
 * abstract class with two concrete subclasses,
 * <code>LocalDistributionManager</code> and
 * <code>ConsoleDistributionManager</code>.  We decided that
 * <code>ConsoleDistributionManager</code> (which was used for the GUI
 * console and admin APIs) did not offer enough interesting
 * functionality to warrant a separate class.  More importantly, it
 * prevented the Cache and admin APIs from being used in the same VM.
 * So, we refactored the code of those two subclasses into
 * <code>DistributionManager</code>.
 *
 * @since 2.0
 *
 * @see DistributionMessage#process
 * @see IgnoredByManager
 */
public class DistributionManager
  implements Externalizable, DM {

  private static final Logger logger = LogService.getLogger();
  
  private static final boolean SYNC_EVENTS =
    Boolean.getBoolean("DistributionManager.syncEvents");

  /**
   * WARNING: setting this to true may break dunit tests.
   * <p>see com.gemstone.gemfire.cache30.ClearMultiVmCallBkDUnitTest
   */
  public static final boolean INLINE_PROCESS = 
    !Boolean.getBoolean("DistributionManager.enqueueOrderedMessages");

  /** Flag indicating whether to use single Serial-Executor thread or 
   * Multiple Serial-executor thread, 
   */
  public static final boolean MULTI_SERIAL_EXECUTORS = 
    !Boolean.getBoolean("DistributionManager.singleSerialExecutor");

  /** The name of the distribution manager (identifies it in GemFire) */
  public static final String NAME = "GemFire";
  
  /** The number of milliseconds to wait for distribution-related
   * things to happen */
  public static final long TIMEOUT =
    Long.getLong("DistributionManager.TIMEOUT", -1).longValue();

  public static final int PUSHER_THREADS =
    Integer.getInteger("DistributionManager.PUSHER_THREADS", 50).intValue();

  public static final int PUSHER_QUEUE_SIZE =
    Integer.getInteger("DistributionManager.PUSHER_QUEUE_SIZE", 4096).intValue();

  
  public static final int MAX_WAITING_THREADS = 
    Integer.getInteger("DistributionManager.MAX_WAITING_THREADS", Integer.MAX_VALUE).intValue();
  
  public static final int MAX_PR_META_DATA_CLEANUP_THREADS = 
      Integer.getInteger("DistributionManager.MAX_PR_META_DATA_CLEANUP_THREADS", 1).intValue();

  public static final int MAX_THREADS = Integer.getInteger("DistributionManager.MAX_THREADS", 100).intValue();
  public static final int MAX_PR_THREADS = Integer.getInteger("DistributionManager.MAX_PR_THREADS", Math.max(Runtime.getRuntime().availableProcessors()*4, 16)).intValue();
  public static final int MAX_FE_THREADS = Integer.getInteger("DistributionManager.MAX_FE_THREADS", Math.max(Runtime.getRuntime().availableProcessors()*4, 16)).intValue();
  //    Integer.getInteger("DistributionManager.MAX_THREADS", max(Runtime.getRuntime().availableProcessors()*2, 2)).intValue();

  public static final int INCOMING_QUEUE_LIMIT =
    Integer.getInteger("DistributionManager.INCOMING_QUEUE_LIMIT", 80000).intValue();
  public static final int INCOMING_QUEUE_THROTTLE =
    Integer.getInteger("DistributionManager.INCOMING_QUEUE_THROTTLE", (int)(INCOMING_QUEUE_LIMIT * 0.75)).intValue();

  /** Throttling based on the Queue byte size */
  public static final double THROTTLE_PERCENT =
    (double) (Integer.getInteger("DistributionManager.SERIAL_QUEUE_THROTTLE_PERCENT", 75).intValue())/100;
  public static final int SERIAL_QUEUE_BYTE_LIMIT =
    Integer.getInteger("DistributionManager.SERIAL_QUEUE_BYTE_LIMIT", (40 * (1024 * 1024))).intValue();
  public static final int SERIAL_QUEUE_THROTTLE =
    Integer.getInteger("DistributionManager.SERIAL_QUEUE_THROTTLE", (int)(SERIAL_QUEUE_BYTE_LIMIT * THROTTLE_PERCENT)).intValue();
  public static final int TOTAL_SERIAL_QUEUE_BYTE_LIMIT =
    Integer.getInteger("DistributionManager.TOTAL_SERIAL_QUEUE_BYTE_LIMIT", (80 * (1024 * 1024))).intValue();
  public static final int TOTAL_SERIAL_QUEUE_THROTTLE =
    Integer.getInteger("DistributionManager.TOTAL_SERIAL_QUEUE_THROTTLE", (int)(SERIAL_QUEUE_BYTE_LIMIT * THROTTLE_PERCENT)).intValue();
  
  /** Throttling based on the Queue item size */
  public static final int SERIAL_QUEUE_SIZE_LIMIT =
    Integer.getInteger("DistributionManager.SERIAL_QUEUE_SIZE_LIMIT", 20000).intValue();
  public static final int SERIAL_QUEUE_SIZE_THROTTLE =
    Integer.getInteger("DistributionManager.SERIAL_QUEUE_SIZE_THROTTLE", (int)(SERIAL_QUEUE_SIZE_LIMIT * THROTTLE_PERCENT)).intValue();

  /** Max number of serial Queue executors, in case of multi-serial-queue executor */
  public static final int MAX_SERIAL_QUEUE_THREAD =
    Integer.getInteger("DistributionManager.MAX_SERIAL_QUEUE_THREAD", 20).intValue();

  /**
   * Whether or not to include link local addresses in the list of addresses we use
   * to determine if two members are no the same host.
   * 
   * Added for normura issue 7033 - they have duplicate link local addresses on different boxes
   */
  public static volatile boolean INCLUDE_LINK_LOCAL_ADDRESSES = 
    Boolean.getBoolean("gemfire.IncludeLinkLocalAddresses");
  
  /** The DM type for regular distribution managers */
  public static final int NORMAL_DM_TYPE = 10;

  /** The DM type for locator distribution managers 
   * @since 7.0
   */
  public static final int LOCATOR_DM_TYPE = 11;

  /** The DM type for Console (admin-only) distribution managers */
  public static final int ADMIN_ONLY_DM_TYPE = 12;

  public static final int LONER_DM_TYPE = 13;

  /**
   * an NIO priority type
   * @see com.gemstone.gemfire.distributed.internal.PooledDistributionMessage
   * @see #SERIAL_EXECUTOR
   * @see #HIGH_PRIORITY_EXECUTOR
   * @see #WAITING_POOL_EXECUTOR
   */
  public static final int STANDARD_EXECUTOR = 73;

  /**
   * an NIO priority type
   * 
   * @see com.gemstone.gemfire.distributed.internal.SerialDistributionMessage
   * @see #STANDARD_EXECUTOR
   */
  public static final int SERIAL_EXECUTOR = 74;

  /**
   * an NIO priority type

   * @see com.gemstone.gemfire.distributed.internal.HighPriorityDistributionMessage
   * @see #STANDARD_EXECUTOR
   */
  public static final int HIGH_PRIORITY_EXECUTOR = 75;
  
  // 76 not in use

  /**
   * an NIO priority type
   * 
   * @see com.gemstone.gemfire.internal.cache.InitialImageOperation
   * @see #STANDARD_EXECUTOR
   */
  public static final int WAITING_POOL_EXECUTOR = 77;

  /**
   * an NIO priority type
   * 
   * @see com.gemstone.gemfire.internal.cache.InitialImageOperation
   * @see #STANDARD_EXECUTOR
   */
  public static final int PARTITIONED_REGION_EXECUTOR = 78;

  
  /**
   * Executor for view related messages
   * 
   * @see com.gemstone.gemfire.distributed.internal.membership.gms.messages.ViewAckMessage
   * @see #STANDARD_EXECUTOR
   */
  public static final int VIEW_EXECUTOR = 79;


  public static final int REGION_FUNCTION_EXECUTION_EXECUTOR = 80;

  /** The number of open  distribution managers in this VM */
  private static int openDMs = 0;

//  /** The stack trace of the last time a console DM was opened */
//  private static Exception openStackTrace;

  /** Is this VM dedicated to administration (like a GUI console or a
   * JMX agent)?  If so, then it creates {@link #ADMIN_ONLY_DM_TYPE}
   * type distribution managers.
   *
   * @since 4.0 
   */
  public static volatile boolean isDedicatedAdminVM = false;
  
  /**
   * Is this admin agent used for a command line console.
   * This flag controls whether connect will throw 
   * an exception or just wait for a DS if one is not
   * available. If true, we will throw an exception.
   * 
   */
  public static volatile boolean isCommandLineAdminVM = false;
  
  
  
  /////////////////////  Instance Fields  //////////////////////

  /** The id of this distribution manager */
  final protected InternalDistributedMember myid;

  /** The distribution manager type of this dm; set in its constructor. */
  private final int dmType;

  /** The <code>MembershipListener</code>s that are registered on this
   * manager. */
  private final ConcurrentMap membershipListeners;

  /** A lock to hold while adding and removing membership listeners */
  protected final Object membershipListenersLock =
    new MembershipListenersLock();
  /** The <code>MembershipListener</code>s that are registered on this
   * manager for ALL members.
   * @since 5.7
   */
  protected volatile Set allMembershipListeners = Collections.EMPTY_SET;
  /**
   * A lock to hold while adding and removing all membership listeners.
   * @since 5.7
   */
  protected final Object allMembershipListenersLock =
    new MembershipListenersLock();
  /** A queue of MemberEvent instances */
  protected final BlockingQueue membershipEventQueue =
    new LinkedBlockingQueue();
  /** Used to invoke registered membership listeners in the background. */
  private Thread memberEventThread;


  /** A brief description of this DistributionManager */
  protected final String description;

  /** Statistics about distribution */
  protected /*final*/ DistributionStats stats;

  /** Did an exception occur in one of the DM threads? */
  protected boolean exceptionInThreads;

  static ThreadLocal isStartupThread = new ThreadLocal();
    
  protected volatile boolean shutdownMsgSent = false;

  /** Set to true when this manager is being shutdown */
  protected volatile boolean closeInProgress = false;
  
  private volatile boolean receivedStartupResponse = false;
  
  private volatile String rejectionMessage = null;

  protected MembershipManager membershipManager;
  
  /** The channel through which distributed communication occurs. */
  protected DistributionChannel channel;

  /**
   * The (non-admin-only) members of the distributed system.  This is a
   * map of memberid->memberid for fast access to canonical ID references.
   * All accesses to this 
   * field must be synchronized on {@link #membersLock}.
   */
  private Map<InternalDistributedMember,InternalDistributedMember> members = Collections.emptyMap();
  /** 
   * All (admin and non-admin) members of the distributed system. All accesses 
   * to this field must be synchronized on {@link #membersLock}.
   */
  private Set membersAndAdmin = Collections.emptySet();
  /** 
   * Map of all locator members of the distributed system. The value is a
   * collection of locator strings that are hosted in that member. All accesses 
   * to this field must be synchronized on {@link #membersLock}.
   */
  private Map<InternalDistributedMember, Collection<String>> hostedLocatorsAll = Collections.emptyMap();
  
  /** 
   * Map of all locator members of the distributed system which have the shared configuration. The value is a
   * collection of locator strings that are hosted in that member. All accesses 
   * to this field must be synchronized on {@link #membersLock}.
   */
  private Map<InternalDistributedMember, Collection<String>> hostedLocatorsWithSharedConfiguration = Collections.emptyMap();

  /**
   * Since 6.6.2 and hereafter we will save the versions here. But pre-6.6.2's
   * StartupResponseMessage does not contain version. We will assign a default
   * version for them.
   */
  public static final String DEFAULT_VERSION_PRE_6_6_2 = "6.6.0.0";
  /** 
   * The lock held while accessing the field references to the following:<br>
   * 1) {@link #members}<br>
   * 2) {@link #membersAndAdmin}<br>
   * 3) {@link #hostedLocatorsAll}<br>
   * 4) {@link #hostedLocatorsWithSharedConfiguration}<br>
   */
  private final Object membersLock = new MembersLock();

  /**
   * The lock held while writing {@link #adminConsoles}.
   */
  private final Object adminConsolesLock = new Object();
  /** 
   * The ids of all known admin consoles
   * Uses Copy on Write. Writers must sync on adminConsolesLock.
   * Readers don't need to sync.
   */
  private volatile Set<InternalDistributedMember> adminConsoles = Collections.emptySet();

  /** The pusher thread */
  //private Thread pusher;

  /** The group of distribution manager threads */
  protected LoggingThreadGroup threadGroup;
  
  /** Message processing thread pool */
  private ThreadPoolExecutor threadPool;

  /** High Priority processing thread pool, used for initializing messages
   *  such as UpdateAttributes and CreateRegion messages
   */
  private ThreadPoolExecutor highPriorityPool;
  
  /** Waiting Pool, used for messages that may have to wait on something.
   *  Use this separate pool with an unbounded queue so that waiting
   *  runnables don't get in the way of other processing threads.
   *  Used for threads that will most likely have to wait for a region to be
   *  finished initializing before it can proceed
   */
  private ThreadPoolExecutor waitingPool;
  
  private ThreadPoolExecutor prMetaDataCleanupThreadPool;
  
  /**
   * Thread used to decouple {@link com.gemstone.gemfire.internal.cache.partitioned.PartitionMessage}s from 
   * {@link com.gemstone.gemfire.internal.cache.DistributedCacheOperation}s </b>
   * @see #SERIAL_EXECUTOR
   */
  private ThreadPoolExecutor partitionedRegionThread;
  private ThreadPoolExecutor partitionedRegionPool;
  private ThreadPoolExecutor functionExecutionThread;
  private ThreadPoolExecutor functionExecutionPool;

  /** Message processing executor for serial, ordered, messages. */
  private ThreadPoolExecutor serialThread;
  
  /** Message processing executor for view messages
   * @see com.gemstone.gemfire.distributed.internal.membership.gms.messages.ViewAckMessage 
   */
  private ThreadPoolExecutor viewThread;
  
  /** If using a throttling queue for the serialThread, we cache the queue here
      so we can see if delivery would block */
  private ThrottlingMemLinkedQueueWithDMStats serialQueue;

  /** a map keyed on InternalDistributedMember, to direct channels to other systems */
  //protected final Map channelMap = CFactory.createCM();

  protected volatile boolean readyForMessages = false;

  /**
   * Set to true once this DM is ready to send messages.
   * Note that it is always ready to send the startup message.
   */
  private volatile boolean readyToSendMsgs = false;
  private final Object readyToSendMsgsLock = new Object();

  /** Is this distribution manager closed? */
  protected volatile boolean closed = false;

  /** The distributed system to which this distribution manager is
   * connected. */
  private InternalDistributedSystem system;

  /** The remote transport configuration for this dm */
  private RemoteTransportConfig transport;

  /** The administration agent associated with this distribution
   * manager. */
  private volatile RemoteGfManagerAgent agent;

  private SerialQueuedExecutorPool serialQueuedExecutorPool;
  
  private final Semaphore parallelGIIs = new Semaphore(InitialImageOperation.MAX_PARALLEL_GIIS);

  /**
   * Map of InetAddress to HashSets of InetAddress, to define equivalences
   * between network interface cards and hosts.
   */
  private final HashMap<InetAddress, Set<InetAddress>> equivalentHosts = new HashMap<InetAddress, Set<InetAddress>>();

  private int distributedSystemId = DistributionConfig.DEFAULT_DISTRIBUTED_SYSTEM_ID;
  
  
  private final Map<InternalDistributedMember, String> redundancyZones = Collections.synchronizedMap(new HashMap<InternalDistributedMember, String>());
  
  private boolean enforceUniqueZone = false;

  private volatile boolean isSharedConfigEnabledForDS = false;

  @Override
  public boolean isSharedConfigurationServiceEnabledForDS() {
    return isSharedConfigEnabledForDS;
  }

  /**
   * Identifier for function execution threads and any of their children
   */
  public static final InheritableThreadLocal<Boolean> isFunctionExecutionThread = new InheritableThreadLocal<Boolean>() {
    @Override
    protected Boolean initialValue() {
      return Boolean.FALSE;
    }
  };
  //////////////////////  Static Methods  //////////////////////

  /**
   * Given two DistributionManager ids, check to see if they are
   * from the same host address.
   * @param id1 a DistributionManager id
   * @param id2 a DistributionManager id
   * @return true if id1 and id2 are from the same host, false otherwise
   */
  public static boolean isSameHost(InternalDistributedMember id1, InternalDistributedMember id2) {
    return (id1.getInetAddress().equals(id2.getInetAddress()));
  }

  // @todo davidw Modify JGroups so that we do not have to send out a
  // {@link StartupMessage}
  /**
   * Creates a new distribution manager and discovers the other members of the
   * distributed system. Note that it does not check to see whether or not this
   * VM already has a distribution manager.
   * 
   * @param system
   *                The distributed system to which this distribution manager
   *                will send messages.
   */
  public static DistributionManager create(InternalDistributedSystem system)
  {

    DistributionManager dm = null;
    
    try {

      int vmKind;
      
      if (Boolean.getBoolean(InternalLocator.FORCE_LOCATOR_DM_TYPE)) {
        // if this DM is starting for a locator, set it to be a locator DM
        vmKind = LOCATOR_DM_TYPE;

      } else if (isDedicatedAdminVM) {
        vmKind = ADMIN_ONLY_DM_TYPE;

      } else {
        vmKind = NORMAL_DM_TYPE;
      }
    
      RemoteTransportConfig transport = new RemoteTransportConfig(system.getConfig(), vmKind);
      transport.setIsReconnectingDS(system.isReconnectingDS());
      transport.setOldDSMembershipInfo(system.oldDSMembershipInfo());
      
      long start = System.currentTimeMillis();

      dm = new DistributionManager(system, transport);
      dm.assertDistributionManagerType();

      {
        InternalDistributedMember id = dm.getDistributionManagerId();
        if (!"".equals(id.getName())) {
          for (InternalDistributedMember m: (List<InternalDistributedMember>)dm.getViewMembers()) {
            if (m.equals(id)) {
              // I'm counting on the members returned by getViewMembers being ordered such that
              // members that joined before us will precede us AND members that join after us
              // will succeed us.
              // SO once we find ourself break out of this loop.
              break;
            }
            if (id.getName().equals(m.getName())) {
              if (dm.getMembershipManager().verifyMember(m, "member is using the name of " + id)) {
                throw new IncompatibleSystemException("Member " + id + " could not join this distributed system because the existing member " + m + " used the same name. Set the \"name\" gemfire property to a unique value.");
              }
            }
          }
        }
        dm.addNewMember(id); // add ourselves
        dm.selectElder(); // ShutdownException could be thrown here
      }

      // Send out a StartupMessage to the other members.
      StartupOperation op = new StartupOperation(dm, transport);

      try {
        if (!dm.sendStartupMessage(op, true)) {
          // We'll we didn't hear back from anyone else.  We assume that
          // we're the first one.
          if (dm.getOtherDistributionManagerIds().size() == 0) {
            logger.info(LocalizedMessage.create(LocalizedStrings.DistributionManager_DIDNT_HEAR_BACK_FROM_ANY_OTHER_SYSTEM_I_AM_THE_FIRST_ONE));
          } else if (transport.isMcastEnabled()) {
            // perform a multicast ping test
            if (!dm.testMulticast()) {
              logger.warn(LocalizedMessage.create(
                  LocalizedStrings.DistributionManager_RECEIVED_NO_STARTUP_RESPONSES_BUT_OTHER_MEMBERS_EXIST_MULTICAST_IS_NOT_RESPONSIVE));
            }
          }
        }
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        // This is ALWAYS bad; don't consult a CancelCriterion.
        throw new InternalGemFireException(LocalizedStrings.DistributionManager_INTERRUPTED_WHILE_WAITING_FOR_FIRST_STARTUPRESPONSEMESSAGE.toLocalizedString(), ex);
      } catch (IncompatibleSystemException ex) {
        logger.fatal(ex.getMessage(), ex);
        throw ex;
      } finally {
        dm.readyToSendMsgs();
      }

      if (logger.isInfoEnabled()) {
        long delta = System.currentTimeMillis() - start;
        Object[] logArgs = new Object[] {
            dm.getDistributionManagerId(),
            transport,
            Integer.valueOf(dm.getOtherDistributionManagerIds().size()),
            dm.getOtherDistributionManagerIds(), 
            (logger.isInfoEnabled(LogMarker.DM) ? " (VERBOSE, took " + delta + " ms)" : ""),
            ((dm.getDMType() == ADMIN_ONLY_DM_TYPE) ? " (admin only)" : (dm.getDMType() == LOCATOR_DM_TYPE) ? " (locator)" : "")
        };
        logger.info(LogMarker.DM, LocalizedMessage.create(
            LocalizedStrings.DistributionManager_DISTRIBUTIONMANAGER_0_STARTED_ON_1_THERE_WERE_2_OTHER_DMS_3_4_5, logArgs));

        MembershipLogger.logStartup(dm.getDistributionManagerId());
      }
      return dm;
    }
    catch (RuntimeException r) {
      if (dm != null) {
        if (logger.isDebugEnabled()) {
          logger.debug("cleaning up incompletely started DistributionManager due to exception", r); 
        }
        dm.uncleanShutdown(true);
      }
      throw r;
    }
  }

  void runUntilShutdown(Runnable r) {
    try {
      r.run();
    }
    catch (CancelException e) {
      if (logger.isTraceEnabled()) {
        logger.trace("Caught shutdown exception", e); 
      }
    }
    catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    }
    catch (Throwable t) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      if (isCloseInProgress()) {
        logger.debug("Caught unusual exception during shutdown: {}", t.getMessage(), t);
      }
      else {
        logger.warn(LocalizedMessage.create(LocalizedStrings.DistributionManager_TASK_FAILED_WITH_EXCEPTION), t);
      }
    }  }
  
  volatile Throwable rootCause = null;
  
  private static class Stopper extends CancelCriterion {
    private DistributionManager dm;
    
    // validateDM is commented out because expiry threads hit it with 
    // an ugly failure... use only for debugging lingering DM bugs
//    private String validateDM() {
//      GemFireCache cache = GemFireCache.getInstance();
//      if (cache == null) {
//        return null; // Distributed system with no cache
//      }
//      Object obj = cache.getDistributedSystem();
//      if (obj == null) {
//        return null; // Cache is very dead
//      }
//      InternalDistributedSystem ids = (InternalDistributedSystem)obj;
//      DM current = ids.getDistributionManager();
//      if (current != dm) {
//        String response = LocalizedStrings.DistributionManager_CURRENT_CACHE_DISTRIBUTIONMANAGER_0_IS_NOT_THE_SAME_AS_1
//        .toLocalizedString(new Object[] { current, dm});
//        return response; 
//      }
//      return null;
//    }
    
    Stopper(DistributionManager dm) {
      this.dm = dm;  
    }
    @Override
    public String cancelInProgress() {
      checkFailure();

      // remove call to validateDM() to fix bug 38356
      
      if (dm.shutdownMsgSent) {
        return LocalizedStrings.DistributionManager__0_MESSAGE_DISTRIBUTION_HAS_TERMINATED.toLocalizedString(dm.toString());
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
        // Caller did not specify  any root cause, so just use our own.
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
      }
      catch (IllegalStateException e2) {
        // Bug 39496 (Jrockit related)  Give up.  The following
        // error is not entirely sane but gives the correct general picture.
        return new DistributedSystemDisconnectedException(reason, rc);
      }
    }
  }
  private final Stopper stopper = new Stopper(this);
  
  public CancelCriterion getCancelCriterion() {
    return stopper;
  }
  
  ///////////////////////  Constructors  ///////////////////////
  
  /**
   * no-arg constructor for Externalizable
   * TODO: does this class really need to implement Externalizable?  I
   * think it only implements that interface for the old copy-sharing
   * shared-memory stuff that's no longer in GemFire
   */
  public DistributionManager() {
    this.elderLock = null;
    this.membershipListeners = null;
    this.myid = null;
    this.description = null;
    this.dmType = 0;
    throw new IllegalAccessError("this constructor should never be invoked");
  }

  /**
   * Creates a new <code>DistributionManager</code> by initializing
   * itself, creating the membership manager and executors
   *
   * @param transport
   *        The configuration for the communications transport
   *
   */
  private DistributionManager(RemoteTransportConfig transport,
                              InternalDistributedSystem system) {

    this.dmType = transport.getVmKind();
    this.system = system;
    this.elderLock = new StoppableReentrantLock(stopper);
    this.transport = transport;

    this.membershipListeners = new ConcurrentHashMap();
    this.distributedSystemId = system.getConfig().getDistributedSystemId();
    {
      long statId = OSProcess.getId();
      /* deadcoded since we don't know the channel id yet.
        if (statId == 0 || statId == -1) {
        statId = getChannelId();
        }
      */
      this.stats = new DistributionStats(system, statId);
      DistributionStats.enableClockStats = system.getConfig().getEnableTimeStatistics();
    }

    this.exceptionInThreads = false;
    
    // Start the processing threads
    final LoggingThreadGroup group =
      LoggingThreadGroup.createThreadGroup("DistributionManager Threads", logger);
    this.threadGroup = group;
    
    boolean finishedConstructor = false;
    try {

    if (MULTI_SERIAL_EXECUTORS) {
      if (logger.isInfoEnabled(LogMarker.DM)) {
        logger.info(LogMarker.DM,
            "Serial Queue info :" + 
            " THROTTLE_PERCENT: " + THROTTLE_PERCENT +
            " SERIAL_QUEUE_BYTE_LIMIT :" + SERIAL_QUEUE_BYTE_LIMIT +   
            " SERIAL_QUEUE_THROTTLE :" + SERIAL_QUEUE_THROTTLE + 
            " TOTAL_SERIAL_QUEUE_BYTE_LIMIT :" + TOTAL_SERIAL_QUEUE_BYTE_LIMIT +  
            " TOTAL_SERIAL_QUEUE_THROTTLE :" + TOTAL_SERIAL_QUEUE_THROTTLE + 
            " SERIAL_QUEUE_SIZE_LIMIT :" + SERIAL_QUEUE_SIZE_LIMIT +
            " SERIAL_QUEUE_SIZE_THROTTLE :" + SERIAL_QUEUE_SIZE_THROTTLE
        ); 
      }
      //  when TCP/IP is disabled we can't throttle the serial queue or we run the risk of 
      // distributed deadlock when we block the UDP reader thread
      boolean throttlingDisabled = system.getConfig().getDisableTcp();
      this.serialQueuedExecutorPool = new SerialQueuedExecutorPool(this.threadGroup, this.stats, throttlingDisabled);
    }
      
    {
      BlockingQueue poolQueue;
      if (SERIAL_QUEUE_BYTE_LIMIT == 0) {
        poolQueue = new OverflowQueueWithDMStats(this.stats.getSerialQueueHelper());
      } else {
        this.serialQueue = new ThrottlingMemLinkedQueueWithDMStats(TOTAL_SERIAL_QUEUE_BYTE_LIMIT, 
            TOTAL_SERIAL_QUEUE_THROTTLE, SERIAL_QUEUE_SIZE_LIMIT, SERIAL_QUEUE_SIZE_THROTTLE, 
            this.stats.getSerialQueueHelper());
        poolQueue = this.serialQueue;
     } 
      ThreadFactory tf = new ThreadFactory() {
        public Thread newThread(final Runnable command) {
          DistributionManager.this.stats.incSerialThreadStarts();
          final Runnable r = new Runnable() {
            public void run() {
              DistributionManager.this.stats.incNumSerialThreads(1);
              try {
              ConnectionTable.threadWantsSharedResources();
              Connection.makeReaderThread();
              runUntilShutdown(command);
              // command.run();
              } finally {
                ConnectionTable.releaseThreadsSockets();
                DistributionManager.this.stats.incNumSerialThreads(-1);
              }
            }
          };
          Thread thread = new Thread(group, r, LocalizedStrings.DistributionManager_SERIAL_MESSAGE_PROCESSOR.toLocalizedString());
          thread.setDaemon(true);
          return thread;
        }
      };
      SerialQueuedExecutorWithDMStats executor = new SerialQueuedExecutorWithDMStats(poolQueue, 
          this.stats.getSerialProcessorHelper(), tf);
      this.serialThread = executor;
    }
    {
      BlockingQueue q = new LinkedBlockingQueue();
      ThreadFactory tf = new ThreadFactory() {
          public Thread newThread(final Runnable command) {
            DistributionManager.this.stats.incViewThreadStarts();
            final Runnable r = new Runnable() {
                public void run() {
                  DistributionManager.this.stats.incNumViewThreads(1);
                  try {
                    ConnectionTable.threadWantsSharedResources();
                    Connection.makeReaderThread();
                    runUntilShutdown(command);
                  } finally {
                    ConnectionTable.releaseThreadsSockets();
                    DistributionManager.this.stats.incNumViewThreads(-1);
                  }
                }
              };
            Thread thread = new Thread(group, r, LocalizedStrings.DistributionManager_VIEW_MESSAGE_PROCESSOR.toLocalizedString());
            thread.setDaemon(true);
            return thread;
          }
        };
      this.viewThread = new SerialQueuedExecutorWithDMStats(q, 
          this.stats.getViewProcessorHelper(), tf);
    }

    {
      BlockingQueue poolQueue;
      if (INCOMING_QUEUE_LIMIT == 0) {
        poolQueue = new OverflowQueueWithDMStats(this.stats.getOverflowQueueHelper());
      } else {
        poolQueue = new OverflowQueueWithDMStats(INCOMING_QUEUE_LIMIT, this.stats.getOverflowQueueHelper());
      }
      ThreadFactory tf = new ThreadFactory() {
          private int next = 0;

          public Thread newThread(final Runnable command) {
            DistributionManager.this.stats.incProcessingThreadStarts();
            final Runnable r = new Runnable() {
                public void run() {
                  DistributionManager.this.stats.incNumProcessingThreads(1);
                  try {
                  ConnectionTable.threadWantsSharedResources();
                  Connection.makeReaderThread();
                  runUntilShutdown(command);
                  } finally {
                    ConnectionTable.releaseThreadsSockets();
                    DistributionManager.this.stats.incNumProcessingThreads(-1);
                  }
                }
              };
            Thread thread = new Thread(group, r, 
               LocalizedStrings.DistributionManager_POOLED_MESSAGE_PROCESSOR.toLocalizedString() + (next++));
            thread.setDaemon(true);
            return thread;
          }
        };
      ThreadPoolExecutor pool =
        new PooledExecutorWithDMStats(poolQueue, MAX_THREADS, this.stats.getNormalPoolHelper(), tf);
      this.threadPool = pool;
    }


    {
      BlockingQueue poolQueue;
      if (INCOMING_QUEUE_LIMIT == 0) {
        poolQueue = new OverflowQueueWithDMStats(this.stats.getHighPriorityQueueHelper());
      } else {
        poolQueue = new OverflowQueueWithDMStats(INCOMING_QUEUE_LIMIT, this.stats.getHighPriorityQueueHelper());
      }
      ThreadFactory tf = new ThreadFactory() {
          private int next = 0;

          public Thread newThread(final Runnable command) {
            DistributionManager.this.stats.incHighPriorityThreadStarts();
            final Runnable r = new Runnable() {
                public void run() {
                  DistributionManager.this.stats.incHighPriorityThreads(1);
                  try {
                    ConnectionTable.threadWantsSharedResources();
                    Connection.makeReaderThread();
                    runUntilShutdown(command);
                  } finally {
                    ConnectionTable.releaseThreadsSockets();
                    DistributionManager.this.stats.incHighPriorityThreads(-1);
                  }
                }
              };
            Thread thread = new Thread(group, r, 
                LocalizedStrings.DistributionManager_POOLED_HIGH_PRIORITY_MESSAGE_PROCESSOR.toLocalizedString() + (next++));
            thread.setDaemon(true);
            return thread;
          }
        };
      this.highPriorityPool = new PooledExecutorWithDMStats(poolQueue, MAX_THREADS, this.stats.getHighPriorityPoolHelper(), tf);
    }


    {
      ThreadFactory tf = new ThreadFactory() {
          private int next = 0;

          public Thread newThread(final Runnable command) {
            DistributionManager.this.stats.incWaitingThreadStarts();
            final Runnable r = new Runnable() {
                public void run() {
                  DistributionManager.this.stats.incWaitingThreads(1);
                  try {
                  ConnectionTable.threadWantsSharedResources();
                  Connection.makeReaderThread();
                  runUntilShutdown(command);
                  } finally {
                   ConnectionTable.releaseThreadsSockets();
                   DistributionManager.this.stats.incWaitingThreads(-1);
                  }
                }
              };
            Thread thread = new Thread(group, r, 
                                       LocalizedStrings.DistributionManager_POOLED_WAITING_MESSAGE_PROCESSOR.toLocalizedString() + (next++));
            thread.setDaemon(true);
            return thread;
          }
        };
      BlockingQueue poolQueue;
      if (MAX_WAITING_THREADS == Integer.MAX_VALUE) {
        // no need for a queue since we have infinite threads
        poolQueue = new SynchronousQueue();
      } else {
        poolQueue = new OverflowQueueWithDMStats(this.stats.getWaitingQueueHelper());
      }
      this.waitingPool = new PooledExecutorWithDMStats(poolQueue,
                                                       MAX_WAITING_THREADS,
                                                       this.stats.getWaitingPoolHelper(),
                                                       tf);
    }
    
    {
      ThreadFactory tf = new ThreadFactory() {
          private int next = 0;

          public Thread newThread(final Runnable command) {
            DistributionManager.this.stats.incWaitingThreadStarts();//will it be ok?
            final Runnable r = new Runnable() {
                public void run() {
                  DistributionManager.this.stats.incWaitingThreads(1);//will it be ok
                  try {
                  ConnectionTable.threadWantsSharedResources();
                  Connection.makeReaderThread();
                  runUntilShutdown(command);
                  } finally {
                   ConnectionTable.releaseThreadsSockets();
                   DistributionManager.this.stats.incWaitingThreads(-1);
                  }
                }
              };
            Thread thread = new Thread(group, r, 
                                       LocalizedStrings.DistributionManager_PR_META_DATA_CLEANUP_MESSAGE_PROCESSOR.toLocalizedString() + (next++));
            thread.setDaemon(true);
            return thread;
          }
        };
      BlockingQueue poolQueue;
      poolQueue = new OverflowQueueWithDMStats(this.stats.getWaitingQueueHelper());
      this.prMetaDataCleanupThreadPool = new PooledExecutorWithDMStats(poolQueue,
                                                        MAX_PR_META_DATA_CLEANUP_THREADS,
                                                       this.stats.getWaitingPoolHelper(),
                                                       tf);
    }

    {
      BlockingQueue poolQueue;
      if (INCOMING_QUEUE_LIMIT == 0) {
        poolQueue = new OverflowQueueWithDMStats(this.stats.getPartitionedRegionQueueHelper());
      } else {
        poolQueue = new OverflowQueueWithDMStats(INCOMING_QUEUE_LIMIT, this.stats.getPartitionedRegionQueueHelper());
      }
      ThreadFactory tf = new ThreadFactory() {
        private int next = 0;

        public Thread newThread(final Runnable command) {
          DistributionManager.this.stats.incPartitionedRegionThreadStarts();
          final Runnable r = new Runnable() {
              public void run() {
                stats.incPartitionedRegionThreads(1);
                try {
                  ConnectionTable.threadWantsSharedResources();
                  Connection.makeReaderThread();
                  runUntilShutdown(command);
                } finally {
                  ConnectionTable.releaseThreadsSockets();
                  stats.incPartitionedRegionThreads(-1);
                }
              }
            };
          Thread thread = new Thread(group, r, 
                                     "PartitionedRegion Message Processor" + (next++));
          thread.setDaemon(true);
          return thread;
        }
      };
      if (MAX_PR_THREADS > 1) {
        this.partitionedRegionPool = new PooledExecutorWithDMStats(poolQueue, 
            MAX_PR_THREADS, this.stats.getPartitionedRegionPoolHelper(), tf);
      } else {
        SerialQueuedExecutorWithDMStats executor = new SerialQueuedExecutorWithDMStats(poolQueue, 
            this.stats.getPartitionedRegionPoolHelper(), tf);
        this.partitionedRegionThread = executor;
      }
      
    }

    {
      BlockingQueue poolQueue;
      if (INCOMING_QUEUE_LIMIT == 0) {
        poolQueue = new OverflowQueueWithDMStats(this.stats.getFunctionExecutionQueueHelper());
      } else {
        poolQueue = new OverflowQueueWithDMStats(INCOMING_QUEUE_LIMIT, this.stats.getFunctionExecutionQueueHelper());
      }
      ThreadFactory tf = new ThreadFactory() {
        private int next = 0;

        public Thread newThread(final Runnable command) {
          DistributionManager.this.stats.incFunctionExecutionThreadStarts();
          final Runnable r = new Runnable() {
              public void run() {
                stats.incFunctionExecutionThreads(1);
                isFunctionExecutionThread.set(Boolean.TRUE);
                try {
                  ConnectionTable.threadWantsSharedResources();
                  Connection.makeReaderThread();
                  runUntilShutdown(command);
                } finally {
                  ConnectionTable.releaseThreadsSockets();
                  stats.incFunctionExecutionThreads(-1);
                }
              }
            };
          Thread thread = new Thread(group, r, 
                                     "Function Execution Processor" + (next++));
          thread.setDaemon(true);
          return thread;
        }
      };
      
      if(MAX_FE_THREADS > 1){
        this.functionExecutionPool = new FunctionExecutionPooledExecutor(poolQueue, 
            MAX_FE_THREADS, this.stats.getFunctionExecutionPoolHelper(), tf,true /*for fn exec*/);
      } else {
        SerialQueuedExecutorWithDMStats executor = new SerialQueuedExecutorWithDMStats(poolQueue, 
            this.stats.getFunctionExecutionPoolHelper(), tf);
        this.functionExecutionThread = executor;
      }
    
    }
    
    if (!SYNC_EVENTS) {
      this.memberEventThread = new Thread(group, new MemberEventInvoker(), 
          "DM-MemberEventInvoker");
      this.memberEventThread.setDaemon(true);
    }

    StringBuffer sb = new StringBuffer(" (took ");

   long start = System.currentTimeMillis();
    
    // Create direct channel first
//    DirectChannel dc = new DirectChannel(new MyListener(this), system.getConfig(), logger, null);
//    setDirectChannelPort(dc.getPort()); // store in a thread local

    // connect to JGroups
    start = System.currentTimeMillis();
    
    MyListener l = new MyListener(this);
    membershipManager = MemberFactory.newMembershipManager(l, system.getConfig(), transport, stats);

    sb.append(System.currentTimeMillis() - start);

    this.myid = membershipManager.getLocalMember();

//    dc.patchUpAddress(this.myid);
//    id.setDirectChannelPort(dc.getPort());

    // create the distribution channel
    this.channel = new DistributionChannel(membershipManager);

    membershipManager.postConnect();
    
    //Assert.assertTrue(this.getChannelMap().size() >= 1);
    //       System.out.println("Channel Map:");
    //       for (Iterator iter = this.getChannelMap().entrySet().iterator();
    //            iter.hasNext(); ) {
    //         Map.Entry entry = (Map.Entry) iter.next();
    //         Object key = entry.getKey();
    //         System.out.println("  " + key + " a " +
    //                            key.getClass().getName() + " -> " +
    //                            entry.getValue());
    //       }

    sb.append(" ms)");

    logger.info(LocalizedMessage.create(LocalizedStrings.DistributionManager_STARTING_DISTRIBUTIONMANAGER_0_1,
        new Object[] { this.myid, (logger.isInfoEnabled(LogMarker.DM) ? sb.toString() : "")}));

    this.description = NAME + " on " + this.myid + " started at "
      + (new Date(System.currentTimeMillis())).toString();

    finishedConstructor = true;
    } finally {
      if (!finishedConstructor) {
        askThreadsToStop(); // fix for bug 42039
      }
    }
  }

  /**
   * Creates a new distribution manager
   *
   * @param system
   *        The distributed system to which this distribution manager
   *        will send messages.
   */
  private DistributionManager(
    InternalDistributedSystem system,
    RemoteTransportConfig transport)
  {
    this(transport, system);

    boolean finishedConstructor = false;
    try {

    isStartupThread.set(Boolean.TRUE);
    
    startThreads();
    
    // Since we need a StartupResponseMessage to make sure licenses
    // are compatible the following has been deadcoded.
//     // For the time being, invoke processStartupResponse()
//     String rejectionMessage = null;
//     if (GemFireVersion.getGemFireVersion().
//         equals(state.getGemFireVersion())) {
//       rejectionMessage = "Rejected new system node " +
//         this.getDistributionManagerId() + " with version \"" +
//         GemFireVersion.getGemFireVersion() +
//         "\" because the distributed system's version is \"" +
//         state.getGemFireVersion() + "\".";
//     }
//     this.processStartupResponse(state.getCacheTime(),
//                         rejectionMessage);

    // Allow events to start being processed.
    membershipManager.startEventProcessing();
    for (;;) {
      this.getCancelCriterion().checkCancelInProgress(null);
      boolean interrupted = Thread.interrupted();
      try {
        membershipManager.waitForEventProcessing();
        break;
      }
      catch (InterruptedException e) {
        interrupted = true;
      }
      finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
    
    synchronized (DistributionManager.class) {
      openDMs++;
    }
    finishedConstructor = true;
    } finally {
      if (!finishedConstructor) {
        askThreadsToStop(); // fix for bug 42039
      }
    }
  }

  ////////////////////  Instance Methods  /////////////////////

  /**
   * Returns true if the two members are on the same equivalent host based 
   * on overlapping IP addresses collected for all NICs during exchange of
   * startup messages.
   * 
   * @param member1 First member
   * @param member2 Second member
   */
  public boolean areOnEquivalentHost(InternalDistributedMember member1,
                                     InternalDistributedMember member2) {
    Set<InetAddress> equivalents1 = getEquivalents(member1.getInetAddress());
    return equivalents1.contains(member2.getInetAddress());
  }
  
  /**
   * Set the host equivalencies for a given host.  This overrides any
   * previous information in the tables.
   * @param equivs list of InetAddress's that all point at same host
   */
  public void setEquivalentHosts(Set<InetAddress> equivs) {
    Iterator<InetAddress> it = equivs.iterator();
    synchronized (equivalentHosts) {
     while (it.hasNext()) {
       equivalentHosts.put(it.next(), Collections.unmodifiableSet(equivs));
     }
    }
  }
  
  public HashMap<InetAddress, Set<InetAddress>> getEquivalentHostsSnapshot() {
    synchronized (this.equivalentHosts) {
      return new HashMap<InetAddress, Set<InetAddress>>(this.equivalentHosts);
    }
  }
  
  /**
   * Return all of the InetAddress's that are equivalent to the given one (same
   * host)
   * @param in host to match up
   * @return all the addresses thus equivalent
   */
  public Set<InetAddress> getEquivalents(InetAddress in) {
    Set<InetAddress> result;
    synchronized (equivalentHosts) {
      result = equivalentHosts.get(in);
    }
    //DS 11/25/08 - It appears that when using VPN, the distributed member
    //id is the vpn address, but that doesn't show up in the equivalents.
    if(result == null) {
      result = Collections.singleton(in);
    }
    return result;
  }
  
  public void setRedundancyZone(InternalDistributedMember member, String redundancyZone) {
    if(redundancyZone != null && !redundancyZone.equals("")) {
      this.redundancyZones.put(member, redundancyZone);
    }
    if (member != getDistributionManagerId()) {
      String relationship = areInSameZone(getDistributionManagerId(), member) ? ""
          : "not ";
      Object[] logArgs = new Object[] { member, relationship };
      logger.info(LocalizedMessage.create(LocalizedStrings.DistributionManager_DISTRIBUTIONMANAGER_MEMBER_0_IS_1_EQUIVALENT, logArgs));
    }
  }

  /**
   * Set the flag indicating that we should enforce unique zones.
   * If we are already enforcing unique zones, keep it that way.
   */
  public void setEnforceUniqueZone(boolean enforceUniqueZone) {
    this.enforceUniqueZone |= enforceUniqueZone;
  }
  
  public boolean enforceUniqueZone() {
    return enforceUniqueZone;
  }
  
  public String getRedundancyZone(InternalDistributedMember member) {
    return redundancyZones.get(member);
  }
  
  /**
   * Asserts that distributionManagerType is LOCAL, GEMFIRE, or
   * ADMIN_ONLY.  Also asserts that the distributionManagerId
   * (jgroups DistributedMember) has a VmKind that matches.
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
          "InternalDistributedMember has a vmKind of " + vmKind + 
          " instead of " + theDmType);
    }
  }

  public int getDMType() {
    return this.dmType;
  }
  
  public List<InternalDistributedMember> getViewMembers() {
    NetView result = null;
    DistributionChannel ch = this.channel;
    if (ch != null) {
      MembershipManager mgr = ch.getMembershipManager();
      if (mgr != null) {
        result = mgr.getView();
        }
    }
    if (result == null) {
      result = new NetView();
    }
    return result.getMembers();
  }
  /* implementation of DM.getOldestMember */
  public DistributedMember getOldestMember(Collection c) throws NoSuchElementException {
    List<InternalDistributedMember> view = getViewMembers();
    for (int i=0; i<view.size(); i++) {
      Object viewMbr = view.get(i);
      Iterator it = c.iterator();
      while (it.hasNext()) {
        Object nextMbr = it.next();
        if (viewMbr.equals(nextMbr)) {
          return (DistributedMember)nextMbr;
        }
      }
    }
    throw new NoSuchElementException(LocalizedStrings.DistributionManager_NONE_OF_THE_GIVEN_MANAGERS_IS_IN_THE_CURRENT_MEMBERSHIP_VIEW.toLocalizedString());
  }
  
  private boolean testMulticast() {
    return this.membershipManager.testMulticast();
  }

  /**
   * Print a membership view (list of {@link InternalDistributedMember}s)
   * 
   * @param v the list
   * @return String
   */
  static public String printView(NetView v) {
    if (v == null)
      return "null";
    
    return v.toString();
  }

  /**
   * Need to do this outside the constructor so that the child
   * constructor can finish.
   */
  protected void startThreads() {
    this.system.setDM(this); // fix for bug 33362
    if (this.memberEventThread != null)
      this.memberEventThread.start();
    try {
      
      // And the distinguished guests today are...
      NetView v = membershipManager.getView();
      logger.info(LocalizedMessage.create(LocalizedStrings.DistributionManager_INITIAL_MEMBERSHIPMANAGER_VIEW___0, printView(v)));
      
      // Add them all to our view
      Iterator<InternalDistributedMember> it = v.getMembers().iterator();
      while (it.hasNext()) {
        addNewMember(it.next());
      }
      
      // Figure out who the elder is...
      selectElder(); // ShutdownException could be thrown here
    } catch (Exception ex) {
      throw new InternalGemFireException(LocalizedStrings.DistributionManager_COULD_NOT_PROCESS_INITIAL_VIEW.toLocalizedString(), ex);
    }
    try {
      getWaitingThreadPool().execute(new Runnable() {
          public void run() {
            // call in background since it might need to send a reply
            // and we are not ready to send messages until startup is finished
            isStartupThread.set(Boolean.TRUE);
            readyForMessages();
          }
        });
    }
    catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    }
    catch (Throwable t) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      logger.fatal(LocalizedMessage.create(LocalizedStrings.DistributionManager_UNCAUGHT_EXCEPTION_CALLING_READYFORMESSAGES), t);
    }
  }

  protected void readyForMessages() {
    synchronized (this) {
      this.readyForMessages = true;
      this.notifyAll();
    }
    membershipManager.startEventProcessing();
  }
  
  protected void waitUntilReadyForMessages() {
    if (readyForMessages)
      return;
//    membershipManager.waitForEventProcessing();
    synchronized (this) {
      for (;;) {
        if (readyForMessages)
          break;
        stopper.checkCancelInProgress(null);
        boolean interrupted = Thread.interrupted();
        try {
          this.wait();
        }
        catch (InterruptedException e) {
          interrupted = true;
          stopper.checkCancelInProgress(e);
        }
        finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
      } // for
    } // synchronized
  }

  /**
   * Call when the DM is ready to send messages.
   */
  private void readyToSendMsgs() {
    synchronized (this.readyToSendMsgsLock) {
      this.readyToSendMsgs = true;
      this.readyToSendMsgsLock.notifyAll();
    }
  }
  /**
   * Return when DM is ready to send out messages.
   * @param msg the messsage that is currently being sent
   */
  protected void waitUntilReadyToSendMsgs(DistributionMessage msg) {
    if (this.readyToSendMsgs) {
      return;
    }
    // another process may have been started in the same view, so we need
    // to be responsive to startup messages and be able to send responses
    if (msg instanceof StartupMessage || msg instanceof StartupResponseMessage
        || msg instanceof AdminMessageType) {
      return;
    }
    if (isStartupThread.get() != null) {
      // let the startup thread send messages
      // the only case I know of that does this is if we happen to log a
      // message during startup and an alert listener has registered.
      return;
    }
//    membershipManager.waitForEventProcessing();
    synchronized (this.readyToSendMsgsLock) {
      for (;;) {
        if (this.readyToSendMsgs)
          break;
        stopper.checkCancelInProgress(null);
        boolean interrupted = Thread.interrupted();
        try {
          this.readyToSendMsgsLock.wait();
        }
        catch (InterruptedException e) {
          interrupted = true;
          stopper.checkCancelInProgress(e);
        }
        finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
      } // for
    } // synchronized
  }
  
  // DM method
  @Override
  public void forceUDPMessagingForCurrentThread() {
    membershipManager.forceUDPMessagingForCurrentThread();
  }
  
  // DM method
  @Override
  public void releaseUDPMessagingForCurrentThread() {
    membershipManager.releaseUDPMessagingForCurrentThread();
  }

  /**
   * Did an exception occur in one of the threads launched by this
   * distribution manager?
   */
  public boolean exceptionInThreads() {
    return this.exceptionInThreads || this.threadGroup.getUncaughtExceptionsCount() > 0;
  }

  /**
   * Clears the boolean that determines whether or not an exception
   * occurred in one of the worker threads.  This method should be
   * used for testing purposes only!
   */
  void clearExceptionInThreads() {
    this.exceptionInThreads = false;
    this.threadGroup.clearUncaughtExceptionsCount();
  }

  /**
   * Returns the current "cache time" in milliseconds since the epoch.
   * The "cache time" takes into account skew among the local clocks
   * on the various machines involved in the cache.
   */
  public long cacheTimeMillis() {
    return this.system.getClock().cacheTimeMillis();
  }



  /**
   * Returns the id of this distribution manager.
   */
  public InternalDistributedMember getDistributionManagerId() {
    return this.myid;
  }

  /**
   * Returns an unmodifiable set containing the identities of all of
   * the known (non-admin-only) distribution managers.
   */
  public Set getDistributionManagerIds() {
    // access to members synchronized under membersLock in order to 
    // ensure serialization
    synchronized (this.membersLock) {
      return this.members.keySet();
    }
  }
  
  /**
   * Adds the entry in {@link #hostedLocatorsAll} for a member with one or more
   * hosted locators. The value is a collection of host[port] strings. If a 
   * bind-address was used for a locator then the form is bind-addr[port].
   *
   * @since 6.6.3
   */
  public void addHostedLocators(InternalDistributedMember member, Collection<String> locators, boolean isSharedConfigurationEnabled) {
    synchronized (this.membersLock) {
      if (locators == null || locators.isEmpty()) {
        throw new IllegalArgumentException("Cannot use empty collection of locators");
      }
      if (this.hostedLocatorsAll.isEmpty()) {
        this.hostedLocatorsAll = new HashMap<InternalDistributedMember, Collection<String>>();
      }
      
      if (!this.isSharedConfigEnabledForDS) {
        this.isSharedConfigEnabledForDS = isSharedConfigurationEnabled;
      }
      
      Map<InternalDistributedMember, Collection<String>> tmp = 
          new HashMap<InternalDistributedMember, Collection<String>>(this.hostedLocatorsAll);
      tmp.remove(member);
      tmp.put(member, locators);
      tmp = Collections.unmodifiableMap(tmp);
      this.hostedLocatorsAll = tmp;
      
      if (isSharedConfigurationEnabled) {
        if (locators == null || locators.isEmpty()) {
          throw new IllegalArgumentException("Cannot use empty collection of locators");
        }
        if (this.hostedLocatorsWithSharedConfiguration.isEmpty()) {
          this.hostedLocatorsWithSharedConfiguration = new HashMap<InternalDistributedMember, Collection<String>>();
        }
        tmp = new HashMap<InternalDistributedMember, Collection<String>>(this.hostedLocatorsWithSharedConfiguration);
        tmp.remove(member);
        tmp.put(member, locators);
        tmp = Collections.unmodifiableMap(tmp);
        this.hostedLocatorsWithSharedConfiguration = tmp;
      }

    }
  }
  
  
  private void removeHostedLocators(InternalDistributedMember member) {
    synchronized (this.membersLock) {
      if (this.hostedLocatorsAll.containsKey(member)) {
        Map<InternalDistributedMember, Collection<String>> tmp = 
            new HashMap<InternalDistributedMember, Collection<String>>(this.hostedLocatorsAll);
        tmp.remove(member);
        if (tmp.isEmpty()) {
          tmp = Collections.emptyMap();
        } else {
          tmp = Collections.unmodifiableMap(tmp);
        }
        this.hostedLocatorsAll = tmp;
      }
      if (this.hostedLocatorsWithSharedConfiguration.containsKey(member)) {
        Map<InternalDistributedMember, Collection<String>> tmp = 
            new HashMap<InternalDistributedMember, Collection<String>>(this.hostedLocatorsWithSharedConfiguration);
        tmp.remove(member);
        if (tmp.isEmpty()) {
          tmp = Collections.emptyMap();
        } else {
          tmp = Collections.unmodifiableMap(tmp);
        }
        this.hostedLocatorsWithSharedConfiguration = tmp;
      }
    }
  }
  
  
  
  /**
   * Gets the value in {@link #hostedLocatorsAll} for a member with one or more
   * hosted locators. The value is a collection of host[port] strings. If a 
   * bind-address was used for a locator then the form is bind-addr[port].
   * 
   * @since 6.6.3
   */
  public Collection<String> getHostedLocators(InternalDistributedMember member) {
    synchronized (this.membersLock) {
      return this.hostedLocatorsAll.get(member);
    }
  }
  
  /**
   * Returns a copy of the map of all members hosting locators. The key is the 
   * member, and the value is a collection of host[port] strings. If a 
   * bind-address was used for a locator then the form is bind-addr[port].
   * 
   * @since 6.6.3
   */
  public Map<InternalDistributedMember, Collection<String>> getAllHostedLocators() {
    synchronized (this.membersLock) {
      return this.hostedLocatorsAll;
    }
  }
  /**
   * Returns a copy of the map of all members hosting locators with shared configuration. The key is the 
   * member, and the value is a collection of host[port] strings. If a 
   * bind-address was used for a locator then the form is bind-addr[port].
   * 
   * @since 8.0
   */
  @Override
  public Map<InternalDistributedMember, Collection<String>> getAllHostedLocatorsWithSharedConfiguration() {
    synchronized (this.membersLock) {
      return this.hostedLocatorsWithSharedConfiguration;
    }
  }
  /**
   * Returns an unmodifiable set containing the identities of all of
   * the known (including admin) distribution managers.
   */
  public Set getDistributionManagerIdsIncludingAdmin() {
    // access to members synchronized under membersLock in order to 
    // ensure serialization
    synchronized (this.membersLock) {
      return this.membersAndAdmin;
    }
  }
  

  /**
   * Returns the low-level distribution channel for this distribution
   * manager. (brought over from ConsoleDistributionManager)
   *
   * @since 4.0
   */
  public DistributionChannel getDistributionChannel() {
    return this.channel;
  }


  /**
   * Returns a private-memory list containing the identities of all
   * the other known distribution managers not including me.
   */
  public Set getOtherDistributionManagerIds() {
    // We return a modified copy of the list, so
    // collect the old list and copy under the lock.
    Set result = new HashSet(getDistributionManagerIds());

    InternalDistributedMember me = getDistributionManagerId();
    result.remove(me);

    // It's okay for my own id to not be in the list of all ids yet.
    return result;
  }
  @Override
  public Set getOtherNormalDistributionManagerIds() {
    // We return a modified copy of the list, so
    // collect the old list and copy under the lock.
    Set result = new HashSet(getNormalDistributionManagerIds());

    InternalDistributedMember me = getDistributionManagerId();
    result.remove(me);

    // It's okay for my own id to not be in the list of all ids yet.
    return result;
  }

  public InternalDistributedMember getCanonicalId(DistributedMember id) {
    // the members set is copy-on-write, so it is safe to iterate over it
    InternalDistributedMember result = this.members.get(id);
    if (result == null) {
      return (InternalDistributedMember)id;
    }
    return result;
  }

  /**
   * Add a membership listener and return other DistribtionManagerIds
   * as an atomic operation
   */
  public Set addMembershipListenerAndGetDistributionManagerIds(MembershipListener l) {
    // switched sync order to fix bug 30360
    synchronized (this.membersLock) {
      // Don't let the members come and go while we are adding this
      // listener.  This ensures that the listener (probably a
      // ReplyProcessor) gets a consistent view of the members.
      addMembershipListener(l);
      // Note it is ok to return the members set
      // because we will never modify the returned set.
      return members.keySet();
    }
  }

  public void addNewMember(InternalDistributedMember member) {
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
        throw new InternalGemFireError(LocalizedStrings.DistributionManager_UNKNOWN_MEMBER_TYPE_0.toLocalizedString(Integer.valueOf(vmType)));
    }
  }

   /**
   * Returns the identity of this <code>DistributionManager</code>
   */
  public InternalDistributedMember getId() {
    return this.myid;
  }

  /**
   * Returns the id of the underlying distribution channel used for
   * communication.
   *
   * @since 3.0
   */
  public long getChannelId() {
    return this.channel.getId();
  }

  /**
   * Adds a message to the outgoing queue.  Note that
   * <code>message</code> should not be modified after it has been
   * added to the queue.  After <code>message</code> is distributed,
   * it will be recycled.
   *
   * @return list of recipients who did not receive the message
   * @throws NotSerializableException if the content is not serializable
   */
  public Set putOutgoingUserData(final DistributionMessage message) 
      throws NotSerializableException {
    return sendMessage(message); 
  }

  /**
   * Send outgoing data; message is guaranteed to be serialized.
   * @return list of recipients who did not receive the message
   * @throws InternalGemFireException if message is not serializable
   */
  public Set putOutgoing(final DistributionMessage msg) {
    try {
      DistributionMessageObserver observer = DistributionMessageObserver.getInstance();
      if(observer != null) {
        observer.beforeSendMessage(this, msg);
      }
      return sendMessage(msg);
    }
    catch (NotSerializableException e) {
      throw new InternalGemFireException(e);
    }
    catch (ToDataException e) {
      // exception from user code
      throw e;
    }
  }

  @Override
  public String toString() {
    return this.description;
  }

  /**
   * @see #closeInProgress
   */
  private final Object shutdownMutex = new Object();
  
  /**
   * Informs other members that this dm is shutting down.
   * Stops the pusher, puller, and processor threads and closes the
   * connection to the transport layer.
   */
  protected void shutdown() {
    // Make sure only one thread initiates shutdown...
    synchronized (shutdownMutex) {
      if (closeInProgress) {
        return;
      }
      this.closeInProgress = true;
    } // synchronized

    // [bruce] log shutdown at info level and with ID to balance the
    // "Starting" message.  recycleConn.conf is hard to debug w/o this
    final String exceptionStatus = (this.exceptionInThreads() ? LocalizedStrings.DistributionManager_AT_LEAST_ONE_EXCEPTION_OCCURRED.toLocalizedString() : "");
    logger.info(LocalizedMessage.create(
        LocalizedStrings.DistributionManager_SHUTTING_DOWN_DISTRIBUTIONMANAGER_0_1,
        new Object[] {this.myid, exceptionStatus}));

    final long start = System.currentTimeMillis();
    try {
      if (this.rootCause instanceof ForcedDisconnectException) {
        if (logger.isDebugEnabled()) {
          logger.debug("inhibiting sending of shutdown message to other members due to forced-disconnect");
        }
      } else {
        // Don't block indefinitely trying to send the shutdown message, in
        // case other VMs in the system are ill-behaved. (bug 34710)
        final Runnable r = new Runnable() {
          public void run() {
            try {
              ConnectionTable.threadWantsSharedResources();
              sendShutdownMessage();
            }
            catch (final CancelException e) {
              // We were terminated.
              logger.debug("Cancelled during shutdown message", e);
            }
          }
        };
        final Thread t = new Thread(threadGroup,
            r, LocalizedStrings.DistributionManager_SHUTDOWN_MESSAGE_THREAD_FOR_0.toLocalizedString(this.myid));
        t.start();
        boolean interrupted = Thread.interrupted();
        try {
          t.join(MAX_STOP_TIME);
        }
        catch (final InterruptedException e) {
          interrupted = true;
          t.interrupt();
          logger.warn(LocalizedMessage.create(
              LocalizedStrings.DistributionManager_INTERRUPTED_SENDING_SHUTDOWN_MESSAGE_TO_PEERS), e);
        }
        finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }

        if (t.isAlive()) {
          t.interrupt();
          logger.warn(LocalizedMessage.create(LocalizedStrings.DistributionManager_FAILED_SENDING_SHUTDOWN_MESSAGE_TO_PEERS_TIMEOUT));
        }
      }
      
    }
    finally {
      this.shutdownMsgSent = true; // in case sendShutdownMessage failed....
      try {
        this.uncleanShutdown(false);
      }
      finally {
        final Long delta = Long.valueOf(System.currentTimeMillis() - start);
        logger.info(LocalizedMessage.create(
          LocalizedStrings.DistributionManager_DISTRIBUTIONMANAGER_STOPPED_IN_0_MS, delta));
      }
    }
  }
  
  private void askThreadsToStop() {
    // Stop executors after they have finished
    ExecutorService es;
    es = this.serialThread;
    if (es != null) {
      es.shutdown();
    }
    es = this.viewThread;
    if (es != null) {
      // Hmmm...OK, I'll let any view events currently in the queue be
      // processed.  Not sure it's very important whether they get
      // handled...
      es.shutdown();
    }
    if (this.serialQueuedExecutorPool != null) {
      this.serialQueuedExecutorPool.shutdown();
    }
    es = this.functionExecutionThread;
    if (es != null) {
      es.shutdown();
    }
    es = this.functionExecutionPool;
    if (es != null) {
      es.shutdown();
    }
    es = this.partitionedRegionThread;
    if (es != null) {
      es.shutdown();
    }
    es = this.partitionedRegionPool;
    if (es != null) {
      es.shutdown();
    }
    es = this.highPriorityPool;
    if (es != null) {
      es.shutdown();
    }
    es = this.waitingPool;
    if (es != null) {
      es.shutdown();
    }
    es = this.prMetaDataCleanupThreadPool;
    if (es != null) {
      es.shutdown();
    }
    es = this.threadPool;
    if (es != null) {
      es.shutdown();
    }
    
    Thread th = this.memberEventThread;
    if (th != null)
      th.interrupt();
  }
  
  private void waitForThreadsToStop(long timeInMillis) throws InterruptedException {
    long start = System.currentTimeMillis();
    long remaining = timeInMillis;
    
    ExecutorService[] allExecutors = new ExecutorService[] {
        this.serialThread, 
        this.viewThread, 
        this.functionExecutionThread, 
        this.functionExecutionPool,
        this.partitionedRegionThread, 
        this.partitionedRegionPool,
        this.highPriorityPool,
        this.waitingPool,
        this.prMetaDataCleanupThreadPool,
        this.threadPool};
    for(ExecutorService es : allExecutors) { 
      if (es != null) {
        es.awaitTermination(remaining, TimeUnit.MILLISECONDS);
      }
      remaining = timeInMillis - (System.currentTimeMillis() - start);
      if(remaining <= 0) {
        return;
      }
    }
    
    
    this.serialQueuedExecutorPool.awaitTermination(remaining, TimeUnit.MILLISECONDS);
    remaining = timeInMillis - (System.currentTimeMillis() - start);
    if(remaining <= 0) {
      return;
    }
    Thread th = this.memberEventThread;
    if (th != null) {
      th.interrupt(); // bug #43452 - this thread sometimes eats interrupts, so we interrupt it again here
      th.join(remaining);
    }
    
  }
  
  /**
   * maximum time, in milliseconds, to wait for all threads to exit
   */
  static private final int MAX_STOP_TIME = 20000;
  
  /**
   * Time to sleep, in milliseconds, while polling to see if threads have 
   * finished
   */
  static private final int STOP_PAUSE_TIME = 1000;
  
  /**
   * Maximum number of interrupt attempts to stop a thread
   */
  static private final int MAX_STOP_ATTEMPTS = 10;
  
  /**
   * Cheap tool to kill a referenced thread
   * 
   * @param t the thread to kill
   */
  private void clobberThread(Thread t) {
    if (t == null)
      return;
    if (t.isAlive()) {
      logger.warn(LocalizedMessage.create(LocalizedStrings.DistributionManager_FORCING_THREAD_STOP_ON__0_, t));
      
      // Start by being nice.
      t.interrupt();
      
// we could be more violent here...
//      t.stop();
      try {
        for (int i = 0; i < MAX_STOP_ATTEMPTS && t.isAlive(); i++) {
          t.join(STOP_PAUSE_TIME);
          t.interrupt();
        }
      }
      catch (InterruptedException ex) {
        logger.debug("Interrupted while attempting to terminate threads.");
        Thread.currentThread().interrupt();
        // just keep going
      }
      
      if (t.isAlive()) {
        logger.warn(LocalizedMessage.create(LocalizedStrings.DistributionManager_CLOBBERTHREAD_THREAD_REFUSED_TO_DIE__0, t));
      }
    }
  }
  
  /**
   * Cheap tool to examine an executor to see if it is still working
   * @param tpe
   * @return true if executor is still active
   */
  private boolean executorAlive(ThreadPoolExecutor tpe, String name)
  {
    if (tpe == null) {
      return false;
    } else {
      int ac = tpe.getActiveCount();
//      boolean result = tpe.getActiveCount() > 0;
      if (ac > 0) {
        if (logger.isDebugEnabled()) {
          logger.debug("Still waiting for {} threads in '{}' pool to exit", ac, name);
        }
        return true;
      } else {
        return false;
      }
    }
  }
  
  /**
   * Wait for the ancillary queues to exit.  Kills them if they are
   * still around.
   *
   */
  private void forceThreadsToStop() {
    long endTime = System.currentTimeMillis() + MAX_STOP_TIME;
    String culprits = "";
    for (;;) {
      boolean stillAlive = false;
      culprits = "";
      if (executorAlive(this.serialThread, "serial thread")) {
        stillAlive = true;
        culprits = culprits + " serial thread;";
      }
      if (executorAlive(this.viewThread, "view thread")) {
        stillAlive = true;
        culprits = culprits + " view thread;";
      }
      if (executorAlive(this.partitionedRegionThread, "partitioned region thread")) {
        stillAlive = true;
        culprits = culprits + " partitioned region thread;";
      }
      if (executorAlive(this.partitionedRegionPool, "partitioned region pool")) {
        stillAlive = true;
        culprits = culprits + " partitioned region pool;";
      }
      if (executorAlive(this.highPriorityPool, "high priority pool")) {
        stillAlive = true;
        culprits = culprits + " high priority pool;";
      }
      if (executorAlive(this.waitingPool, "waiting pool")) {
        stillAlive = true;
        culprits = culprits + " waiting pool;";
      }
      if (executorAlive(this.prMetaDataCleanupThreadPool, "prMetaDataCleanupThreadPool")) {
        stillAlive = true;
        culprits = culprits + " special waiting pool;";
      }
      if (executorAlive(this.threadPool, "thread pool")) {
        stillAlive = true;
        culprits = culprits + " thread pool;";
      }
      
      if (!stillAlive)
        return;
      
      long now = System.currentTimeMillis();
      if (now >= endTime)
        break;
      
      try {
        Thread.sleep(STOP_PAUSE_TIME);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        // Desperation, the shutdown thread is being killed.  Don't
        // consult a CancelCriterion.
        logger.warn(LocalizedMessage.create(
            LocalizedStrings.DistributionManager_INTERRUPTED_DURING_SHUTDOWN), e); 
        break;
      }
    } // for
    
    logger.warn(LocalizedMessage.create(
        LocalizedStrings.DistributionManager_DAEMON_THREADS_ARE_SLOW_TO_STOP_CULPRITS_INCLUDE_0, culprits));
    
    // Kill with no mercy
    if (this.serialThread != null) {
      this.serialThread.shutdownNow();
    }
    if (this.viewThread != null) {
      this.viewThread.shutdownNow();
    }    
    if (this.functionExecutionThread != null) {
      this.functionExecutionThread.shutdownNow();
    }
    if (this.functionExecutionPool != null) {
      this.functionExecutionPool.shutdownNow();
    }
    if (this.partitionedRegionThread != null) {
      this.partitionedRegionThread.shutdownNow();
    }
    if (this.partitionedRegionPool != null) {
      this.partitionedRegionPool.shutdownNow();
    }
    if (this.highPriorityPool != null) {
      this.highPriorityPool.shutdownNow();
    }
    if (this.waitingPool != null) {
      this.waitingPool.shutdownNow();
    }
    if (this.prMetaDataCleanupThreadPool != null) {
      this.prMetaDataCleanupThreadPool.shutdownNow();
    }
    if (this.threadPool != null) {
      this.threadPool.shutdownNow();
    }
    
    Thread th = this.memberEventThread;
    if (th != null) {
      clobberThread(th);
    }
  }
  
  private volatile boolean shutdownInProgress = false;

  /** guard for membershipViewIdAcknowledged */
  private final Object membershipViewIdGuard = new Object();
  
  /** the latest view ID that has been processed by all membership listeners */
  private long membershipViewIdAcknowledged;
  
  public boolean shutdownInProgress() {
    return this.shutdownInProgress;
  }
  
  /**
   * Stops the pusher, puller and processor threads and closes the
   * connection to the transport layer.  This should only be used from
   * shutdown() or from the dm initialization code
   */
  private void uncleanShutdown(boolean duringStartup)
  {
    try {
      this.closeInProgress = true; // set here also to fix bug 36736
      removeAllHealthMonitors();
      shutdownInProgress = true;
      if (this.channel != null) {
        this.channel.setShutDown();
      }
      
      askThreadsToStop();

      // wait a moment before asking threads to terminate
      try { waitForThreadsToStop(1000); } 
      catch (InterruptedException ie) {
        // No need to reset interrupt bit, we're really trying to quit...
      }
      forceThreadsToStop();
      
//      // bug36329: desperation measure, send a second interrupt?
//      try { Thread.sleep(1000); } 
//      catch (InterruptedException ie) {
//        // No need to reset interrupt bit, we're really trying to quit...
//      }
//      forceThreadsToStop();
    } // try
    finally {
      // ABSOLUTELY ESSENTIAL that we close the distribution channel!
      try {
        // For safety, but channel close in a finally AFTER this...
        if (this.stats != null) {
          this.stats.close();
          try { Thread.sleep(100); } 
          catch (InterruptedException ie) {
            // No need to reset interrupt bit, we're really trying to quit...
          }
        }
      }
      finally {
        if (this.channel != null) {
          logger.info(LocalizedMessage.create(LocalizedStrings.DistributionManager_NOW_CLOSING_DISTRIBUTION_FOR__0, this.myid));
          this.channel.disconnect(duringStartup);
//          this.channel = null;  DO NOT NULL OUT INSTANCE VARIABLES AT SHUTDOWN - bug #42087
        }
      }
    }
  }

  /**
   * Returns the distributed system to which this distribution manager
   * is connected.
   */
  public InternalDistributedSystem getSystem() {
    return this.system;
  }
  
  /**
   * Returns the transport configuration for this distribution manager
   * @since 5.0
   */
  public RemoteTransportConfig getTransport() {
    return this.transport;
  }


  /**
   * Adds a <code>MembershipListener</code> to this distribution
   * manager.
   */
  public void addMembershipListener(MembershipListener l) {
    this.membershipListeners.putIfAbsent(l, Boolean.TRUE);
  }

  /**
   * Removes a <code>MembershipListener</code> from this distribution
   * manager.
   *
   * @throws IllegalArgumentException
   *         <code>l</code> was not registered on this distribution
   *         manager
   */
  public void removeMembershipListener(MembershipListener l) {
    this.membershipListeners.remove(l);
  }

  /**
   * Adds a <code>MembershipListener</code> to this distribution
   * manager.
   * @since 5.7
   */
  public void addAllMembershipListener(MembershipListener l) {
    synchronized (this.allMembershipListenersLock) {
      Set newAllMembershipListeners =
        new HashSet(this.allMembershipListeners);
      newAllMembershipListeners.add(l);
      this.allMembershipListeners = newAllMembershipListeners;
    }
  }

  /**
   * Removes a <code>MembershipListener</code> listening for all members
   * from this distribution manager.
   *
   * @throws IllegalArgumentException
   *         <code>l</code> was not registered on this distribution
   *         manager
   * @since 5.7
   */
  public void removeAllMembershipListener(MembershipListener l) {
    synchronized (this.allMembershipListenersLock) {
      Set newAllMembershipListeners =
        new HashSet(this.allMembershipListeners);
      if (!newAllMembershipListeners.remove(l)) {
        // There seems to be a race condition in which
        // multiple departure events can be registered
        // on the same peer.  We regard this as benign.
        // FIXME when membership events become sane again
//        String s = "MembershipListener was never registered";
//        throw new IllegalArgumentException(s);
      }
      this.allMembershipListeners = newAllMembershipListeners;
    }
  }

  /**
   * Returns true if this DM or the DistributedSystem owned by
   * it is closing or is closed.
   */
  private boolean isCloseInProgress() {
    if (closeInProgress) {
      return true;
    }
    InternalDistributedSystem ds = getSystem();
    if (ds != null && ds.isDisconnecting()) {
      return true;
    }
    return false;
  }
  
  private void handleViewInstalledEvent(ViewInstalledEvent ev) {
    synchronized(this.membershipViewIdGuard) {
      this.membershipViewIdAcknowledged = ev.getViewId();
      this.membershipViewIdGuard.notifyAll();
    }
  }
  
  /**
   * This stalls waiting for the current membership view (as seen by the
   * membership manager) to be acknowledged by all membership listeners
   */
  public void waitForViewInstallation(long id) throws InterruptedException {
    if (id <= this.membershipViewIdAcknowledged) {
      return;
    }
    synchronized(this.membershipViewIdGuard) { 
      while (this.membershipViewIdAcknowledged < id && this.stopper.cancelInProgress() == null) {
        if (logger.isDebugEnabled()) {
          logger.debug("waiting for view {}.  Current DM view processed by all listeners is {}", id, this.membershipViewIdAcknowledged);
        }
        this.membershipViewIdGuard.wait();
      }
    }
  }

  protected void handleMemberEvent(MemberEvent ev) {
    ev.handleEvent(this);
  }

  /**
   * This thread processes member events as they occur.
   * 
   * @see com.gemstone.gemfire.distributed.internal.DistributionManager.MemberCrashedEvent
   * @see com.gemstone.gemfire.distributed.internal.DistributionManager.MemberJoinedEvent
   * @see com.gemstone.gemfire.distributed.internal.DistributionManager.MemberDepartedEvent
   *
   */
  protected class MemberEventInvoker implements Runnable {


    @SuppressWarnings("synthetic-access")
    public void run() {
      for (;;) {
        SystemFailure.checkFailure(); 
        // bug 41539 - member events need to be delivered during shutdown
        //             or reply processors may hang waiting for replies from
        //             departed members
//        if (getCancelCriterion().cancelInProgress() != null) {
//          break; // no message, just quit
//        }
        if (!DistributionManager.this.system.isConnected &&
            DistributionManager.this.isClosed()) {
          break;
        }
        try {
          MemberEvent ev = (MemberEvent)DistributionManager.this
              .membershipEventQueue.take();
          handleMemberEvent(ev);
        }
        catch (InterruptedException e) {
          if (isCloseInProgress()) {
            if (logger.isTraceEnabled()) {
              logger.trace("MemberEventInvoker: InterruptedException during shutdown");
            }
          }
          else {
            logger.warn(LocalizedMessage.create(LocalizedStrings.DistributionManager_UNEXPECTED_INTERRUPTEDEXCEPTION), e);
          }
          break;
        }
        catch (DistributedSystemDisconnectedException e) {
          break;
        }
        catch (CancelException e) {
          if (isCloseInProgress()) {
            if (logger.isTraceEnabled()) {
              logger.trace("MemberEventInvoker: cancelled");
            }
          }
          else {
            logger.warn(LocalizedMessage.create(LocalizedStrings.DistributionManager_UNEXPECTED_CANCELLATION), e);
          }
          break;
        }
        catch (Exception e) {
          logger.fatal(LocalizedMessage.create(LocalizedStrings.DistributionManager_UNCAUGHT_EXCEPTION_PROCESSING_MEMBER_EVENT), e);
        }
      } // for
      if (logger.isTraceEnabled()) {
        logger.trace("MemberEventInvoker on {} stopped", DistributionManager.this);
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
        this.membershipEventQueue.put(ev);
      } catch (InterruptedException ex) {
        interrupted = true;
        stopper.checkCancelInProgress(ex);
        handleMemberEvent(ev); // FIXME why???
      }
      finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }
  

  /**
   * Stops the threads associated with this distribution manager and
   * closes the connection to the transport layer.
   */
  public void close() {
    if (!closed) {
      this.shutdown();
      logger.info(LocalizedMessage.create(
        LocalizedStrings.DistributionManager_MARKING_DISTRIBUTIONMANAGER_0_AS_CLOSED, this.myid));
      MembershipLogger.logShutdown(this.myid);
      closed = true;
      synchronized (DistributionManager.class) {
        openDMs--;
      }
    }
  }

  public void throwIfDistributionStopped() {
    if (this.shutdownMsgSent) {
      throw new DistributedSystemDisconnectedException(LocalizedStrings.DistributionManager_MESSAGE_DISTRIBUTION_HAS_TERMINATED.toLocalizedString(), this.getRootCause());
    }
  }
  
  /**
   * Returns true if this distribution manager has been closed.
   */
  public boolean isClosed() {
    return this.closed;
  }

  /**
   * Makes note of a new administration console (admin-only member).
   */
  public void addAdminConsole(InternalDistributedMember theId) {
    logger.info(LocalizedMessage.create(
        LocalizedStrings.DistributionManager_NEW_ADMINISTRATION_MEMBER_DETECTED_AT_0, theId));
    synchronized(this.adminConsolesLock) {
      HashSet tmp = new HashSet(this.adminConsoles);
      tmp.add(theId);
      this.adminConsoles = Collections.unmodifiableSet(tmp);
    }
  }

  public DMStats getStats() {
    return this.stats;
  }
  
  public DistributionConfig getConfig() {
    DistributionConfig result = null;
    InternalDistributedSystem sys = getSystem();
    if (sys != null) {
      result = system.getConfig();
    }
    return result;
  }

//   /**
//    * Initializes and returns a <code>DistributedSystem</code> to be
//    * sent to new members of the distributed system.
//    *
//    * @since 3.0
//    */
//   protected DistributedState getNewDistributedState() {
//     DistributedState state = new DistributedState();
//     state.setGemFireVersion(GemFireVersion.getGemFireVersion());
//     state.setCacheTime(this.cacheTimeMillis());
//     return state;
//}
  
  private static final int STARTUP_TIMEOUT =
  Integer.getInteger("DistributionManager.STARTUP_TIMEOUT", 15000).intValue();

  public static final boolean DEBUG_NO_ACKNOWLEDGEMENTS = Boolean.getBoolean("DistributionManager.DEBUG_NO_ACKNOWLEDGEMENTS");

  public Set getAllOtherMembers() {
    Set result = new HashSet(getDistributionManagerIdsIncludingAdmin());
    result.remove(getDistributionManagerId());
    return result;
  }
  
  @Override // DM method
  public void retainMembersWithSameOrNewerVersion(Collection<InternalDistributedMember> members, Version version) {
    for (Iterator<InternalDistributedMember> it = members.iterator(); it.hasNext(); ) {
      InternalDistributedMember id = it.next();
      if (id.getVersionObject().compareTo(version) < 0) {
        it.remove();
      }
    }
  }
  
  @Override // DM method
  public void removeMembersWithSameOrNewerVersion(Collection<InternalDistributedMember> members, Version version) {
    for (Iterator<InternalDistributedMember> it = members.iterator(); it.hasNext(); ) {
      InternalDistributedMember id = it.next();
      if (id.getVersionObject().compareTo(version) >= 0) {
        it.remove();
      }
    }
  }
  
  /**
   * Add a membership listener for all members
   * and return other DistribtionManagerIds as an atomic operation
   * @since 5.7
   */
  public Set addAllMembershipListenerAndGetAllIds(MembershipListener l) {
    // TO fix this deadlock:
    // "View Message Processor":
    //  waiting to lock monitor 0x080f691c (object 0xe3ba7680, a com.gemstone.gemfire.distributed.internal.DistributionManager$MembersLock),
    //  which is held by "RMI TCP Connection(259)-10.80.10.55"
    // "RMI TCP Connection(259)-10.80.10.55":
    //  waiting to lock monitor 0x080f6598 (object 0xe3bacd90, a com.gemstone.gemfire.distributed.internal.membership.jgroup.JGroupMembershipManager$ViewLock),
    //  which is held by "View Message Processor"
    // NEED to prevent view changes while installing a listener.
    DistributionChannel ch = this.channel;
    if (ch != null) {
      MembershipManager mgr = ch.getMembershipManager();
      if (mgr != null) {
        mgr.getViewLock().writeLock().lock();
        try {
          synchronized (this.membersLock) {
            // Don't let the members come and go while we are adding this
            // listener.  This ensures that the listener (probably a
            // ReplyProcessor) gets a consistent view of the members.
            addAllMembershipListener(l);
            return getDistributionManagerIdsIncludingAdmin();
          }
        } finally {
          mgr.getViewLock().writeLock().unlock();
        }
      }
    }
    // If we have no channel or MembershipManager then the view is empty
    synchronized (this.membersLock) {
      // Don't let the members come and go while we are adding this
      // listener.  This ensures that the listener (probably a
      // ReplyProcessor) gets a consistent view of the members.
      addAllMembershipListener(l);
      return Collections.EMPTY_SET;
    }
  }
  
  /**
   * Sends a startup message and waits for a response.
   * Returns true if response received; false if it timed out or there are no peers.
   */
  protected boolean sendStartupMessage(StartupOperation op, boolean cancelOnTimeout)
    throws InterruptedException
  {
    if (Thread.interrupted()) throw new InterruptedException();
    this.receivedStartupResponse = false;
    boolean ok = false;

    // Be sure to add ourself to the equivalencies list!
    Set equivs = StartupMessage.getMyAddresses(this);
    if (equivs == null || equivs.size() == 0) {
      // no network interface
      equivs = new HashSet();
      try {
        equivs.add(SocketCreator.getLocalHost());
      } catch (UnknownHostException e) {
        // can't even get localhost
        if (getViewMembers().size() > 1) {
          throw new SystemConnectException("Unable to examine network cards and other members exist");
        }
      }
    }
    setEquivalentHosts(equivs);
    setEnforceUniqueZone(getConfig().getEnforceUniqueHost());
    String redundancyZone = getConfig().getRedundancyZone();
    if(redundancyZone != null && !redundancyZone.equals("")) {
      setEnforceUniqueZone(true);
    }
    setRedundancyZone(getDistributionManagerId(), redundancyZone);
    if (logger.isDebugEnabled()) {
      StringBuffer sb = new StringBuffer();
      sb.append("Equivalent IPs for this host: ");
      Iterator it = equivs.iterator();
      while (it.hasNext()) {
        InetAddress in = (InetAddress)it.next();
        sb.append(in.toString());
        if (it.hasNext()) {
          sb.append(", ");
        }
      } // while
      logger.debug(sb);
    }
    
    // we need to send this to everyone else; even admin vm
    Set allOthers = new HashSet(getViewMembers());
    allOthers.remove(getDistributionManagerId());

    if (allOthers.isEmpty()) {
      return false; // no peers, we are alone.
    }

    try {
      ok = op.sendStartupMessage(allOthers, STARTUP_TIMEOUT, equivs,
          redundancyZone, enforceUniqueZone());
    }
    catch (Exception re) {
      throw new SystemConnectException(LocalizedStrings.DistributionManager_ONE_OR_MORE_PEERS_GENERATED_EXCEPTIONS_DURING_CONNECTION_ATTEMPT.toLocalizedString(), re);
    }
    if (this.rejectionMessage != null) {
      throw new IncompatibleSystemException(rejectionMessage);
    }

    boolean isAdminDM = 
      getId().getVmKind() == DistributionManager.ADMIN_ONLY_DM_TYPE
      || getId().getVmKind() == DistributionManager.LOCATOR_DM_TYPE
      || DistributionManager.isDedicatedAdminVM
      || Boolean.getBoolean(InternalLocator.FORCE_LOCATOR_DM_TYPE);
    
    boolean receivedAny = this.receivedStartupResponse;

    if (!ok) { // someone didn't reply
      int unresponsiveCount;
      
      synchronized (unfinishedStartupsLock) {
        if (unfinishedStartups == null)
          unresponsiveCount = 0;
        else
          unresponsiveCount = unfinishedStartups.size();        

        if (unresponsiveCount != 0) {
          if (Boolean.getBoolean("DistributionManager.requireAllStartupResponses")) {
            throw new SystemConnectException(LocalizedStrings.DistributionManager_NO_STARTUP_REPLIES_FROM_0.toLocalizedString(unfinishedStartups));
          }
        }
      } // synchronized


      // Bug 35887:
      // If there are other members, we must receive at least _one_ response
      if (allOthers.size() != 0) { // there exist others
        if (!receivedAny) { // and none responded
          StringBuffer sb = new StringBuffer();
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
          throw new SystemConnectException(LocalizedStrings.DistributionManager_RECEIVED_NO_CONNECTION_ACKNOWLEDGMENTS_FROM_ANY_OF_THE_0_SENIOR_CACHE_MEMBERS_1.toLocalizedString(new Object[] {Integer.toString(allOthers.size()), sb.toString()}));
        } // and none responded
      } // there exist others
      
      InternalDistributedMember e = getElderId();
      if (e != null) { // an elder exists
        boolean unresponsiveElder;
        synchronized (unfinishedStartupsLock) {
          if (unfinishedStartups == null)
            unresponsiveElder = false;
          else
            unresponsiveElder = unfinishedStartups.contains(e);
        }
        if (unresponsiveElder) {
          logger.warn(LocalizedMessage.create(
              LocalizedStrings.DistributionManager_FORCING_AN_ELDER_JOIN_EVENT_SINCE_A_STARTUP_RESPONSE_WAS_NOT_RECEIVED_FROM_ELDER__0_, e));
          handleManagerStartup(e);
        }
      } // an elder exists
    } // someone didn't reply
    return receivedAny;
  }

  /**
   * List of InternalDistributedMember's that we have
   * not received startup replies from.  If null, we have
   * not finished sending the startup message.
   * <p>
   * Must be synchronized using {@link #unfinishedStartupsLock}
   */
  private Set unfinishedStartups = null;
  
  /**
   * Synchronization for {@link #unfinishedStartups}
   */
  private final Object unfinishedStartupsLock = new Object();

  public void setUnfinishedStartups(Collection s) {
    synchronized (unfinishedStartupsLock) {
      Assert.assertTrue(unfinishedStartups == null, 
          "Set unfinished startups twice");
      unfinishedStartups = new HashSet(s);
      
      // OK, I don't _quite_ trust the list to be current, so let's
      // prune it here.
      Iterator it = unfinishedStartups.iterator();
      synchronized (this.membersLock) {
        while (it.hasNext()) {
          InternalDistributedMember m = (InternalDistributedMember)it.next();
          if (!isCurrentMember(m)) {
            it.remove();
          }
        } // while
      } // synchronized
    }
  }
  
  public void removeUnfinishedStartup(InternalDistributedMember m,
      boolean departed) {
    synchronized (unfinishedStartupsLock) {
      if (logger.isDebugEnabled()) {
        logger.debug("removeUnfinishedStartup for {} wtih {}", m, unfinishedStartups);
      }
      if (unfinishedStartups == null)
        return; // not yet done with startup
      if (!unfinishedStartups.remove(m))
        return;
      StringId msg = null;      
      if (departed) {
        msg = LocalizedStrings.DistributionManager_STOPPED_WAITING_FOR_STARTUP_REPLY_FROM_0_BECAUSE_THE_PEER_DEPARTED_THE_VIEW;
      }
      else {
        msg = LocalizedStrings.DistributionManager_STOPPED_WAITING_FOR_STARTUP_REPLY_FROM_0_BECAUSE_THE_REPLY_WAS_FINALLY_RECEIVED;
      }
      logger.info(LocalizedMessage.create(msg, m));
      int numLeft = unfinishedStartups.size();
      if (numLeft != 0) {
        logger.info(LocalizedMessage.create(
        LocalizedStrings.DistributionManager_STILL_AWAITING_0_RESPONSES_FROM_1, 
        new Object[] {Integer.valueOf(numLeft), unfinishedStartups}));
      }
    } // synchronized
  }
  
  /**
   * Processes the first startup response.
   *
   * @see StartupResponseMessage#process
   */
  void processStartupResponse(InternalDistributedMember sender,
      long otherCacheTime, String theRejectionMessage) {
    removeUnfinishedStartup(sender, false);
    synchronized (this) {
      if (!this.receivedStartupResponse) {
        this.receivedStartupResponse = true;
      }
      if (theRejectionMessage != null && this.rejectionMessage == null) {
        // remember the first non-null rejection. This fixes bug 33266
        this.rejectionMessage = theRejectionMessage;
      }
    }
  }

  /**
   * Processes the first startup response.
   *
   * @see StartupResponseMessage#process
   */
  void processStartupResponse(InternalDistributedMember sender,
      String theRejectionMessage) {
    removeUnfinishedStartup(sender, false);
    synchronized (this) {
      if (!this.receivedStartupResponse) {
        // only set the cacheTimeDelta once
        this.receivedStartupResponse = true;
      }
      if (theRejectionMessage != null && this.rejectionMessage == null) {
        // remember the first non-null rejection. This fixes bug 33266
        this.rejectionMessage = theRejectionMessage;
      }
    }
  }
  
  /**
   * Based on a recent JGroups view, return a member that might be the
   * next elder.
   * @return the elder candidate, possibly this VM.
   */
  private InternalDistributedMember getElderCandidate() {
    List<InternalDistributedMember> theMembers = getViewMembers();
    
//    Assert.assertTrue(!closeInProgress 
//        && theMembers.contains(this.myid)); // bug36202?
    
    int elderCandidates = 0;
    Iterator<InternalDistributedMember> it;
    
    // for bug #50510 we need to know if there are any members older than v8.0
    it = theMembers.iterator();
    boolean anyPre80Members = false;
    while (it.hasNext()) {
      InternalDistributedMember member = it.next();
      if (member.getVersionObject().compareTo(Version.GFE_80) < 0) {
        anyPre80Members = true;
      }
    }

    // determine number of elder candidates (unless adam)
    if (!this.adam) {
      it = theMembers.iterator();
      while (it.hasNext()) {
        InternalDistributedMember member = it.next();
        int managerType = member.getVmKind();
        if (managerType == ADMIN_ONLY_DM_TYPE)
          continue;
        
        if (managerType == LOCATOR_DM_TYPE) {
          // Fix for #50510 - pre-8.0 members will not let a locator be the elder
          // so we need to make the same decision here
          if (anyPre80Members) {
            continue;
          }
        }
        
        // Fix for #45566.  Using a surprise member as the elder can cause a
        // deadlock.
        if (getMembershipManager().isSurpriseMember(member)) {
          continue;
        }
        
        elderCandidates++;
        if (elderCandidates > 1) {
          // If we have more than one candidate then we are not adam
          break;
        }
      } // while
    }
    
    // Second pass over members...
    it = theMembers.iterator();
    while (it.hasNext()) {
      InternalDistributedMember member = it.next(); 
      int managerType = member.getVmKind();
      if (managerType == ADMIN_ONLY_DM_TYPE)
        continue;

      if (managerType == LOCATOR_DM_TYPE) {
        // Fix for #50510 - pre-8.0 members will not let a locator be the elder
        // so we need to make the same decision here
        if (anyPre80Members) {
          continue;
        }
      }
      
      // Fix for #45566.  Using a surprise member as the elder can cause a
      // deadlock.
      if (getMembershipManager().isSurpriseMember(member)) {
        continue;
      }

      if (member.equals(this.myid)) { // c'est moi
        if (!this.adam && elderCandidates == 1) {
          this.adam = true;
          logger.info(LocalizedMessage.create(LocalizedStrings.DistributionManager_0_IS_THE_ELDER_AND_THE_ONLY_MEMBER, this.myid));
        } else {
          logger.info(LocalizedMessage.create(LocalizedStrings.DistributionManager_I_0_AM_THE_ELDER, this.myid));
        }
      } // c'est moi
      return member;
    } // while
    // If we get this far then no elder exists
    return null;
  }
  
  /**
   * Select a new elder
   *
   */
  protected void selectElder() {
    getSystem().getCancelCriterion().checkCancelInProgress(null); // bug 37884, if DS is disconnecting, throw exception
    
    // Once we are the elder, we're stuck until we leave the view.
    if (this.myid.equals(this.elder)) {
      return;
    }

    // Determine who is the elder...
    InternalDistributedMember candidate = getElderCandidate();
    if (candidate == null) {
      changeElder(null);
      return; // No valid elder in current context
    }

    // Carefully switch to new elder
    synchronized (this.elderMonitor) {
      if (!candidate.equals(this.elder)) {
        if (logger.isDebugEnabled()) {
          logger.debug("The elder is: {} (was {})", candidate, this.elder);
        }
        changeElder(candidate);
      }
    } // synchronized
  }
  
  private String prettifyReason(String r) {
    final String str = "java.io.IOException:";
    if (r.startsWith(str)) {
      return r.substring(str.length());
    }
    return r;
  }

  /**
   * Returns true if id was removed.
   * Returns false if it was not in the list of managers.
   */
  private boolean removeManager(InternalDistributedMember theId, 
      boolean crashed, String p_reason) {
    String reason = p_reason;
    boolean result = false; // initialization shouldn't be required, but...

    // Test once before acquiring the lock, fault tolerance for potentially
    // recursive (and deadlock) conditions -- bug33626
    // Note that it is always safe to _read_ {@link members} without locking
    if (isCurrentMember(theId)) {
      // Destroy underlying member's resources
      reason = prettifyReason(reason);
      synchronized (this.membersLock) {
        if (logger.isDebugEnabled()) {
          logger.debug("DistributionManager: removing member <{}>; crashed {}; reason = {}", theId, crashed, reason);
        }
        Map<InternalDistributedMember,InternalDistributedMember> tmp = new HashMap(this.members);
        if (tmp.remove(theId) != null) {
          // Note we don't modify in place. This allows reader to get snapshots
          // without locking.
          if (tmp.isEmpty()) {
            tmp = Collections.EMPTY_MAP;
          } else {
            tmp = Collections.unmodifiableMap(tmp);
          }
          this.members = tmp;
          result = true;

        } else {
          result = false;
          // Don't get upset since this can happen twice due to
          // an explicit remove followed by an implicit one caused
          // by a JavaGroup view change
        }
        Set tmp2 = new HashSet(this.membersAndAdmin);
        if(tmp2.remove(theId)) {
          if (tmp2.isEmpty()) {
            tmp2 = Collections.EMPTY_SET;
          } else {
            tmp2 = Collections.unmodifiableSet(tmp2);
          }
          this.membersAndAdmin = tmp2;
        }
        this.removeHostedLocators(theId);
      } // synchronized
    } // if
    
    // In any event, make sure that this member is no longer an elder.
    if (!theId.equals(myid) && theId.equals(elder)) {
      try {
        selectElder();
      }
      catch (DistributedSystemDisconnectedException e) {
        // ignore
      }
    }
    
    redundancyZones.remove(theId);
    
    return result;
  }

 /**
   * Makes note of a new distribution manager that has started up in
   * the distributed cache.  Invokes the appropriately listeners.
   *
   * @param theId
   *        The id of the distribution manager starting up
   *
   */
  private void handleManagerStartup(InternalDistributedMember theId) {
    HashMap<InternalDistributedMember,InternalDistributedMember> tmp = null;
    synchronized (this.membersLock) {
      // Note test is under membersLock
      if (members.containsKey(theId)) {
        return; // already accounted for
      }

      // Note we don't modify in place. This allows reader to get snapshots
      // without locking.
      tmp = new HashMap(this.members);
      tmp.put(theId,theId);
      this.members = Collections.unmodifiableMap(tmp);
      
      Set stmp = new HashSet(this.membersAndAdmin);
      stmp.add(theId);
      this.membersAndAdmin = Collections.unmodifiableSet(stmp);
    } // synchronized

    if (theId.getVmKind() != DistributionManager.LOCATOR_DM_TYPE) {
      this.stats.incNodes(1);
    }
    logger.info(LocalizedMessage.create(
        LocalizedStrings.DistributionManager_ADMITTING_MEMBER_0_NOW_THERE_ARE_1_NONADMIN_MEMBERS,
        new Object[] { theId, Integer.valueOf(tmp.size())}));
    addMemberEvent(new MemberJoinedEvent(theId));
  }

  /**
   *   Return true if id is a current member of our system.
   */
  public boolean isCurrentMember(InternalDistributedMember id) {
    Set m;
    synchronized (this.membersLock) {
      // access to members synchronized under membersLock in order to 
      // ensure serialization
      m = this.membersAndAdmin;
    }
    return m.contains(id);
  }
  
  /**
   * Makes note of a new console that has started up in
   * the distributed cache.
   *
   */
  private void handleConsoleStartup(InternalDistributedMember theId) {
    // if we have an all listener then notify it NOW.
    HashSet tmp = null;
    synchronized (this.membersLock) {
      // Note test is under membersLock
      if (membersAndAdmin.contains(theId))
        return; // already accounted for

      // Note we don't modify in place. This allows reader to get snapshots
      // without locking.
      tmp = new HashSet(this.membersAndAdmin);
      tmp.add(theId);
      this.membersAndAdmin = Collections.unmodifiableSet(tmp);
    } // synchronized
    
    for (Iterator iter = allMembershipListeners.iterator();
         iter.hasNext(); ) {
      MembershipListener listener = (MembershipListener) iter.next();
      listener.memberJoined(theId);
    }
    logger.info(LocalizedMessage.create(LocalizedStrings.DistributionManager_DMMEMBERSHIP_ADMITTING_NEW_ADMINISTRATION_MEMBER__0_, theId));
    // Note that we don't add the member to the list of admin consoles until
    // we receive a message from them.
  }

  /**
   * Process an incoming distribution message.
   * This includes scheduling it correctly based on the message's
   * nioPriority (executor type)
   */
  public void handleIncomingDMsg(DistributionMessage message) {
    /* disabled - not being used
       if (message instanceof OutgoingMessageWrapper) {
       putOutgoing(((OutgoingMessageWrapper)message).getMessage());
       return;
       }
    */

//     long latency = message.getLatency();
//     this.stats.incMessageTransitTime(latency * 1000000L);
//     message.resetTimestamp();
    stats.incReceivedMessages(1L);
    stats.incReceivedBytes(message.getBytesRead());
    stats.incMessageChannelTime(message.resetTimestamp());

 //   message.setRecipient(DistributionManager.this.getId());

    if (logger.isDebugEnabled()) {
      logger.debug("Received message '{}' from <{}>", message, message.getSender());
    }
    scheduleIncomingMessage(message);
  }

  /**
   * Makes note of a console that has shut down.
   * @param theId
   *        The id of the console shutting down
   * @param crashed only true if we detect this id to be gone from
   *         a javagroup view
   *
   * @see AdminConsoleDisconnectMessage#process
   */
  public void handleConsoleShutdown(InternalDistributedMember theId, boolean crashed,
      String reason) {
    boolean removedConsole = false;
    boolean removedMember = false;
    synchronized (this.membersLock) {
      // to fix bug 39747 we can only remove this member from
      // membersAndAdmin if he is not in members.
      // This happens when we have an admin guy colocated with a normal DS.
      // In this case we need for the normal DS to shutdown or crash.
      if (!this.members.containsKey(theId)) {
        if (logger.isDebugEnabled())
          logger.debug("DistributionManager: removing admin member <{}>; crashed = {}; reason = {}", theId, crashed, reason);
        Set tmp = new HashSet(this.membersAndAdmin);
        if (tmp.remove(theId)) {
          // Note we don't modify in place. This allows reader to get snapshots
          // without locking.
          if (tmp.isEmpty()) {
            tmp = Collections.EMPTY_SET;
          } else {
            tmp = Collections.unmodifiableSet(tmp);
          }
          this.membersAndAdmin = tmp;
          removedMember = true;
        } else {
          // Don't get upset since this can happen twice due to
          // an explicit remove followed by an implicit one caused
          // by a JavaGroup view change
        }
      }
      removeHostedLocators(theId);
    }
    synchronized(this.adminConsolesLock) {
      if (this.adminConsoles.contains(theId)) {
        removedConsole = true;
        Set tmp = new HashSet(this.adminConsoles);
        tmp.remove(theId);
        if (tmp.isEmpty()) {
          tmp = Collections.EMPTY_SET;
        } else {
          tmp = Collections.unmodifiableSet(tmp);
        }
        this.adminConsoles = tmp;
      }
    }
    if (removedMember) {
      for (Iterator iter = allMembershipListeners.iterator();
           iter.hasNext(); ) {
        MembershipListener listener = (MembershipListener) iter.next();
        listener.memberDeparted(theId, crashed);
      }
    }
    if (removedConsole) {
      StringId msg = null;
      if (crashed) {
        msg = LocalizedStrings.DistributionManager_ADMINISTRATION_MEMBER_AT_0_CRASHED_1;
      } else {
        msg = LocalizedStrings.DistributionManager_ADMINISTRATION_MEMBER_AT_0_CLOSED_1;
      }
      logger.info(LocalizedMessage.create(msg, new Object[] {theId, reason}));
    }
    
    redundancyZones.remove(theId);
  }
  
  public void shutdownMessageReceived(InternalDistributedMember theId, String reason) {
    this.membershipManager.shutdownMessageReceived(theId, reason);
    handleManagerDeparture(theId, false, LocalizedStrings.ShutdownMessage_SHUTDOWN_MESSAGE_RECEIVED.toLocalizedString());
  }

  /** used by the DistributedMembershipListener and startup and shutdown operations, this
      method decrements the number of nodes and handles lower-level clean up of
      the resources used by the departed manager */
  public void handleManagerDeparture(InternalDistributedMember theId, 
      boolean p_crashed, String p_reason) {
    boolean crashed = p_crashed;
    String reason = p_reason;

    AlertAppender.getInstance().removeAlertListener(theId);

    // this fixes a race introduced in 5.0.1 by the fact that an explicit
    // shutdown will cause a member to no longer be in our DM membership
    // but still in the javagroup view.
    try {
      selectElder();
    }
    catch (DistributedSystemDisconnectedException e) {
      // keep going
    }
    
    
    
    int vmType = theId.getVmKind();
    if (vmType == ADMIN_ONLY_DM_TYPE) {
      removeUnfinishedStartup(theId, true);
      handleConsoleShutdown(theId, crashed, reason);
      return;
    }
    
    // not an admin VM...
    if (!isCurrentMember(theId)) {
      return; // fault tolerance
    }
    removeUnfinishedStartup(theId, true);

    if (removeManager(theId, crashed, reason)) {
      if (theId.getVmKind() != DistributionManager.LOCATOR_DM_TYPE) {
        this.stats.incNodes(-1);
      }
      StringId msg;
      if (crashed && ! isCloseInProgress()) {
        msg = LocalizedStrings.DistributionManager_MEMBER_AT_0_UNEXPECTEDLY_LEFT_THE_DISTRIBUTED_CACHE_1;
        addMemberEvent(new MemberCrashedEvent(theId, reason));
      } else {
        msg = LocalizedStrings.DistributionManager_MEMBER_AT_0_GRACEFULLY_LEFT_THE_DISTRIBUTED_CACHE_1;
        addMemberEvent(new MemberDepartedEvent(theId, reason));
      }
      logger.info(LocalizedMessage.create(msg, new Object[] {theId, prettifyReason(reason)}));
      
      // Remove this manager from the serialQueueExecutor.
      if (this.serialQueuedExecutorPool != null)
      {
        serialQueuedExecutorPool.handleMemberDeparture(theId);
      }
    }
  }

  /**
   * @param reason TODO
   */
  public void handleManagerSuspect(InternalDistributedMember suspect, 
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
  
  public void handleViewInstalled(NetView view) {
    addMemberEvent(new ViewInstalledEvent(view));
  }

  public void handleQuorumLost(Set<InternalDistributedMember> failures, List<InternalDistributedMember> remaining) {
    addMemberEvent(new QuorumLostEvent(failures, remaining));
  }

  /**
   * Sends the shutdown message.  Not all DistributionManagers need to
   * do this.
   */
  protected void sendShutdownMessage() {
    if (getDMType() == ADMIN_ONLY_DM_TYPE && Locator.getLocators().size() == 0) {
//     [bruce] changed above "if" to have ShutdownMessage sent by locators.
//     Otherwise the system can hang because an admin member does not trigger
//     member-left notification unless a new view is received showing the departure.
//     If two locators are simultaneously shut down this may not occur.
      return;
    }

    ShutdownMessage m = new ShutdownMessage();
    InternalDistributedMember theId =
      this.getDistributionManagerId();
    m.setDistributionManagerId(theId);
    Set allOthers = new HashSet(getViewMembers());
    allOthers.remove(getDistributionManagerId());
//    ReplyProcessor21 rp = new ReplyProcessor21(this, allOthers);
//    m.setProcessorId(rp.getProcessorId());
//    m.setMulticast(system.getConfig().getMcastPort() != 0);
    m.setRecipients(allOthers);

    //Address recipient = (Address) m.getRecipient();
    if (logger.isTraceEnabled()) {
      logger.trace("{} Sending {} to {}", this.getDistributionManagerId(), m, m.getRecipientsDescription());
    }

    try {
      //m.resetTimestamp(); // nanotimers across systems don't match
      long startTime = DistributionStats.getStatTime();
      channel.send(m.getRecipients(), m, this, stats);
      this.stats.incSentMessages(1L);
      if (DistributionStats.enableClockStats) {
        stats.incSentMessagesTime(DistributionStats.getStatTime()-startTime);
      }
    } catch (CancelException e) {
      logger.debug("CancelException caught sending shutdown: {}", e.getMessage(), e);
    } catch (Exception ex2) {
      logger.fatal(LocalizedMessage.create(LocalizedStrings.DistributionManager_WHILE_SENDING_SHUTDOWN_MESSAGE), ex2);
    }
    finally {
      // Even if the message wasn't sent, *lie* about it, so that
      // everyone believes that message distribution is done.
      this.shutdownMsgSent = true;
    }
  }

  /**
   * Returns the executor for the given type of processor.
   *
   */
  public final Executor getExecutor(int processorType, InternalDistributedMember sender) {
    switch(processorType) {
      case STANDARD_EXECUTOR:
        return getThreadPool();
      case SERIAL_EXECUTOR:
        return getSerialExecutor(sender);
      case VIEW_EXECUTOR:
        return this.viewThread;
      case HIGH_PRIORITY_EXECUTOR:
        return getHighPriorityThreadPool();
      case WAITING_POOL_EXECUTOR:
        return getWaitingThreadPool();
      case PARTITIONED_REGION_EXECUTOR:
        return getPartitionedRegionExcecutor();
      case REGION_FUNCTION_EXECUTION_EXECUTOR:
        return getFunctionExcecutor();
      default:
        throw new InternalGemFireError(
            LocalizedStrings.DistributionManager_UNKNOWN_PROCESSOR_TYPE
                .toLocalizedString(processorType));
    }
  }
  
//  /**
//   * Return a shortened name of a class that excludes the package
//   */
//  private static String shortenClassName(String className) {
//    int index = className.lastIndexOf('.');
//    if (index != -1) {
//      return className.substring(index + 1);
//
//    } else {
//      return className;
//    }
//  }

  /**
   * Send a message that is guaranteed to be serialized
   * @param msg
   * @return the recipients who did not receive the message
   */
  protected Set sendOutgoingSerialized(DistributionMessage msg) {
    try {
      return sendOutgoing(msg);
    }
    catch (NotSerializableException e) {
      throw new InternalGemFireException(e);
    }
    catch (ToDataException e) {
      // exception from user code
      throw e;
    }
  }
  
  /**
   * Actually does the work of sending a message out over the
   * distribution channel.
   *
   * @param message the message to send
   * @return list of recipients that did not receive the message because
   * they left the view (null if all received it or it was sent to
   * {@link DistributionMessage#ALL_RECIPIENTS}.
   * @throws NotSerializableException
   *         If <code>message</code> cannot be serialized
   */
  protected Set sendOutgoing(DistributionMessage message)
    throws  NotSerializableException {
    long startTime = DistributionStats.getStatTime();
    
    Set result = channel.send(message.getRecipients(), message,
                             DistributionManager.this, 
                             this.stats);
    long endTime = 0L;
    if (DistributionStats.enableClockStats) {
      endTime = NanoTimer.getTime();
    }
    boolean sentToAll = message.forAll();

    if (sentToAll) {
      stats.incBroadcastMessages(1L);
      if (DistributionStats.enableClockStats) {
        stats.incBroadcastMessagesTime(endTime-startTime);
      }
    }
    stats.incSentMessages(1L);    
    if (DistributionStats.enableClockStats) {
      stats.incSentMessagesTime(endTime-startTime);
      stats.incDistributeMessageTime(endTime - message.getTimestamp());
    }
    
    return result;
  }



  /**
   * @return recipients who did not receive the message
   * @throws NotSerializableException
   *         If <codE>message</code> cannot be serialized
   */
  Set sendMessage(DistributionMessage message) 
      throws NotSerializableException {
    Set result = null;
    try {
      // Verify we're not too far into the shutdown
      stopper.checkCancelInProgress(null);
      
      // avoid race condition during startup
      waitUntilReadyToSendMsgs(message);
      
      result = sendOutgoing(message);
    } catch (NotSerializableException ex) {
      throw ex; // serialization error in user data
    } catch (ToDataException ex) {
      throw ex; // serialization error in user data
    }
    catch (ReenteredConnectException ex) {
      throw ex; // Recursively tried to get the same connection
    } 
    catch (CancelException ex) {
      throw ex; // bug 37194, shutdown conditions
    }
    catch (InvalidDeltaException ide) {
      logger.info(LocalizedMessage.create(LocalizedStrings.DistributionManager_CAUGHT_EXCEPTION_WHILE_SENDING_DELTA), ide.getCause());
      throw (RuntimeException)ide.getCause();
    }
    catch (Exception ex) {
      DistributionManager.this.exceptionInThreads = true;
      String receiver = "NULL";
      if (message != null) {
        receiver = message.getRecipientsDescription();
      }
      
      logger.fatal(LocalizedMessage.create(LocalizedStrings.DistributionManager_WHILE_PUSHING_MESSAGE_0_TO_1, new Object[] {message, receiver}), ex);
      if (message == null || message.forAll())
        return null;
      result = new HashSet();
      for (int i = 0; i < message.getRecipients().length; i ++)
        result.add(message.getRecipients()[i]);
      return result;
   /*   if (ex instanceof com.gemstone.gemfire.GemFireIpcResourceException) {
        return;
      }*/
    }
    return result;
  }


  /**
   * Schedule a given message appropriately, depending upon its
   * executor kind.
   * 
   * @param message
   */
  protected void scheduleIncomingMessage(DistributionMessage message)
  {
    /* Potential race condition between starting up and getting other
     * distribution manager ids -- DM will only be initialized upto
     * the point at which it called startThreads
     */
    waitUntilReadyForMessages();
    message.schedule(DistributionManager.this);
  }

  /**
   * Mutex to control access to {@link #waitingForElderChange}
   * or {@link #elder}.
   */
  protected final Object elderMonitor = new Object();

  /**
   * Must be read/written while holding {@link #elderMonitor}
   * 
   * @see #elderChangeWait()
   */
  private boolean waitingForElderChange = false;

  /**
   * @see DM#isAdam()
   */
  private boolean adam = false;
  
  /**
   * This is the "elder" member of the distributed system, responsible
   * for certain types of arbitration.
   * 
   * Must hold {@link #elderMonitor} in order to change this.
   * 
   * @see #getElderId()
   */
  protected volatile InternalDistributedMember elder = null;
  
  public boolean isAdam() {
    return this.adam;
  }
  
  public InternalDistributedMember getElderId() 
    throws DistributedSystemDisconnectedException {
//    membershipManager.waitForEventProcessing();
    if (closeInProgress) {
      throw new DistributedSystemDisconnectedException(LocalizedStrings.DistributionManager_NO_VALID_ELDER_WHEN_SYSTEM_IS_SHUTTING_DOWN.toLocalizedString(), this.getRootCause());
    }
    getSystem().getCancelCriterion().checkCancelInProgress(null);

    // Cache a recent value of the elder
    InternalDistributedMember result = elder;
    if (result != null && membershipManager.memberExists(result)) {
      return result;
    }
    logger.info(LocalizedMessage.create(LocalizedStrings.DistributionManager_ELDER__0__IS_NOT_CURRENTLY_AN_ACTIVE_MEMBER_SELECTING_NEW_ELDER, elder));

    selectElder(); // ShutdownException can be thrown here
    logger.info(LocalizedMessage.create(LocalizedStrings.DistributionManager_NEWLY_SELECTED_ELDER_IS_NOW__0_, elder));
    return elder;
  }

  public boolean isElder() {
    return getId().equals(elder);
  }
  public boolean isLoner() {
    return false;
  }

  private final StoppableReentrantLock elderLock;
  private ElderState elderState;
  private volatile boolean elderStateInitialized;
  
  public ElderState getElderState(boolean force, boolean useTryLock) {
    if (force) {
      if (logger.isDebugEnabled()) {
        if (!this.myid.equals(this.elder)) {
          logger.debug("Forcing myself, {}, to be the elder.", this.myid);
        }
      }
      changeElder(this.myid);
    }
    if (force || this.myid.equals(elder)) {
      // we are the elder
      if (this.elderStateInitialized) {
        return this.elderState;
      }
      return getElderStateWithTryLock(useTryLock);
    } else {
      // we are not the elder so return null
      return null;
    }
  }

  /**  
   * Usage: GrantorRequestProcessor calls getElderState with useTryLock set
   * to true if the becomeGrantor Collaboration is already acquired.
   * <p>
   * This tryLock is attempted and if it fails, an exception is thrown to
   * cause a Doug Lea style back-off (p. 149). It throws an exception because
   * it needs to back down a couple of packages and I didn't want to couple
   * this pkg too tightly with the dlock pkg.
   * <p>
   * GrantorRequestProcessor catches the exception, releases and reacquires
   * the Collaboration, and then comes back here to attempt the tryLock
   * again. Currently nothing will stop it from re-attempting forever. It
   * has to get the ElderState and cannot give up, but it can free up the
   * Collaboration and then re-enter it. The other thread holding the
   * elder lock will hold it only briefly. I've added a volatile called
   * elderStateInitialized which should cause this back-off to occur only
   * once in the life of a vm... once the elder, always the elder.
   * <p>
   * TODO: Collaboration lock is no longer used. Do we need to to use tryLock?
   */
  private ElderState getElderStateWithTryLock(boolean useTryLock) {
    boolean locked = false;
    if (useTryLock) {
      boolean interrupted = Thread.interrupted();
      try {
        locked = this.elderLock.tryLock(2000);
      }
      catch (InterruptedException e) {
        interrupted = true;
        getCancelCriterion().checkCancelInProgress(e);
        // one last attempt and then allow it to fail for back-off...
        locked = this.elderLock.tryLock();
      }
      finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    } else {
      locked = true;
      this.elderLock.lock();
    }
    if (!locked) {
      // try-lock must have failed
      throw new IllegalStateException(LocalizedStrings.DistributionManager_POSSIBLE_DEADLOCK_DETECTED.toLocalizedString());
    }
    try {
      if (this.elderState == null) {
        this.elderState = new ElderState(this);
      }
    }
    finally {
      this.elderLock.unlock();
    }
    this.elderStateInitialized = true;
//    if (Thread.currentThread().isInterrupted())
//      throw new RuntimeException("Interrupted");
    return this.elderState;
  }

  /**
   * Waits until elder if newElder or newElder is no longer a member
   * @return true if newElder is the elder; false if he is no longer a member
   * or we are the elder.
   */
  public boolean waitForElder(final InternalDistributedMember desiredElder) {
    MembershipListener l = null;
    try {
//      Assert.assertTrue(
//          desiredElder.getVmKind() != DistributionManager.ADMIN_ONLY_DM_TYPE);
      synchronized (this.elderMonitor) {
        while (true) {
          if (closeInProgress)
            return false;
          InternalDistributedMember currentElder = this.elder;
//          Assert.assertTrue( 
//              currentElder.getVmKind() != DistributionManager.ADMIN_ONLY_DM_TYPE);
          if (desiredElder.equals(currentElder)) {
            return true;
          }
          if (!isCurrentMember(desiredElder)) {
            return false; // no longer present
          }
          if (this.myid.equals(currentElder)) {
            // Once we become the elder we no longer allow anyone else to be the
            // elder so don't let them wait anymore.
            return false;
          }
          if (l == null) {
            l = new MembershipListener() {
                public void memberJoined(InternalDistributedMember theId) {
                  // nothing needed
                }
                public void memberDeparted(InternalDistributedMember theId, boolean crashed) {
                  if (desiredElder.equals(theId)) {
                    notifyElderChangeWaiters();
                  }
                }
                public void memberSuspect(InternalDistributedMember id,
                    InternalDistributedMember whoSuspected, String reason) {
                }
                public void viewInstalled(NetView view) {
                }
                public void quorumLost(Set<InternalDistributedMember> failures, List<InternalDistributedMember> remaining) {
                }
            };
            addMembershipListener(l);
          }
          logger.info(LocalizedMessage.create(LocalizedStrings.DistributionManager_CHANGING_ELDER_FROM_0_TO_1,
              new Object[] {currentElder, desiredElder}));
          elderChangeWait();
        } // while true
      }
    } finally {
      if (l != null) {
        removeMembershipListener(l);
      }
    }
  }
  /**
   * Set the elder to newElder and notify anyone waiting for it to change
   */
  protected void changeElder(InternalDistributedMember newElder) {
    synchronized (this.elderMonitor) {
      if (newElder != null &&
          this.myid != null && !this.myid.equals(newElder)) {
        if (this.myid.equals(this.elder)) {
          // someone else changed the elder while this thread was off cpu
          if (logger.isDebugEnabled()) {
            logger.debug("changeElder found this VM to be the elder and is taking an early out");
          }
          return;
        }
      }
      this.elder = newElder;
      if (this.waitingForElderChange) {
        this.waitingForElderChange = false;
        this.elderMonitor.notifyAll();
      }
    }
  }
  /**
   * Used to wakeup someone in elderChangeWait even though the elder has not changed
   */
  protected void notifyElderChangeWaiters() {
    synchronized (this.elderMonitor) {
      if (this.waitingForElderChange) {
        this.waitingForElderChange = false;
        this.elderMonitor.notifyAll();
      }
    }
  }

  /**
   * Must be called holding {@link #elderMonitor} lock
   */
  private void elderChangeWait() {
    // This is OK since we're holding the elderMonitor lock, so no
    // new events will come through until the wait() below.
    this.waitingForElderChange = true;
    
    while (this.waitingForElderChange) {
      stopper.checkCancelInProgress(null);
      boolean interrupted = Thread.interrupted();
      try {
        this.elderMonitor.wait();
        break;
      } 
      catch (InterruptedException ignore) {
        interrupted = true;
      }
      finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    } // while
   }

  /**
   * getThreadPool gets this distribution manager's message-processing thread
   * pool
   */
  public ExecutorService getThreadPool() {
    return this.threadPool;
  }

  /**
   * Return the high-priority message-processing executor */
  public ExecutorService getHighPriorityThreadPool() {
    return this.highPriorityPool;
  }
  
  /**
   * Return the waiting message-processing executor 
   */
  public ExecutorService getWaitingThreadPool() {
    return this.waitingPool;
  }
  
  /**
   * Return the waiting message-processing executor 
   */
  public ExecutorService getPrMetaDataCleanupThreadPool() {
    return this.prMetaDataCleanupThreadPool;
  }

  /**
   * Return the waiting message-processing executor 
   */
  public Executor getPartitionedRegionExcecutor() {
    if (this.partitionedRegionThread != null) {
      return this.partitionedRegionThread;
    } else {
      return this.partitionedRegionPool;
    }
  }

  /**
   * Return the waiting message-processing executor 
   */
  public Executor getFunctionExcecutor() {
    if (this.functionExecutionThread != null) {
      return this.functionExecutionThread;
    } else {
      return this.functionExecutionPool;
    }
  }
  
  private Executor getSerialExecutor(InternalDistributedMember sender) {
     if (MULTI_SERIAL_EXECUTORS) {
       return this.serialQueuedExecutorPool.getThrottledSerialExecutor(sender);
     } else {
       return this.serialThread;
     }
  }
    
  /** returns the serialThread's queue if throttling is being used, null if not */
  public OverflowQueueWithDMStats getSerialQueue(InternalDistributedMember sender) {
    if (MULTI_SERIAL_EXECUTORS) {  
      return this.serialQueuedExecutorPool.getSerialQueue(sender);
    } else {
      return this.serialQueue;
    }
  }
  
  /**
   * Sets the administration agent associated with this distribution
   * manager.
   *
   * @since 4.0
   */
  public void setAgent(RemoteGfManagerAgent agent) {
    // Don't let the agent be set twice.  There should be a one-to-one
    // correspondence between admin agent and distribution manager.
    if (agent != null) {
      if (this.agent != null) {
        throw new IllegalStateException(LocalizedStrings.DistributionManager_THERE_IS_ALREADY_AN_ADMIN_AGENT_ASSOCIATED_WITH_THIS_DISTRIBUTION_MANAGER.toLocalizedString());
      }

    } else {
      if (this.agent == null) {
        throw new IllegalStateException(LocalizedStrings.DistributionManager_THERE_WAS_NEVER_AN_ADMIN_AGENT_ASSOCIATED_WITH_THIS_DISTRIBUTION_MANAGER.toLocalizedString());
      }
    }
    this.agent = agent;
  }

  /**
   * Returns the agent that owns this distribution manager.
   * (in ConsoleDistributionManager)
   * @since 3.5
   */
  public RemoteGfManagerAgent getAgent(){
    return this.agent;
  }

  /**
   * Returns a description of the distribution configuration used for
   * this distribution manager. (in ConsoleDistributionManager)
   *
   * @return <code>null</code> if no admin {@linkplain #getAgent
   *         agent} is associated with this distribution manager
   *
   * @since 3.5
   */
  public String getDistributionConfigDescription() {
    if (this.agent == null) {
      return null;

    } else {
      return this.agent.getTransport().toString();
    }
  } 

  /**
   * A <code>DistributionManager</code> is not intented to be
   * serialized.  This method throws an {@link
   * UnsupportedOperationException} to prevent a
   * <code>DistributionManager</code> from being copy shared.
   */
  public void writeExternal(ObjectOutput out) throws IOException {
    throw new UnsupportedOperationException(LocalizedStrings.DistributionManager_DISTRIBUTIONMANAGERS_SHOULD_NOT_BE_COPY_SHARED.toLocalizedString());
  }

  /**
   * A <code>DistributionManager</code> is not intented to be
   * serialized.  This method throws an {@link
   * UnsupportedOperationException} to prevent a
   * <code>DistributionManager</code> from being copy shared.
   */
  public void readExternal(ObjectInput out)
    throws IOException, ClassNotFoundException {
    throw new UnsupportedOperationException(LocalizedStrings.DistributionManager_DISTRIBUTIONMANAGERS_SHOULD_NOT_BE_COPY_SHARED.toLocalizedString());
  }

  /* -----------------------------Health Monitor------------------------------ */
  private final ConcurrentMap hmMap = new ConcurrentHashMap();

  /**
   * Returns the health monitor for this distribution manager and owner.
   * @param owner the agent that owns the returned monitor
   * @return the health monitor created by the owner; <code>null</code>
   *    if the owner has now created a monitor.
   * @since 3.5
   */
  public HealthMonitor getHealthMonitor(InternalDistributedMember owner) {
    return (HealthMonitor)this.hmMap.get(owner);
  }
  /**
   * Returns the health monitor for this distribution manager.
   *
   * @param owner the agent that owns the created monitor
   * @param cfg the configuration to use when creating the monitor
   * @since 3.5
   */
  public void createHealthMonitor(InternalDistributedMember owner,
                                  GemFireHealthConfig cfg) {
    if (closeInProgress) {
      return;
    }
    {
      final HealthMonitor hm = getHealthMonitor(owner);
      if (hm != null) {
        hm.stop();
        this.hmMap.remove(owner);
      }
    }
    {
      HealthMonitorImpl newHm = new HealthMonitorImpl(owner, cfg, this);
      newHm.start();
      this.hmMap.put(owner, newHm);
    }
  }
  /**
   * Remove a monitor that was previously created.
   * @param owner the agent that owns the monitor to remove
   */
  public void removeHealthMonitor(InternalDistributedMember owner, int theId) {
    final HealthMonitor hm = getHealthMonitor(owner);
    if (hm != null && hm.getId() == theId) {
      hm.stop();
      this.hmMap.remove(owner);
    }
  }
  public void removeAllHealthMonitors() {
    Iterator it = this.hmMap.values().iterator();
    while (it.hasNext()) {
      HealthMonitor hm = (HealthMonitor)it.next();
      hm.stop();
      it.remove();
    }
  }

  // For feature request #32887
  public Set getAdminMemberSet() {
    return this.adminConsoles;
  }
  
  /** Returns count of members filling the specified role */
  public int getRoleCount(Role role) {
    int count = 0;
    Set mbrs = getDistributionManagerIds();
    for (Iterator mbrIter = mbrs.iterator(); mbrIter.hasNext();) {
      Set roles = ((InternalDistributedMember) mbrIter.next()).getRoles();
      for (Iterator rolesIter = roles.iterator(); rolesIter.hasNext();) {
        Role mbrRole = (Role) rolesIter.next();
        if (mbrRole.equals(role)) {
          count++;
          break;
        }
      }
    }
    return count;
  }
  
  /** Returns true if at least one member is filling the specified role */
  public boolean isRolePresent(Role role) {
    Set mbrs = getDistributionManagerIds();
    for (Iterator mbrIter = mbrs.iterator(); mbrIter.hasNext();) {
      Set roles = ((InternalDistributedMember) mbrIter.next()).getRoles();
      for (Iterator rolesIter = roles.iterator(); rolesIter.hasNext();) {
        Role mbrRole = (Role) rolesIter.next();
        if (mbrRole.equals(role)) {
          return true;
        }
      }
    }
    return false;
  }
  
  /** Returns a set of all roles currently in the distributed system. */
  public Set getAllRoles() {
    Set allRoles = new HashSet();
    Set mbrs = getDistributionManagerIds();
    for (Iterator mbrIter = mbrs.iterator(); mbrIter.hasNext();) {
      Set roles = ((InternalDistributedMember) mbrIter.next()).getRoles();
      for (Iterator rolesIter = roles.iterator(); rolesIter.hasNext();) {
        Role mbrRole = (Role) rolesIter.next();
        allRoles.add(mbrRole);
      }
    }
    return allRoles;
  }
  
  /** Returns the membership manager for this distributed system.
      The membership manager owns the membership set and handles
      all communications.   The manager should NOT be used to
      bypass DistributionManager to send or receive messages.<p>
      This method was added to allow hydra to obtain thread-local
      data for transport from one thread to another. */
  public MembershipManager getMembershipManager() {
    // NOTE: do not add cancellation checks here.  This method is
    // used during auto-reconnect after the DS has been closed
    return membershipManager;
  }


  //////////////////////  Inner Classes  //////////////////////


  /**
   * This class is used for DM's multi serial executor.
   * The serial messages are managed/executed by multiple serial thread.
   * This class takes care of executing messages related to a sender 
   * using the same thread.
   */
  static private class SerialQueuedExecutorPool  {
    /** To store the serial threads */
    ConcurrentMap serialQueuedExecutorMap = new ConcurrentHashMap(MAX_SERIAL_QUEUE_THREAD);
    
    /** To store the queue associated with thread */
    Map serialQueuedMap = new HashMap(MAX_SERIAL_QUEUE_THREAD);
    
    /** Holds mapping between sender to the serial thread-id */
    Map senderToSerialQueueIdMap = new HashMap();
    
    /** Holds info about unused thread, a thread is marked unused when the 
     *  member associated with it has left distribution system. 
     */
    ArrayList threadMarkedForUse = new ArrayList();
    
    DistributionStats stats;
    ThreadGroup threadGroup;
    
    final boolean throttlingDisabled;
    
    /**
     * Constructor.
     * @param group thread group to which the threads will belog to.
     * @param stats 
     */
    SerialQueuedExecutorPool(ThreadGroup group, DistributionStats stats, boolean throttlingDisabled) {
      this.threadGroup = group;
      this.stats = stats;
      this.throttlingDisabled = throttlingDisabled;
    }

    /*
     * Returns an id of the thread in serialQueuedExecutorMap, thats mapped to the 
     * given seder.
     * 
     * @param sender 
     * @param createNew boolean flag to indicate whether to create a new id, if id
     *                  doesnot exists. 
     */
    private Integer getQueueId(InternalDistributedMember sender, boolean createNew) {
      // Create a new Id.
      Integer queueId;
      
      synchronized (senderToSerialQueueIdMap)
      { 
        // Check if there is a executor associated with this sender.
        queueId = (Integer)senderToSerialQueueIdMap.get(sender);
        
        if (!createNew || queueId != null){
          return queueId;
        }
        
        // Create new.
        // Check if any threads are availabe that is marked for Use.
        if (!threadMarkedForUse.isEmpty()){
          queueId = (Integer)threadMarkedForUse.remove(0);
        }
        // If Map is full, use the threads in round-robin fashion.
        if (queueId == null){
          queueId =  Integer.valueOf((serialQueuedExecutorMap.size() + 1) % MAX_SERIAL_QUEUE_THREAD);
        }
        senderToSerialQueueIdMap.put(sender, queueId);
      }    
      return queueId;      
    }
    
    /*
     * Returns the queue associated with this sender.
     * Used in FlowControl for throttling (based on queue size).
     */
    public OverflowQueueWithDMStats getSerialQueue(InternalDistributedMember sender) {
      Integer queueId = getQueueId(sender, false);
      if (queueId == null){
        return null;
      }
      return (OverflowQueueWithDMStats)serialQueuedMap.get(queueId);
    }

    /*
     * Returns the serial queue executor, before returning the thread this 
     * applies throttling, based on the total serial queue size (total - sum 
     * of all the serial queue size). 
     * The throttling is applied during put event, this doesnt block the extract 
     * operation on the queue. 
     * 
     */
    public SerialQueuedExecutorWithDMStats getThrottledSerialExecutor(InternalDistributedMember sender) {
      SerialQueuedExecutorWithDMStats executor = getSerialExecutor(sender);

      // Get the total serial queue size.
      int totalSerialQueueMemSize = stats.getSerialQueueBytes();

      // for tcp socket reader threads, this code throttles the thread
      // to keep the sender-side from overwhelming the receiver.
      // UDP readers are throttled in the FC protocol, which queries
      // the queue to see if it should throttle
      if (stats.getSerialQueueBytes() > TOTAL_SERIAL_QUEUE_THROTTLE  &&
          !DistributionMessage.isPreciousThread())
      {
        do { 
          boolean interrupted = Thread.interrupted();
          try {
            float throttlePercent = (float)(totalSerialQueueMemSize - TOTAL_SERIAL_QUEUE_THROTTLE) / (float)(TOTAL_SERIAL_QUEUE_BYTE_LIMIT - TOTAL_SERIAL_QUEUE_THROTTLE);
            int sleep = (int)(100.0 * throttlePercent);
            sleep = Math.max(sleep, 1);
            Thread.sleep(sleep);
          } catch (InterruptedException ex) {
            interrupted = true;
            // FIXME-InterruptedException
            // Perhaps we should return null here?
          }
          finally {
            if (interrupted) {
              Thread.currentThread().interrupt();
            }
          }
          this.stats.getSerialQueueHelper().incThrottleCount();
        } while (stats.getSerialQueueBytes() >= TOTAL_SERIAL_QUEUE_BYTE_LIMIT);
      }      
      return executor;    
    }

    /*
     * Returns the serial queue executor for the given sender.
     */
    public SerialQueuedExecutorWithDMStats getSerialExecutor(InternalDistributedMember sender) {
      SerialQueuedExecutorWithDMStats executor = null;      
      Integer queueId = getQueueId(sender, true);
      if ((executor = (SerialQueuedExecutorWithDMStats)serialQueuedExecutorMap.get(queueId)) != null){
        return executor;
      }
      // If executor doesn't exists for this sender, create one.
      executor = createSerialExecutor(queueId);
      
      serialQueuedExecutorMap.put(queueId, executor);  
         
      if (logger.isDebugEnabled()){
        logger.debug("Created Serial Queued Executor With queueId {}. Total number of live Serial Threads :{}", 
            queueId, serialQueuedExecutorMap.size());          
      }
      stats.incSerialPooledThread();
      return executor;
    }

    /*
     * Creates a serial queue executor.
     */
    private SerialQueuedExecutorWithDMStats createSerialExecutor(final Integer id) {
      
      BlockingQueue poolQueue;
      
      if (SERIAL_QUEUE_BYTE_LIMIT == 0 || this.throttlingDisabled) {
        poolQueue = new OverflowQueueWithDMStats(stats.getSerialQueueHelper());
      } else {
        poolQueue = new ThrottlingMemLinkedQueueWithDMStats(SERIAL_QUEUE_BYTE_LIMIT, SERIAL_QUEUE_THROTTLE, SERIAL_QUEUE_SIZE_LIMIT, SERIAL_QUEUE_SIZE_THROTTLE, this.stats.getSerialQueueHelper());
      }
      
      serialQueuedMap.put(id, poolQueue);
      
      ThreadFactory tf = new ThreadFactory() {
        public Thread newThread(final Runnable command) {
          SerialQueuedExecutorPool.this.stats.incSerialPooledThreadStarts();
          final Runnable r = new Runnable() {
            public void run() {
              ConnectionTable.threadWantsSharedResources();
              Connection.makeReaderThread();
              try {
                command.run();
              } finally {
                ConnectionTable.releaseThreadsSockets();
              }
            }
          };
          
          Thread thread = new Thread(threadGroup, r, "Pooled Serial Message Processor " + id);
          thread.setDaemon(true);
          return thread;
        }
      };
      return new SerialQueuedExecutorWithDMStats(poolQueue, this.stats.getSerialPooledProcessorHelper(), tf);
    }
    
    /*
     * Does cleanup relating to this member. And marks the serial executor associated
     * with this member for re-use.
     */
    public void handleMemberDeparture(InternalDistributedMember member)
    {
      Integer queueId = getQueueId(member, false);
      if (queueId == null){
        return;
      }
      
      boolean isUsed = false;
      
      synchronized (senderToSerialQueueIdMap)
      { 
        senderToSerialQueueIdMap.remove(member);
        
        // Check if any other members are using the same executor.
        for (Iterator iter = senderToSerialQueueIdMap.values().iterator(); iter.hasNext();) {
          Integer value = (Integer)iter.next();
          if (value.equals(queueId))
          {
            isUsed = true;
            break;
          }
        } 
        
        // If not used mark this as unused.
        if (!isUsed)
        {
          if (logger.isInfoEnabled(LogMarker.DM))
            logger.info(LogMarker.DM, LocalizedMessage.create(
                LocalizedStrings.DistributionManager_MARKING_THE_SERIALQUEUEDEXECUTOR_WITH_ID__0__USED_BY_THE_MEMBER__1__TO_BE_UNUSED,
                new Object[] {queueId, member}));      
          
          threadMarkedForUse.add(queueId);
        }        
      }
    }
    
    public void awaitTermination(long time, TimeUnit unit) throws InterruptedException {
      long timeNanos = unit.toNanos(time);
      long remainingNanos = timeNanos;
      long start = System.nanoTime();
      for (Iterator iter = serialQueuedExecutorMap.values().iterator(); iter.hasNext();) {
        ExecutorService executor = (ExecutorService)iter.next();
        executor.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS);
        remainingNanos = timeNanos = (System.nanoTime() - start);
        if(remainingNanos <= 0) {
          return;
        }
      }
    }
    
    protected void shutdown(){
      for (Iterator iter = serialQueuedExecutorMap.values().iterator(); iter.hasNext();) {
        ExecutorService executor = (ExecutorService)iter.next();
        executor.shutdown();
      }
    }
  }

  /**
   * A simple class used for locking the list of members of the
   * distributed system.  We give this lock its own class so that it
   * shows up nicely in stack traces.
   */
  private static final class MembersLock  {
    protected MembersLock() {

    }
  }

  /**
   * A simple class used for locking the list of membership listeners.
   * We give this lock its own class so that it shows up nicely in
   * stack traces.
   */
  private static final class MembershipListenersLock  {
    protected MembershipListenersLock() {
    }
  }

  /**
   * This is the listener implementation for responding from events from
   * the Membership Manager.
   *
   */
  private final class MyListener implements DistributedMembershipListener {
    DistributionManager dm;

    public MyListener(DistributionManager dm) {
      this.dm = dm;
    }
    
    public boolean isShutdownMsgSent() {
      return shutdownMsgSent;
    }
    
    public void membershipFailure(String reason, Throwable t) {
      exceptionInThreads = true;
      DistributionManager.this.rootCause = t;
      getSystem().disconnect(reason, t, true);
    }

     public void messageReceived(DistributionMessage message) {
      handleIncomingDMsg(message);
    }

    public void newMemberConnected(InternalDistributedMember member) {
      // Do not elect the elder here as surprise members invoke this callback
      // without holding the view lock.  That can cause a race condition and
      // subsequent deadlock (#45566).  Elder selection is now done when a view
      // is installed.
      dm.addNewMember(member);
    }

    public void memberDeparted(InternalDistributedMember theId, boolean crashed, String reason) {
      boolean wasAdmin = getAdminMemberSet().contains(theId);
      if (wasAdmin) {
        // Pretend we received an AdminConsoleDisconnectMessage from the console that
        // is no longer in the JavaGroup view.
        // He must have died without sending a ShutdownMessage.
        // This fixes bug 28454.
        AdminConsoleDisconnectMessage message = new AdminConsoleDisconnectMessage();
        message.setSender(theId);
        message.setCrashed(crashed);
        message.setAlertListenerExpected(true);
        message.setIgnoreAlertListenerRemovalFailure(true); // we don't know if it was a listener so don't issue a warning
        message.setRecipient(myid);
        message.setReason(reason); //added for #37950
        handleIncomingDMsg(message);
      }
      dm.handleManagerDeparture(theId, crashed, reason);
    }
    
    public void memberSuspect(InternalDistributedMember suspect, InternalDistributedMember whoSuspected, String reason) {
      dm.handleManagerSuspect(suspect, whoSuspected, reason);
    }
    
    public void viewInstalled(NetView view) {
      processElderSelection();
      dm.handleViewInstalled(view);
    }
    
    /** this is invoked when quorum is being lost, before the view has been installed */
    public void quorumLost(Set<InternalDistributedMember> failures, List<InternalDistributedMember> remaining) {
      dm.handleQuorumLost(failures, remaining);
    }

    public DistributionManager getDM()
    {
      return dm;
    }
    
    private void processElderSelection() {
      // If we currently had no elder, this member might be the elder;
      // go through the selection process and decide now.
      try {
        dm.selectElder();
      }
      catch (DistributedSystemDisconnectedException e) {
        // ignore
      }
    }
  }
  
 
  private static abstract class MemberEvent  {
    
    private InternalDistributedMember id;
    MemberEvent(InternalDistributedMember id) {
      this.id = id;
    }
    public InternalDistributedMember getId() {
      return this.id;
    }

    public void handleEvent(DistributionManager manager) {
      handleEvent(manager, manager.membershipListeners.keySet());
      handleEvent(manager, manager.allMembershipListeners);
    }

    protected abstract void handleEvent(MembershipListener listener);

    protected void handleEvent(DistributionManager manager, Set<MembershipListener> membershipListeners) {
      for (MembershipListener listener : membershipListeners) {
        try {
          handleEvent(listener);
        } catch (CancelException e) {
          if (manager.isCloseInProgress()) {
            if (logger.isTraceEnabled()) {
              logger.trace("MemberEventInvoker: cancelled");
            }
          }
          else {
            logger.warn(LocalizedMessage.create(LocalizedStrings.DistributionManager_UNEXPECTED_CANCELLATION), e);
          }
          break;
        } catch (VirtualMachineError err) {
          SystemFailure.initiateFailure(err);
          // If this ever returns, rethrow the error.  We're poisoned
          // now, so don't let this thread continue.
          throw err;
        } catch (Throwable t) {
          // Whenever you catch Error or Throwable, you must also
          // catch VirtualMachineError (see above).  However, there is
          // _still_ a possibility that you are dealing with a cascading
          // error condition, so you also need to check to see if the JVM
          // is still usable:
          SystemFailure.checkFailure();
          logger.warn(LocalizedMessage.create(LocalizedStrings.DistributionManager_EXCEPTION_WHILE_CALLING_MEMBERSHIP_LISTENER_FOR_EVENT__0, this), t);
        }
      }
    }
}
  
  /**
   * This is an event reflecting that a InternalDistributedMember has joined
   * the system.
   * 
   *
   */
  private static final class MemberJoinedEvent extends MemberEvent  {
    MemberJoinedEvent(InternalDistributedMember id) {
      super(id);
    }
    @Override
    public String toString() {
      return "member " + getId() + " joined";
    }
    @Override
    protected void handleEvent(MembershipListener listener) {
      listener.memberJoined(getId());	
    }
  }
  
  /**
   * This is an event reflecting that a InternalDistributedMember has left the system.
   *
   */
  private static final class MemberDepartedEvent extends MemberEvent  {
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
    protected void handleEvent(MembershipListener listener) {
     listener.memberDeparted(getId(), false);	
    }
  }
  
  /**
   * This is an event reflecting that a InternalDistributedMember has left the
   * system in an unexpected way.
   * 
   *
   */
  private static final class MemberCrashedEvent extends MemberEvent  {
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
    protected void handleEvent(MembershipListener listener) {
      listener.memberDeparted(getId(), true/*crashed*/);	
    }
  }

  /**
   * This is an event reflecting that a InternalDistributedMember may be missing
   * but has not yet left the system.
   */
  private static final class MemberSuspectEvent extends MemberEvent {
    InternalDistributedMember whoSuspected;
    String reason;
    MemberSuspectEvent(InternalDistributedMember suspect, InternalDistributedMember whoSuspected, String reason) {
      super(suspect);
      this.whoSuspected = whoSuspected;
      this.reason = reason;
    }
    public InternalDistributedMember whoSuspected() {
      return this.whoSuspected;
    }
    
    public String getReason() {
      return this.reason;
    }
    
    @Override
    public String toString() {
      return "member " + getId() + " suspected by: " + this.whoSuspected + " reason: " + reason;
    }
    @Override
    protected void handleEvent(MembershipListener listener) {
      listener.memberSuspect(getId(), whoSuspected(), reason);	
    }
  }
  
  private static final class ViewInstalledEvent extends MemberEvent {
    NetView view;
    ViewInstalledEvent(NetView view) {
      super(null);
      this.view = view;
    }
    public long getViewId() {
      return view.getViewId();
    }
    @Override
    public String toString() {
      return "view installed: " + this.view;
    }
    @Override
    public void handleEvent(DistributionManager manager) {
      manager.handleViewInstalledEvent(this);
    }
    @Override
    protected void handleEvent(MembershipListener listener) {
      throw new UnsupportedOperationException();
    }
  }

  private static final class QuorumLostEvent extends MemberEvent {
    Set<InternalDistributedMember> failures;
    List<InternalDistributedMember> remaining;
    
    QuorumLostEvent(Set<InternalDistributedMember> failures, List<InternalDistributedMember> remaining) {
      super(null);
      this.failures = failures;
      this.remaining = remaining;
    }
    public Set<InternalDistributedMember> getFailures() {
      return this.failures;
    }
    public List<InternalDistributedMember> getRemaining() {
      return this.remaining;
    }
    @Override
    public String toString() {
      return "quorum lost.  failures=" + failures + "; remaining=" + remaining;
    }
    @Override
    protected void handleEvent(MembershipListener listener) {
      listener.quorumLost(getFailures(), getRemaining());	
    }
  }


  /* (non-Javadoc)
   * @see com.gemstone.gemfire.distributed.internal.DM#getRootCause()
   */
  public Throwable getRootCause() {
    return this.rootCause;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.distributed.internal.DM#setRootCause(java.lang.Throwable)
   */
  public void setRootCause(Throwable t) {
    this.rootCause = t;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.distributed.internal.DM#getMembersOnThisHost()
   * @since gemfire59poc
   */
  public Set<InternalDistributedMember> getMembersInThisZone() {
    return getMembersInSameZone(getDistributionManagerId());
  }
  
  public Set<InternalDistributedMember> getMembersInSameZone(InternalDistributedMember targetMember) {
    Set<InternalDistributedMember> buddyMembers = new HashSet<InternalDistributedMember>();
    if(!redundancyZones.isEmpty()) {
      synchronized(redundancyZones) {
        String targetZone = redundancyZones.get(targetMember);
        for(Map.Entry<InternalDistributedMember, String> entry : redundancyZones.entrySet()) {
          if(entry.getValue().equals(targetZone)) {
            buddyMembers.add(entry.getKey());
          }
        }
      }
    } else {
      buddyMembers.add(targetMember);
      Set targetAddrs = getEquivalents(targetMember.getInetAddress());
      for (Iterator i = getDistributionManagerIds().iterator(); i.hasNext();) {
        InternalDistributedMember o = (InternalDistributedMember)i.next();
        if (SetUtils.intersectsWith(targetAddrs, getEquivalents(o.getInetAddress()))) {
          buddyMembers.add(o);
        }
      }
    }
    return buddyMembers;
  }
  
  public boolean areInSameZone(InternalDistributedMember member1,
      InternalDistributedMember member2) {
    
    if(!redundancyZones.isEmpty()) {
      String zone1 = redundancyZones.get(member1);
      String zone2 = redundancyZones.get(member2);
      return zone1 != null && zone1.equals(zone2);
    } else {
      return areOnEquivalentHost(member1, member2);
    }
  }

  public void acquireGIIPermitUninterruptibly() {
    this.parallelGIIs.acquireUninterruptibly();
    this.stats.incInitialImageRequestsInProgress(1);
  }
  
  public void releaseGIIPermit() {
    this.stats.incInitialImageRequestsInProgress(-1);
    this.parallelGIIs.release();
  }

  public void setDistributedSystemId(int distributedSystemId) {
    if (distributedSystemId != -1) {
      this.distributedSystemId = distributedSystemId;
    }
  }
  
  public int getDistributedSystemId() {
    return this.distributedSystemId;
  }
  
  /**
   * this causes all members in the system to log thread dumps
   * If useNative is true we attempt to use OSProcess native code
   * for the dumps.  This goes to stdout instead of the system.log files.
   */
  public void printDistributedSystemStacks(boolean useNative) {
    printStacks(new HashSet(getDistributionManagerIds()), useNative);
  }
  
  /**
   * this causes the given InternalDistributedMembers to log thread dumps.
   * If useNative is true we attempt to use OSProcess native code
   * for the dumps.  This goes to stdout instead of the system.log files.
   */
  public void printStacks(Collection ids, boolean useNative) {
    Set requiresMessage = new HashSet();
    if (ids.contains(myid)) {
      OSProcess.printStacks(0, useNative);
    }
    if (useNative) {
      requiresMessage.addAll(ids);
      ids.remove(myid);
    } else {
      for (Iterator it=ids.iterator(); it.hasNext(); ) {
        InternalDistributedMember mbr = (InternalDistributedMember)it.next();
        if (mbr.getProcessId() > 0 && mbr.getInetAddress().equals(this.myid.getInetAddress())) {
          if (!mbr.equals(myid)) {
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

  public Set<DistributedMember> getGroupMembers(String group) {
    HashSet<DistributedMember> result = null;
    for (DistributedMember m: (Set<DistributedMember>)getDistributionManagerIdsIncludingAdmin()) {
      if (m.getGroups().contains(group)) {
        if (result == null) {
          result = new HashSet<DistributedMember>();
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
  public Set getNormalDistributionManagerIds() {
    // access to members synchronized under membersLock in order to 
    // ensure serialization
    synchronized (this.membersLock) {
      HashSet<InternalDistributedMember> result = new HashSet<InternalDistributedMember>();
      for (InternalDistributedMember m: this.members.keySet()) {
        if (m.getVmKind() != DistributionManager.LOCATOR_DM_TYPE) {
          result.add(m);
        }
      }
      return result;
    }
  }
  
  public Set<InternalDistributedMember> getLocatorDistributionManagerIds() {
    // access to members synchronized under membersLock in order to 
    // ensure serialization
    synchronized (this.membersLock) {
      HashSet<InternalDistributedMember> result = new HashSet<InternalDistributedMember>();
      for (InternalDistributedMember m: this.members.keySet()) {
        if (m.getVmKind() == DistributionManager.LOCATOR_DM_TYPE) {
          result.add(m);
        }
      }
      return result;
    }
  }
}
