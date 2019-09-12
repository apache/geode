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
package org.apache.geode.internal.admin.remote;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.IncompatibleSystemException;
import org.apache.geode.SystemFailure;
import org.apache.geode.admin.OperationCancelledException;
import org.apache.geode.admin.RuntimeAdminException;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem.DisconnectListener;
import org.apache.geode.distributed.internal.InternalDistributedSystem.ReconnectListener;
import org.apache.geode.distributed.internal.MembershipListener;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.admin.Alert;
import org.apache.geode.internal.admin.AlertListener;
import org.apache.geode.internal.admin.ApplicationVM;
import org.apache.geode.internal.admin.CacheCollector;
import org.apache.geode.internal.admin.CacheSnapshot;
import org.apache.geode.internal.admin.GemFireVM;
import org.apache.geode.internal.admin.GfManagerAgent;
import org.apache.geode.internal.admin.GfManagerAgentConfig;
import org.apache.geode.internal.admin.JoinLeaveListener;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LogWriterFactory;
import org.apache.geode.internal.logging.LoggingThread;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.security.AuthenticationFailedException;

/**
 * An implementation of <code>GfManagerAgent</code> that uses a {@link ClusterDistributionManager}
 * to communicate with other members of the distributed system. Because it is a
 * <code>MembershipListener</code> it is alerted when members join and leave the distributed system.
 * It also implements support for {@link JoinLeaveListener}s as well suport for collecting and
 * collating the pieces of a {@linkplain CacheCollector cache snapshot}.
 */
public
// Note that since we export the instances in a public list,
// I'm not permitting subclasses
class RemoteGfManagerAgent implements GfManagerAgent {

  private static final Logger logger = LogService.getLogger();

  // instance variables

  /**
   * The connection to the distributed system through which this admin agent communicates
   *
   * @since GemFire 4.0
   */
  protected volatile InternalDistributedSystem system;
  private final Object systemLock = new Object();

  /** Is this agent connected to the distributed system */
  protected volatile boolean connected = false;

  /** Is this agent listening for messages from the distributed system? */
  private volatile boolean listening = true;

  /**
   * A daemon thread that continuously attempts to connect to the distributed system.
   */
  private DSConnectionDaemon daemon;

  /** An alert listener that receives alerts */
  private final AlertListener alertListener;

  /** The level at which alerts are logged */
  protected /* final */ int alertLevel;

  /** The transport configuration used to connect to the distributed system */
  private final RemoteTransportConfig transport;

  /**
   * Optional display name used for
   * {@link org.apache.geode.distributed.DistributedSystem#getName()}.
   */
  private final String displayName;

  /** The <code>JoinLeaveListener</code>s registered with this agent */
  protected volatile Set listeners = Collections.EMPTY_SET;
  private final Object listenersLock = new Object();

  private CacheCollector collector;

  /**
   * The known application VMs. Maps member id to RemoteApplicationVM instances
   */
  protected volatile Map membersMap = Collections.EMPTY_MAP;
  private final Object membersLock = new Object();

  // LOG: used to log WARN for AuthenticationFailedException
  private final InternalLogWriter securityLogWriter;

  /**
   * A queue of <code>SnapshotResultMessage</code>s the are processed by a SnapshotResultDispatcher
   */
  protected BlockingQueue snapshotResults = new LinkedBlockingQueue();

  /** A thread that asynchronously handles incoming cache snapshots. */
  private SnapshotResultDispatcher snapshotDispatcher;

  /** Thread that processes membership joins */
  protected JoinProcessor joinProcessor;

  /** Ordered List for processing of pendingJoins; elements are InternalDistributedMember */
  protected volatile List pendingJoins = Collections.EMPTY_LIST;

  /** Lock for altering the contents of the pendingJoins Map and List */
  private final Object pendingJoinsLock = new Object();

  /** Id of the currentJoin that is being processed */
  protected volatile InternalDistributedMember currentJoin;

  /**
   * True if the currentJoin needs to be aborted because the member has left
   */
  protected volatile boolean abortCurrentJoin = false;

  /**
   * Has this <code>RemoteGfManagerAgent</code> been initialized? That is, after it has been
   * connected has this agent created admin objects for the initial members of the distributed
   * system?
   */
  protected volatile boolean initialized = false;

  /**
   * Has this agent manager been disconnected?
   */
  private boolean disconnected = false;

  /** DM MembershipListener for this RemoteGfManagerAgent */
  private MyMembershipListener myMembershipListener;
  private final Object myMembershipListenerLock = new Object();

  private DisconnectListener disconnectListener;

  private static final Object enumerationSync = new Object();

  /**
   * Safe to read, updates controlled by {@link #enumerationSync}
   */
  @MakeNotStatic
  private static volatile ArrayList allAgents = new ArrayList();

  private static void addAgent(RemoteGfManagerAgent toAdd) {
    synchronized (enumerationSync) {
      ArrayList replace = new ArrayList(allAgents);
      replace.add(toAdd);
      allAgents = replace;
    }
  }

  private static void removeAgent(RemoteGfManagerAgent toRemove) {
    synchronized (enumerationSync) {
      ArrayList replace = new ArrayList(allAgents);
      replace.remove(toRemove);
      allAgents = replace;
    }
  }

  /**
   * break any potential circularity in {@link #loadEmergencyClasses()}
   */
  @MakeNotStatic
  private static volatile boolean emergencyClassesLoaded = false;

  /**
   * Ensure that the InternalDistributedSystem class gets loaded.
   *
   * @see SystemFailure#loadEmergencyClasses()
   */
  public static void loadEmergencyClasses() {
    if (emergencyClassesLoaded)
      return;
    emergencyClassesLoaded = true;
    InternalDistributedSystem.loadEmergencyClasses();
  }

  /**
   * Close all of the RemoteGfManagerAgent's.
   *
   * @see SystemFailure#emergencyClose()
   */
  public static void emergencyClose() {
    ArrayList members = allAgents; // volatile fetch
    for (int i = 0; i < members.size(); i++) {
      RemoteGfManagerAgent each = (RemoteGfManagerAgent) members.get(i);
      each.system.emergencyClose();
    }
  }

  /**
   * Return a recent (though possibly incomplete) list of all existing agents
   *
   * @return list of agents
   */
  public static ArrayList getAgents() {
    return allAgents;
  }

  // constructors

  /**
   * Creates a new <code>RemoteGfManagerAgent</code> accord to the given config. Along the way it
   * (attempts to) connects to the distributed system.
   */
  public RemoteGfManagerAgent(GfManagerAgentConfig cfg) {
    if (!(cfg.getTransport() instanceof RemoteTransportConfig)) {
      throw new IllegalArgumentException(
          String.format("Expected %s to be a RemoteTransportConfig",
              cfg.getTransport()));
    }
    this.transport = (RemoteTransportConfig) cfg.getTransport();
    this.displayName = cfg.getDisplayName();
    this.alertListener = cfg.getAlertListener();
    if (this.alertListener != null) {
      if (this.alertListener instanceof JoinLeaveListener) {
        addJoinLeaveListener((JoinLeaveListener) this.alertListener);
      }
    }
    int tmp = cfg.getAlertLevel();
    if (this.alertListener == null) {
      tmp = Alert.OFF;
    }
    alertLevel = tmp;

    // LOG: get LogWriter from the AdminDistributedSystemImpl -- used for
    // AuthenticationFailedException
    InternalLogWriter logWriter = cfg.getLogWriter();
    if (logWriter == null) {
      throw new NullPointerException("LogWriter must not be null");
    }
    if (logWriter.isSecure()) {
      this.securityLogWriter = logWriter;
    } else {
      this.securityLogWriter = LogWriterFactory.toSecurityLogWriter(logWriter);
    }

    this.disconnectListener = cfg.getDisconnectListener();

    this.joinProcessor = new JoinProcessor();
    this.joinProcessor.start();

    join();

    snapshotDispatcher = new SnapshotResultDispatcher();
    snapshotDispatcher.start();

    // Note that this makes this instance externally visible.
    // This is why this class is final.
    addAgent(this);
  }

  private void join() {
    daemon = new DSConnectionDaemon();
    daemon.start();
    // give the daemon some time to get us connected
    // we don't want to wait forever since there may be no one to connect to
    try {
      long endTime = System.currentTimeMillis() + 2000; // wait 2 seconds
      while (!connected && daemon.isAlive() && System.currentTimeMillis() < endTime) {
        daemon.join(200);
      }
    } catch (InterruptedException ignore) {
      Thread.currentThread().interrupt();
      // Peremptory cancellation check, but keep going
      this.system.getCancelCriterion().checkCancelInProgress(ignore);
    }
  }

  // static methods


  /**
   * Handles an <code>ExecutionException</code> by examining its cause and throwing an appropriate
   * runtime exception.
   */
  private static void handle(ExecutionException ex) {
    Throwable cause = ex.getCause();

    if (cause instanceof OperationCancelledException) {
      // Operation was cancelled, we don't necessary want to propagate
      // this up to the user.
      return;
    }
    if (cause instanceof DistributedSystemDisconnectedException) {
      throw new DistributedSystemDisconnectedException("While waiting for Future", ex);
    }

    // Don't just throw the cause because the stack trace can be
    // misleading. For instance, the cause might have occurred in a
    // different thread. In addition to the cause, we also want to
    // know which code was waiting for the Future.
    throw new RuntimeAdminException(
        "An ExceputionException was thrown while waiting for Future.",
        ex);
  }

  // Object methodsg

  @Override
  public String toString() {
    return "Distributed System " + this.transport;
  }


  // GfManagerAgent methods

  /**
   * Disconnects this agent from the distributed system. If this is a dedicated administration VM,
   * then the underlying connection to the distributed system is also closed.
   *
   * @return true if this agent was disconnected as a result of the invocation
   * @see RemoteGemFireVM#disconnect
   */
  @Override
  public boolean disconnect() {
    boolean disconnectedTrue = false;
    synchronized (this) {
      if (disconnected) {
        return false;
      }
      disconnected = true;
      disconnectedTrue = true;
    }
    try {
      listening = false;
      // joinProcessor.interrupt();
      joinProcessor.shutDown();
      boolean removeListener = this.alertLevel != Alert.OFF;
      if (this.isConnected() || (this.membersMap.size() > 0)) { // Hot fix from 6.6.3
        RemoteApplicationVM[] apps = (RemoteApplicationVM[]) listApplications(disconnectedTrue);
        for (int i = 0; i < apps.length; i++) {
          try {
            apps[i].disconnect(removeListener); // hit NPE here in ConsoleDistributionManagerTest so
                                                // fixed listApplications to exclude nulls returned
                                                // from future
          } catch (RuntimeException ignore) {
            // if we can't notify a member that we are disconnecting don't throw
            // an exception. We need to finish disconnecting other resources.
          }
        }
        try {
          DistributionManager dm = system.getDistributionManager();
          synchronized (this.myMembershipListenerLock) {
            if (this.myMembershipListener != null) {
              dm.removeMembershipListener(this.myMembershipListener);
            }
          }

          if (dm instanceof ClusterDistributionManager) {
            ((ClusterDistributionManager) dm).setAgent(null);
          }

        } catch (IllegalArgumentException ignore) {
          // this can happen when connectToDS has not yet done the add

        } catch (DistributedSystemDisconnectedException de) {
          // ignore a forced disconnect and finish clean-up
        }

        synchronized (systemLock) {
          if (system != null && ClusterDistributionManager.isDedicatedAdminVM()
              && system.isConnected()) {
            system.disconnect();
          }
          this.system = null;
        }
        this.connected = false;
      }

      daemon.shutDown();

      if (snapshotDispatcher != null) {
        snapshotDispatcher.shutDown();
      }
    } finally {
      removeAgent(this);
    }
    return true;
  }

  @Override
  public boolean isListening() {
    return listening;
  }

  /**
   * Returns whether or not this manager agent has created admin objects for the initial members of
   * the distributed system.
   *
   * @since GemFire 4.0
   */
  @Override
  public boolean isInitialized() {
    return this.initialized;
  }

  @Override
  public boolean isConnected() {
    return this.connected && system != null && system.isConnected();
  }

  @Override
  public ApplicationVM[] listApplications() {
    return listApplications(false);
  }

  public ApplicationVM[] listApplications(boolean disconnected) {// Hot fix from 6.6.3
    if (isConnected() || (this.membersMap.size() > 0 && disconnected)) {
      // removed synchronization on members...
      Collection remoteApplicationVMs = new ArrayList(this.membersMap.size());
      VMS: for (Iterator iter = this.membersMap.values().iterator(); iter.hasNext();) {
        Future future = (Future) iter.next();
        for (;;) {
          try {
            this.system.getCancelCriterion().checkCancelInProgress(null);
          } catch (DistributedSystemDisconnectedException de) {
            // ignore during forced disconnect
          }
          boolean interrupted = Thread.interrupted();
          try {
            Object obj = future.get();
            if (obj != null) {
              remoteApplicationVMs.add(obj);
            }
            break;
          } catch (InterruptedException ex) {
            interrupted = true;
          } catch (CancellationException ex) {
            continue VMS;
          } catch (ExecutionException ex) {
            handle(ex);
            continue VMS;
          } finally {
            if (interrupted) {
              Thread.currentThread().interrupt();
            }
          }
        } // for
      } // VMS

      RemoteApplicationVM[] array = new RemoteApplicationVM[remoteApplicationVMs.size()];
      remoteApplicationVMs.toArray(array);
      return array;

    } else {
      return new RemoteApplicationVM[0];
    }
  }

  @Override
  public GfManagerAgent[] listPeers() {
    return new GfManagerAgent[0];
  }

  /**
   * Registers a <code>JoinLeaveListener</code>. on this agent that is notified when membership in
   * the distributed system changes.
   */
  @Override
  public void addJoinLeaveListener(JoinLeaveListener observer) {
    synchronized (this.listenersLock) {
      final Set oldListeners = this.listeners;
      if (!oldListeners.contains(observer)) {
        final Set newListeners = new HashSet(oldListeners);
        newListeners.add(observer);
        this.listeners = newListeners;
      }
    }
  }

  /**
   * Deregisters a <code>JoinLeaveListener</code> from this agent.
   */
  @Override
  public void removeJoinLeaveListener(JoinLeaveListener observer) {
    synchronized (this.listenersLock) {
      final Set oldListeners = this.listeners;
      if (oldListeners.contains(observer)) {
        final Set newListeners = new HashSet(oldListeners);
        if (newListeners.remove(observer)) {
          this.listeners = newListeners;
        }
      }
    }
  }

  /**
   * Sets the <code>CacheCollector</code> that <code>CacheSnapshot</code>s are delivered to.
   */
  @Override
  public synchronized void setCacheCollector(CacheCollector collector) {
    this.collector = collector;
  }

  // misc instance methods

  public RemoteTransportConfig getTransport() {
    return this.transport;
  }

  /** Is this thread currently sending a message? */
  private static final ThreadLocal sending = new ThreadLocal() {
    @Override
    protected Object initialValue() {
      return Boolean.FALSE;
    }
  };

  /**
   * Sends an AdminRequest and waits for the AdminReponse
   */
  AdminResponse sendAndWait(AdminRequest msg) {
    try {
      if (((Boolean) sending.get()).booleanValue()) {
        throw new OperationCancelledException(
            String.format("Recursion detected while sending %s",
                msg));

      } else {
        sending.set(Boolean.TRUE);
      }

      ClusterDistributionManager dm =
          (ClusterDistributionManager) this.system.getDistributionManager();
      if (isConnected()) {
        return msg.sendAndWait(dm);
      } else {
        // bug 39824: generate CancelException if we're shutting down
        dm.getCancelCriterion().checkCancelInProgress(null);
        throw new RuntimeAdminException(
            String.format("%s is not currently connected.",
                this));
      }

    } finally {
      sending.set(Boolean.FALSE);
    }
  }

  /**
   * Sends a message and does not wait for a response
   */
  void sendAsync(DistributionMessage msg) {
    if (system != null) {
      system.getDistributionManager().putOutgoing(msg);
    }
  }

  /**
   * Returns the distributed system member (application VM or system VM) with the given
   * <code>id</code>.
   */
  public RemoteGemFireVM getMemberById(InternalDistributedMember id) {
    return getApplicationById(id);
  }

  /**
   * Returns the application VM with the given <code>id</code>.
   */
  public RemoteApplicationVM getApplicationById(InternalDistributedMember id) {
    if (isConnected()) {
      // removed synchronized(members)
      Future future = (Future) this.membersMap.get(id);
      if (future == null) {
        return null;
      }
      for (;;) {
        this.system.getCancelCriterion().checkCancelInProgress(null);
        boolean interrupted = Thread.interrupted();
        try {
          return (RemoteApplicationVM) future.get();
        } catch (InterruptedException ex) {
          interrupted = true;
        } catch (CancellationException ex) {
          return null;
        } catch (ExecutionException ex) {
          handle(ex);
          return null;
        } finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
      } // for
    } else {
      return null;
    }
  }

  /**
   * Returns a representation of the application VM with the given distributed system id. If there
   * does not already exist a representation for that member, a new one is created.
   */
  private RemoteApplicationVM addMember(final InternalDistributedMember id) {
    boolean runFuture = false;
    FutureTask future;
    synchronized (this.membersLock) {
      final Map oldMembersMap = this.membersMap;
      future = (FutureTask) oldMembersMap.get(id);
      if (future == null) {
        runFuture = true;
        if (this.abortCurrentJoin)
          return null;
        future = new FutureTask(new Callable() {
          @Override
          public Object call() throws Exception {
            // Do this work in a Future to avoid deadlock seen in
            // bug 31562.
            RemoteGfManagerAgent agent = RemoteGfManagerAgent.this;
            RemoteApplicationVM result = new RemoteApplicationVM(agent, id, alertLevel);
            result.startStatDispatcher();
            if (agent.abortCurrentJoin) {
              result.stopStatListening();
              return null;
            }
            return result;
          }
        });
        Map newMembersMap = new HashMap(oldMembersMap);
        newMembersMap.put(id, future);
        if (this.abortCurrentJoin)
          return null;
        this.membersMap = newMembersMap;
      }
    }

    if (runFuture) {
      // Run future outside of membersLock
      future.run();
    }

    for (;;) {
      this.system.getCancelCriterion().checkCancelInProgress(null);
      boolean interrupted = Thread.interrupted();
      try {
        return (RemoteApplicationVM) future.get();
      } catch (InterruptedException ex) {
        interrupted = true;
      } catch (CancellationException ex) {
        return null;
      } catch (ExecutionException ex) {
        handle(ex);
        return null;
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    } // for
  }

  /**
   * Removes and returns the representation of the application VM member of the distributed system
   * with the given <code>id</code>.
   */
  protected RemoteApplicationVM removeMember(InternalDistributedMember id) {
    Future future = null;
    synchronized (this.membersLock) {
      Map oldMembersMap = this.membersMap;
      if (oldMembersMap.containsKey(id)) {
        Map newMembersMap = new HashMap(oldMembersMap);
        future = (Future) newMembersMap.remove(id);
        if (future != null) {
          this.membersMap = newMembersMap;
        }
      }
    }

    if (future != null) {
      future.cancel(true);
      for (;;) {
        synchronized (systemLock) {
          if (system == null) {
            return null;
          }
          this.system.getCancelCriterion().checkCancelInProgress(null);
        }

        boolean interrupted = Thread.interrupted();
        try {
          return (RemoteApplicationVM) future.get();
        } catch (InterruptedException ex) {
          interrupted = true;
        } catch (CancellationException ex) {
          return null;
        } catch (ExecutionException ex) {
          handle(ex);
          return null;
        } finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
      } // for

    } else {
      return null;
    }
  }

  /**
   * Places a <code>SnapshotResultMessage</code> on a queue to be processed asynchronously.
   */
  void enqueueSnapshotResults(SnapshotResultMessage msg) {
    if (!isListening()) {
      return;
    }
    for (;;) {
      this.system.getCancelCriterion().checkCancelInProgress(null);
      boolean interrupted = Thread.interrupted();
      try {
        snapshotResults.put(msg);
        break;
      } catch (InterruptedException ignore) {
        interrupted = true;
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  /**
   * Sends the given <code>alert</code> to this agent's {@link AlertListener}.
   */
  void callAlertListener(Alert alert) {
    if (!isListening()) {
      return;
    }
    if (alertListener != null && alert.getLevel() >= this.alertLevel) {
      alertListener.alert(alert);
    }
  }

  /**
   * Invokes the {@link CacheCollector#resultsReturned} method of this agent's cache collector.
   */
  protected void callCacheCollector(CacheSnapshot results, InternalDistributedMember sender,
      int snapshotId) {
    if (!isListening()) {
      return;
    }
    if (collector != null) {
      GemFireVM vm = getMemberById(sender);
      if (vm != null) {
        collector.resultsReturned(results, vm, snapshotId);
      }
    }
  }

  protected DisconnectListener getDisconnectListener() {
    synchronized (this) {
      return this.disconnectListener;
    }
  }

  /**
   * Creates a new {@link InternalDistributedSystem} connection for this agent. If one alread
   * exists, it is <code>disconnected</code> and a new one is created.
   */
  protected void connectToDS() {
    if (!isListening()) {
      return;
    }
    Properties props = this.transport.toDSProperties();
    if (this.displayName != null && this.displayName.length() > 0) {
      props.setProperty("name", this.displayName);
    }

    synchronized (systemLock) {
      if (system != null) {
        system.disconnect();
        system = null;
      }
      this.system = (InternalDistributedSystem) InternalDistributedSystem.connectForAdmin(props);
    }

    DistributionManager dm = system.getDistributionManager();
    if (dm instanceof ClusterDistributionManager) {
      ((ClusterDistributionManager) dm).setAgent(this);
    }

    synchronized (this) {
      this.disconnected = false;
    }

    this.system.addDisconnectListener(new InternalDistributedSystem.DisconnectListener() {
      @Override
      public String toString() {
        return String.format("Disconnect listener for %s",
            RemoteGfManagerAgent.this);
      }

      @Override
      public void onDisconnect(InternalDistributedSystem sys) {
        // Before the disconnect handler is called, the InternalDistributedSystem has already marked
        // itself for
        // the disconnection. Hence the check for RemoteGfManagerAgent.this.isConnected() always
        // returns false.
        // Hence commenting the same.
        // if(RemoteGfManagerAgent.this.isConnected()) {
        boolean reconnect = sys.isReconnecting();
        if (!reconnect) {
          final DisconnectListener listener = RemoteGfManagerAgent.this.getDisconnectListener();
          if (RemoteGfManagerAgent.this.disconnect()) { // returns true if disconnected during this
                                                        // call
            if (listener != null) {
              listener.onDisconnect(sys);
            }
          }
        }
      }
    });
    InternalDistributedSystem.addReconnectListener(new ReconnectListener() {
      @Override
      public void reconnecting(InternalDistributedSystem oldsys) {}

      @Override
      public void onReconnect(InternalDistributedSystem oldsys, InternalDistributedSystem newsys) {
        if (logger.isDebugEnabled()) {
          logger
              .debug("RemoteGfManagerAgent.onReconnect attempting to join new distributed system");
        }
        join();
      }
    });

    synchronized (this.myMembershipListenerLock) {
      this.myMembershipListener = new MyMembershipListener();
      dm.addMembershipListener(this.myMembershipListener);
      List initialMembers = dm.getDistributionManagerIds();
      this.myMembershipListener.addMembers(new HashSet(initialMembers));

      if (logger.isDebugEnabled()) {
        StringBuffer sb = new StringBuffer("[RemoteGfManagerAgent] ");
        sb.append("Connected to DS with ");
        sb.append(initialMembers.size());
        sb.append(" members: ");

        for (Iterator it = initialMembers.iterator(); it.hasNext();) {
          InternalDistributedMember member = (InternalDistributedMember) it.next();
          sb.append(member);
          sb.append(" ");
        }

        this.logger.debug(sb.toString());
      }

      connected = true;

      for (Iterator it = initialMembers.iterator(); it.hasNext();) {
        InternalDistributedMember member = (InternalDistributedMember) it.next();

        // Create the admin objects synchronously. We don't need to use
        // the JoinProcess when we first start up.
        try {
          handleJoined(member);
        } catch (OperationCancelledException ex) {
          if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
            logger.trace(LogMarker.DM_VERBOSE, "join cancelled by departure");
          }
        }
      }

      this.initialized = true;
    } // sync
  }

  //////////// inner classes ///////////////////////////

  /**
   * A Daemon thread that asynchronously connects to the distributed system. It is necessary to
   * nicely handle the situation when the user accidentally connects to a distributed system with no
   * members or attempts to connect to a distributed system whose member run a different version of
   * GemFire.
   */
  private class DSConnectionDaemon extends LoggingThread {
    /** Has this thread been told to stop? */
    private volatile boolean shutDown = false;

    protected DSConnectionDaemon() {
      super("DSConnectionDaemon");
    }

    public void shutDown() {
      shutDown = true;
      this.interrupt();
    }

    /**
     * Keep trying to connect to the distributed system. If we have problems connecting, the agent
     * will not be marked as "connected".
     */
    @Override
    public void run() {
      TOP: while (!shutDown) {
        SystemFailure.checkFailure();
        try {
          connected = false;
          initialized = false;
          if (!shutDown) {
            connectToDS();

            // If we're successful, this thread is done
            if (isListening()) {
              Assert.assertTrue(system != null);
            }
            return;
          }
        } catch (IncompatibleSystemException ise) {
          logger.fatal(ise.getMessage(), ise);
          callAlertListener(new VersionMismatchAlert(RemoteGfManagerAgent.this, ise.getMessage()));
        } catch (Exception e) {
          for (Throwable cause = e; cause != null; cause = cause.getCause()) {
            if (cause instanceof InterruptedException) {
              // We were interrupted while connecting. Most likely we
              // are being shutdown.
              if (shutDown) {
                break TOP;
              }
            }

            if (cause instanceof AuthenticationFailedException) {
              // Incorrect credentials. Log & Shutdown
              shutDown = true;
              securityLogWriter.warning(
                  "[RemoteGfManagerAgent]: An AuthenticationFailedException was caught while connecting to DS",
                  e);
              break TOP;
            }
          }

          logger.debug("[RemoteGfManagerAgent] While connecting to DS", e);
        }
        try {
          sleep(1000);
        } catch (InterruptedException ignore) {
          // We're just exiting, no need to restore the interrupt bit.
        }
      }
      connected = false;
      initialized = false;
    }

  } // end DSConnectionDaemon

  /**
   * A daemon thread that reads {@link SnapshotResultMessage}s from a queue and invokes the
   * <code>CacheCollector</code> accordingly.
   */
  private class SnapshotResultDispatcher extends LoggingThread {
    private volatile boolean shutDown = false;

    public SnapshotResultDispatcher() {
      super("SnapshotResultDispatcher");
    }

    public void shutDown() {
      shutDown = true;
      this.interrupt();
    }

    @Override
    public void run() {
      while (!shutDown) {
        SystemFailure.checkFailure();
        try {
          SnapshotResultMessage msg = (SnapshotResultMessage) snapshotResults.take();
          callCacheCollector(msg.getSnapshot(), msg.getSender(), msg.getSnapshotId());
          yield(); // TODO: this is a hot thread
        } catch (InterruptedException ignore) {
          // We'll just exit, no need to reset interrupt bit.
          if (shutDown) {
            break;
          }
          logger.warn("Ignoring strange interrupt", ignore);
        } catch (Exception ex) {
          logger.fatal(ex.getMessage(), ex);
        }
      }
    }
  } // end SnapshotResultDispatcher

  @Override
  public DistributionManager getDM() {
    InternalDistributedSystem sys = this.system;
    if (sys == null) {
      return null;
    }
    return sys.getDistributionManager();
  }

  /** Returns the bind address this vm uses to connect to this system (Kirk Lund) */
  public String getBindAddress() {
    return this.transport.getBindAddress();
  }

  /** Returns true if this vm is using a non-default bind address (Kirk Lund) */
  public boolean hasNonDefaultBindAddress() {
    if (getBindAddress() == null)
      return false;
    return !DistributionConfig.DEFAULT_BIND_ADDRESS.equals(getBindAddress());
  }

  /**
   * Sets the alert level for this manager agent. Sends a {@link AlertLevelChangeMessage} to each
   * member of the distributed system.
   */
  @Override
  public void setAlertLevel(int level) {
    this.alertLevel = level;
    AlertLevelChangeMessage m = AlertLevelChangeMessage.create(level);
    sendAsync(m);
  }

  /**
   * Returns the distributed system administered by this agent.
   */
  @Override
  public InternalDistributedSystem getDSConnection() {
    return this.system;
  }

  /**
   * Handles a membership join asynchronously from the memberJoined notification. Sets and clears
   * current join. Also makes several checks to support aborting of the current join.
   */
  protected void handleJoined(InternalDistributedMember id) {
    if (!isListening()) {
      return;
    }

    // set current join and begin handling of it...
    this.currentJoin = id;
    try {
      GemFireVM member = null;
      switch (id.getVmKind()) {
        case ClusterDistributionManager.NORMAL_DM_TYPE:
          member = addMember(id);
          break;
        case ClusterDistributionManager.LOCATOR_DM_TYPE:
          break;
        case ClusterDistributionManager.ADMIN_ONLY_DM_TYPE:
          break;
        case ClusterDistributionManager.LONER_DM_TYPE:
          break; // should this ever happen? :-)
        default:
          throw new IllegalArgumentException(String.format("Unknown VM Kind: %s",
              Integer.valueOf(id.getVmKind())));
      }

      // if we have a valid member process it...
      if (this.abortCurrentJoin) {
        return;
      }
      if (member != null) {
        if (this.abortCurrentJoin) {
          return;
        }
        for (Iterator it = this.listeners.iterator(); it.hasNext();) {
          if (this.abortCurrentJoin) {
            return;
          }
          JoinLeaveListener l = (JoinLeaveListener) it.next();
          try {
            l.nodeJoined(RemoteGfManagerAgent.this, member);
          } catch (VirtualMachineError e) {
            SystemFailure.initiateFailure(e);
            throw e;
          } catch (Throwable e) {
            SystemFailure.checkFailure();
            logger.warn("Listener threw an exception.", e);
          }
        }
      }

    } finally {
      // finished this join so remove it...
      removePendingJoins(id);

      // clean up current join and abort flag...
      if (this.abortCurrentJoin) {
        logger.info("aborted  {}", id);
      }
      this.currentJoin = null;
      this.abortCurrentJoin = false;
    }
  }

  /**
   * Adds a pending join entry. Note: attempting to reuse the same ArrayList instance results in
   * some interesting deadlocks.
   */
  protected void addPendingJoin(InternalDistributedMember id) {
    synchronized (this.pendingJoinsLock) {
      List oldPendingJoins = this.pendingJoins;
      if (!oldPendingJoins.contains(id)) {
        List newPendingJoins = new ArrayList(oldPendingJoins);
        newPendingJoins.add(id);
        this.pendingJoins = newPendingJoins;
      }
    }
  }

  /**
   * Removes any pending joins that match the member id. Note: attempting to reuse the same
   * ArrayList instance results in some interesting deadlocks.
   */
  private void removePendingJoins(InternalDistributedMember id) {
    synchronized (this.pendingJoinsLock) {
      List oldPendingJoins = this.pendingJoins;
      if (oldPendingJoins.contains(id)) {
        List newPendingJoins = new ArrayList(oldPendingJoins);
        newPendingJoins.remove(id);
        this.pendingJoins = newPendingJoins;
      }
    }
  }

  /**
   * Cancels any pending joins that match the member id.
   */
  protected void cancelPendingJoins(InternalDistributedMember id) {
    try {
      // pause the join processor thread...
      this.joinProcessor.pauseHandling();

      // remove any further pending joins (should't be any)...
      removePendingJoins(id);

      // abort any in-process handling of the member id...
      this.joinProcessor.abort(id);
    } finally {
      AdminWaiters.cancelWaiters(id);
      this.joinProcessor.resumeHandling();
    }
  }

  /**
   * Processes pending membership joins in a dedicated thread. If we processed the joins in the same
   * thread as the membership handler, then we run the risk of getting deadlocks and such.
   */
  // FIXME: Revisit/redesign this code
  private class JoinProcessor extends LoggingThread {
    private volatile boolean paused = false;
    private volatile boolean shutDown = false;
    private volatile InternalDistributedMember id;
    private final Object lock = new Object();

    public JoinProcessor() {
      super("JoinProcessor");
    }

    public void shutDown() {
      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
        logger.trace(LogMarker.DM_VERBOSE, "JoinProcessor: shutting down");
      }
      this.shutDown = true;
      this.interrupt();
    }

    private void pauseHandling() {
      this.paused = true;
    }

    private void resumeHandling() {
      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
        logger.trace(LogMarker.DM_VERBOSE, "JoinProcessor: resuming.  Is alive? {}",
            this.isAlive());
      }

      // unpause if paused during a cancel...
      this.paused = false;

      // notify to wake up...
      synchronized (this.lock) {
        this.lock.notify();
      }
    }

    public void abort(InternalDistributedMember memberId) {
      // abort if current join matches id...
      if (memberId.equals(RemoteGfManagerAgent.this.currentJoin)) {
        RemoteGfManagerAgent.this.abortCurrentJoin = true;
        this.interrupt();
      }
      // cancel handling of current event if it matches id...
      if (this.id != null && this.id.equals(memberId)) {
        this.id = null;
      }
    }

    @Override
    public void run() {
      /* Used to check whether there were pendingJoins before waiting */
      boolean noPendingJoins = false;
      OUTER: while (!this.shutDown) {
        SystemFailure.checkFailure();
        try {
          if (!RemoteGfManagerAgent.this.isListening()) {
            shutDown();
          }

          noPendingJoins = RemoteGfManagerAgent.this.pendingJoins.isEmpty();
          if (noPendingJoins && logger.isDebugEnabled()) {
            logger.debug("Pausing as there are no pending joins ... ");
          }

          // if paused OR no pendingJoins then just wait...
          if (this.paused || noPendingJoins) {
            if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
              logger.trace(LogMarker.DM_VERBOSE, "JoinProcessor is about to wait...");
            }
            synchronized (this.lock) {
              this.lock.wait();
            }
          }
          if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
            logger.trace(LogMarker.DM_VERBOSE, "JoinProcessor has woken up...");
          }
          if (this.paused)
            continue;

          // if no join was already in process or if aborted, get a new one...
          if (this.id == null) {
            List pendingJoinsRef = RemoteGfManagerAgent.this.pendingJoins;
            if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
              logger.trace(LogMarker.DM_VERBOSE, "JoinProcessor pendingJoins: {}",
                  pendingJoinsRef.size());
            }
            if (pendingJoinsRef.size() > 0) {
              this.id = (InternalDistributedMember) pendingJoinsRef.get(0);
              if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
                logger.trace(LogMarker.DM_VERBOSE, "JoinProcessor got a membership event for {}",
                    this.id);
              }
            }
          }
          if (this.paused)
            continue;

          // process the join...
          if (this.id != null) {
            if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
              logger.trace(LogMarker.DM_VERBOSE, "JoinProcessor handling join for {}", this.id);
            }
            try {
              RemoteGfManagerAgent.this.handleJoined(this.id);
            } finally {
              this.id = null;
            }
          }

        } catch (CancelException e) {
          // we're done!
          shutDown = true; // for safety
          break;
        } catch (InterruptedException ignore) {
          // When this thread is "paused", it is interrupted
          if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
            logger.trace(LogMarker.DM_VERBOSE, "JoinProcessor has been interrupted...");
          }
          if (shutDown) {
            break;
          }
          if (this.paused || noPendingJoins) {// fix for #39893
            /*
             * JoinProcessor waits when: 1. this.paused is set to true 2. There are no pending joins
             *
             * If the JoinProcessor is interrupted when it was waiting due to second reason, it
             * should still continue after catching InterruptedException. From code, currently,
             * JoinProcessor is interrupted through JoinProcessor.abort() method which is called
             * when a member departs as part of cancelPendingJoin().
             */
            if (logger.isDebugEnabled()) {
              logger.debug("JoinProcessor was interrupted when it was paused, now resuming ...",
                  ignore);
            }
            noPendingJoins = false;
            continue;
          }
          break; // Panic!

        } catch (OperationCancelledException ex) {
          if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
            logger.trace(LogMarker.DM_VERBOSE, "join cancelled by departure");
          }
          continue;

        } catch (VirtualMachineError err) {
          SystemFailure.initiateFailure(err);
          // If this ever returns, rethrow the error. We're poisoned
          // now, so don't let this thread continue.
          throw err;
        } catch (Throwable e) {
          // Whenever you catch Error or Throwable, you must also
          // catch VirtualMachineError (see above). However, there is
          // _still_ a possibility that you are dealing with a cascading
          // error condition, so you also need to check to see if the JVM
          // is still usable:
          SystemFailure.checkFailure();
          for (Throwable cause = e.getCause(); cause != null; cause = cause.getCause()) {
            if (cause instanceof InterruptedException) {
              if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
                logger.trace(LogMarker.DM_VERBOSE, "JoinProcessor has been interrupted...");
              }
              continue OUTER;
            }
          }
          logger.error("JoinProcessor caught exception...", e);
        } // catch Throwable
      } // while !shutDown
    }
  }

  private class MyMembershipListener implements MembershipListener {

    private final Set distributedMembers = new HashSet();

    protected MyMembershipListener() {}

    protected void addMembers(Set initMembers) {
      distributedMembers.addAll(initMembers);
    }

    /**
     * This method is invoked when a new member joins the system. If the member is an application VM
     * or a GemFire system manager, we note it.
     *
     * @see JoinLeaveListener#nodeJoined
     */
    @Override
    public void memberJoined(DistributionManager distributionManager,
        final InternalDistributedMember id) {
      if (!isListening()) {
        return;
      }
      synchronized (this) {
        if (!this.distributedMembers.contains(id)) {
          this.distributedMembers.add(id);
          addPendingJoin(id);
          joinProcessor.resumeHandling();
        }
      }
    }

    @Override
    public void memberSuspect(DistributionManager distributionManager, InternalDistributedMember id,
        InternalDistributedMember whoSuspected, String reason) {}

    @Override
    public void quorumLost(DistributionManager distributionManager,
        Set<InternalDistributedMember> failures, List<InternalDistributedMember> remaining) {}

    /**
     * This method is invoked after a member has explicitly left the system. It may not get invoked
     * if a member becomes unreachable due to crash or network problems.
     *
     * @see JoinLeaveListener#nodeCrashed
     * @see JoinLeaveListener#nodeLeft
     */
    @Override
    public void memberDeparted(DistributionManager distributionManager,
        InternalDistributedMember id, boolean crashed) {
      synchronized (this) {
        if (!this.distributedMembers.remove(id)) {
          return;
        }
        cancelPendingJoins(id);
        if (!isListening()) {
          return;
        }
      }

      // remove the member...

      RemoteGemFireVM member = null;
      switch (id.getVmKind()) {
        case ClusterDistributionManager.NORMAL_DM_TYPE:
          member = removeMember(id);
          break;
        case ClusterDistributionManager.ADMIN_ONLY_DM_TYPE:
          break;
        case ClusterDistributionManager.LOCATOR_DM_TYPE:
          break;
        case ClusterDistributionManager.LONER_DM_TYPE:
          break; // should this ever happen?
        default:
          throw new IllegalArgumentException(
              "Unknown VM Kind");
      }

      // clean up and notify JoinLeaveListeners...
      if (member != null) {
        for (Iterator it = listeners.iterator(); it.hasNext();) {
          JoinLeaveListener l = (JoinLeaveListener) it.next();
          if (crashed) {
            l.nodeCrashed(RemoteGfManagerAgent.this, member);
          } else {
            l.nodeLeft(RemoteGfManagerAgent.this, member);
          }
        }
        member.stopStatListening();
      }
    }
  }

}
