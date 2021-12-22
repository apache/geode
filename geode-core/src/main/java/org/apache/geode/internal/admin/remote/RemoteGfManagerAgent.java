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

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.apache.geode.internal.logging.log4j.LogMarker.DM_VERBOSE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.IncompatibleSystemException;
import org.apache.geode.SystemFailure;
import org.apache.geode.admin.OperationCancelledException;
import org.apache.geode.admin.RuntimeAdminException;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
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
import org.apache.geode.internal.logging.LogWriterFactory;
import org.apache.geode.logging.internal.executors.LoggingThread;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.security.AuthenticationFailedException;

/**
 * An implementation of {@code GfManagerAgent} that uses a {@link ClusterDistributionManager}
 * to communicate with other members of the distributed system. Because it is a
 * {@code MembershipListener} it is alerted when members join and leave the distributed system.
 * It also implements support for {@link JoinLeaveListener}s as well suport for collecting and
 * collating the pieces of a {@linkplain CacheCollector cache snapshot}.
 */
public class RemoteGfManagerAgent implements GfManagerAgent {

  private static final Logger logger = LogService.getLogger();

  /**
   * The connection to the distributed system through which this admin agent communicates
   *
   * @since GemFire 4.0
   */
  private volatile InternalDistributedSystem system;

  private final Object systemLock = new Object();

  /**
   * Is this agent connected to the distributed system
   */
  private volatile boolean connected;

  /**
   * Is this agent listening for messages from the distributed system?
   */
  private volatile boolean listening = true;

  /**
   * A daemon thread that continuously attempts to connect to the distributed system.
   */
  private DSConnectionDaemon daemon;

  /**
   * An alert listener that receives alerts
   */
  private final AlertListener alertListener;

  /**
   * The level at which alerts are logged
   */
  private int alertLevel;

  /**
   * The transport configuration used to connect to the distributed system
   */
  private final RemoteTransportConfig transport;

  /**
   * Optional display name used for {@link DistributedSystem#getName()}.
   */
  private final String displayName;

  /**
   * The {@code JoinLeaveListener}s registered with this agent
   */
  private volatile Set<JoinLeaveListener> listeners = emptySet();
  private final Object listenersLock = new Object();

  private CacheCollector collector;

  /**
   * The known application VMs. Maps member id to RemoteApplicationVM instances
   */
  private volatile Map<InternalDistributedMember, Future<RemoteApplicationVM>> membersMap =
      emptyMap();
  private final Object membersLock = new Object();

  private final InternalLogWriter securityLogWriter;

  /**
   * A queue of {@code SnapshotResultMessage}s the are processed by a SnapshotResultDispatcher
   */
  private final BlockingQueue<SnapshotResultMessage> snapshotResults = new LinkedBlockingQueue<>();

  /**
   * A thread that asynchronously handles incoming cache snapshots.
   */
  private final SnapshotResultDispatcher snapshotDispatcher;

  /**
   * Thread that processes membership joins
   */
  private final JoinProcessor joinProcessor;

  /**
   * Ordered List for processing of pendingJoins; elements are InternalDistributedMember
   */
  private volatile List<InternalDistributedMember> pendingJoins = emptyList();

  /**
   * Lock for altering the contents of the pendingJoins Map and List
   */
  private final Object pendingJoinsLock = new Object();

  /**
   * Id of the currentJoin that is being processed
   */
  private volatile InternalDistributedMember currentJoin;

  /**
   * True if the currentJoin needs to be aborted because the member has left
   */
  private volatile boolean abortCurrentJoin;

  /**
   * Has this {@code RemoteGfManagerAgent} been initialized? That is, after it has been
   * connected has this agent created admin objects for the initial members of the distributed
   * system?
   */
  private volatile boolean initialized;

  /**
   * Has this agent manager been disconnected?
   */
  private boolean disconnected;

  /**
   * DM MembershipListener for this RemoteGfManagerAgent
   */
  private MyMembershipListener myMembershipListener;
  private final Object myMembershipListenerLock = new Object();

  private final DisconnectListener disconnectListener;

  private static final Object enumerationSync = new Object();

  /**
   * Safe to read, updates controlled by {@link #enumerationSync}
   */
  @MakeNotStatic
  private static volatile List<RemoteGfManagerAgent> allAgents = new ArrayList<>();

  /**
   * Is this thread currently sending a message?
   */
  private static final ThreadLocal<Boolean> sending = ThreadLocal.withInitial(() -> Boolean.FALSE);

  private final Function<Properties, InternalDistributedSystem> distributedSystemFactory;
  private final Function<RemoteGfManagerAgent, DSConnectionDaemon> dsConnectionDaemonFactory;
  private final Function<RemoteGfManagerAgent, MyMembershipListener> myMembershipListenerFactory;

  private static void addAgent(RemoteGfManagerAgent toAdd) {
    synchronized (enumerationSync) {
      List<RemoteGfManagerAgent> replace = new ArrayList<>(allAgents);
      replace.add(toAdd);
      allAgents = replace;
    }
  }

  private static void removeAgent(RemoteGfManagerAgent toRemove) {
    synchronized (enumerationSync) {
      List<RemoteGfManagerAgent> replace = new ArrayList<>(allAgents);
      replace.remove(toRemove);
      allAgents = replace;
    }
  }

  /**
   * Close all of the RemoteGfManagerAgent's.
   *
   * @see SystemFailure#emergencyClose()
   */
  public static void emergencyClose() {
    // volatile fetch
    List members = allAgents;
    for (Object member : members) {
      RemoteGfManagerAgent each = (RemoteGfManagerAgent) member;
      each.system.emergencyClose();
    }
  }

  /**
   * Creates a new {@code RemoteGfManagerAgent} accord to the given config. Along the way it
   * (attempts to) connects to the distributed system.
   */
  public RemoteGfManagerAgent(GfManagerAgentConfig cfg) {
    this(cfg,
        props -> (InternalDistributedSystem) InternalDistributedSystem.connectForAdmin(props),
        JoinProcessor::new,
        SnapshotResultDispatcher::new,
        DSConnectionDaemon::new,
        MyMembershipListener::new);
  }

  @VisibleForTesting
  RemoteGfManagerAgent(GfManagerAgentConfig cfg,
      Function<Properties, InternalDistributedSystem> distributedSystemFactory,
      Function<RemoteGfManagerAgent, JoinProcessor> joinProcessorFactory,
      Function<RemoteGfManagerAgent, SnapshotResultDispatcher> snapshotResultDispatcherFactory,
      Function<RemoteGfManagerAgent, DSConnectionDaemon> dsConnectionDaemonFactory,
      Function<RemoteGfManagerAgent, MyMembershipListener> myMembershipListenerFactory) {
    if (!(cfg.getTransport() instanceof RemoteTransportConfig)) {
      throw new IllegalArgumentException(
          String.format("Expected %s to be a RemoteTransportConfig", cfg.getTransport()));
    }

    this.distributedSystemFactory = distributedSystemFactory;
    this.dsConnectionDaemonFactory = dsConnectionDaemonFactory;
    this.myMembershipListenerFactory = myMembershipListenerFactory;

    transport = (RemoteTransportConfig) cfg.getTransport();
    displayName = cfg.getDisplayName();
    alertListener = cfg.getAlertListener();
    if (alertListener != null) {
      if (alertListener instanceof JoinLeaveListener) {
        addJoinLeaveListener((JoinLeaveListener) alertListener);
      }
    }
    int tmp = cfg.getAlertLevel();
    if (alertListener == null) {
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
      securityLogWriter = logWriter;
    } else {
      securityLogWriter = LogWriterFactory.toSecurityLogWriter(logWriter);
    }

    disconnectListener = cfg.getDisconnectListener();

    joinProcessor = joinProcessorFactory.apply(this);
    joinProcessor.start();

    join();

    snapshotDispatcher = snapshotResultDispatcherFactory.apply(this);
    snapshotDispatcher.start();

    addAgent(this);
  }

  private void join() {
    daemon = dsConnectionDaemonFactory.apply(this);
    daemon.start();
    // give the daemon some time to get us connected
    // we don't want to wait forever since there may be no one to connect to
    try {
      long endTime = System.currentTimeMillis() + 2000; // wait 2 seconds
      while (!connected && daemon.isAlive() && System.currentTimeMillis() < endTime) {
        daemon.join(200);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      // Peremptory cancellation check, but keep going
      system.getCancelCriterion().checkCancelInProgress(e);
    }
  }

  /**
   * Handles an {@code ExecutionException} by examining its cause and throwing an appropriate
   * runtime exception.
   */
  private static void handle(ExecutionException ex) {
    Throwable cause = ex.getCause();

    if (cause instanceof OperationCancelledException) {
      // Operation was cancelled, we don't necessary want to propagate this up to the user.
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
        "An ExecutionException was thrown while waiting for Future.", ex);
  }

  @Override
  public String toString() {
    return "Distributed System " + transport;
  }

  /**
   * Disconnects this agent from the distributed system. If this is a dedicated administration VM,
   * then the underlying connection to the distributed system is also closed.
   *
   * @return true if this agent was disconnected as a result of the invocation
   * @see RemoteGemFireVM#disconnect
   */
  @Override
  public boolean disconnect() {
    boolean disconnectedTrue;
    synchronized (this) {
      if (disconnected) {
        return false;
      }
      disconnected = true;
      disconnectedTrue = true;
    }
    try {
      listening = false;
      joinProcessor.shutDown();
      boolean removeListener = alertLevel != Alert.OFF;
      if (isConnected() || !membersMap.isEmpty()) {
        RemoteApplicationVM[] apps = (RemoteApplicationVM[]) listApplications(disconnectedTrue);
        for (RemoteApplicationVM app : apps) {
          try {
            app.disconnect(removeListener);
            // hit NPE here in ConsoleDistributionManagerTest so
            // fixed listApplications to exclude nulls returned from future
          } catch (RuntimeException ignore) {
            // if we can't notify a member that we are disconnecting don't throw
            // an exception. We need to finish disconnecting other resources.
          }
        }
        try {
          DistributionManager dm = system.getDistributionManager();
          synchronized (myMembershipListenerLock) {
            if (myMembershipListener != null) {
              dm.removeMembershipListener(myMembershipListener);
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
          if (system != null
              && ClusterDistributionManager.isDedicatedAdminVM()
              && system.isConnected()) {
            system.disconnect();
          }
          system = null;
        }
        connected = false;
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
    return initialized;
  }

  @Override
  public boolean isConnected() {
    return connected && system != null && system.isConnected();
  }

  @Override
  public ApplicationVM[] listApplications() {
    return listApplications(false);
  }

  private ApplicationVM[] listApplications(boolean disconnected) {
    if (isConnected() || !membersMap.isEmpty() && disconnected) {
      // removed synchronization on members...
      Collection<RemoteApplicationVM> remoteApplicationVMs = new ArrayList<>(membersMap.size());
      VMS: for (Future<RemoteApplicationVM> future : membersMap.values()) {
        for (;;) {
          try {
            system.getCancelCriterion().checkCancelInProgress(null);
          } catch (DistributedSystemDisconnectedException de) {
            // ignore during forced disconnect
          }
          boolean interrupted = Thread.interrupted();
          try {
            RemoteApplicationVM app = future.get();
            if (app != null) {
              remoteApplicationVMs.add(app);
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
        }
      }

      RemoteApplicationVM[] array = new RemoteApplicationVM[remoteApplicationVMs.size()];
      remoteApplicationVMs.toArray(array);
      return array;

    }
    return new RemoteApplicationVM[0];
  }

  @Override
  public GfManagerAgent[] listPeers() {
    return new GfManagerAgent[0];
  }

  /**
   * Registers a {@code JoinLeaveListener}. on this agent that is notified when membership in
   * the distributed system changes.
   */
  @Override
  public void addJoinLeaveListener(JoinLeaveListener observer) {
    synchronized (listenersLock) {
      Set<JoinLeaveListener> oldListeners = listeners;
      if (!oldListeners.contains(observer)) {
        Set<JoinLeaveListener> newListeners = new HashSet<>(oldListeners);
        newListeners.add(observer);
        listeners = newListeners;
      }
    }
  }

  /**
   * Deregisters a {@code JoinLeaveListener} from this agent.
   */
  @Override
  public void removeJoinLeaveListener(JoinLeaveListener observer) {
    synchronized (listenersLock) {
      Set<JoinLeaveListener> oldListeners = listeners;
      if (oldListeners.contains(observer)) {
        Set<JoinLeaveListener> newListeners = new HashSet<>(oldListeners);
        if (newListeners.remove(observer)) {
          listeners = newListeners;
        }
      }
    }
  }

  /**
   * Sets the {@code CacheCollector} that {@code CacheSnapshot}s are delivered to.
   */
  @Override
  public synchronized void setCacheCollector(CacheCollector collector) {
    this.collector = collector;
  }

  public RemoteTransportConfig getTransport() {
    return transport;
  }

  /**
   * Sends an AdminRequest and waits for the AdminReponse
   */
  AdminResponse sendAndWait(AdminRequest msg) {
    try {
      if (sending.get()) {
        throw new OperationCancelledException(
            String.format("Recursion detected while sending %s", msg));
      }
      sending.set(Boolean.TRUE);

      ClusterDistributionManager dm = (ClusterDistributionManager) system.getDistributionManager();
      if (isConnected()) {
        return msg.sendAndWait(dm);
      }
      // generate CancelException if we're shutting down
      dm.getCancelCriterion().checkCancelInProgress(null);
      throw new RuntimeAdminException(
          String.format("%s is not currently connected.", this));

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
   * {@code id}.
   */
  RemoteGemFireVM getMemberById(InternalDistributedMember id) {
    return getApplicationById(id);
  }

  /**
   * Returns the application VM with the given {@code id}.
   */
  private RemoteApplicationVM getApplicationById(InternalDistributedMember id) {
    if (isConnected()) {
      Future future = membersMap.get(id);
      if (future == null) {
        return null;
      }
      for (;;) {
        system.getCancelCriterion().checkCancelInProgress(null);
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
      }
    }
    return null;
  }

  /**
   * Returns a representation of the application VM with the given distributed system id. If there
   * does not already exist a representation for that member, a new one is created.
   */
  private RemoteApplicationVM addMember(InternalDistributedMember id) {
    boolean runFuture = false;
    Future<RemoteApplicationVM> future;
    synchronized (membersLock) {
      Map<InternalDistributedMember, Future<RemoteApplicationVM>> oldMembersMap = membersMap;
      future = oldMembersMap.get(id);
      if (future == null) {
        runFuture = true;
        if (abortCurrentJoin) {
          return null;
        }
        future = new FutureTask<>(() -> {
          // Do this work in a Future to avoid deadlock
          RemoteGfManagerAgent agent = this;
          RemoteApplicationVM result = new RemoteApplicationVM(agent, id, alertLevel);
          result.startStatDispatcher();
          if (agent.abortCurrentJoin) {
            result.stopStatListening();
            return null;
          }
          return result;
        });
        Map<InternalDistributedMember, Future<RemoteApplicationVM>> newMembersMap =
            new HashMap<>(oldMembersMap);
        newMembersMap.put(id, future);
        if (abortCurrentJoin) {
          return null;
        }
        membersMap = newMembersMap;
      }
    }

    if (runFuture) {
      // Run future outside of membersLock
      ((Runnable) future).run();
    }

    for (;;) {
      system.getCancelCriterion().checkCancelInProgress(null);
      boolean interrupted = Thread.interrupted();
      try {
        return future.get();
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
    }
  }

  /**
   * Removes and returns the representation of the application VM member of the distributed system
   * with the given {@code id}.
   */
  @VisibleForTesting
  RemoteApplicationVM removeMember(InternalDistributedMember id) {
    Future future = null;
    synchronized (membersLock) {
      Map<InternalDistributedMember, Future<RemoteApplicationVM>> oldMembersMap = membersMap;
      if (oldMembersMap.containsKey(id)) {
        Map<InternalDistributedMember, Future<RemoteApplicationVM>> newMembersMap =
            new HashMap<>(oldMembersMap);
        future = newMembersMap.remove(id);
        if (future != null) {
          membersMap = newMembersMap;
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
          system.getCancelCriterion().checkCancelInProgress(null);
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
      }
    }
    return null;
  }

  /**
   * Places a {@code SnapshotResultMessage} on a queue to be processed asynchronously.
   */
  void enqueueSnapshotResults(SnapshotResultMessage msg) {
    if (!isListening()) {
      return;
    }
    for (;;) {
      system.getCancelCriterion().checkCancelInProgress(null);
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
   * Sends the given {@code alert} to this agent's {@link AlertListener}.
   */
  void callAlertListener(Alert alert) {
    if (!isListening()) {
      return;
    }
    if (alertListener != null && alert.getLevel() >= alertLevel) {
      alertListener.alert(alert);
    }
  }

  /**
   * Invokes the {@link CacheCollector#resultsReturned} method of this agent's cache collector.
   */
  private void callCacheCollector(CacheSnapshot results, InternalDistributedMember sender,
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

  private DisconnectListener getDisconnectListener() {
    synchronized (this) {
      return disconnectListener;
    }
  }

  /**
   * Creates a new {@link InternalDistributedSystem} connection for this agent. If one alread
   * exists, it is {@code disconnected} and a new one is created.
   */
  private void connectToDS() {
    if (!isListening()) {
      return;
    }
    Properties props = transport.toDSProperties();
    if (displayName != null && !displayName.isEmpty()) {
      props.setProperty("name", displayName);
    }

    synchronized (systemLock) {
      if (system != null) {
        system.disconnect();
        system = null;
      }
      system = distributedSystemFactory.apply(props);
    }

    DistributionManager dm = system.getDistributionManager();
    if (dm instanceof ClusterDistributionManager) {
      ((ClusterDistributionManager) dm).setAgent(this);
    }

    synchronized (this) {
      disconnected = false;
    }

    system.addDisconnectListener(new InternalDistributedSystem.DisconnectListener() {
      @Override
      public String toString() {
        return String.format("Disconnect listener for %s", RemoteGfManagerAgent.this);
      }

      @Override
      public void onDisconnect(InternalDistributedSystem sys) {
        // Before the disconnect handler is called, the InternalDistributedSystem has already marked
        // itself for the disconnection. Hence the check for RemoteGfManagerAgent.this.isConnected()
        // always returns false. Hence commenting the same.
        boolean reconnect = sys.isReconnecting();
        if (!reconnect) {
          DisconnectListener listener = getDisconnectListener();
          if (disconnect()) {
            // returns true if disconnected during this call
            if (listener != null) {
              listener.onDisconnect(sys);
            }
          }
        }
      }
    });
    InternalDistributedSystem.addReconnectListener(new ReconnectListener() {
      @Override
      public void reconnecting(InternalDistributedSystem oldsys) {
        // nothing
      }

      @Override
      public void onReconnect(InternalDistributedSystem oldsys, InternalDistributedSystem newsys) {
        if (logger.isDebugEnabled()) {
          logger
              .debug("RemoteGfManagerAgent.onReconnect attempting to join new distributed system");
        }
        join();
      }
    });

    synchronized (myMembershipListenerLock) {
      myMembershipListener = myMembershipListenerFactory.apply(this);
      dm.addMembershipListener(myMembershipListener);
      Set<InternalDistributedMember> initialMembers = dm.getDistributionManagerIds();
      myMembershipListener.addMembers(new HashSet<>(initialMembers));

      if (logger.isDebugEnabled()) {
        StringBuilder sb = new StringBuilder("[RemoteGfManagerAgent] ");
        sb.append("Connected to DS with ");
        sb.append(initialMembers.size());
        sb.append(" members: ");

        for (InternalDistributedMember member : initialMembers) {
          sb.append(member);
          sb.append(" ");
        }

        logger.debug(sb.toString());
      }

      connected = true;

      for (InternalDistributedMember member : initialMembers) {
        // Create the admin objects synchronously. We don't need to use
        // the JoinProcess when we first start up.
        try {
          handleJoined(member);
        } catch (OperationCancelledException ex) {
          if (logger.isTraceEnabled(DM_VERBOSE)) {
            logger.trace(DM_VERBOSE, "join cancelled by departure");
          }
        }
      }

      initialized = true;
    }
  }

  @Override
  public DistributionManager getDM() {
    InternalDistributedSystem sys = system;
    if (sys == null) {
      return null;
    }
    return sys.getDistributionManager();
  }

  /**
   * Sets the alert level for this manager agent. Sends a {@link AlertLevelChangeMessage} to each
   * member of the distributed system.
   */
  @Override
  public void setAlertLevel(int level) {
    alertLevel = level;
    AlertLevelChangeMessage m = AlertLevelChangeMessage.create(level);
    sendAsync(m);
  }

  /**
   * Returns the distributed system administered by this agent.
   */
  @Override
  public InternalDistributedSystem getDSConnection() {
    return system;
  }

  @VisibleForTesting
  void setDSConnection(InternalDistributedSystem system) {
    this.system = system;
  }

  /**
   * Handles a membership join asynchronously from the memberJoined notification. Sets and clears
   * current join. Also makes several checks to support aborting of the current join.
   */
  private void handleJoined(InternalDistributedMember id) {
    if (!isListening()) {
      return;
    }

    // set current join and begin handling of it...
    currentJoin = id;
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
          throw new IllegalArgumentException(String.format("Unknown VM Kind: %s", id.getVmKind()));
      }

      // if we have a valid member process it...
      if (abortCurrentJoin) {
        return;
      }
      if (member != null) {
        if (abortCurrentJoin) {
          return;
        }
        for (JoinLeaveListener listener : listeners) {
          if (abortCurrentJoin) {
            return;
          }
          try {
            listener.nodeJoined(this, member);
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
      if (abortCurrentJoin) {
        logger.info("aborted  {}", id);
      }
      currentJoin = null;
      abortCurrentJoin = false;
    }
  }

  /**
   * Adds a pending join entry. Note: attempting to reuse the same ArrayList instance results in
   * some interesting deadlocks.
   */
  private void addPendingJoin(InternalDistributedMember id) {
    synchronized (pendingJoinsLock) {
      List<InternalDistributedMember> oldPendingJoins = pendingJoins;
      if (!oldPendingJoins.contains(id)) {
        List<InternalDistributedMember> newPendingJoins = new ArrayList<>(oldPendingJoins);
        newPendingJoins.add(id);
        pendingJoins = newPendingJoins;
      }
    }
  }

  /**
   * Removes any pending joins that match the member id. Note: attempting to reuse the same
   * ArrayList instance results in some interesting deadlocks.
   */
  private void removePendingJoins(InternalDistributedMember id) {
    synchronized (pendingJoinsLock) {
      List<InternalDistributedMember> oldPendingJoins = pendingJoins;
      if (oldPendingJoins.contains(id)) {
        List<InternalDistributedMember> newPendingJoins = new ArrayList<>(oldPendingJoins);
        newPendingJoins.remove(id);
        pendingJoins = newPendingJoins;
      }
    }
  }

  /**
   * Cancels any pending joins that match the member id.
   */
  private void cancelPendingJoins(InternalDistributedMember id) {
    try {
      // pause the join processor thread...
      joinProcessor.pauseHandling();

      // remove any further pending joins (should't be any)...
      removePendingJoins(id);

      // abort any in-process handling of the member id...
      joinProcessor.abort(id);
    } finally {
      AdminWaiters.cancelWaiters(id);
      joinProcessor.resumeHandling();
    }
  }

  @VisibleForTesting
  void setMembersMap(Map<InternalDistributedMember, Future<RemoteApplicationVM>> membersMap) {
    synchronized (membersLock) {
      this.membersMap = membersMap;
    }
  }

  /**
   * A Daemon thread that asynchronously connects to the distributed system. It is necessary to
   * nicely handle the situation when the user accidentally connects to a distributed system with no
   * members or attempts to connect to a distributed system whose member run a different version of
   * GemFire.
   */
  @VisibleForTesting
  static class DSConnectionDaemon extends LoggingThread {

    private final RemoteGfManagerAgent agent;

    /**
     * Has this thread been told to stop?
     */
    private volatile boolean shutDown;

    private DSConnectionDaemon(RemoteGfManagerAgent agent) {
      super("DSConnectionDaemon");
      this.agent = agent;
    }

    public void shutDown() {
      shutDown = true;
      interrupt();
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
          agent.connected = false;
          agent.initialized = false;
          if (!shutDown) {
            agent.connectToDS();

            // If we're successful, this thread is done
            if (agent.isListening()) {
              Assert.assertTrue(agent.system != null);
            }
            return;
          }
        } catch (IncompatibleSystemException ise) {
          logger.fatal(ise.getMessage(), ise);
          agent.callAlertListener(new VersionMismatchAlert(agent, ise.getMessage()));
        } catch (Exception e) {
          for (Throwable cause = e; cause != null; cause = cause.getCause()) {
            if (cause instanceof InterruptedException) {
              // We were interrupted while connecting. Most likely we are being shutdown.
              if (shutDown) {
                break TOP;
              }
            }

            if (cause instanceof AuthenticationFailedException) {
              // Incorrect credentials. Log & Shutdown
              shutDown = true;
              agent.securityLogWriter.warning(
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
      agent.connected = false;
      agent.initialized = false;
    }
  }

  /**
   * A daemon thread that reads {@link SnapshotResultMessage}s from a queue and invokes the
   * {@code CacheCollector} accordingly.
   */
  @VisibleForTesting
  static class SnapshotResultDispatcher extends LoggingThread {

    private final RemoteGfManagerAgent agent;

    private volatile boolean shutDown;

    private SnapshotResultDispatcher(RemoteGfManagerAgent agent) {
      super("SnapshotResultDispatcher");
      this.agent = agent;
    }

    public void shutDown() {
      shutDown = true;
      interrupt();
    }

    @Override
    public void run() {
      while (!shutDown) {
        SystemFailure.checkFailure();
        try {
          SnapshotResultMessage msg = agent.snapshotResults.take();
          agent.callCacheCollector(msg.getSnapshot(), msg.getSender(), msg.getSnapshotId());
          yield();
        } catch (InterruptedException e) {
          // We'll just exit, no need to reset interrupt bit.
          if (shutDown) {
            break;
          }
          logger.warn("Ignoring strange interrupt", e);
        } catch (Exception ex) {
          logger.fatal(ex.getMessage(), ex);
        }
      }
    }
  }

  /**
   * Processes pending membership joins in a dedicated thread. If we processed the joins in the same
   * thread as the membership handler, then we run the risk of getting deadlocks and such.
   */
  @VisibleForTesting
  static class JoinProcessor extends LoggingThread {

    private final RemoteGfManagerAgent agent;
    private final Object lock = new Object();

    private volatile boolean paused;
    private volatile boolean shutDown;
    private volatile InternalDistributedMember id;

    private JoinProcessor(RemoteGfManagerAgent agent) {
      super("JoinProcessor");
      this.agent = agent;
    }

    public void shutDown() {
      if (logger.isTraceEnabled(DM_VERBOSE)) {
        logger.trace(DM_VERBOSE, "JoinProcessor: shutting down");
      }
      shutDown = true;
      interrupt();
    }

    private void pauseHandling() {
      paused = true;
    }

    private void resumeHandling() {
      if (logger.isTraceEnabled(DM_VERBOSE)) {
        logger.trace(DM_VERBOSE, "JoinProcessor: resuming.  Is alive? {}",
            isAlive());
      }

      // unpause if paused during a cancel...
      paused = false;

      // notify to wake up...
      synchronized (lock) {
        lock.notifyAll();
      }
    }

    public void abort(InternalDistributedMember memberId) {
      // abort if current join matches id...
      if (memberId.equals(agent.currentJoin)) {
        agent.abortCurrentJoin = true;
        interrupt();
      }
      // cancel handling of current event if it matches id...
      if (memberId.equals(id)) {
        id = null;
      }
    }

    @Override
    public void run() {
      /* Used to check whether there were pendingJoins before waiting */
      boolean noPendingJoins = false;
      OUTER: while (!shutDown) {
        SystemFailure.checkFailure();
        try {
          if (!agent.isListening()) {
            shutDown();
          }

          noPendingJoins = agent.pendingJoins.isEmpty();
          if (noPendingJoins && logger.isDebugEnabled()) {
            logger.debug("Pausing as there are no pending joins ... ");
          }

          // if paused OR no pendingJoins then just wait...
          if (paused || noPendingJoins) {
            if (logger.isTraceEnabled(DM_VERBOSE)) {
              logger.trace(DM_VERBOSE, "JoinProcessor is about to wait...");
            }
            synchronized (lock) {
              lock.wait();
            }
          }
          if (logger.isTraceEnabled(DM_VERBOSE)) {
            logger.trace(DM_VERBOSE, "JoinProcessor has woken up...");
          }
          if (paused) {
            continue;
          }

          // if no join was already in process or if aborted, get a new one...
          if (id == null) {
            List pendingJoinsRef = agent.pendingJoins;
            if (logger.isTraceEnabled(DM_VERBOSE)) {
              logger.trace(DM_VERBOSE, "JoinProcessor pendingJoins: {}", pendingJoinsRef.size());
            }
            if (!pendingJoinsRef.isEmpty()) {
              id = (InternalDistributedMember) pendingJoinsRef.get(0);
              if (logger.isTraceEnabled(DM_VERBOSE)) {
                logger.trace(DM_VERBOSE, "JoinProcessor got a membership event for {}", id);
              }
            }
          }
          if (paused) {
            continue;
          }

          // process the join...
          if (id != null) {
            if (logger.isTraceEnabled(DM_VERBOSE)) {
              logger.trace(DM_VERBOSE, "JoinProcessor handling join for {}", id);
            }
            try {
              agent.handleJoined(id);
            } finally {
              id = null;
            }
          }

        } catch (CancelException e) {
          // we're done!
          shutDown = true; // for safety
          break;
        } catch (InterruptedException e) {
          // When this thread is "paused", it is interrupted
          if (logger.isTraceEnabled(DM_VERBOSE)) {
            logger.trace(DM_VERBOSE, "JoinProcessor has been interrupted...");
          }
          if (shutDown) {
            break;
          }
          if (paused || noPendingJoins) {
            /*
             * JoinProcessor waits when: 1. this.paused is set to true 2. There are no pending joins
             *
             * If the JoinProcessor is interrupted when it was waiting due to second reason, it
             * should still continue after catching InterruptedException. From code, currently,
             * JoinProcessor is interrupted through JoinProcessor.abort() method which is called
             * when a member departs as part of cancelPendingJoin().
             */
            if (logger.isDebugEnabled()) {
              logger.debug("JoinProcessor was interrupted when it was paused, now resuming ...", e);
            }
            noPendingJoins = false;
            continue;
          }
          break; // Panic!

        } catch (OperationCancelledException ex) {
          if (logger.isTraceEnabled(DM_VERBOSE)) {
            logger.trace(DM_VERBOSE, "join cancelled by departure");
          }

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
              if (logger.isTraceEnabled(DM_VERBOSE)) {
                logger.trace(DM_VERBOSE, "JoinProcessor has been interrupted...");
              }
              continue OUTER;
            }
          }
          logger.error("JoinProcessor caught exception...", e);
        }
      }
    }
  }

  @VisibleForTesting
  static class MyMembershipListener implements MembershipListener {

    private final Set<InternalDistributedMember> distributedMembers = new HashSet<>();

    private final RemoteGfManagerAgent agent;

    private MyMembershipListener(RemoteGfManagerAgent agent) {
      this.agent = agent;
    }

    private void addMembers(Set initMembers) {
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
        InternalDistributedMember id) {
      if (!agent.isListening()) {
        return;
      }
      synchronized (this) {
        if (!distributedMembers.contains(id)) {
          distributedMembers.add(id);
          agent.addPendingJoin(id);
          agent.joinProcessor.resumeHandling();
        }
      }
    }

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
        if (!distributedMembers.remove(id)) {
          return;
        }
        agent.cancelPendingJoins(id);
        if (!agent.isListening()) {
          return;
        }
      }

      // remove the member...

      RemoteGemFireVM member = null;
      switch (id.getVmKind()) {
        case ClusterDistributionManager.NORMAL_DM_TYPE:
          member = agent.removeMember(id);
          break;
        case ClusterDistributionManager.ADMIN_ONLY_DM_TYPE:
          break;
        case ClusterDistributionManager.LOCATOR_DM_TYPE:
          break;
        case ClusterDistributionManager.LONER_DM_TYPE:
          break; // should this ever happen?
        default:
          throw new IllegalArgumentException("Unknown VM Kind");
      }

      // clean up and notify JoinLeaveListeners...
      if (member != null) {
        for (JoinLeaveListener listener : agent.listeners) {
          if (crashed) {
            listener.nodeCrashed(agent, member);
          } else {
            listener.nodeLeft(agent, member);
          }
        }
        member.stopStatListening();
      }
    }
  }
}
