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
package org.apache.geode.internal.tcp;

import java.io.IOException;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.Logger;

import org.apache.geode.SystemFailure;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.MembershipManager;
import org.apache.geode.distributed.internal.membership.gms.mgr.GMSMembershipManager;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.SystemTimer;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LoggingThreadGroup;
import org.apache.geode.internal.logging.log4j.AlertAppender;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.internal.net.SocketCloser;

/**
 * <p>
 * ConnectionTable holds all of the Connection objects in a conduit. Connections represent a pipe
 * between two endpoints represented by generic DistributedMembers.
 * </p>
 *
 * @since GemFire 2.1
 */
public class ConnectionTable {
  private static final Logger logger = LogService.getLogger();

  /** warning when descriptor limit reached */
  private static boolean ulimitWarningIssued;

  /**
   * true if the current thread wants non-shared resources
   */
  private static ThreadLocal threadWantsOwnResources = new ThreadLocal();

  /**
   * Used for messages whose order must be preserved Only connections used for sending messages, and
   * receiving acks, will be put in this map.
   */
  protected final Map orderedConnectionMap = new ConcurrentHashMap();

  /**
   * ordered connections local to this thread. Note that accesses to the resulting map must be
   * synchronized because of static cleanup.
   */
  // ThreadLocal<Map>
  protected final ThreadLocal<Map> threadOrderedConnMap;

  /**
   * List of thread-owned ordered connection maps, for cleanup
   *
   * Accesses to the maps in this list need to be synchronized on their instance.
   */
  private final List threadConnMaps;

  /**
   * Timer to kill idle threads
   *
   * guarded.By this
   */
  private SystemTimer idleConnTimer;

  /**
   * Used to find connections owned by threads. The key is the same one used in
   * threadOrderedConnMap. The value is an ArrayList since we can have any number of connections
   * with the same key.
   */
  private ConcurrentMap threadConnectionMap;

  /**
   * Used for all non-ordered messages. Only connections used for sending messages, and receiving
   * acks, will be put in this map.
   */
  protected final Map unorderedConnectionMap = new ConcurrentHashMap();

  /**
   * Used for all accepted connections. These connections are read only; we never send messages,
   * except for acks; only receive.
   *
   * Consists of a list of Connection
   */
  private final List receivers = new ArrayList();

  /**
   * the conduit for this table
   */
  private final TCPConduit owner;

  /**
   * true if this table is no longer in use
   */
  private volatile boolean closed = false;

  /**
   * Executor used by p2p reader and p2p handshaker threads.
   */
  private final Executor p2pReaderThreadPool;
  /**
   * Number of seconds to wait before timing out an unused p2p reader thread. Default is 120 (2
   * minutes).
   */
  private static final long READER_POOL_KEEP_ALIVE_TIME =
      Long.getLong("p2p.READER_POOL_KEEP_ALIVE_TIME", 120).longValue();

  private final SocketCloser socketCloser;

  /**
   * The most recent instance to be created
   *
   * TODO this assumes no more than one instance is created at a time?
   */
  private static final AtomicReference lastInstance = new AtomicReference();

  /**
   * A set of sockets that are in the process of being connected
   */
  private Map connectingSockets = new HashMap();

  /**
   * Cause calling thread to share communication resources with other threads.
   */
  public static void threadWantsSharedResources() {
    threadWantsOwnResources.set(Boolean.FALSE);
  }

  /**
   * Cause calling thread to acquire exclusive access to communication resources. Exclusive access
   * may not be available in which case this call is ignored.
   */
  public static void threadWantsOwnResources() {
    threadWantsOwnResources.set(Boolean.TRUE);
  }

  /**
   * Returns true if calling thread owns its own communication resources.
   */
  boolean threadOwnsResources() {
    DistributionManager d = getDM();
    if (d != null) {
      return d.getSystem().threadOwnsResources() && !AlertAppender.isThreadAlerting();
    }
    return false;

    // Boolean b = getThreadOwnsResourcesRegistration();
    // if (b == null) {
    // // thread does not have a preference so return default
    // return !this.owner.shareSockets;
    // return false;
    // } else {
    // return b.booleanValue();
    // }
  }

  public static Boolean getThreadOwnsResourcesRegistration() {
    return (Boolean) threadWantsOwnResources.get();
  }

  public TCPConduit getOwner() {
    return owner;
  }


  private ConnectionTable(TCPConduit conduit) throws IOException {
    this.owner = conduit;
    this.idleConnTimer = (this.owner.idleConnectionTimeout != 0)
        ? new SystemTimer(conduit.getDM().getSystem(), true) : null;
    this.threadOrderedConnMap = new ThreadLocal();
    this.threadConnMaps = new ArrayList();
    this.threadConnectionMap = new ConcurrentHashMap();
    this.p2pReaderThreadPool = createThreadPoolForIO(conduit.getDM().getSystem().isShareSockets());
    this.socketCloser = new SocketCloser();
  }

  private Executor createThreadPoolForIO(boolean conserveSockets) {
    Executor executor = null;
    final ThreadGroup connectionRWGroup =
        LoggingThreadGroup.createThreadGroup("P2P Reader Threads", logger);
    if (conserveSockets) {
      executor = new Executor() {
        @Override
        public void execute(Runnable command) {
          Thread th = new Thread(connectionRWGroup, command);
          th.setDaemon(true);
          th.start();
        }
      };
    } else {
      BlockingQueue synchronousQueue = new SynchronousQueue();
      ThreadFactory tf = new ThreadFactory() {
        public Thread newThread(final Runnable command) {
          Thread thread = new Thread(connectionRWGroup, command);
          thread.setDaemon(true);
          return thread;
        }
      };
      executor = new ThreadPoolExecutor(1, Integer.MAX_VALUE, READER_POOL_KEEP_ALIVE_TIME,
          TimeUnit.SECONDS, synchronousQueue, tf);
    }
    return executor;
  }

  /** conduit sends connected() after establishing the server socket */
  // protected void connected() {
  // /* NOMUX: if (TCPConduit.useNIO) {
  // inputMuxManager.connected();
  // }*/
  // }

  /** conduit calls acceptConnection after an accept */
  protected void acceptConnection(Socket sock, PeerConnectionFactory peerConnectionFactory)
      throws IOException, ConnectionException, InterruptedException {
    InetAddress connAddress = sock.getInetAddress(); // for bug 44736
    boolean finishedConnecting = false;
    Connection connection = null;
    // boolean exceptionLogged = false;
    try {
      connection = peerConnectionFactory.createReceiver(this, sock);

      // check for shutdown (so it doesn't get missed in the finally block)
      this.owner.getCancelCriterion().checkCancelInProgress(null);
      finishedConnecting = true;
    } catch (IOException ex) {
      // check for shutdown...
      this.owner.getCancelCriterion().checkCancelInProgress(ex);
      logger.warn(LocalizedMessage.create(
          LocalizedStrings.ConnectionTable_FAILED_TO_ACCEPT_CONNECTION_FROM_0_BECAUSE_1,
          new Object[] {(connAddress != null ? connAddress : "unavailable address"), ex}));
      throw ex;
    } catch (ConnectionException ex) {
      // check for shutdown...
      this.owner.getCancelCriterion().checkCancelInProgress(ex);
      logger.warn(LocalizedMessage.create(
          LocalizedStrings.ConnectionTable_FAILED_TO_ACCEPT_CONNECTION_FROM_0_BECAUSE_1,
          new Object[] {(connAddress != null ? connAddress : "unavailable address"), ex}));
      throw ex;
    } finally {
      // note: no need to call incFailedAccept here because it will be done
      // in our caller.
      // no need to log error here since caller will log warning

      if (connection != null && !finishedConnecting) {
        // we must be throwing from checkCancelInProgress so close the connection
        closeCon(LocalizedStrings.ConnectionTable_CANCEL_AFTER_ACCEPT.toLocalizedString(),
            connection);
        connection = null;
      }
    }

    if (connection != null) {
      synchronized (this.receivers) {
        this.owner.getStats().incReceivers();
        if (this.closed) {
          closeCon(LocalizedStrings.ConnectionTable_CONNECTION_TABLE_NO_LONGER_IN_USE
              .toLocalizedString(), connection);
          return;
        }
        // If connection.stopped is false, any connection cleanup thread will not yet have acquired
        // the receiver synchronization to remove the receiver. Therefore we can safely add it here.
        if (!(connection.isSocketClosed() || connection.isReceiverStopped())) {
          this.receivers.add(connection);
        }
      }
      if (logger.isDebugEnabled()) {
        logger.debug("Accepted {} myAddr={} theirAddr={}", connection, getConduit().getMemberId(),
            connection.remoteAddr);
      }
    }
  }



  /**
   * Process a newly created PendingConnection
   *
   * @param id DistributedMember on which the connection is created
   * @param sharedResource whether the connection is used by multiple threads
   * @param preserveOrder whether to preserve order
   * @param m map to add the connection to
   * @param pc the PendingConnection to process
   * @param startTime the ms clock start time for the operation
   * @param ackThreshold the ms ack-wait-threshold, or zero
   * @param ackSAThreshold the ms ack-severe_alert-threshold, or zero
   * @return the Connection, or null if someone else already created or closed it
   * @throws IOException if unable to connect
   */
  private Connection handleNewPendingConnection(DistributedMember id, boolean sharedResource,
      boolean preserveOrder, Map m, PendingConnection pc, long startTime, long ackThreshold,
      long ackSAThreshold) throws IOException, DistributedSystemDisconnectedException {
    // handle new pending connection
    Connection con = null;
    try {
      con = Connection.createSender(owner.getMembershipManager(), this, preserveOrder, id,
          sharedResource, startTime, ackThreshold, ackSAThreshold);
      this.owner.getStats().incSenders(sharedResource, preserveOrder);
    } finally {
      // our connection failed to notify anyone waiting for our pending con
      if (con == null) {
        this.owner.getStats().incFailedConnect();
        synchronized (m) {
          Object rmObj = m.remove(id);
          if (rmObj != pc && rmObj != null) {
            // put it back since it was not our pc
            m.put(id, rmObj);
          }
        }
        pc.notifyWaiters(null);
        // we must be throwing an exception
      }
    } // finally

    // Update our list of connections -- either the
    // orderedConnectionMap or unorderedConnectionMap
    //
    // Note that we added the entry _before_ we attempted the connect,
    // so it's possible something else got through in the mean time...
    synchronized (m) {
      Object e = m.get(id);
      if (e == pc) {
        m.put(id, con);
      } else if (e == null) {
        // someone closed our pending connection
        // so cleanup the connection we created
        con.requestClose(
            LocalizedStrings.ConnectionTable_PENDING_CONNECTION_CANCELLED.toLocalizedString());
        con = null;
      } else {
        if (e instanceof Connection) {
          Connection newCon = (Connection) e;
          if (!newCon.connected) {
            // Fix for bug 31590
            // someone closed our pending connect
            // so cleanup the connection we created
            if (con != null) {
              con.requestClose(
                  LocalizedStrings.ConnectionTable_PENDING_CONNECTION_CLOSED.toLocalizedString());
              con = null;
            }
          } else {
            // This should not happen. It means that someone else
            // created the connection which should only happen if
            // our Connection was rejected.
            // Assert.assertTrue(false);
            // The above assertion was commented out to try the
            // following with bug 32680
            if (con != null) {
              con.requestClose(LocalizedStrings.ConnectionTable_SOMEONE_ELSE_CREATED_THE_CONNECTION
                  .toLocalizedString());
            }
            con = newCon;
          }
        }
      }
    }
    pc.notifyWaiters(con);
    if (con != null && logger.isDebugEnabled()) {
      logger.debug("handleNewPendingConnection {} myAddr={} theirAddr={}", con,
          getConduit().getMemberId(), con.remoteAddr);
    }

    return con;
  }

  /**
   * unordered or conserve-sockets=true note that unordered connections are currently always shared
   *
   * @param id the DistributedMember on which we are creating a connection
   * @param scheduleTimeout whether unordered connection should time out
   * @param preserveOrder whether to preserve order
   * @param startTime the ms clock start time for the operation
   * @param ackTimeout the ms ack-wait-threshold, or zero
   * @param ackSATimeout the ms ack-severe-alert-threshold, or zero
   * @return the new Connection, or null if an error
   * @throws IOException if unable to create the connection
   */
  private Connection getSharedConnection(DistributedMember id, boolean scheduleTimeout,
      boolean preserveOrder, long startTime, long ackTimeout, long ackSATimeout)
      throws IOException, DistributedSystemDisconnectedException {
    Connection result = null;

    final Map m = preserveOrder ? this.orderedConnectionMap : this.unorderedConnectionMap;

    PendingConnection pc = null; // new connection, if needed
    Object mEntry = null; // existing connection (if we don't create a new one)

    // Look for pending connection
    synchronized (m) {
      mEntry = m.get(id);
      if (mEntry != null && (mEntry instanceof Connection)) {
        Connection existingCon = (Connection) mEntry;
        if (!existingCon.connected) {
          mEntry = null;
        }
      }
      if (mEntry == null) {
        pc = new PendingConnection(preserveOrder, id);
        m.put(id, pc);
      }
    } // synchronized

    if (pc != null) {
      result = handleNewPendingConnection(id, true /* fixes bug 43386 */, preserveOrder, m, pc,
          startTime, ackTimeout, ackSATimeout);
      if (!preserveOrder && scheduleTimeout) {
        scheduleIdleTimeout(result);
      }
    } else { // we have existing connection
      if (mEntry instanceof PendingConnection) {

        if (AlertAppender.isThreadAlerting()) {
          // do not change the text of this exception - it is looked for in exception handlers
          throw new IOException("Cannot form connection to alert listener " + id);
        }

        result = ((PendingConnection) mEntry).waitForConnect(this.owner.getMembershipManager(),
            startTime, ackTimeout, ackSATimeout);
        if (logger.isDebugEnabled()) {
          if (result != null) {
            logger.debug("getSharedConnection {} myAddr={} theirAddr={}", result,
                getConduit().getMemberId(), result.remoteAddr);
          } else {
            logger.debug("getSharedConnection: Connect failed");
          }
        }
      } else {
        result = (Connection) mEntry;
      }
    } // we have existing connection

    return result;
  }

  /**
   * Must be looking for an ordered connection that this thread owns
   *
   * @param id stub on which to create the connection
   * @param startTime the ms clock start time for the operation
   * @param ackTimeout the ms ack-wait-threshold, or zero
   * @param ackSATimeout the ms ack-severe-alert-threshold, or zero
   * @return the connection, or null if an error
   * @throws IOException if the connection could not be created
   */
  Connection getThreadOwnedConnection(DistributedMember id, long startTime, long ackTimeout,
      long ackSATimeout) throws IOException, DistributedSystemDisconnectedException {
    Connection result = null;

    // Look for result in the thread local
    Map m = (Map) this.threadOrderedConnMap.get();
    if (m == null) {
      // First time for this thread. Create thread local
      m = new HashMap();
      synchronized (this.threadConnMaps) {
        if (this.closed) {
          owner.getCancelCriterion().checkCancelInProgress(null);
          throw new DistributedSystemDisconnectedException(
              LocalizedStrings.ConnectionTable_CONNECTION_TABLE_IS_CLOSED.toLocalizedString());
        }
        // check for stale references and remove them.
        for (Iterator it = this.threadConnMaps.iterator(); it.hasNext();) {
          Reference r = (Reference) it.next();
          if (r.get() == null) {
            it.remove();
          }
        } // for
        this.threadConnMaps.add(new WeakReference(m)); // ref added for bug 38011
      } // synchronized
      this.threadOrderedConnMap.set(m);
    } else {
      // Consult thread local.
      synchronized (m) {
        result = (Connection) m.get(id);
      }
      if (result != null && result.timedOut) {
        result = null;
      }
    }
    if (result != null)
      return result;

    // OK, we have to create a new connection.
    result = Connection.createSender(owner.getMembershipManager(), this, true /* preserveOrder */,
        id, false /* shared */, startTime, ackTimeout, ackSATimeout);
    if (logger.isDebugEnabled()) {
      logger.debug("ConnectionTable: created an ordered connection: {}", result);
    }
    this.owner.getStats().incSenders(false/* shared */, true /* preserveOrder */);

    // Update the list of connections owned by this thread....

    if (this.threadConnectionMap == null) {
      // This instance is being destroyed; fail the operation
      closeCon(
          LocalizedStrings.ConnectionTable_CONNECTION_TABLE_BEING_DESTROYED.toLocalizedString(),
          result);
      return null;
    }

    ArrayList al = (ArrayList) this.threadConnectionMap.get(id);
    if (al == null) {
      // First connection for this DistributedMember. Make sure list for this
      // stub is created if it isn't already there.
      al = new ArrayList();

      // Since it's a concurrent map, we just try to put it and then
      // return whichever we got.
      Object o = this.threadConnectionMap.putIfAbsent(id, al);
      if (o != null) {
        al = (ArrayList) o;
      }
    }

    // Add our Connection to the list
    synchronized (al) {
      al.add(result);
    }

    // Finally, add the connection to our thread local map.
    synchronized (m) {
      m.put(id, result);
    }

    scheduleIdleTimeout(result);
    return result;
  }

  /** schedule an idle-connection timeout task */
  private void scheduleIdleTimeout(Connection conn) {
    if (conn == null) {
      // fix for bug 43529
      return;
    }
    // Set the idle timeout
    if (this.owner.idleConnectionTimeout != 0) {
      try {
        synchronized (this) {
          if (!this.closed) {
            IdleConnTT task = new IdleConnTT(conn);
            conn.setIdleTimeoutTask(task);
            this.getIdleConnTimer().scheduleAtFixedRate(task, this.owner.idleConnectionTimeout,
                this.owner.idleConnectionTimeout);
          }
        }
      } catch (IllegalStateException e) {
        if (conn.isClosing()) {
          // bug #45077 - connection is closed before we schedule the timeout task,
          // causing the task to be canceled
          return;
        }
        logger.debug("Got an illegal state exception: {}", e.getMessage(), e);
        // Unfortunately, cancelInProgress() is not set until *after*
        // the shutdown message has been sent, so we need to check the
        // "closeInProgress" bit instead.
        owner.getCancelCriterion().checkCancelInProgress(null);
        Throwable cause = owner.getShutdownCause();
        if (cause == null) {
          cause = e;
        }
        throw new DistributedSystemDisconnectedException(
            LocalizedStrings.ConnectionTable_THE_DISTRIBUTED_SYSTEM_IS_SHUTTING_DOWN
                .toLocalizedString(),
            cause);
      }
    }
  }

  /**
   * Get a new connection
   *
   * @param id the DistributedMember on which to create the connection
   * @param preserveOrder whether order should be preserved
   * @param startTime the ms clock start time
   * @param ackTimeout the ms ack-wait-threshold, or zero
   * @param ackSATimeout the ms ack-severe-alert-threshold, or zero
   * @return the new Connection, or null if a problem
   * @throws java.io.IOException if the connection could not be created
   */
  protected Connection get(DistributedMember id, boolean preserveOrder, long startTime,
      long ackTimeout, long ackSATimeout)
      throws java.io.IOException, DistributedSystemDisconnectedException {
    if (this.closed) {
      this.owner.getCancelCriterion().checkCancelInProgress(null);
      throw new DistributedSystemDisconnectedException(
          LocalizedStrings.ConnectionTable_CONNECTION_TABLE_IS_CLOSED.toLocalizedString());
    }
    Connection result = null;
    boolean threadOwnsResources = threadOwnsResources();
    if (!preserveOrder || !threadOwnsResources) {
      result = getSharedConnection(id, threadOwnsResources, preserveOrder, startTime, ackTimeout,
          ackSATimeout);
    } else {
      result = getThreadOwnedConnection(id, startTime, ackTimeout, ackSATimeout);
    }
    if (result != null) {
      Assert.assertTrue(result.preserveOrder == preserveOrder);
    }
    return result;
  }

  protected synchronized void fileDescriptorsExhausted() {
    if (!ulimitWarningIssued) {
      ulimitWarningIssued = true;
      logger.fatal(LocalizedMessage.create(
          LocalizedStrings.ConnectionTable_OUT_OF_FILE_DESCRIPTORS_USING_SHARED_CONNECTION));
      InternalDistributedSystem.getAnyInstance().setShareSockets(true);
      threadWantsOwnResources = new ThreadLocal();
    }
  }

  protected TCPConduit getConduit() {
    return owner;
  }

  public boolean isClosed() {
    return this.closed;
  }

  private static void closeCon(String reason, Object c) {
    closeCon(reason, c, false);
  }

  private static void closeCon(String reason, Object c, boolean beingSick) {
    if (c == null) {
      return;
    }
    if (c instanceof Connection) {
      ((Connection) c).closePartialConnect(reason, beingSick); // fix for bug 31666
    } else {
      ((PendingConnection) c).notifyWaiters(null);
    }
  }

  /**
   * returns the idle connection timer, or null if the connection table is closed. guarded by a sync
   * on the connection table
   */
  protected synchronized SystemTimer getIdleConnTimer() {
    if (this.closed) {
      return null;
    }
    if (this.idleConnTimer == null) {
      this.idleConnTimer = new SystemTimer(getDM().getSystem(), true);
    }
    return this.idleConnTimer;
  }

  protected void close() {
    /*
     * NOMUX if (inputMuxManager != null) { inputMuxManager.stop(); }
     */
    if (this.closed) {
      return;
    }
    this.closed = true;
    synchronized (this) {
      if (this.idleConnTimer != null) {
        this.idleConnTimer.cancel();
      }
    }
    synchronized (this.orderedConnectionMap) {
      for (Iterator it = this.orderedConnectionMap.values().iterator(); it.hasNext();) {
        closeCon(
            LocalizedStrings.ConnectionTable_CONNECTION_TABLE_BEING_DESTROYED.toLocalizedString(),
            it.next());
      }
      this.orderedConnectionMap.clear();
    }
    synchronized (this.unorderedConnectionMap) {
      for (Iterator it = this.unorderedConnectionMap.values().iterator(); it.hasNext();) {
        closeCon(
            LocalizedStrings.ConnectionTable_CONNECTION_TABLE_BEING_DESTROYED.toLocalizedString(),
            it.next());
      }
      this.unorderedConnectionMap.clear();
    }
    if (this.threadConnectionMap != null) {
      this.threadConnectionMap = null;
    }
    if (this.threadConnMaps != null) {
      synchronized (this.threadConnMaps) {
        for (Iterator it = this.threadConnMaps.iterator(); it.hasNext();) {
          Reference r = (Reference) it.next();
          Map m = (Map) r.get();
          if (m != null) {
            synchronized (m) {
              for (Iterator mit = m.values().iterator(); mit.hasNext();) {
                closeCon(LocalizedStrings.ConnectionTable_CONNECTION_TABLE_BEING_DESTROYED
                    .toLocalizedString(), mit.next());
              }
            }
          }
        }
        this.threadConnMaps.clear();
      }
    }
    {
      Executor localExec = this.p2pReaderThreadPool;
      if (localExec != null) {
        if (localExec instanceof ExecutorService) {
          ((ExecutorService) localExec).shutdown();
        }
      }
    }
    closeReceivers(false);

    Map m = (Map) this.threadOrderedConnMap.get();
    if (m != null) {
      synchronized (m) {
        m.clear();
      }
    }
    this.socketCloser.close();
  }

  public void executeCommand(Runnable runnable) {
    Executor local = this.p2pReaderThreadPool;
    if (local != null) {
      local.execute(runnable);
    }
  }

  /**
   * Close all receiving threads. This is used during shutdown and is also used by a test hook that
   * makes us deaf to incoming messages.
   *
   * @param beingSick a test hook to simulate a sick process
   */
  protected void closeReceivers(boolean beingSick) {
    synchronized (this.receivers) {
      for (Iterator it = this.receivers.iterator(); it.hasNext();) {
        Connection con = (Connection) it.next();
        if (!beingSick || con.preserveOrder) {
          closeCon(
              LocalizedStrings.ConnectionTable_CONNECTION_TABLE_BEING_DESTROYED.toLocalizedString(),
              con, beingSick);
          it.remove();
        }
      }
      // now close any sockets being formed
      synchronized (connectingSockets) {
        for (Iterator it = connectingSockets.entrySet().iterator(); it.hasNext();) {
          Map.Entry entry = (Map.Entry) it.next();
          // ConnectingSocketInfo info = (ConnectingSocketInfo)entry.getValue();
          try {
            ((Socket) entry.getKey()).close();
          } catch (IOException e) {
            // ignored - we're shutting down
          }
          it.remove();
        }
      }
    }
  }


  protected void removeReceiver(Object con) {
    synchronized (this.receivers) {
      this.receivers.remove(con);
    }
  }

  /**
   * Return true if our owner already knows that this endpoint is departing
   */
  protected boolean isEndpointShuttingDown(DistributedMember id) {
    return giveUpOnMember(owner.getDM().getMembershipManager(), id);
  }

  protected boolean giveUpOnMember(MembershipManager mgr, DistributedMember remoteAddr) {
    return !mgr.memberExists(remoteAddr) || mgr.isShunned(remoteAddr) || mgr.shutdownInProgress();
  }

  /** remove an endpoint and notify the membership manager of the departure */
  protected void removeEndpoint(DistributedMember stub, String reason) {
    removeEndpoint(stub, reason, true);
  }

  protected void removeEndpoint(DistributedMember memberID, String reason,
      boolean notifyDisconnect) {
    if (this.closed) {
      return;
    }
    boolean needsRemoval = false;
    synchronized (this.orderedConnectionMap) {
      if (this.orderedConnectionMap.get(memberID) != null)
        needsRemoval = true;
    }
    if (!needsRemoval) {
      synchronized (this.unorderedConnectionMap) {
        if (this.unorderedConnectionMap.get(memberID) != null)
          needsRemoval = true;
      }
    }
    if (!needsRemoval) {
      ConcurrentMap cm = this.threadConnectionMap;
      if (cm != null) {
        ArrayList al = (ArrayList) cm.get(memberID);
        needsRemoval = al != null && al.size() > 0;
      }
    }

    if (needsRemoval) {
      InternalDistributedMember remoteAddress = null;
      synchronized (this.orderedConnectionMap) {
        Object c = this.orderedConnectionMap.remove(memberID);
        if (c instanceof Connection) {
          remoteAddress = ((Connection) c).getRemoteAddress();
        }
        closeCon(reason, c);
      }
      synchronized (this.unorderedConnectionMap) {
        Object c = this.unorderedConnectionMap.remove(memberID);
        if (remoteAddress == null && (c instanceof Connection)) {
          remoteAddress = ((Connection) c).getRemoteAddress();
        }
        closeCon(reason, c);
      }

      {
        ConcurrentMap cm = this.threadConnectionMap;
        if (cm != null) {
          ArrayList al = (ArrayList) cm.remove(memberID);
          if (al != null) {
            synchronized (al) {
              for (Iterator it = al.iterator(); it.hasNext();) {
                Object c = it.next();
                if (remoteAddress == null && (c instanceof Connection)) {
                  remoteAddress = ((Connection) c).getRemoteAddress();
                }
                closeCon(reason, c);
              }
              al.clear();
            }
          }
        }
      }

      // close any sockets that are in the process of being connected
      Set toRemove = new HashSet();
      synchronized (connectingSockets) {
        for (Iterator it = connectingSockets.entrySet().iterator(); it.hasNext();) {
          Map.Entry entry = (Map.Entry) it.next();
          ConnectingSocketInfo info = (ConnectingSocketInfo) entry.getValue();
          if (info.peerAddress.equals(((InternalDistributedMember) memberID).getInetAddress())) {
            toRemove.add(entry.getKey());
            it.remove();
          }
        }
      }
      for (Iterator it = toRemove.iterator(); it.hasNext();) {
        Socket sock = (Socket) it.next();
        try {
          sock.close();
        } catch (IOException e) {
          if (logger.isDebugEnabled()) {
            logger.debug("caught exception while trying to close connecting socket for {}",
                memberID, e);
          }
        }
      }

      // close any receivers
      // avoid deadlock when a NIC has failed by closing connections outside
      // of the receivers sync (bug 38731)
      toRemove.clear();
      synchronized (this.receivers) {
        for (Iterator it = receivers.iterator(); it.hasNext();) {
          Connection con = (Connection) it.next();
          if (memberID.equals(con.getRemoteAddress())) {
            it.remove();
            toRemove.add(con);
          }
        }
      }
      for (Iterator it = toRemove.iterator(); it.hasNext();) {
        Connection con = (Connection) it.next();
        closeCon(reason, con);
      }
      if (notifyDisconnect) {
        // Before the removal of TCPConduit Stub addresses this used
        // to call MembershipManager.getMemberForStub, which checked
        // for a shutdown in progress and threw this exception:
        if (owner.getDM().shutdownInProgress()) {
          throw new DistributedSystemDisconnectedException("Shutdown in progress",
              owner.getDM().getMembershipManager().getShutdownCause());
        }
      }

      if (remoteAddress != null) {
        this.socketCloser.releaseResourcesForAddress(remoteAddress.toString());
      }
    }
  }

  SocketCloser getSocketCloser() {
    return this.socketCloser;
  }

  /** check to see if there are still any receiver threads for the given end-point */
  protected boolean hasReceiversFor(DistributedMember endPoint) {
    synchronized (this.receivers) {
      for (Iterator it = receivers.iterator(); it.hasNext();) {
        Connection con = (Connection) it.next();
        if (endPoint.equals(con.getRemoteAddress())) {
          return true;
        }
      }
    }
    return false;
  }

  private static void removeFromThreadConMap(ConcurrentMap cm, DistributedMember stub,
      Connection c) {
    if (cm != null) {
      ArrayList al = (ArrayList) cm.get(stub);
      if (al != null) {
        synchronized (al) {
          al.remove(c);
        }
      }
    }
  }

  protected void removeThreadConnection(DistributedMember stub, Connection c) {
    /*
     * if (this.closed) { return; }
     */
    removeFromThreadConMap(this.threadConnectionMap, stub, c);
    Map m = (Map) this.threadOrderedConnMap.get();
    if (m != null) {
      // Static cleanup thread might intervene, so we MUST synchronize
      synchronized (m) {
        if (m.get(stub) == c) {
          m.remove(stub);
        }
      } // synchronized
    } // m != null
  }

  void removeSharedConnection(String reason, DistributedMember stub, boolean ordered,
      Connection c) {
    if (this.closed) {
      return;
    }
    if (ordered) {
      synchronized (this.orderedConnectionMap) {
        if (this.orderedConnectionMap.get(stub) == c) {
          closeCon(reason, this.orderedConnectionMap.remove(stub));
        }
      }
    } else {
      synchronized (this.unorderedConnectionMap) {
        if (this.unorderedConnectionMap.get(stub) == c) {
          closeCon(reason, this.unorderedConnectionMap.remove(stub));
        }
      }
    }
  }

  /**
   * Just ensure that this class gets loaded.
   *
   * @see SystemFailure#loadEmergencyClasses()
   */
  public static void loadEmergencyClasses() {
    // don't go any further, Frodo!
  }

  /**
   * Clears lastInstance. Does not yet close underlying sockets, but probably not strictly
   * necessary.
   *
   * @see SystemFailure#emergencyClose()
   */
  public static void emergencyClose() {
    ConnectionTable ct = (ConnectionTable) lastInstance.get();
    if (ct == null) {
      return;
    }
    lastInstance.set(null);
  }

  public void removeAndCloseThreadOwnedSockets() {
    Map m = (Map) this.threadOrderedConnMap.get();
    if (m != null) {
      // Static cleanup may intervene; we MUST synchronize.
      synchronized (m) {
        Iterator it = m.entrySet().iterator();
        while (it.hasNext()) {
          Map.Entry me = (Map.Entry) it.next();
          DistributedMember stub = (DistributedMember) me.getKey();
          Connection c = (Connection) me.getValue();
          removeFromThreadConMap(this.threadConnectionMap, stub, c);
          it.remove();
          closeCon(LocalizedStrings.ConnectionTable_THREAD_FINALIZATION.toLocalizedString(), c);
        } // while
      } // synchronized m
    }
  }

  public static void releaseThreadsSockets() {
    ConnectionTable ct = (ConnectionTable) lastInstance.get();
    if (ct == null) {
      return;
    }
    ct.removeAndCloseThreadOwnedSockets();
    // lastInstance = null;
  }

  /**
   * records the current outgoing message count on all thread-owned ordered connections. This does
   * not synchronize or stop new connections from being formed or new messages from being sent
   *
   * @since GemFire 5.1
   */
  protected void getThreadOwnedOrderedConnectionState(DistributedMember member, Map result) {

    ConcurrentMap cm = this.threadConnectionMap;
    if (cm != null) {
      ArrayList al = (ArrayList) cm.get(member);
      if (al != null) {
        synchronized (al) {
          al = new ArrayList(al);
        }

        for (Iterator it = al.iterator(); it.hasNext();) {
          Connection conn = (Connection) it.next();
          if (!conn.isSharedResource() && conn.getOriginatedHere() && conn.getPreserveOrder()) {
            result.put(Long.valueOf(conn.getUniqueId()), Long.valueOf(conn.getMessagesSent()));
          }
        }
      }
    }
  }

  /**
   * wait for the given incoming connections to receive at least the associated number of messages
   */
  protected void waitForThreadOwnedOrderedConnectionState(DistributedMember member,
      Map connectionStates) throws InterruptedException {
    if (Thread.interrupted())
      throw new InterruptedException(); // wisest to do this before the synchronize below
    List r = null;
    synchronized (receivers) {
      r = new ArrayList(receivers);
    }
    for (Iterator it = r.iterator(); it.hasNext();) {
      Connection con = (Connection) it.next();
      if (!con.stopped && !con.isClosing() && !con.getOriginatedHere() && con.getPreserveOrder()
          && member.equals(con.getRemoteAddress())) {
        Long state = (Long) connectionStates.remove(Long.valueOf(con.getUniqueId()));
        if (state != null) {
          long count = state.longValue();
          while (!con.stopped && !con.isClosing() && con.getMessagesReceived() < count) {
            if (logger.isDebugEnabled()) {
              logger.debug("Waiting for connection {}/{} currently={} need={}",
                  con.getRemoteAddress(), con.getUniqueId(), con.getMessagesReceived(), count);
            }
            Thread.sleep(100);
          }
        }
      }
    }
    if (connectionStates.size() > 0) {
      if (logger.isDebugEnabled()) {
        StringBuffer sb = new StringBuffer(1000);
        sb.append("These connections from ");
        sb.append(member);
        sb.append("could not be located during waitForThreadOwnedOrderedConnectionState: ");
        for (Iterator it = connectionStates.entrySet().iterator(); it.hasNext();) {
          Map.Entry entry = (Map.Entry) it.next();
          sb.append(entry.getKey()).append('(').append(entry.getValue()).append(')');
          if (it.hasNext()) {
            sb.append(',');
          }
        }
        logger.debug(sb);
      }
    }
  }

  protected DistributionManager getDM() {
    return this.owner.getDM();
  }

  // public boolean isShuttingDown() {
  // return this.owner.isShuttingDown();
  // }

  // protected void cleanupHighWater() {
  // cleanup(highWater);
  // }

  // protected void cleanupLowWater() {
  // cleanup(lowWater);
  // }

  // private void cleanup(int maxConnections) {
  /*
   * if (maxConnections == 0 || maxConnections >= connections.size()) { return; } while
   * (connections.size() > maxConnections) { Connection oldest = null; synchronized(connections) {
   * for (Iterator iter = connections.values().iterator(); iter.hasNext(); ) { Connection c =
   * (Connection)iter.next(); if (oldest == null || c.getTimeStamp() < oldest.getTimeStamp()) {
   * oldest = c; } } } // sanity check - don't close anything fresher than 10 seconds or // we'll
   * start thrashing if (oldest.getTimeStamp() > (System.currentTimeMillis() - 10000)) { if
   * (owner.lowWaterConnectionCount > 0) { owner.lowWaterConnectionCount += 10; } if
   * (owner.highWaterConnectionCount > 0) { owner.highWaterConnectionCount += 10; } new Object[] {
   * owner.lowWaterConnectionCount, owner.highWaterConnectionCount }); break; } if (oldest != null)
   * { oldest.close(); } }
   */
  // }

  /*
   * public void dumpConnectionTable() { Iterator iter = connectionMap.keySet().iterator(); while
   * (iter.hasNext()) { Object key = iter.next(); Object val = connectionMap.get(key); } }
   */
  private /* static */ class PendingConnection {
    /**
     * true if this connection is still pending
     */
    private boolean pending = true;

    /**
     * the connection we are waiting on
     */
    private Connection conn = null;

    /**
     * whether the connection preserves message ordering
     */
    private final boolean preserveOrder;

    /**
     * the stub we are connecting to
     */
    private final DistributedMember id;

    private final Thread connectingThread;

    public PendingConnection(boolean preserveOrder, DistributedMember id) {
      this.preserveOrder = preserveOrder;
      this.id = id;
      this.connectingThread = Thread.currentThread();
    }

    /**
     * Synchronously set the connection and notify waiters that we are ready.
     *
     * @param c the new connection
     */
    public synchronized void notifyWaiters(Connection c) {
      if (!this.pending)
        return; // already done.

      this.conn = c;
      this.pending = false;
      if (logger.isDebugEnabled()) {
        logger.debug("Notifying waiters that pending {} connection to {} is ready; {}",
            ((this.preserveOrder) ? "ordered" : "unordered"), this.id, this);
      }
      this.notifyAll();
    }

    /**
     * Wait for a connection
     *
     * @param mgr the membership manager that can instigate suspect processing if necessary
     * @param startTime the ms clock start time for the operation
     * @param ackTimeout the ms ack-wait-threshold, or zero
     * @param ackSATimeout the ms ack-severe-alert-threshold, or zero
     * @return the new connection
     */
    public synchronized Connection waitForConnect(MembershipManager mgr, long startTime,
        long ackTimeout, long ackSATimeout) throws IOException {
      if (connectingThread == Thread.currentThread()) {
        throw new ReenteredConnectException("This thread is already trying to connect");
      }

      final Map m = this.preserveOrder ? orderedConnectionMap : unorderedConnectionMap;

      boolean severeAlertIssued = false;
      boolean suspected = false;
      DistributedMember targetMember = null;
      if (ackSATimeout > 0) {
        targetMember = this.id;
      }

      for (;;) {
        if (!this.pending)
          break;
        getConduit().getCancelCriterion().checkCancelInProgress(null);

        // wait a little bit...
        boolean interrupted = Thread.interrupted();
        try {
          this.wait(100); // spurious wakeup ok
        } catch (InterruptedException ignore) {
          interrupted = true;
          getConduit().getCancelCriterion().checkCancelInProgress(ignore);
        } finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }

        if (!this.pending)
          break;

        // Still pending...
        long now = System.currentTimeMillis();
        if (!severeAlertIssued && ackSATimeout > 0 && startTime + ackTimeout < now) {
          if (startTime + ackTimeout + ackSATimeout < now) {
            if (targetMember != null) {
              logger.fatal(LocalizedMessage.create(
                  LocalizedStrings.ConnectionTable_UNABLE_TO_FORM_A_TCPIP_CONNECTION_TO_0_IN_OVER_1_SECONDS,
                  new Object[] {targetMember, (ackSATimeout + ackTimeout) / 1000}));
            }
            severeAlertIssued = true;
          } else if (!suspected) {
            logger.warn(LocalizedMessage.create(
                LocalizedStrings.ConnectionTable_UNABLE_TO_FORM_A_TCPIP_CONNECTION_TO_0_IN_OVER_1_SECONDS,
                new Object[] {this.id, (ackTimeout) / 1000}));
            ((GMSMembershipManager) mgr).suspectMember(targetMember,
                "Unable to form a TCP/IP connection in a reasonable amount of time");
            suspected = true;
          }
        }

        Object e;
        // synchronized (m) {
        e = m.get(this.id);
        // }
        if (e == this) {
          if (logger.isDebugEnabled()) {
            logger.debug("Waiting for pending connection to complete: {} connection to {}; {}",
                ((this.preserveOrder) ? "ordered" : "unordered"), this.id, this);
          }
          continue;
        }

        // Odd state change. Process and exit.
        if (logger.isDebugEnabled()) {
          logger.debug("Pending connection changed to {} unexpectedly", e);
        }

        if (e == null) {
          // We were removed
          notifyWaiters(null);
          break;
        } else if (e instanceof Connection) {
          notifyWaiters((Connection) e);
          break;
        } else {
          // defer to the new instance
          return ((PendingConnection) e).waitForConnect(mgr, startTime, ackTimeout, ackSATimeout);
        }

      } // for
      return this.conn;

    }
  }


  private static class IdleConnTT extends SystemTimer.SystemTimerTask {

    private Connection c;

    IdleConnTT(Connection c) {
      this.c = c;
    }

    @Override
    public boolean cancel() {
      Connection con = this.c;
      if (con != null) {
        con.cleanUpOnIdleTaskCancel();
      }
      this.c = null;
      return super.cancel();
    }

    @Override
    public void run2() {
      Connection con = this.c;
      if (con != null) {
        if (con.checkForIdleTimeout()) {
          cancel();
        }
      }
    }
  }

  public static ConnectionTable create(TCPConduit conduit) throws IOException {
    ConnectionTable ct = new ConnectionTable(conduit);
    lastInstance.set(ct);
    return ct;
  }

  /** keep track of a socket that is trying to connect() for shutdown purposes */
  public void addConnectingSocket(Socket socket, InetAddress addr) {
    synchronized (connectingSockets) {
      connectingSockets.put(socket, new ConnectingSocketInfo(addr));
    }
  }

  /** remove a socket from the tracked set. It should be connected at this point */
  public void removeConnectingSocket(Socket socket) {
    synchronized (connectingSockets) {
      connectingSockets.remove(socket);
    }
  }


  private static class ConnectingSocketInfo {
    InetAddress peerAddress;
    Thread connectingThread;

    public ConnectingSocketInfo(InetAddress addr) {
      this.peerAddress = addr;
      this.connectingThread = Thread.currentThread();
    }
  }

  public int getNumberOfReceivers() {
    return receivers.size();
  }
}
