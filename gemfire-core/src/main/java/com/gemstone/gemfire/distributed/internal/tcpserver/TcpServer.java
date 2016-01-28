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
package com.gemstone.gemfire.distributed.internal.tcpserver;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.SSLException;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.distributed.internal.DistributionConfigImpl;
import com.gemstone.gemfire.distributed.internal.DistributionStats;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.PoolStatHelper;
import com.gemstone.gemfire.distributed.internal.PooledExecutorWithDMStats;
import com.gemstone.gemfire.distributed.internal.SharedConfiguration;
import com.gemstone.gemfire.internal.DSFIDFactory;
import com.gemstone.gemfire.internal.GemFireVersion;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.VersionedDataInputStream;
import com.gemstone.gemfire.internal.VersionedDataOutputStream;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * TCP server which listens on a port and delegates requests to a request
 * handler. The server uses expects messages containing a global version number,
 * followed by a DataSerializable object
 * 
 * This code was factored out of GossipServer.java to allow multiple handlers to
 * share the same gossip server port.
 * 
 * @author dsmith
 * @since 5.7
 * 
 */
public class TcpServer {
  /**
   * The version of the tcp server protocol
   * <p>
   * This should be incremented if the gossip message structures change
   * 
   * 1000 - gemfire 5.5 - using java serialization
   * 1001 - 5.7 - using DataSerializable and supporting server locator messages.
   * 1002 - 7.1 - sending GemFire version along with GOSSIP_VERSION in each request.
   * 
   * with the addition of support for all old versions of clients you can no
   * longer change this version number
   */
  public final static int GOSSIPVERSION = 1002;
  // Don't change it ever. We did NOT send GemFire version in a Gossip request till 1001 version.
  // This GOSSIPVERSION is used in _getVersionForAddress request for getting GemFire version of a GossipServer.
  public final static int OLDGOSSIPVERSION = 1001;

  private static/* GemStoneAddition */final Map GOSSIP_TO_GEMFIRE_VERSION_MAP = new HashMap();

  // For test purpose only
  public static boolean isTesting = false;
  // Non-final field for testing to avoid any security holes in system.
  public static int TESTVERSION = GOSSIPVERSION;
  public static int OLDTESTVERSION = OLDGOSSIPVERSION;

  public static final long SHUTDOWN_WAIT_TIME = 60 * 1000;
  private static int MAX_POOL_SIZE = Integer.getInteger("gemfire.TcpServer.MAX_POOL_SIZE", 100).intValue();
  private static int POOL_IDLE_TIMEOUT = 60 * 1000;
  
  private static final Logger log = LogService.getLogger();
  
  protected/*GemStoneAddition*/ final/*GemStoneAddition*/ static int READ_TIMEOUT = Integer.getInteger("gemfire.TcpServer.READ_TIMEOUT", 60 * 1000).intValue();
  //This is for backwards compatibility. The p2p.backlog flag used to be the only way to configure the locator backlog.
  private static final int P2P_BACKLOG = Integer.getInteger("p2p.backlog", 1000).intValue();
  private static final int BACKLOG = Integer.getInteger("gemfire.TcpServer.BACKLOG", P2P_BACKLOG).intValue();

  // private int port=7500;
  private final int port;
  private/*GemStoneAddition*/ ServerSocket srv_sock = null;
  private InetAddress bind_address;
  private volatile boolean shuttingDown = false; // GemStoneAddition
  private final PoolStatHelper poolHelper;
  private/*GemStoneAddition*/ final TcpHandler handler;
  
  private PooledExecutorWithDMStats executor;
  private final ThreadGroup threadGroup;
  private final String threadName;
  private volatile Thread serverThread;

  /**
   * GemStoneAddition - Initialize versions map.
   * Warning: This map must be compatible with all GemFire versions being
   * handled by this member "With different GOSSIPVERION". If GOSSIPVERIONS
   * are same for then current GOSSIPVERSION should be used.
   *
   * @since 7.1
   */
  static {
    GOSSIP_TO_GEMFIRE_VERSION_MAP.put(GOSSIPVERSION, Version.GFE_71.ordinal());
    GOSSIP_TO_GEMFIRE_VERSION_MAP.put(OLDGOSSIPVERSION, Version.GFE_57.ordinal());
  }

  public TcpServer(int port, InetAddress bind_address, Properties sslConfig,
      DistributionConfigImpl cfg, TcpHandler handler, PoolStatHelper poolHelper, ThreadGroup threadGroup, String threadName) {
    this.port = port;
    this.bind_address = bind_address;
    this.handler = handler;
    this.poolHelper = poolHelper;
    // register DSFID types first; invoked explicitly so that all message type
    // initializations do not happen in first deserialization on a possibly
    // "precious" thread
    DSFIDFactory.registerTypes();

    this.executor = createExecutor(poolHelper, threadGroup);
    this.threadGroup = threadGroup;
    this.threadName = threadName;

    if (cfg == null) {
      if (sslConfig == null) {
        sslConfig = new Properties();
      }
      cfg = new DistributionConfigImpl(sslConfig);
    }
    SocketCreator.getDefaultInstance(cfg);
  }

  private static PooledExecutorWithDMStats createExecutor(PoolStatHelper poolHelper, final ThreadGroup threadGroup) {
    ThreadFactory factory = new ThreadFactory() {
      private final AtomicInteger threadNum = new AtomicInteger();
      
      public Thread newThread(Runnable r) {
        Thread thread = new Thread(threadGroup, r, "locator request thread[" + threadNum.incrementAndGet() + "]");
        thread.setDaemon(true);
        return thread;
      }
    };
    
    return new PooledExecutorWithDMStats(new SynchronousQueue(), MAX_POOL_SIZE, poolHelper, factory, POOL_IDLE_TIMEOUT, new ThreadPoolExecutor.CallerRunsPolicy());
  }

  public void restarting(InternalDistributedSystem ds, GemFireCacheImpl cache, SharedConfiguration sharedConfig) throws IOException {
    this.shuttingDown = false;
    this.handler.restarting(ds, cache, sharedConfig);
    startServerThread();
    this.executor = createExecutor(this.poolHelper, this.threadGroup);
    this.log.info("TcpServer@"+System.identityHashCode(this)+" restarting: completed.  Server thread="+serverThread+"@"+System.identityHashCode(serverThread)+";alive="+serverThread.isAlive());
  }
  
  public void start() throws IOException {
    this.shuttingDown = false;
    handler.init(this);
    startServerThread();
  }
  
  private void startServerThread() throws IOException {
    if (srv_sock == null || srv_sock.isClosed()) {
      if (bind_address == null) {
        srv_sock = SocketCreator.getDefaultInstance().createServerSocket(port,
            BACKLOG);
        // srv_sock=new ServerSocket(port, 20); // backlog of 20 connections
        bind_address = srv_sock.getInetAddress();
      } else
        srv_sock = SocketCreator.getDefaultInstance().createServerSocket(port,
            BACKLOG, bind_address);
      // srv_sock=new ServerSocket(port, 20, bind_address); // backlog of 20
      // connections
      {
        if (log.isInfoEnabled())
          log.info("Locator was created at " + new Date());
        if (log.isInfoEnabled())
          log.info("Listening on port " + port + " bound on address "
              + bind_address);
      }
      srv_sock.setReuseAddress(true); // GemStoneAddition
    }
    if (serverThread == null || !serverThread.isAlive()) {
      serverThread = new Thread(threadGroup, threadName) {
        @Override // GemStoneAddition
        public void run() {
          TcpServer.this.run();
        }
      };
      serverThread.setDaemon(true);
      serverThread.start();
    }
  }
  
  public void join(long millis) throws InterruptedException {
    if(serverThread != null) {
      serverThread.join(millis);
    }
  }
  
  public void join() throws InterruptedException {
//    this.log.info("TcpServer@"+System.identityHashCode(this)+" join() invoked.  Server thread="+serverThread+"@"+System.identityHashCode(serverThread)+";alive="+serverThread.isAlive());
    if(serverThread != null) { 
      serverThread.join();
    }
  }
  
  public boolean isAlive() {
    return serverThread != null && serverThread.isAlive();
  }
  
  public boolean isShuttingDown() {
    return this.shuttingDown;
  }
  
  public SocketAddress getBindAddress() {
    return srv_sock.getLocalSocketAddress(); 
  }

  protected void run() {
    Socket sock = null;

    while (!shuttingDown) {
      if (SystemFailure.getFailure() != null) {
        // Allocate no objects here!
        try {
          srv_sock.close();
        } catch (IOException e) {
          // ignore
        }
        SystemFailure.checkFailure(); // throws
      }
      try {
        try {
          sock = srv_sock.accept();
        } catch (SSLException ex) {
          // SW: This is the case when there is a problem in locator
          // SSL configuration, so need to exit otherwise goes into an
          // infinite loop just filling the logs
          log.error("Locator stopping due to SSL configuration problem.", ex);
          shuttingDown = true;
          continue;
        }
        processRequest(sock);
        // looping=false; GemStoneAddition change
      } catch (Exception ex) {
        if (!shuttingDown) {
          log.error("exception=", ex);
        }
        continue;
      }
    }

    try {
      srv_sock.close();

    } catch (java.io.IOException ex) {
      log.warn(
          "exception closing server socket during shutdown", ex);
    }

    if (shuttingDown) {
      log.info("locator shutting down");
      executor.shutdown();
      try {
        executor.awaitTermination(SHUTDOWN_WAIT_TIME, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      handler.shutDown();
      synchronized (this) {
//        this.shutDown = true;
        this.notifyAll();
      }
    }
  }

  /**
   * fix for bug 33711 - client requests are spun off to another thread for
   * processing. Requests are synchronized in processGossip.
   */
  private void processRequest(final Socket sock) {
    Runnable clientTask = new Runnable() {
      @SuppressWarnings("synthetic-access")
      public void run() {
        long startTime = DistributionStats.getStatTime();
        DataInputStream input = null;
        Object request, response;
        try {
          SocketCreator.getDefaultInstance().configureServerSSLSocket(sock);
          // if(log.isInfoEnabled()) log.info("accepted connection from " +
          // sock.getInetAddress() +
          // ':' + sock.getPort());
          //sock.setSoLinger(true, 500);
          sock.setSoTimeout(READ_TIMEOUT);
          try {
            input = new DataInputStream(sock.getInputStream());
          } catch (StreamCorruptedException e) {
            // bug 36679: Some garbage can be left on the socket stream
            // if a peer disappears at exactly the wrong moment.
            log.debug(
                "Discarding illegal request from "
                    + (sock.getInetAddress().getHostAddress() + ":" + sock
                        .getPort()), e);
            return;
          }

          int gossipVersion = input.readInt();
          short versionOrdinal = Version.CURRENT_ORDINAL;
          // Create a versioned stream to remember sender's GemFire version
          if (gossipVersion <= getCurrentGossipVersion()
              && GOSSIP_TO_GEMFIRE_VERSION_MAP.containsKey(gossipVersion)) {
            versionOrdinal = (short) GOSSIP_TO_GEMFIRE_VERSION_MAP
                .get(gossipVersion);
//            if (gossipVersion < getCurrentGossipVersion()) {
//              if (log.isTraceEnabled()) {
//                log.debug(
//                    "Received request from "
//                        + sock.getInetAddress().getHostAddress()
//                        + " This locator is running: " + getCurrentGossipVersion()
//                        + ", but request was version: " + gossipVersion
//                        + ", version ordinal: " + versionOrdinal);
//              }
//            }
          }
          // Close the socket. We can not accept requests from a newer version
          else {
            sock.close();
            return;
          }
          // Get exactly correct version of client from inputstream.
          if (Version.GFE_71.compareTo(versionOrdinal) <= 0) {
            versionOrdinal = input.readShort();
          }

          if (log.isDebugEnabled() && versionOrdinal != Version.CURRENT_ORDINAL) {
            log.debug("Locator reading request from " + sock.getInetAddress() + " with version " + Version.fromOrdinal(versionOrdinal, false));
          }
          input = new VersionedDataInputStream(input, Version.fromOrdinal(
                  versionOrdinal, false));
          request = DataSerializer.readObject(input);
          if (log.isDebugEnabled()) {
            log.debug("Locator received request " + request + " from " + sock.getInetAddress());
          }
          if (request instanceof ShutdownRequest) {
            shuttingDown = true;
            //Don't call shutdown from within the worker thread, see java bug #6576792. This bug exists
            // in the backport as well. Closing the socket will cause our acceptor thread to shutdown
            //the executor
            //executor.shutdown();
            srv_sock.close();
            response = new ShutdownResponse();
          } else if (request instanceof InfoRequest) {
            response = handleInfoRequest(request);
          } else if (request instanceof VersionRequest) {
            response = handleVersionRequest(request);
          } else {
            response = handler.processRequest(request);
          }

          handler.endRequest(request, startTime);
          
          startTime = DistributionStats.getStatTime();
          if (response != null) {
            DataOutputStream output = new DataOutputStream(sock.getOutputStream());
            if (versionOrdinal != Version.CURRENT_ORDINAL) {
              output = new VersionedDataOutputStream(output, Version.fromOrdinal(versionOrdinal, false));
            }
            DataSerializer.writeObject(response, output);

            output.flush();
          }

          handler.endResponse(request,startTime);

        } catch (EOFException ex) {
          // client went away - ignore
        } catch (CancelException ex) {
          // ignore
        } catch (ClassNotFoundException ex) {
          String sender = null;
          if (sock != null) {
            sender = sock.getInetAddress().getHostAddress();
          }
          log.info(
              "Unable to process request from " + sender + " exception=" + ex.getMessage());
        } catch (Exception ex) {
          String sender = null;
          if (sock != null) {
            sender = sock.getInetAddress().getHostAddress();
          }
          if(ex instanceof IOException) {
            //IOException could be caused by a client failure. Don't
            //log with severe.
            if (!sock.isClosed()) {
              log.info(
                  "Exception in processing request from " + sender, ex);
            }
          }
          else {
            log.fatal("Exception in processing request from " + 
                sender, ex);
          }

        } catch (VirtualMachineError err) {
          SystemFailure.initiateFailure(err);
          // If this ever returns, rethrow the error.  We're poisoned
          // now, so don't let this thread continue.
          throw err;
        } catch (Throwable ex) {
          // Whenever you catch Error or Throwable, you must also
          // catch VirtualMachineError (see above).  However, there is
          // _still_ a possibility that you are dealing with a cascading
          // error condition, so you also need to check to see if the JVM
          // is still usable:
          SystemFailure.checkFailure();
          String sender = null;
          if (sock != null) {
            sender = sock.getInetAddress().getHostAddress();
          }
          try {
            log.fatal("Exception in processing request from " +
                sender, ex);
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
            // for surviving and debugging exceptions getting the logger
            t.printStackTrace();
          }
        } finally {
          try {
            sock.close();
          } catch (IOException e) {
            // ignore
          }
        }
      }
    };
    executor.execute(clientTask);
  }

  protected Object handleInfoRequest(Object request) {
    String[] info = new String[2];
    info[0] = System.getProperty("user.dir");

    URL url = GemFireVersion.getJarURL();
    if (url == null) {
      String s = "Could not find gemfire jar";
      throw new IllegalStateException(s);
    }

    File gemfireJar = new File(url.getPath());
    File lib = gemfireJar.getParentFile();
    File product = lib.getParentFile();
    info[1] = product.getAbsolutePath();

    return new InfoResponse(info);
  }

  protected Object handleVersionRequest(Object request) {
    VersionResponse response = new VersionResponse();
    response.setVersionOrdinal(Version.CURRENT_ORDINAL);
    return response;
  }

  /**
   * Returns GossipVersion for older Gemfire versions.
   * 
   * @param ordinal
   * @return gossip version
   */
  public static int getGossipVersionForOrdinal(short ordinal) {

    // Sanity check
    short closest = -1;
    int closestGV = getCurrentGossipVersion();

    if (ordinal < Version.CURRENT_ORDINAL) {
      Iterator<Map.Entry> itr = TcpServer.GOSSIP_TO_GEMFIRE_VERSION_MAP.entrySet().iterator();
      while (itr.hasNext()) {
        Map.Entry entry = itr.next();
        short o = ((Short)entry.getValue()).shortValue();
        if (o == ordinal) {
          return ((Integer)entry.getKey()).intValue();
        } else if (o < ordinal && o > closest ) {
          closest = o;
          closestGV = ((Integer)entry.getKey()).intValue();
        }
      }
    }

    return closestGV;
  }

  public static int getCurrentGossipVersion() {
    return TcpServer.isTesting ? TcpServer.TESTVERSION
        : TcpServer.GOSSIPVERSION;
  }

  public static int getOldGossipVersion() {
    return TcpServer.isTesting ? TcpServer.OLDTESTVERSION
        : TcpServer.OLDGOSSIPVERSION;
  }

  public static Map getGossipVersionMapForTestOnly() {
    return GOSSIP_TO_GEMFIRE_VERSION_MAP;
  }
}
