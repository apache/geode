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
package com.gemstone.gemfire.internal;

import com.gemstone.gemfire.admin.internal.InetAddressUtil;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Properties;

/**
 * MigrationServer creates a cache using a supplied cache.xml and then
 * opens a server socket that a MigrationClient connects to and requests
 * the data from a Region.  MigrationServer sends the data to
 * the MigrationClient using normal java serialization in
 * order to allow migration from incompatible versions of DataSerializer.
 * Keys and values stored in the cache must serialize and deserialize correctly.
 * <p>
 * Command line arguments are<br>
 * &nbsp;&nbsp;cache-xml-file-name (required)<br>
 * &nbsp;&nbsp;listen port (defaults to 10553)<br>
 * &nbsp;&nbsp;bind address (defaults to listing on all interfaces)<br>
 * <p>
 * Both the MigrationClient and MigrationServer must be configured to have
 * the appropriate domain classes in their CLASSPATH, or errors will be
 * encountered during deserialization.
 * <P>
 * Details of the transfers can be viewed by setting the system property
 * Migration.VERBOSE=true.
 * <p>
 * For example,
 * <pre>
 * java -cp $MYCLASSES:migration.jar:$GEMFIRE/lib/geode-dependencies.jar \
 *   com.gemstone.gemfire.internal.MigrationServer cacheDescription.xml
 * </pre><p>
 * Where the cacheDescription.xml file might look like this:
 * <pre>
 * &lt!DOCTYPE cache PUBLIC
  "-//GemStone Systems, Inc.//GemFire Declarative Caching 5.7//EN"
  "http://www.gemstone.com/dtd/cache5_7.dtd"&gt
&ltcache is-server="false"&gt
  &ltregion name="root"&gt
    &ltregion-attributes scope="distributed-no-ack"&gt
    &lt/region-attributes&gt

    &ltregion name="Test"&gt
      &ltregion-attributes data-policy="persistent-replicate"&gt

        &ltdisk-write-attributes&gt
          &ltsynchronous-writes/&gt
        &lt/disk-write-attributes&gt

        &ltdisk-dirs&gt
          &ltdisk-dir&gtdiskfiles&lt/disk-dir&gt
        &lt/disk-dirs&gt

        &lteviction-attributes&gt
          &ltlru-memory-size maximum="100" action="overflow-to-disk"/&gt
        &lt/eviction-attributes&gt

      &lt/region-attributes&gt
    &lt/region&gt &lt!-- Test region --&gt
  &lt/region&gt &lt!-- root region --&gt
&lt/cache&gt

 * </pre><p>
 * The client is then run with a different cache description having different
 * disk-dirs to hold the migrated information.
 * 
 * @since GemFire 6.0.1
 */
public class MigrationServer {
  final static boolean VERBOSE = Boolean.getBoolean("Migration.VERBOSE");
  
  final static int VERSION = 551; // version for backward communications compatibility
  
  protected static final int CODE_ERROR = 0;
  protected static final int CODE_ENTRY = 1; /* serialized key, serialized value */
  protected static final int CODE_COMPLETED = 2;

  public static void main(String[] args) throws Exception {
    int argIdx = 0;
    String cacheXmlFileName = "cache.xml";
    String bindAddressName = null;
    int listenPort = 10533;
    
    if (args.length > 0) {
      cacheXmlFileName = args[argIdx++];
    } else {
      System.err.println("MigrationServer cache-xml-file [server-address] [server-port]");
    }
    if (args.length > argIdx) {
      listenPort = Integer.parseInt(args[argIdx++]);
    }
    if (args.length > argIdx) {
      bindAddressName = args[argIdx++];
    }
    
    MigrationServer instance = null;
    try {
      instance = new MigrationServer(cacheXmlFileName, bindAddressName, listenPort);
    } catch (IllegalArgumentException e) {
      System.err.println(e.getMessage());
      System.exit(1);
    }
    instance.createDistributedSystem();
    instance.createCache();
    instance.serve();
  }


  private InetAddress bindAddress;
  private int listenPort;
  private ServerSocket serverSocket;
  private DistributedSystem distributedSystem;
  private File cacheXmlFile;
  private Cache cache;
  
  
  /**
   * Create a MigrationServer to be used with a DistributedSystem and Cache
   * that are created using GemFire APIs
   * @param bindAddressName the NIC to bind to, or null to use all interfaces
   * @param listenPort the port to listen on
   */
  public MigrationServer(String bindAddressName, int listenPort) {
    this.listenPort = listenPort;
    if (bindAddressName != null) {
      if (!isLocalHost(bindAddressName)) {
        throw new IllegalArgumentException("Error - bind address is not an address of this machine: '" + bindAddressName + "'");
      }
      try {
        this.bindAddress = InetAddress.getByName(bindAddressName);
      } catch (IOException e) {
        throw new IllegalArgumentException("Error - bind address cannot be resolved: '" + bindAddressName + "'");
      }
    }
    try {
      if (this.bindAddress != null) {
        this.serverSocket = new ServerSocket();
        SocketAddress addr = new InetSocketAddress(this.bindAddress, listenPort);
        this.serverSocket.bind(addr);
      } else {
        this.serverSocket = new ServerSocket(listenPort);
      }
      if (VERBOSE) {
        System.out.println("created server socket " + serverSocket);
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("Port is already in use", e);
    }
  }
  
  /**
   * this is for use by main()
   * 
   * @param cacheXmlFileName the name of the xml file describing the cache, or null
   * @param bindAddressName the name of the NIC to bind to, or null
   * @param listenPort the port to listen on (must not be zero)
   */
  private MigrationServer(String cacheXmlFileName, String bindAddressName, int listenPort) {
    this(bindAddressName, listenPort);
    this.cacheXmlFile = new File(cacheXmlFileName);
    if (!this.cacheXmlFile.exists()) {
      // in 6.x this should be localizable
      System.err.println("Warning - file not found in local directory: '" + cacheXmlFileName + "'");
    }
  }

  /**
   * Create a distributed system.  If this method is not invoked before running
   * the MigrationServer, an existing distributed system must exist for the
   * server to use.
   * 
   * @throws Exception if there are any problems
   */
  private void createDistributedSystem() throws Exception {
    Properties dsProps = new Properties();
    // if no discovery information has been explicitly given, use a loner ds 
    if (System.getProperty("gemfire." + DistributionConfig.MCAST_PORT_NAME) == null
        && System.getProperty("gemfire." + DistributionConfig.LOCATORS_NAME) == null) {
      dsProps.put(DistributionConfig.MCAST_PORT_NAME, "0");
    }
    dsProps.put(DistributionConfig.LOG_FILE_NAME, "migrationServer.log");
    if (this.cacheXmlFile != null) {
      dsProps.put(DistributionConfig.CACHE_XML_FILE_NAME, this.cacheXmlFile.getName());
    }
    this.distributedSystem = DistributedSystem.connect(dsProps);
    if (VERBOSE) {
      System.out.println("created distributed system " + this.distributedSystem);
    }
  }
  
  
  /**
   * create the cache to be used by this migration server
   * @throws Exception if there are any problems
   */
  private void createCache() throws Exception {
    if (this.distributedSystem == null) {
      this.distributedSystem = InternalDistributedSystem.getConnectedInstance();
    }
    this.cache = CacheFactory.create(this.distributedSystem);
    if (VERBOSE) {
      System.out.println("created cache " + this.cache);
    }
  }
    

  /**
   * This locates the distributed system and cache, if they have not been
   * created by this server, and then listens for requests from MigrationClient
   * processes.
   * @throws IllegalStateException if an attempt is made to reuse a server that has been stopped
   */
  public void serve() throws Exception {
    if (this.serverSocket == null || this.serverSocket.isClosed()) {
      throw new IllegalStateException("This server has been closed and cannot be reused");
    }
    try {
      if (this.distributedSystem == null) {
        this.distributedSystem = InternalDistributedSystem.getConnectedInstance();
      }
      if (this.cache == null) {
        this.cache = GemFireCacheImpl.getInstance();
      }
      if (this.bindAddress != null) {
        System.out.println("Migration server on port " + this.listenPort +
            " bound to " + this.bindAddress + " is ready for client requets");
      } else {
        System.out.println("Migration server on port " + this.listenPort +
            " is ready for client requests");
      }
      for (;;) {
        if (Thread.interrupted() || this.serverSocket.isClosed()) {
          return;
        }
        Socket clientSocket;
        try {
          clientSocket = this.serverSocket.accept();
        } catch (java.net.SocketException e) {
          return;
        }
        (new RequestHandler(clientSocket)).serveClientRequest();
      }
    } finally {
      System.out.println("Closing migration server");
      try {
        this.serverSocket.close();
      } catch (Exception e) {
        this.serverSocket = null;
      }
    }
  }
  
  /**
   * this causes the migration server to stop serving after it finishes dispatching
   * any in-process requests
   * @throws IOException if there is a problem closing the server socket
   */
  public void stop() throws IOException {
    if (this.serverSocket != null && !this.serverSocket.isClosed()) {
      this.serverSocket.close();
    }
  }

  /**
   * get the cache being used by this migration server
   * @return the cache, or null if a cache has not yet been associated with this server
   */
  public Cache getCache() {
    return this.cache;
  }
  
  /**
   * get the distributed system being used by this migration server
   * @return the distributed system, or null if a system has not yet been associated with this server
   */
  public DistributedSystem getDistributedSystem() {
    return this.distributedSystem;
  }
  

  
  
  
  
  // copied from 6.0 SocketCreator
  public static boolean isLocalHost(Object host) {
    if (host instanceof InetAddress) {
      if (InetAddressUtil.LOCALHOST.equals(host)) {
        return true;
      }
      else {
        try {
          Enumeration en=NetworkInterface.getNetworkInterfaces();
          while(en.hasMoreElements()) {
            NetworkInterface i=(NetworkInterface)en.nextElement();
            for(Enumeration en2=i.getInetAddresses(); en2.hasMoreElements();) {
              InetAddress addr=(InetAddress)en2.nextElement();
              if (host.equals(addr)) {
                return true;
              }
            }
          }
          return false;
        }
        catch (SocketException e) {
          throw new IllegalArgumentException(LocalizedStrings.InetAddressUtil_UNABLE_TO_QUERY_NETWORK_INTERFACE.toLocalizedString(), e);
        }
      }
    }
    else {
      return isLocalHost(toInetAddress(host.toString()));
    }
  }

  // copied from 6.0 SocketCreator
  public static InetAddress toInetAddress(String host) {
    if (host == null || host.length() == 0) {
      return null;
    }
    try {
      if (host.indexOf("/") > -1) {
        return InetAddress.getByName(host.substring(host.indexOf("/") + 1));
      }
      else {
        return InetAddress.getByName(host);
      }
    } catch (java.net.UnknownHostException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  
  
  
  
  //               R E Q U E S T   H A N D L E R
  
  
  class RequestHandler implements Runnable {
    Socket clientSocket;
    DataInputStream dis;
    DataOutputStream dos;

    RequestHandler(Socket clientSocket) throws IOException {
      this.clientSocket = clientSocket;
      dos = new DataOutputStream(this.clientSocket.getOutputStream());
      dis = new DataInputStream(this.clientSocket.getInputStream());
    }
    
    
    // for now this is a blocking operation - multithread later if necessary
    void serveClientRequest() {
      try {
        run();
      }
      finally {
        if (!this.clientSocket.isClosed()) {
          try {
            this.clientSocket.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }
    }
    
    public void run() {
      try {
        // first exchange version information so we can communicate correctly
        dos.writeShort(VERSION);
        int version = dis.readShort();
        handleRequest(version);
      }
      catch (IOException e) {
        System.err.println("Trouble dispatching request: " + e.getMessage());
        return;
      }
      finally {
        try {
          this.clientSocket.close();
        } catch (IOException e) {
          System.err.println("Trouble closing client socket: " + e.getMessage());
        }
      }
    }
    
    /**
     * read and dispatch a single request on client socket
     * @param clientVersion
     */
    private void handleRequest(int clientVersion) {
      // for now we ignore the client version in the server.  The client
      // is typically of a later release than the server, and this information
      // is given to the server in case a situation arises where it's needed
      try {
        ClientRequest req = ClientRequest.readRequest(this.clientSocket, dis, dos);
        if (req != null) {
          System.out.println("Processing " + req + " from " + this.clientSocket.getInetAddress().getHostAddress());
          req.process(MigrationServer.this);
          dos.flush();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    
  }
  
  
  //           R E Q U E S T   C L A S S E S
  
  
  
  static abstract class ClientRequest {
    Socket clientSocket;
    DataInputStream dsi;
    DataOutputStream dso;
    
    final static int REGION_REQUEST = 1;
    
    /**
     * Use readRequest to create a new request object, not this constructor.
     * Subclasses may refine this constructor to perform other initialization
     * @param dsi socket's input stream
     * @param dso socket's output stream
     * @throws IOException if there are any problems reading initialization information
     */
    ClientRequest(Socket clientSocket, DataInputStream dsi, DataOutputStream dso) throws IOException {
      this.clientSocket = clientSocket;
      this.dsi = dsi;
      this.dso = dso;
    }

    /**
     * Read and return a request from a client
     * @param clientSocket
     * @param dsi socket input stream
     * @param dso socket output stream
     * @return the new request
     * @throws IOException
     */
    static ClientRequest readRequest(Socket clientSocket, DataInputStream dsi, DataOutputStream dso) throws IOException {
      int requestType = dsi.readShort();
      switch (requestType) {
      case REGION_REQUEST:
        return new RegionRequest(clientSocket, dsi, dso);
      }
      String errorMessage = "Type of request is not implemented in this server";
      dso.writeShort(CODE_ERROR);
      dso.writeUTF(errorMessage);
      System.err.println("Migration server received unknown type of request ("
          + requestType + ") from " + clientSocket.getInetAddress().getHostAddress());
      return null;
    }
    
    void writeErrorResponse(String message) throws IOException {
      this.dso.writeShort(CODE_ERROR);
      this.dso.writeUTF(message);
    }

    abstract void process(MigrationServer server) throws IOException ;
    
  }
  
  /**
   * RegionRequest represents a request for the keys and values of a Region
   * from a client.
   */
  static class RegionRequest extends ClientRequest {
    String regionName;
    
    RegionRequest(Socket clientSocket, DataInputStream dsi, DataOutputStream dso) throws IOException {
      super(clientSocket, dsi, dso);
      regionName = dsi.readUTF();
    }
    
    @Override
    public String toString() {
      return "request for contents of region '" + this.regionName + "'";
    }
    
    @Override
    void process(MigrationServer server) throws IOException {
      Cache cache = server.getCache();
      Region region = null;
      try {
        region = cache.getRegion(regionName);
        if (region == null) {
          String errorMessage = "Error: region " + this.regionName + " not found in cache"; 
          System.err.println(errorMessage);
          writeErrorResponse(errorMessage);
        }
      } catch (IllegalArgumentException e) {
        String errorMessage = "Error: malformed region name"; 
        System.err.println(errorMessage);
        writeErrorResponse(errorMessage);
      }
      try {
        for (Iterator it = region.entrySet().iterator(); it.hasNext(); ) {
          sendEntry((Region.Entry)it.next());
        }
        this.dso.writeShort(CODE_COMPLETED);
      }
      catch (Exception e) {
        writeErrorResponse(e.getMessage());
      }
    }
    
    private void sendEntry(Region.Entry entry) throws Exception {
      Object key = entry.getKey();
      Object value = entry.getValue();
      if ( !(key instanceof Serializable) ) {
        throw new IOException("Could not serialize entry for '" + key + "'");
      }
      if ( !(value instanceof Serializable) ) {
        throw new IOException("Could not serialize entry for '" + key + "'");
      }
      if (VERBOSE) {
        System.out.println("Sending " + key);
      }
      dso.writeShort(CODE_ENTRY);
      (new ObjectOutputStream(clientSocket.getOutputStream())).writeObject(key);
      (new ObjectOutputStream(clientSocket.getOutputStream())).writeObject(value);
    }
  }

}
