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

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.net.SocketCreator;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Properties;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.LOCATORS;
import static com.gemstone.gemfire.distributed.ConfigurationProperties.MCAST_PORT;

/**
 * MigrationClient is used to retrieve all of the data for a region from
 * a MigrationServer.  First start a MigrationServer using one version of
 * GemFire, then connect to it using a MigrationClient with another version
 * of GemFire.
 * <p>
 * Command line arguments are<br>
 * &nbsp;&nbsp;region name (required)<br>
 * &nbsp;&nbsp;cache-xml-file-name (required)<br>
 * &nbsp;&nbsp;server port (defaults to 10553)<br>
 * &nbsp;&nbsp;server address (defaults to local host)
 *<p>
 * The region should be defined in the cache-xml file, and must also be
 * defined in the server's cache-xml file.
 *<p> 
 * <p>
 * Typically, the cache-xml file will be exactly the same as the one used
 * by the MigrationServer with different disk-dirs settings.  When Region 
 * entries are transfered from the server to the client, they are then 
 * stored in new files in these directories.
 * 
 * @since GemFire 6.0.1
 *
 */
public class MigrationClient {
  final static boolean VERBOSE = MigrationServer.VERBOSE;
  
  final static int VERSION = 551; // version for backward communications compatibility

  protected static final int CODE_ERROR = MigrationServer.CODE_ERROR;
  protected static final int CODE_ENTRY = MigrationServer.CODE_ENTRY; /* serialized key, serialized value */
  protected static final int CODE_COMPLETED = MigrationServer.CODE_COMPLETED;
  
  public static void main(String[] args) throws Exception {
    int argIdx = 0;
    String cacheXmlFileName = null;
    String regionName = null;
    String bindAddressName = null;
    int serverPort = 10533;
    
    if (args.length > argIdx+1) {
      regionName = args[argIdx++];
      cacheXmlFileName = args[argIdx++];
    } else {
      System.err.println("MigrationClient regionName [cache-xml-file] [server-port] [server-address]");
      return;
    }
    if (args.length > argIdx) {
      serverPort = Integer.parseInt(args[argIdx++]);
    }
    if (args.length > argIdx) {
      bindAddressName = args[argIdx++];
    }
    
    MigrationClient instance = null;
    try {
      instance = new MigrationClient(cacheXmlFileName, bindAddressName, serverPort);
    } catch (IllegalArgumentException e) {
      System.err.println(e.getMessage());
      System.exit(1);
    }
    instance.createDistributedSystem();
    instance.createCache();
    instance.getRegion(regionName);
  }


  private InetAddress serverAddress;
  private int port;
  private DistributedSystem distributedSystem;
  private File cacheXmlFile;
  private Cache cache;

  private Socket server;
  private int serverVersion;
  private DataInputStream dis;
  private DataOutputStream dos;
  
  
  /**
   * Create a MigrationClient to be used with a DistributedSystem and Cache
   * that are created using GemFire APIs
   * @param bindAddressName the server's address
   * @param serverPort the server's port
   */
  public MigrationClient(String bindAddressName, int serverPort) {
    this.port = serverPort;
    try {
      this.serverAddress = InetAddress.getByName(bindAddressName);
    } catch (IOException e) {
      throw new IllegalArgumentException("Error - bind address cannot be resolved: '" + bindAddressName + "'");
    }
  }
  
  /**
   * this is for use by main()
   * 
   * @param cacheXmlFileName the name of the xml file describing the cache, or null
   * @param bindAddressName the name of the NIC to bind to, or null
   * @param serverPort the port to connect to (must not be zero)
   */
  private MigrationClient(String cacheXmlFileName, String bindAddressName, int serverPort) {
    this(bindAddressName, serverPort);
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
    if (System.getProperty(DistributionConfig.GEMFIRE_PREFIX + MCAST_PORT) == null
        && System.getProperty(DistributionConfig.GEMFIRE_PREFIX + LOCATORS) == null) {
      dsProps.put(MCAST_PORT, "0");
    }
    dsProps.put(LOG_FILE, "migrationClient.log");
    if (this.cacheXmlFile != null) {
      dsProps.put(CACHE_XML_FILE, this.cacheXmlFile.getName());
    }
    this.distributedSystem = DistributedSystem.connect(dsProps);
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
  }
  
  private void initDSAndCache() {
    if (this.distributedSystem == null) {
      this.distributedSystem = InternalDistributedSystem.getConnectedInstance();
    }
    if (this.cache == null) {
      this.cache = GemFireCacheImpl.getInstance();
    }
  }  

  public Region getRegion(String regionName) throws IOException, ClassNotFoundException {
    initDSAndCache();
    Region region = null;
    region = this.cache.getRegion(regionName);
    try {
      connectToServer();
      if (this.serverVersion != VERSION) {
        System.out.println("Don't know how to deal with version " + this.serverVersion);
        throw new IOException("Server has incompatible version of MigrationServer");
      }

      this.dos.writeShort(MigrationServer.ClientRequest.REGION_REQUEST);
      this.dos.writeUTF(regionName);
      this.dos.flush();

      boolean done = false;
      while (!done) {
        int responseCode = -1;
        try {
          responseCode = this.dis.readShort();
        } catch (EOFException e) {
        }
        switch (responseCode) {
        case -1:
          throw new IOException("Server socket was closed while receiving entries");
        case CODE_COMPLETED:
          done = true;
          break;
        case CODE_ERROR:
          String errorString = this.dis.readUTF();
          System.err.println("Server responded with error: '" + errorString + "'");
          throw new IOException(errorString);
        case CODE_ENTRY:
          Object key = (new ObjectInputStream(server.getInputStream())).readObject();
          Object value = (new ObjectInputStream(server.getInputStream())).readObject();
          if (VERBOSE) {
            System.out.println("received " + key);
          }
          region.put(key, value);
          break;
        }
      }
    } finally {
      if (server != null && !server.isClosed()) {
        server.close();
      }
    }
    return region;
  }
  
  
  private void connectToServer() throws IOException {
    this.server = new Socket();
    SocketAddress addr;
    if (this.serverAddress != null) {
      addr = new InetSocketAddress(this.serverAddress, this.port);
    } else {
      addr = new InetSocketAddress(SocketCreator.getLocalHost(), this.port);
    }
    if (VERBOSE) {
      System.out.println("connecting to " + addr);
    }
    this.server.connect(addr);
    this.dos = new DataOutputStream(this.server.getOutputStream());
    this.dos.writeShort(VERSION);
    this.dis = new DataInputStream(this.server.getInputStream());
    this.serverVersion = this.dis.readShort();
  }
}
