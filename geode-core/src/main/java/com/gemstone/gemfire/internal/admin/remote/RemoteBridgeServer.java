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
package com.gemstone.gemfire.internal.admin.remote;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Set;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.ClientSession;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.cache.InterestRegistrationListener;
import com.gemstone.gemfire.cache.server.ServerLoad;
import com.gemstone.gemfire.cache.server.ServerLoadProbe;
import com.gemstone.gemfire.cache.server.ServerLoadProbeAdapter;
import com.gemstone.gemfire.cache.server.ServerMetrics;
import com.gemstone.gemfire.cache.server.ClientSubscriptionConfig;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.admin.AdminBridgeServer;
import com.gemstone.gemfire.internal.cache.AbstractCacheServer;
import com.gemstone.gemfire.internal.cache.CacheServerImpl;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * A remote (serializable) implementation of <code>BridgeServer</code>
 * that is passed between administration VMs and VMs that host caches
 * with bridge servers.
 *
 * @author David Whitlock
 * @since 4.0
 */
public class RemoteBridgeServer
  extends AbstractCacheServer
  implements AdminBridgeServer, DataSerializable {

  private static final long serialVersionUID = 8417391824652384959L;

  /** Is this bridge server running? */
  private boolean isRunning;

  /** The id of this bridge server */
  private int id;
  

//  /**
//   * The name of the directory in which to store overflowed files for client ha
//   * queue
//   */
//  private String overflowDirectory=null;
  //////////////////////  Constructors  //////////////////////

  /**
   * A "copy constructor" that creates a
   * <code>RemoteBridgeServer</code> from the contents of the given
   * <code>BridgeServerImpl</code>.
   */
  RemoteBridgeServer(CacheServerImpl impl) {
    super(null);
    this.port = impl.getPort();
    this.bindAddress = impl.getBindAddress();
    this.hostnameForClients = impl.getHostnameForClients();
    if (CacheServerImpl.ENABLE_NOTIFY_BY_SUBSCRIPTION_FALSE) {
      this.notifyBySubscription = impl.getNotifyBySubscription();
    }
    this.socketBufferSize = impl.getSocketBufferSize();
    this.maximumTimeBetweenPings = impl.getMaximumTimeBetweenPings();
    this.isRunning = impl.isRunning();
    this.maxConnections = impl.getMaxConnections();
    this.maxThreads = impl.getMaxThreads();
    this.id = System.identityHashCode(impl);
    this.maximumMessageCount = impl.getMaximumMessageCount();
    this.messageTimeToLive = impl.getMessageTimeToLive();
    this.groups = impl.getGroups();
    this.loadProbe = getProbe(impl.getLoadProbe());
    this.loadPollInterval = impl.getLoadPollInterval();
    this.tcpNoDelay = impl.getTcpNoDelay();
//  added for configuration of ha overflow
    ClientSubscriptionConfig cscimpl = impl.getClientSubscriptionConfig();    
    this.clientSubscriptionConfig.setEvictionPolicy(cscimpl.getEvictionPolicy());
    this.clientSubscriptionConfig.setCapacity(cscimpl.getCapacity());
    String diskStoreName = cscimpl.getDiskStoreName();
    if (diskStoreName != null) {
      this.clientSubscriptionConfig.setDiskStoreName(diskStoreName);
    } else {
      this.clientSubscriptionConfig.setOverflowDirectory(cscimpl.getOverflowDirectory());
    }
  }
  
  private ServerLoadProbe getProbe(ServerLoadProbe probe) {
    if(probe == null) {
      return new RemoteLoadProbe("");
    }
    if(probe instanceof Serializable) {
      return probe;
    }
    else {
      return new RemoteLoadProbe(probe.toString());
    }
  }

  /**
   * Constructor for de-serialization
   */
  public RemoteBridgeServer() {
    super(null);
  }

  ////////////////////  Instance Methods  ////////////////////
  
  @Override
  public void start() throws IOException {
    throw new UnsupportedOperationException(LocalizedStrings.RemoteBridgeServer_A_REMOTE_BRIDGESERVER_CANNOT_BE_STARTED.toLocalizedString());
  }

  public boolean isRunning() {
    return this.isRunning;
  }

  public void stop() {
    throw new UnsupportedOperationException(LocalizedStrings.RemoteBridgeServer_A_REMOTE_BRIDGESERVER_CANNOT_BE_STOPPED.toLocalizedString());
  }

  /**
   * Returns the cache that is served by this bridge server or
   * <code>null</code> if this server is not running.
   */
  @Override
  public Cache getCache() {
    throw new UnsupportedOperationException(LocalizedStrings.RemoteBridgeServer_CANNOT_GET_THE_CACHE_OF_A_REMOTE_BRIDGESERVER.toLocalizedString());
  }
  
  public ClientSession getClientSession(String durableClientId) {
    String s = LocalizedStrings.RemoteBridgeServer_CANNOT_GET_CLIENT_SESSION.toLocalizedString();
    throw new UnsupportedOperationException(s);
  }

  public ClientSession getClientSession(DistributedMember member) {
    String s = LocalizedStrings.RemoteBridgeServer_CANNOT_GET_CLIENT_SESSION.toLocalizedString();
    throw new UnsupportedOperationException(s);
  }

  public Set getAllClientSessions() {
    String s = LocalizedStrings.RemoteBridgeServer_CANNOT_GET_ALL_CLIENT_SESSIONS.toLocalizedString();
    throw new UnsupportedOperationException(s);
  }

  public ClientSubscriptionConfig getClientSubscriptionConfig(){
    return this.clientSubscriptionConfig;
  }

  public int getId() {
    return this.id;
  }
  
  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.port);
    out.writeBoolean(this.notifyBySubscription);
    out.writeBoolean(this.isRunning);
    out.writeInt(this.maxConnections);
    out.writeInt(this.id);
    out.writeInt(this.maximumTimeBetweenPings);
    out.writeInt(this.maximumMessageCount);
    out.writeInt(this.messageTimeToLive);
    out.writeInt(this.maxThreads);
    DataSerializer.writeString(this.bindAddress, out);
    DataSerializer.writeStringArray(this.groups, out);
    DataSerializer.writeString(this.hostnameForClients, out);
    DataSerializer.writeObject(this.loadProbe, out);
    DataSerializer.writePrimitiveLong(this.loadPollInterval, out);
    out.writeInt(this.socketBufferSize);
    out.writeBoolean(this.tcpNoDelay);
    out.writeInt(this.getClientSubscriptionConfig().getCapacity());
    DataSerializer.writeString(this.getClientSubscriptionConfig()
        .getEvictionPolicy(), out);
    DataSerializer.writeString(this.getClientSubscriptionConfig()
        .getDiskStoreName(), out);
    if (this.getClientSubscriptionConfig().getDiskStoreName() == null) {
      DataSerializer.writeString(this.getClientSubscriptionConfig()
          .getOverflowDirectory(), out);
    }
  }

  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
	  
    this.port = in.readInt();
    this.notifyBySubscription = in.readBoolean();
    this.isRunning = in.readBoolean();
    this.maxConnections = in.readInt();
    this.id = in.readInt();
    this.maximumTimeBetweenPings = in .readInt();
    this.maximumMessageCount = in.readInt();
    this.messageTimeToLive = in.readInt();
    this.maxThreads = in.readInt();
    setBindAddress(DataSerializer.readString(in));
    setGroups(DataSerializer.readStringArray(in));
    setHostnameForClients(DataSerializer.readString(in));
    setLoadProbe((ServerLoadProbe)DataSerializer.readObject(in));
    setLoadPollInterval(DataSerializer.readPrimitiveLong(in));
    this.socketBufferSize = in.readInt();
    this.tcpNoDelay = in.readBoolean();
    this.getClientSubscriptionConfig().setCapacity(in.readInt());
    this.getClientSubscriptionConfig().setEvictionPolicy(
        DataSerializer.readString(in));
    String diskStoreName = DataSerializer.readString(in);
    if (diskStoreName != null) {
      this.getClientSubscriptionConfig().setDiskStoreName(diskStoreName);
    } else {
      this.getClientSubscriptionConfig().setOverflowDirectory(
          DataSerializer.readString(in));
    }
  }
  
  private static class RemoteLoadProbe extends ServerLoadProbeAdapter {
    /** The description of this callback */
    private final String desc;

    public RemoteLoadProbe(String desc) {
      this.desc = desc;
    }

    public ServerLoad getLoad(ServerMetrics metrics) {
      return null;
    }
    
    @Override
    public String toString() {
      return desc;
    }
  }  
  /**
   * Registers a new <code>InterestRegistrationListener</code> with the set of
   * <code>InterestRegistrationListener</code>s.
   * 
   * @param listener
   *                The <code>InterestRegistrationListener</code> to register
   * 
   * @since 5.8Beta
   */
  public void registerInterestRegistrationListener(
      InterestRegistrationListener listener) {
    final String s = LocalizedStrings.
      RemoteBridgeServer_INTERESTREGISTRATIONLISTENERS_CANNOT_BE_REGISTERED_ON_A_REMOTE_BRIDGESERVER
      .toLocalizedString();
    throw new UnsupportedOperationException(s);
  }

  /**
   * Unregisters an existing <code>InterestRegistrationListener</code> from
   * the set of <code>InterestRegistrationListener</code>s.
   * 
   * @param listener
   *                The <code>InterestRegistrationListener</code> to
   *                unregister
   * 
   * @since 5.8Beta
   */
  public void unregisterInterestRegistrationListener(
      InterestRegistrationListener listener) {
    final String s = LocalizedStrings.
      RemoteBridgeServer_INTERESTREGISTRATIONLISTENERS_CANNOT_BE_UNREGISTERED_FROM_A_REMOTE_BRIDGESERVER
      .toLocalizedString();
    throw new UnsupportedOperationException(s);
  }

  /**
   * Returns a read-only set of <code>InterestRegistrationListener</code>s
   * registered with this notifier.
   * 
   * @return a read-only set of <code>InterestRegistrationListener</code>s
   *         registered with this notifier
   * 
   * @since 5.8Beta
   */
  public Set getInterestRegistrationListeners() {
    final String s = LocalizedStrings.
      RemoteBridgeServer_INTERESTREGISTRATIONLISTENERS_CANNOT_BE_RETRIEVED_FROM_A_REMOTE_BRIDGESERVER
      .toLocalizedString();
    throw new UnsupportedOperationException(s);
  }
}
