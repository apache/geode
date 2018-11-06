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

import java.io.File;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.geode.SystemFailure;
import org.apache.geode.admin.AdminException;
import org.apache.geode.admin.GemFireHealth;
import org.apache.geode.admin.GemFireHealthConfig;
import org.apache.geode.admin.GemFireMemberStatus;
import org.apache.geode.admin.OperationCancelledException;
import org.apache.geode.admin.RegionSubRegionSnapshot;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Config;
import org.apache.geode.internal.admin.AdminBridgeServer;
import org.apache.geode.internal.admin.Alert;
import org.apache.geode.internal.admin.CacheInfo;
import org.apache.geode.internal.admin.DLockInfo;
import org.apache.geode.internal.admin.GemFireVM;
import org.apache.geode.internal.admin.GfManagerAgent;
import org.apache.geode.internal.admin.HealthListener;
import org.apache.geode.internal.admin.ListenerIdMap;
import org.apache.geode.internal.admin.Stat;
import org.apache.geode.internal.admin.StatAlertDefinition;
import org.apache.geode.internal.admin.StatListener;
import org.apache.geode.internal.admin.StatResource;
import org.apache.geode.internal.logging.LoggingThread;

/**
 * Provides access to a remote gemfire VM for purposes of gathering statistics and other info
 * specific to that VM.
 *
 */
public abstract class RemoteGemFireVM implements GemFireVM {

  protected final RemoteGfManagerAgent agent;
  protected final InternalDistributedMember id;

  private volatile String name = null;
  private volatile InetAddress host = null;
  private volatile File workingDir = null;
  private volatile Date birthDate = null;
  private volatile File gemfireDir = null;
  private volatile Boolean isDedicatedCacheServer = null;

  /**
   * Keeps track of the <code>StatListener</code>s registered on statistics sampled in the remote
   * VM. key is listenerId; value is StatListener.
   */
  protected ListenerIdMap statListeners = new ListenerIdMap();
  private final Object statListenersLock = new Object();

  /**
   * A thread that asynchronously dispatches callbacks to <code>StatListener</code>s.
   */
  protected final StatDispatcher dispatcher;

  /**
   * The classpath from which to load the classes of objects inspected from this remote VM.
   */
  protected volatile String inspectionClasspath;

  /**
   * Has the distributed system member represented by this <code>GemFireVM</code> become
   * unreachable? If so, we should not try to communicate with it.
   */
  protected volatile boolean unreachable;

  /** How are objects in this remote VM inspected? */
  protected volatile int cacheInspectionMode = LIGHTWEIGHT_CACHE_VALUE;

  protected final Object healthLock = new Object();
  protected HealthListener healthListener = null;
  protected int healthListenerId = 0;

  // constructors

  /**
   * Creates a <code>RemoteApplicationVM</code> in a given distributed system (<code>agent</code>)
   * with the given <code>id</code>.
   * <p/>
   * You MUST invoke {@link #startStatDispatcher()} immediately after constructing an instance.
   *
   * @param alertLevel The level of {@link Alert}s that this administration console should receive
   *        from this member of the distributed system.
   */
  public RemoteGemFireVM(RemoteGfManagerAgent agent, InternalDistributedMember id, int alertLevel) {
    this.agent = agent;
    if (id == null) {
      throw new NullPointerException(
          "Cannot create a RemoteGemFireVM with a null id.");
    }
    this.id = id;
    this.dispatcher = new StatDispatcher();
    sendAsync(AdminConsoleMessage.create(alertLevel));
  }

  public void startStatDispatcher() {
    this.dispatcher.start();
  }

  // Object methods

  @Override // GemStoneAddition
  public String toString() {
    // fix for 31198
    String vmName = null;
    try {
      vmName = getName();
    } catch (OperationCancelledException e) {
      // ignore and leave name equal to null
    }
    if (vmName == null || vmName.length() == 0) {
      return this.id.toString();
    }
    return vmName;
  }

  @Override // GemStoneAddition
  public int hashCode() {
    return this.id.hashCode();
  }

  // GemFireVM methods

  /**
   * Returns the name of the remote system connection.
   */
  public String getName() {
    if (this.name == null) {
      initialize();
    }
    return name;
  }

  /**
   * Returns the host the manager is running on.
   */
  public InetAddress getHost() {
    if (this.host == null) {
      initialize();
    }
    return host;
  }

  public File getWorkingDirectory() {
    if (this.workingDir == null) {
      initialize();
    }
    return this.workingDir;
  }

  public File getGeodeHomeDir() {
    if (this.gemfireDir == null) {
      initialize();
    }
    return gemfireDir;
  }

  public Date getBirthDate() {
    if (this.birthDate == null) {
      initialize();
    }
    return birthDate;
  }

  public boolean isDedicatedCacheServer() {
    if (this.isDedicatedCacheServer == null) {
      initialize();
    }
    return this.isDedicatedCacheServer.booleanValue();
  }

  private void initialize() {
    FetchHostResponse response = (FetchHostResponse) sendAndWait(FetchHostRequest.create());
    this.name = response.getName();
    this.host = response.getHost();
    this.workingDir = response.getWorkingDirectory();
    this.gemfireDir = response.getGeodeHomeDir();
    this.birthDate = new Date(response.getBirthDate());
    this.isDedicatedCacheServer = Boolean.valueOf(response.isDedicatedCacheServer());
  }

  /**
   * Retrieves all statistic resources from the remote vm.
   *
   * @return array of all statistic resources
   *
   * @see FetchStatsRequest
   * @see FetchStatsResponse#getAllStats
   */
  public StatResource[] getAllStats() {
    FetchStatsResponse resp = (FetchStatsResponse) sendAndWait(FetchStatsRequest.create(null));
    return resp.getAllStats(this);
  }

  /**
   * Retrieves all statistic resources from the remote VM except for those involving SharedClass.
   *
   * @return array of non-SharedClass statistic resources
   *
   * @see FetchStatsRequest
   * @see FetchStatsResponse#getStats
   */
  public StatResource[] getStats(String statisticsTypeName) {
    FetchStatsResponse resp =
        (FetchStatsResponse) sendAndWait(FetchStatsRequest.create(statisticsTypeName));
    return resp.getAllStats(this);
  }

  /**
   * Returns information about distributed locks held by the remote VM.
   *
   * @see FetchDistLockInfoRequest
   */
  public DLockInfo[] getDistributedLockInfo() {
    FetchDistLockInfoResponse resp =
        (FetchDistLockInfoResponse) sendAndWait(FetchDistLockInfoRequest.create());
    return resp.getLockInfos();
  }

  /**
   * Adds a <code>StatListener</code> that is notified when a statistic in a given statistics
   * instance changes value.
   *
   * @param observer The listener to be notified
   * @param observedResource The statistics instance to be observed
   * @param observedStat The statistic to be observed
   *
   * @see AddStatListenerRequest
   */
  public void addStatListener(StatListener observer, StatResource observedResource,
      Stat observedStat) {
    AddStatListenerResponse resp = (AddStatListenerResponse) sendAndWait(
        AddStatListenerRequest.create(observedResource, observedStat));
    int listenerId = resp.getListenerId();
    synchronized (this.statListenersLock) {
      this.statListeners.put(listenerId, observer);
    }
  }

  /**
   * Notes that several statistics values have been updated in the distributed system member modeled
   * by this <code>RemoteGemFireVM</code> and invokes the {@link StatListener}s accordingly. Note
   * that the listener notification happens asynchronously.
   *
   * @param timestamp The time at which the statistics were sampled
   * @param listenerIds The <code>id</code>s of the <code>StatListener</code>s to be notified.
   * @param values The new values of the statistics
   */
  public void callStatListeners(long timestamp, int[] listenerIds, double[] values) {
    dispatcher.put(new DispatchArgs(timestamp, listenerIds, values));
  }



  /**
   * Invokes the callback methods on a bunch of <code>StatListener</code>s in response to a
   * statistics update message being received. This method is invoked in its own thread.
   *
   * for each listener in statListeners call stat value changed if its id is in listenerIds call
   * stat value unchanged if its id is not in listenerIds call cancelStatListener and
   * statListeners.remove if its id is negative in listenerIds
   *
   * @see #cancelStatListener
   */
  protected void internalCallStatListeners(long timestamp, int[] listenerIds, double[] values) {
    ListenerIdMap.Entry[] entries = null;
    List listenersToRemove = new ArrayList();
    synchronized (this.statListenersLock) {
      entries = this.statListeners.entries();
    }

    for (int j = 0; j < entries.length; j++) {
      int listenerId = entries[j].getKey();
      StatListener sl = (StatListener) entries[j].getValue();
      int i;
      for (i = 0; i < listenerIds.length; i++) {
        if (listenerIds[i] == listenerId || listenerIds[i] == -listenerId) {
          break;
        }
      }
      if (i == listenerIds.length) {
        sl.statValueUnchanged(timestamp);
      } else if (listenerIds[i] < 0) { // Stat resource went away
        listenersToRemove.add(Integer.valueOf(listenerId));
      } else {
        sl.statValueChanged(values[i], timestamp);
      }
    }

    synchronized (this.statListenersLock) {
      for (Iterator iter = listenersToRemove.iterator(); iter.hasNext();) {
        int i = ((Integer) iter.next()).intValue();
        statListeners.remove(i);
        cancelStatListener(i);
      }
    }
  }

  /**
   * Sends a message to the remote VM letting it know that the listener with the given id no longer
   * needs events set to it.
   */
  private void cancelStatListener(int listenerId) {
    sendAndWait(CancelStatListenerRequest.create(listenerId));
  }

  /**
   * Removes a <code>StatListener</code> that receives updates from the remote member VM.
   */
  public void removeStatListener(StatListener observer) {
    int listenerId = -1;
    boolean foundIt = false;
    synchronized (this.statListenersLock) {
      ListenerIdMap.EntryIterator it = this.statListeners.iterator();
      ListenerIdMap.Entry e = it.next();
      while (e != null) {
        if (e.getValue() == observer) {
          foundIt = true;
          listenerId = e.getKey();
          this.statListeners.remove(listenerId);
          break;
        }
        e = it.next();
      }
    }
    if (foundIt) {
      cancelStatListener(listenerId);
    }
  }

  /**
   * Returns the configuration of the remote VM.
   *
   * @see FetchSysCfgRequest
   */
  public void addHealthListener(HealthListener observer, GemFireHealthConfig cfg) {
    synchronized (this.healthLock) {
      this.healthListener = observer;
      AddHealthListenerResponse response =
          (AddHealthListenerResponse) sendAndWait(AddHealthListenerRequest.create(cfg));
      this.healthListenerId = response.getHealthListenerId();
    }
  }

  public void removeHealthListener() {
    synchronized (this.healthLock) {
      this.healthListener = null;
      if (this.healthListenerId != 0) {
        sendAndWait(RemoveHealthListenerRequest.create(this.healthListenerId));
        this.healthListenerId = 0;
      }
    }
  }

  public void resetHealthStatus() {
    synchronized (this.healthLock) {
      if (this.healthListenerId != 0) {
        sendAndWait(ResetHealthStatusRequest.create(this.healthListenerId));
      }
    }
  }

  public String[] getHealthDiagnosis(GemFireHealth.Health healthCode) {
    synchronized (this.healthLock) {
      if (this.healthListenerId != 0) {
        FetchHealthDiagnosisResponse response = (FetchHealthDiagnosisResponse) sendAndWait(
            FetchHealthDiagnosisRequest.create(this.healthListenerId, healthCode));
        return response.getDiagnosis();
      } else {
        return new String[] {};
      }
    }
  }

  /**
   * Called by HealthListenerMessage when the message is received.
   */
  void callHealthListeners(int listenerId, GemFireHealth.Health newStatus) {
    HealthListener hl = null;
    synchronized (this.healthLock) {
      // Make sure this call was to the current listener
      if (this.healthListenerId == listenerId) {
        hl = this.healthListener;
      }
    }
    if (hl != null) {
      try {
        hl.healthChanged(this, newStatus);
      } catch (RuntimeException ignore) {
      }
    }
  }

  public Config getConfig() {
    FetchSysCfgResponse response = (FetchSysCfgResponse) sendAndWait(FetchSysCfgRequest.create());
    return response.getConfig();
  }

  /**
   * Returns the runtime {@link org.apache.geode.admin.GemFireMemberStatus} from the vm The idea is
   * this snapshot is similar to stats that represent the current state of a running VM. However,
   * this is a bit higher level than a stat
   */
  public GemFireMemberStatus getSnapshot() {
    RefreshMemberSnapshotResponse response =
        (RefreshMemberSnapshotResponse) sendAndWait(RefreshMemberSnapshotRequest.create());
    return response.getSnapshot();
  }

  /**
   * Returns the runtime {@link org.apache.geode.admin.RegionSubRegionSnapshot} from the vm The idea
   * is this snapshot is quickly salvageable to present a cache's region's info
   */
  public RegionSubRegionSnapshot getRegionSnapshot() {
    RegionSubRegionsSizeResponse response =
        (RegionSubRegionsSizeResponse) sendAndWait(RegionSubRegionSizeRequest.create());
    return response.getSnapshot();
  }

  /**
   * Updates the configuration of the remote VM.
   *
   * @see StoreSysCfgRequest
   */
  public void setConfig(Config cfg) {
    /* StoreSysCfgResponse response = (StoreSysCfgResponse) */
    sendAndWait(StoreSysCfgRequest.create(cfg));
  }

  /**
   * Returns the agent for the distributed system to which this remote VM belongs.
   */
  public GfManagerAgent getManagerAgent() {
    return this.agent;
  }


  // public String tailSystemLog(){
  // TailLogResponse resp = (TailLogResponse)sendAndWait(TailLogRequest.create());
  // return resp.getTail();
  // }

  public String[] getSystemLogs() {
    TailLogResponse resp = (TailLogResponse) sendAndWait(TailLogRequest.create());
    String main = resp.getTail();
    String child = resp.getChildTail();
    String[] retVal = null;
    if (main != null) {
      if (child != null) {
        retVal = new String[] {main, child};
      } else {
        retVal = new String[] {main};
      }
    } else {
      retVal = new String[0];
    }
    return retVal;
  }

  public String getVersionInfo() {
    VersionInfoResponse resp = (VersionInfoResponse) sendAndWait(VersionInfoRequest.create());
    return resp.getVersionInfo();
  }

  /**
   * Returns information about the root <code>Region</code>s hosted in the remote VM.
   *
   * @see RootRegionRequest
   */
  public Region[] getRootRegions() {
    RootRegionResponse resp = (RootRegionResponse) sendAndWait(RootRegionRequest.create());
    return resp.getRegions(this);
  }

  public Region getRegion(CacheInfo c, String path) {
    RegionResponse resp = (RegionResponse) sendAndWait(RegionRequest.createForGet(c, path));
    return resp.getRegion(this);
  }

  public Region createVMRootRegion(CacheInfo c, String regionPath, RegionAttributes attrs)
      throws AdminException {

    RegionResponse resp =
        (RegionResponse) sendAndWait(RegionRequest.createForCreateRoot(c, regionPath, attrs));

    Exception ex = resp.getException();
    if (ex != null) {
      throw new AdminException(
          String.format("An Exception was thrown while creating VM root region %s",
              regionPath),
          ex);
    } else {
      return resp.getRegion(this);
    }
  }

  public Region createSubregion(CacheInfo c, String parentPath, String regionPath,
      RegionAttributes attrs) throws AdminException {

    RegionResponse resp = (RegionResponse) sendAndWait(
        RegionRequest.createForCreateSubregion(c, parentPath, regionPath, attrs));

    Exception ex = resp.getException();
    if (ex != null) {
      throw new AdminException(String.format("While creating subregion %s of %s",
          new Object[] {regionPath, parentPath}), ex);
    } else {
      return resp.getRegion(this);
    }
  }

  public void setCacheInspectionMode(int mode) {
    this.cacheInspectionMode = mode;
  }

  public int getCacheInspectionMode() {
    return this.cacheInspectionMode;
  }

  /**
   * Takes a snapshot of a <code>Region</code> hosted in the remote VM.
   *
   * @param regionName The name of the <Code>Region</code>
   * @param snapshotId The sequence number of the snapshot
   *
   * @see AppCacheSnapshotMessage
   */
  public void takeRegionSnapshot(String regionName, int snapshotId) {
    sendAsync(AppCacheSnapshotMessage.create(regionName, snapshotId));
  }

  // public void flushSnapshots() {
  // sendAsync(FlushAppCacheSnapshotMessage.create());
  // }


  // public boolean hasCache() {
  // throw new UnsupportedOperationException("Not yet implemented");
  // }

  // additional instance methods
  RemoteStat[] getResourceStatsByID(long rsrcId) {
    FetchResourceAttributesResponse response = (FetchResourceAttributesResponse) sendAndWait(
        FetchResourceAttributesRequest.create(rsrcId));
    return response.getStats();
  }


  // void fireCacheCreated(RemoteApplicationProcess app) {
  // app.setSystemManager(this);
  // synchronized(this.connectionListeners) {
  // Iterator iter = this.connectionListeners.iterator();
  // while (iter.hasNext()) {
  // ((ConnectionListener)iter.next()).cacheCreated(this, app);
  // }
  // }
  // }

  // void fireCacheClosed(RemoteApplicationProcess app) {
  // app.setSystemManager(this);
  // synchronized(this.connectionListeners) {
  // Iterator iter = this.connectionListeners.iterator();
  // while (iter.hasNext()) {
  // ((ConnectionListener)iter.next()).cacheClosed(this, app);
  // }
  // }
  // }

  public InternalDistributedMember getId() {
    return this.id;
  }

  public CacheInfo getCacheInfo() {
    CacheInfoResponse resp = (CacheInfoResponse) sendAndWait(CacheInfoRequest.create());
    RemoteCacheInfo result = resp.getCacheInfo();
    if (result != null) {
      result.setGemFireVM(this);
    }
    return result;
  }

  /**
   * Checks whether a durable-queue for a given client is present on the system member represented
   * by this RemoteGemFireVM
   *
   * @param durableClientId - the 'durable-client-id' for the client
   * @return - true if the member contains a durable-queue for the given client
   *
   * @since GemFire 5.6
   */
  public boolean hasDurableClient(String durableClientId) {
    DurableClientInfoResponse resp =
        (DurableClientInfoResponse) sendAndWait(DurableClientInfoRequest.create(durableClientId,
            DurableClientInfoRequest.HAS_DURABLE_CLIENT_REQUEST));
    boolean result = resp.getResultBoolean();
    return result;
  }

  /**
   * Checks whether the system member represented by this RemoteGemFireVM is hosting a primary
   * durable-queue for the client
   *
   * @param durableClientId - the 'durable-client-id' for the client
   * @return - true if the member contains a primary durable-queue for the given client
   *
   * @since GemFire 5.6
   */
  public boolean isPrimaryForDurableClient(String durableClientId) {
    DurableClientInfoResponse resp =
        (DurableClientInfoResponse) sendAndWait(DurableClientInfoRequest.create(durableClientId,
            DurableClientInfoRequest.IS_PRIMARY_FOR_DURABLE_CLIENT_REQUEST));
    boolean result = resp.getResultBoolean();
    return result;
  }

  public CacheInfo setCacheLockTimeout(CacheInfo c, int v) throws AdminException {
    return setCacheConfigValue(c, LOCK_TIMEOUT_CODE, v);
  }

  public CacheInfo setCacheLockLease(CacheInfo c, int v) throws AdminException {
    return setCacheConfigValue(c, LOCK_LEASE_CODE, v);
  }

  public CacheInfo setCacheSearchTimeout(CacheInfo c, int v) throws AdminException {
    return setCacheConfigValue(c, SEARCH_TIMEOUT_CODE, v);
  }

  public AdminBridgeServer addCacheServer(CacheInfo cache) throws AdminException {

    BridgeServerRequest request = BridgeServerRequest.createForAdd(cache);
    BridgeServerResponse response = (BridgeServerResponse) sendAndWait(request);
    if (response.getException() != null) {
      Exception ex = response.getException();
      throw new AdminException(ex.getMessage(), ex);

    } else {
      return response.getBridgeInfo();
    }
  }

  public AdminBridgeServer getBridgeInfo(CacheInfo cache, int bridgeRef) throws AdminException {

    BridgeServerRequest request = BridgeServerRequest.createForInfo(cache, bridgeRef);
    BridgeServerResponse response = (BridgeServerResponse) sendAndWait(request);
    if (response.getException() != null) {
      Exception ex = response.getException();
      throw new AdminException(ex.getMessage(), ex);

    } else {
      return response.getBridgeInfo();
    }
  }

  public AdminBridgeServer startBridgeServer(CacheInfo cache, AdminBridgeServer bridge)
      throws AdminException {

    BridgeServerRequest request =
        BridgeServerRequest.createForStart(cache, (RemoteBridgeServer) bridge);
    BridgeServerResponse response = (BridgeServerResponse) sendAndWait(request);
    if (response.getException() != null) {
      Exception ex = response.getException();
      throw new AdminException(ex.getMessage(), ex);

    } else {
      return response.getBridgeInfo();
    }
  }

  public AdminBridgeServer stopBridgeServer(CacheInfo cache, AdminBridgeServer bridge)
      throws AdminException {

    BridgeServerRequest request =
        BridgeServerRequest.createForStop(cache, (RemoteBridgeServer) bridge);
    BridgeServerResponse response = (BridgeServerResponse) sendAndWait(request);
    if (response.getException() != null) {
      Exception ex = response.getException();
      throw new AdminException(ex.getMessage(), ex);

    } else {
      return response.getBridgeInfo();
    }
  }


  static final int LOCK_TIMEOUT_CODE = 1;
  static final int LOCK_LEASE_CODE = 2;
  static final int SEARCH_TIMEOUT_CODE = 3;

  /**
   * Changes a Cache configuration value in a remote cache by sending the remote member a
   * {@link CacheConfigRequest}.
   *
   * @param c The remote cache to be changed
   * @param opCode The code of the operation to perform
   * @param value A value that is an argument to the operation
   *
   * @throws AdminException If a problem is encountered in the remote VM while changing the
   *         configuration.
   */
  private CacheInfo setCacheConfigValue(CacheInfo c, int opCode, int value) throws AdminException {

    if (c.isClosed()) {
      return c;
    }
    CacheConfigResponse resp =
        (CacheConfigResponse) sendAndWait(CacheConfigRequest.create(c, opCode, value));
    if (resp.getException() != null) {
      Exception ex = resp.getException();
      throw new AdminException(ex.getMessage(), ex);

    } else if (resp.getCacheInfo() == null) {
      c.setClosed();
      return c;

    } else {
      return resp.getCacheInfo();
    }
  }

  /**
   * Stops listening for statistics updates. Invoked when this <code>GemFireVM</code> disconnects or
   * when this member leaves the distributed system.
   */
  public void stopStatListening() {
    synchronized (this.statListenersLock) {
      this.statListeners = new ListenerIdMap(); // we don't provide a way to empty a ListenerIdMap
      unreachable = true;
    }
    dispatcher.stopDispatching();
  }

  /**
   * Invoked by the {@link RemoteGfManagerAgent} when the member that this <code>GemFireVM</code>
   * represents has left the distributed system.
   *
   * @param alertListenerRegistered Was there an alert listener registered for this
   *        <code>GemFireVM</code>'s agent? If so, the alert listeners should be removed in the
   *        member VMs.
   *
   * @see AdminConsoleDisconnectMessage
   */
  public void disconnect(boolean alertListenerRegistered) {
    // Thread.dumpStack();
    try {
      AdminConsoleDisconnectMessage msg = AdminConsoleDisconnectMessage.create();
      msg.setAlertListenerExpected(alertListenerRegistered);
      msg.setCrashed(false);
      sendAsync(msg);
    } finally {
      stopStatListening();
    }
  }


  public void setInspectionClasspath(String classpath) {
    this.inspectionClasspath = classpath;
  }

  public String getInspectionClasspath() {
    return this.inspectionClasspath;
  }

  // inner classes

  /**
   * A daemon thread that reads org.apache.geode.internal.admin.remote.RemoteGemFireVM.DispatchArgs
   * off of a queue and delivers callbacks to the appropriate
   * {@link org.apache.geode.internal.admin.StatListener}.
   */
  private class StatDispatcher extends LoggingThread {
    private BlockingQueue queue = new LinkedBlockingQueue();
    private volatile boolean stopped = false;

    protected StatDispatcher() {
      super("StatDispatcher");
    }

    protected synchronized void stopDispatching() {
      this.stopped = true;
      this.interrupt();
    }

    @Override // GemStoneAddition
    public void run() {
      while (!stopped) {
        SystemFailure.checkFailure();
        try {
          DispatchArgs args = (DispatchArgs) queue.take();
          internalCallStatListeners(args.timestamp, args.listenerIds, args.values);

        } catch (InterruptedException ex) {
          // No need to reset the interrupt bit, we'll just exit.
          break;

        } catch (Exception ignore) {
        }
      }
    }

    protected void put(DispatchArgs args) {
      for (;;) {
        RemoteGemFireVM.this.agent.getDM().getCancelCriterion().checkCancelInProgress(null);
        boolean interrupted = Thread.interrupted();
        try {
          queue.put(args);
          break;
        } catch (InterruptedException ignore) {
          interrupted = true;
        } finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
      } // for
    }
  }

  /**
   * Encapsulates an update to several statistics
   */
  private static class DispatchArgs {
    protected final long timestamp;
    protected final int[] listenerIds;
    protected final double[] values;

    /**
     * Creates a new <code>DispatchArgs</code>
     *
     * @param timestamp The time at which the statistics were sampled
     * @param listenerIds The ids of the <code>StatListener</code>s on which callbacks should be
     *        invoked.
     * @param values The new values of the statistics
     */
    protected DispatchArgs(long timestamp, int[] listenerIds, double[] values) {
      this.timestamp = timestamp;
      this.listenerIds = listenerIds;
      this.values = values;
    }
  }

  /**
   * Sends an AdminRequest to this dm (that is, to member of the distributed system represented by
   * this <code>RemoteGemFireVM</code>) and waits for the AdminReponse.
   */
  AdminResponse sendAndWait(AdminRequest msg) {
    if (unreachable) {
      throw new OperationCancelledException(
          String.format("%s is unreachable. It has either left or crashed.",
              this.name));
    }
    if (this.id == null) {
      throw new NullPointerException(
          "The id of this RemoteGemFireVM is null!");
    }
    msg.setRecipient(this.id);
    msg.setModifiedClasspath(inspectionClasspath);
    return agent.sendAndWait(msg);
  }

  /**
   * Sends a message to this dm (that is, to member of the distributed system represented by this
   * <code>RemoteGemFireVM</code>) and does not wait for a response
   */
  void sendAsync(DistributionMessage msg) {
    msg.setRecipient(id);
    if (msg instanceof AdminRequest) {
      ((AdminRequest) msg).setModifiedClasspath(inspectionClasspath);
    }
    agent.sendAsync(msg);
  }

  /**
   * This method should be used to set the Alerts Manager for the member agent. Stat Alerts
   * Aggregator would use this method to set stat Alerts Manager with the available alert
   * definitions and the refresh interval set for each member joining the distributed system.
   *
   * @param alertDefs Stat Alert Definitions to set for the Alerts Manager
   * @param refreshInterval refresh interval to be used by the Alerts Manager
   * @param setRemotely whether to be set on remote VM
   */
  public void setAlertsManager(StatAlertDefinition[] alertDefs, long refreshInterval,
      boolean setRemotely) {
    if (setRemotely) {
      // TODO: is the check for valid AdminResponse required
      sendAsync(StatAlertsManagerAssignMessage.create(alertDefs, refreshInterval));
    }
  }

  /**
   * This method would be used to set refresh interval for the GemFireVM. This method would mostly
   * be called on each member after initial set up whenever the refresh interval is changed.
   *
   * @param refreshInterval refresh interval to set (in milliseconds)
   */
  public void setRefreshInterval(long refreshInterval) {
    sendAsync(ChangeRefreshIntervalMessage.create(refreshInterval));
  }

  /**
   * This method would be used to set Sta Alert Definitions for the GemFireVM. This method would
   * mostly be called on each member after initial set up whenever one or more Stat Alert
   * Definitions get added/updated/removed.
   *
   * @param alertDefs an array of StaAlertDefinition objects
   * @param actionCode one of UpdateAlertDefinitionRequest.ADD_ALERT_DEFINITION,
   *        UpdateAlertDefinitionRequestUPDATE_ALERT_DEFINITION,
   *        UpdateAlertDefinitionRequest.REMOVE_ALERT_DEFINITION
   */
  public void updateAlertDefinitions(StatAlertDefinition[] alertDefs, int actionCode) {
    // TODO: is the check for valid AdminResponse required
    sendAsync(UpdateAlertDefinitionMessage.create(alertDefs, actionCode));
  }

}
