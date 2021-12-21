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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.server.ServerLoad;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.logging.internal.executors.LoggingExecutors;

/**
 * A data structure used to hold load information for a locator
 *
 * @since GemFire 5.7
 *
 */
public class LocatorLoadSnapshot {

  private static final String LOAD_IMBALANCE_THRESHOLD_PROPERTY_NAME =
      "gemfire.locator-load-imbalance-threshold";

  public static final float DEFAULT_LOAD_IMBALANCE_THRESHOLD = 10;

  private final Map<ServerLocation, String[]> serverGroupMap = new HashMap<>();

  private final Map<String, Map<ServerLocationAndMemberId, LoadHolder>> connectionLoadMap =
      new HashMap<>();

  private final Map<String, Map<ServerLocation, LoadHolder>> queueLoadMap = new HashMap<>();

  private final ConcurrentMap<EstimateMapKey, LoadEstimateTask> estimateMap =
      new ConcurrentHashMap<>();

  /**
   * when replacing a client's current server we do not move a client from a highly loaded server to
   * a less loaded server until imbalance reaches this threshold. Then we aggressively move clients
   * until balance is achieved.
   */
  private final float loadImbalanceThreshold;

  /**
   * when the loadImbalanceThreshold is hit this variable will be true and it will remain true until
   * balance is achieved.
   */
  private boolean rebalancing;

  private final ScheduledExecutorService estimateTimeoutProcessor =
      LoggingExecutors.newScheduledThreadPool(1, "loadEstimateTimeoutProcessor", false);

  public LocatorLoadSnapshot() {
    connectionLoadMap.put(null, new HashMap<>());
    queueLoadMap.put(null, new HashMap<>());
    String property = System.getProperty(LOAD_IMBALANCE_THRESHOLD_PROPERTY_NAME);
    if (property != null) {
      loadImbalanceThreshold = Float.parseFloat(property);
    } else {
      loadImbalanceThreshold = DEFAULT_LOAD_IMBALANCE_THRESHOLD;
    }
  }

  /**
   * Add a new server to the load snapshot.
   */
  public synchronized void addServer(ServerLocation location, String memberId, String[] groups,
      ServerLoad initialLoad, long loadPollInterval) {
    serverGroupMap.put(location, groups);
    LoadHolder connectionLoad =
        new LoadHolder(location, initialLoad.getConnectionLoad(),
            initialLoad.getLoadPerConnection(), loadPollInterval);
    addGroups(connectionLoadMap, groups, connectionLoad, memberId);
    LoadHolder queueLoad = new LoadHolder(location,
        initialLoad.getSubscriptionConnectionLoad(),
        initialLoad.getLoadPerSubscriptionConnection(), loadPollInterval);
    addGroups(queueLoadMap, groups, queueLoad);
    updateLoad(location, memberId, initialLoad);
  }

  /**
   * Remove a server from the load snapshot.
   */
  public synchronized void removeServer(ServerLocation location, String memberId) {
    String[] groups = serverGroupMap.remove(location);
    /*
     * Adding null check for #41522 - we were getting a remove from a BridgeServer that was shutting
     * down and the ServerLocation wasn't in this map. The root cause isn't 100% clear but it might
     * be a race from profile add / remove from different channels.
     */
    if (groups != null) {
      removeFromMap(connectionLoadMap, groups, location, memberId);
      removeFromMap(queueLoadMap, groups, location);
    }
  }

  public void updateLoad(ServerLocation location, String memberId, ServerLoad newLoad) {
    updateLoad(location, memberId, newLoad, null);
  }

  /**
   * Update the load information for a server that was previously added.
   */
  synchronized void updateLoad(ServerLocation location, String memberId, ServerLoad newLoad,
      List<ClientProxyMembershipID> clientIds) {

    String[] groups = serverGroupMap.get(location);
    // the server was asynchronously removed, so don't do anything.
    if (groups == null) {
      return;
    }

    if (clientIds != null) {
      for (ClientProxyMembershipID clientId : clientIds) {
        cancelClientEstimate(clientId, location);
      }
    }

    updateMap(connectionLoadMap, location, memberId, newLoad.getConnectionLoad(),
        newLoad.getLoadPerConnection());
    updateMap(queueLoadMap, location, newLoad.getSubscriptionConnectionLoad(),
        newLoad.getLoadPerSubscriptionConnection());
  }

  public synchronized boolean hasBalancedConnections(String group) {
    if ("".equals(group)) {
      group = null;
    }

    Map<ServerLocationAndMemberId, LoadHolder> groupServers = connectionLoadMap.get(group);
    return isBalanced(groupServers);
  }

  private synchronized boolean isBalanced(Map<ServerLocationAndMemberId, LoadHolder> groupServers) {
    return isBalanced(groupServers, false);
  }

  private synchronized boolean isBalanced(Map<ServerLocationAndMemberId, LoadHolder> groupServers,
      boolean withThresholdCheck) {
    if (groupServers == null || groupServers.isEmpty()) {
      return true;
    }

    float bestLoad = Float.MAX_VALUE;
    float largestLoadPerConnection = Float.MIN_VALUE;
    float worstLoad = Float.MIN_VALUE;

    for (Entry<ServerLocationAndMemberId, LoadHolder> loadHolderEntry : groupServers.entrySet()) {
      LoadHolder nextLoadReference = loadHolderEntry.getValue();
      float nextLoad = nextLoadReference.getLoad();
      float nextLoadPerConnection = nextLoadReference.getLoadPerConnection();

      if (nextLoad < bestLoad) {
        bestLoad = nextLoad;
      }
      if (nextLoad > worstLoad) {
        worstLoad = nextLoad;
      }
      if (nextLoadPerConnection > largestLoadPerConnection) {
        largestLoadPerConnection = nextLoadPerConnection;
      }
    }

    boolean balanced = (worstLoad - bestLoad) <= largestLoadPerConnection;

    if (withThresholdCheck) {
      balanced = thresholdCheck(bestLoad, worstLoad, largestLoadPerConnection, balanced);
    }

    return balanced;
  }


  /**
   * In order to keep from ping-ponging clients around the cluster we don't move a client unless
   * imbalance is greater than the loadImbalanceThreshold.
   * <p>
   * When the threshold is reached we report imbalance until proper balance is achieved.
   * </p>
   * <p>
   * This method has the side-effect of setting the <code>rebalancing</code> instance variable
   * which, at the time of this writing, is only used by this method.
   * </p>
   */
  private synchronized boolean thresholdCheck(float bestLoad, float worstLoad,
      float largestLoadPerConnection, boolean balanced) {
    if (rebalancing) {
      if (balanced) {
        rebalancing = false;
      }
      return balanced;
    }

    // see if we're out of balance enough to trigger rebalancing or whether we
    // should tolerate the imbalance
    if (!balanced) {
      float imbalance = worstLoad - bestLoad;
      if (imbalance >= (largestLoadPerConnection * loadImbalanceThreshold)) {
        rebalancing = true;
      } else {
        // we're not in balance but are within the threshold
        balanced = true;
      }
    }
    return balanced;
  }

  synchronized boolean isRebalancing() {
    return rebalancing;
  }

  /**
   * Pick the least loaded server in the given group
   *
   * @param group the group, or null or "" if the client has no server group.
   * @param excludedServers a list of servers to exclude as choices
   * @return the least loaded server, or null if there are no servers that aren't excluded.
   */
  public synchronized ServerLocation getServerForConnection(String group,
      Set<ServerLocation> excludedServers) {
    if ("".equals(group)) {
      group = null;
    }

    Map<ServerLocationAndMemberId, LoadHolder> groupServers = connectionLoadMap.get(group);
    if (groupServers == null || groupServers.isEmpty()) {
      return null;
    }

    {
      List bestLHs = findBestServers(groupServers, excludedServers, 1);
      if (bestLHs.isEmpty()) {
        return null;
      }
      LoadHolder lh = (LoadHolder) bestLHs.get(0);
      lh.incConnections();
      return lh.getLocation();
    }
  }

  public synchronized ArrayList getServers(String group) {
    if ("".equals(group)) {
      group = null;
    }
    Map<ServerLocationAndMemberId, LoadHolder> groupServers = connectionLoadMap.get(group);
    if (groupServers == null || groupServers.isEmpty()) {
      return null;
    }
    ArrayList result = new ArrayList<>();
    for (ServerLocationAndMemberId locationAndMemberId : groupServers.keySet()) {
      result.add(locationAndMemberId.getServerLocation());
    }
    return result;
  }

  public void shutDown() {
    estimateTimeoutProcessor.shutdown();
  }

  /**
   * Pick the least loaded server in the given group if currentServer is the most loaded server.
   *
   * @param group the group, or null or "" if the client has no server group.
   * @param excludedServers a list of servers to exclude as choices
   * @return currentServer if it is not the most loaded, null if there are no servers that aren't
   *         excluded, otherwise the least loaded server in the group.
   */
  public synchronized ServerLocation getReplacementServerForConnection(ServerLocation currentServer,
      String group, Set<ServerLocation> excludedServers) {
    if ("".equals(group)) {
      group = null;
    }

    Map<ServerLocationAndMemberId, LoadHolder> groupServers = connectionLoadMap.get(group);
    if (groupServers == null || groupServers.isEmpty()) {
      return null;
    }

    // check to see if we are currently balanced
    if (isBalanced(groupServers, true)) {
      // if we are then return currentServer
      return currentServer;
    }

    LoadHolder currentServerLH = isCurrentServerMostLoaded(currentServer, groupServers);
    if (currentServerLH == null) {
      return currentServer;
    }
    {
      List<LoadHolder> bestLHs = findBestServers(groupServers, excludedServers, 1);
      if (bestLHs.isEmpty()) {
        return null;
      }
      LoadHolder bestLH = bestLHs.get(0);
      currentServerLH.decConnections();
      bestLH.incConnections();
      return bestLH.getLocation();
    }
  }

  /**
   * Pick the least loaded servers in the given group.
   *
   * @param group the group, or null or "" if the client has no server group.
   * @param excludedServers a list of servers to exclude as choices
   * @param count how many distinct servers to pick.
   * @return a list containing the best servers. The size of the list will be less than or equal to
   *         count, depending on if there are enough servers available.
   */
  public List getServersForQueue(String group, Set<ServerLocation> excludedServers, int count) {
    return getServersForQueue(null, group, excludedServers, count);
  }

  /**
   * Pick the least loaded servers in the given group.
   *
   * @param id the id of the client creating the queue
   * @param group the group, or null or "" if the client has no server group.
   * @param excludedServers a list of servers to exclude as choices
   * @param count how many distinct servers to pick.
   * @return a list containing the best servers. The size of the list will be less than or equal to
   *         count, depending on if there are enough servers available.
   */
  synchronized List<ServerLocation> getServersForQueue(ClientProxyMembershipID id, String group,
      Set<ServerLocation> excludedServers, int count) {
    if ("".equals(group)) {
      group = null;
    }

    Map<ServerLocation, LoadHolder> groupServers = queueLoadMap.get(group);

    if (groupServers == null || groupServers.isEmpty()) {
      return Collections.emptyList();
    }
    {
      List<LoadHolder> bestLHs = findBestServers(groupServers, excludedServers, count);
      ArrayList<ServerLocation> result = new ArrayList<>(bestLHs.size());

      if (id != null) {
        ClientProxyMembershipID.Identity actualId = id.getIdentity();
        for (LoadHolder load : bestLHs) {
          EstimateMapKey key = new EstimateMapKey(actualId, load.getLocation());
          LoadEstimateTask task = new LoadEstimateTask(key, load);
          try {
            final long MIN_TIMEOUT = 60000; // 1 minute
            long timeout = load.getLoadPollInterval() * 2;
            if (timeout < MIN_TIMEOUT) {
              timeout = MIN_TIMEOUT;
            }
            task.setFuture(estimateTimeoutProcessor.schedule(task, timeout, TimeUnit.MILLISECONDS));
            addEstimate(key, task);
          } catch (RejectedExecutionException e) {
            // ignore, the timer has been cancelled, which means we're shutting
            // down.
          }
          result.add(load.getLocation());
        }
      } else {
        for (LoadHolder load : bestLHs) {
          load.incConnections();
          result.add(load.getLocation());
        }
      }
      return result;
    }
  }

  /**
   * Test hook to get the current load for all servers Returns a map of ServerLocation->Load for
   * each server.
   */
  public synchronized Map<ServerLocation, ServerLoad> getLoadMap() {
    Map<ServerLocationAndMemberId, LoadHolder> connectionMap = connectionLoadMap.get(null);
    Map<ServerLocation, LoadHolder> queueMap = queueLoadMap.get(null);
    Map<ServerLocation, ServerLoad> result = new HashMap<>();

    for (Entry<ServerLocationAndMemberId, LoadHolder> entry : connectionMap
        .entrySet()) {
      ServerLocation location = entry.getKey().getServerLocation();
      LoadHolder connectionLoad = entry.getValue();
      LoadHolder queueLoad = queueMap.get(location);
      // was asynchronously removed
      if (queueLoad == null) {
        continue;
      }
      result.put(location,
          new ServerLoad(connectionLoad.getLoad(), connectionLoad.getLoadPerConnection(),
              queueLoad.getLoad(), queueLoad.getLoadPerConnection()));
    }

    return result;
  }

  @VisibleForTesting
  void addGroups(Map<String, Map<ServerLocation, LoadHolder>> map, String[] groups,
      LoadHolder holder) {
    for (String group : groups) {
      Map<ServerLocation, LoadHolder> groupMap = map.computeIfAbsent(group, k -> new HashMap<>());
      groupMap.put(holder.getLocation(), holder);
    }
    // Special case for GatewayReceiver where we don't put those serverlocation against holder
    if (!(groups.length > 0 && groups[0].equals(GatewayReceiver.RECEIVER_GROUP))) {
      Map<ServerLocation, LoadHolder> groupMap = map.computeIfAbsent(null, k -> new HashMap<>());
      groupMap.put(holder.getLocation(), holder);
    }
  }

  @VisibleForTesting
  void addGroups(Map<String, Map<ServerLocationAndMemberId, LoadHolder>> map,
      String[] groups,
      LoadHolder holder, String memberId) {
    for (String group : groups) {
      Map<ServerLocationAndMemberId, LoadHolder> groupMap =
          map.computeIfAbsent(group, k -> new HashMap<>());
      groupMap.put(new ServerLocationAndMemberId(holder.getLocation(), memberId), holder);
    }
    // Special case for GatewayReceiver where we don't put those serverlocation against holder
    if (!(groups.length > 0 && groups[0].equals(GatewayReceiver.RECEIVER_GROUP))) {
      Map<ServerLocationAndMemberId, LoadHolder> groupMap =
          map.computeIfAbsent(null, k -> new HashMap<>());
      groupMap.put(new ServerLocationAndMemberId(holder.getLocation(), memberId), holder);
    }
  }

  @VisibleForTesting
  void removeFromMap(Map<String, Map<ServerLocation, LoadHolder>> map, String[] groups,
      ServerLocation location) {
    for (String group : groups) {
      Map<ServerLocation, LoadHolder> groupMap = map.get(group);
      if (groupMap != null) {
        groupMap.remove(location);
        if (groupMap.size() == 0) {
          map.remove(group);
        }
      }
    }
    Map groupMap = map.get(null);
    groupMap.remove(location);
  }

  @VisibleForTesting
  void removeFromMap(Map<String, Map<ServerLocationAndMemberId, LoadHolder>> map,
      String[] groups,
      ServerLocation location, String memberId) {
    ServerLocationAndMemberId locationAndMemberId =
        new ServerLocationAndMemberId(location, memberId);
    for (String group : groups) {
      Map<ServerLocationAndMemberId, LoadHolder> groupMap = map.get(group);
      if (groupMap != null) {
        groupMap.remove(locationAndMemberId);
        if (groupMap.size() == 0) {
          map.remove(group);
        }
      }
    }
    Map groupMap = map.get(null);
    groupMap.remove(locationAndMemberId);
  }

  @VisibleForTesting
  void updateMap(Map map, ServerLocation location, float load, float loadPerConnection) {
    updateMap(map, location, "", load, loadPerConnection);
  }

  @VisibleForTesting
  void updateMap(Map map, ServerLocation location, String memberId, float load,
      float loadPerConnection) {
    Map groupMap = (Map) map.get(null);
    LoadHolder holder;
    if (memberId.equals("")) {
      holder = (LoadHolder) groupMap.get(location);
    } else {
      ServerLocationAndMemberId locationAndMemberId =
          new ServerLocationAndMemberId(location, memberId);
      holder = (LoadHolder) groupMap.get(locationAndMemberId);
    }
    if (holder != null) {
      holder.setLoad(load, loadPerConnection);
    }
  }

  /**
   *
   * @param groupServers the servers to consider
   * @param excludedServers servers to exclude
   * @param count how many you want. a negative number means all of them in order of best to worst
   * @return a list of best...worst server LoadHolders
   */
  @VisibleForTesting
  List<LoadHolder> findBestServers(
      Map<?, LoadHolder> groupServers,
      Set<ServerLocation> excludedServers, int count) {

    if (count == 0) {
      return new ArrayList<>();
    }

    TreeSet<LoadHolder> bestEntries = new TreeSet<>((l1, l2) -> {
      int difference = Float.compare(l1.getLoad(), l2.getLoad());
      if (difference != 0) {
        return difference;
      }
      ServerLocation sl1 = l1.getLocation();
      ServerLocation sl2 = l2.getLocation();
      return sl1.compareTo(sl2);
    });

    boolean retainAll = (count < 0);
    float lastBestLoad = Float.MAX_VALUE;

    for (Map.Entry<?, LoadHolder> loadEntry : groupServers.entrySet()) {
      ServerLocation location;
      Object key = loadEntry.getKey();
      if (key instanceof ServerLocationAndMemberId) {
        location = ((ServerLocationAndMemberId) key).getServerLocation();
      } else if (key instanceof ServerLocation) {
        location = ((ServerLocation) key);
      } else {
        throw new InternalGemFireException(
            "findBestServers method was called with incorrect type parameters.");
      }
      if (excludedServers.contains(location)) {
        continue;
      }

      LoadHolder nextLoadReference = loadEntry.getValue();
      float nextLoad = nextLoadReference.getLoad();

      if ((bestEntries.size() < count) || retainAll || (nextLoad < lastBestLoad)) {
        bestEntries.add(nextLoadReference);
        if (!retainAll && (bestEntries.size() > count)) {
          bestEntries.remove(bestEntries.last());
        }
        LoadHolder lastBestHolder = bestEntries.last();
        lastBestLoad = lastBestHolder.getLoad();
      }
    }

    return new ArrayList<>(bestEntries);
  }

  /**
   * If it is most loaded then return its LoadHolder; otherwise return null;
   */
  @VisibleForTesting
  LoadHolder isCurrentServerMostLoaded(ServerLocation currentServer,
      Map<ServerLocationAndMemberId, LoadHolder> groupServers) {

    // Check if there are keys in the map that contains currentServer.
    LoadHolder currentLH = null;
    for (ServerLocationAndMemberId locationAndMemberId : groupServers.keySet()) {
      if (currentServer.equals(locationAndMemberId.getServerLocation())) {
        currentLH = groupServers.get(locationAndMemberId);
        break;
      }
    }
    if (currentLH == null) {
      return null;
    }
    final float currentLoad = currentLH.getLoad();
    for (Map.Entry<ServerLocationAndMemberId, LoadHolder> loadEntry : groupServers.entrySet()) {
      ServerLocation location = loadEntry.getKey().getServerLocation();
      if (location.equals(currentServer)) {
        continue;
      }
      LoadHolder nextLoadReference = loadEntry.getValue();
      float nextLoad = nextLoadReference.getLoad();
      if (nextLoad > currentLoad) {
        // found a server who has a higher load than us
        return null;
      }
    }
    return currentLH;
  }

  private void cancelClientEstimate(ClientProxyMembershipID id, ServerLocation location) {
    if (id != null) {
      removeAndCancelEstimate(new EstimateMapKey(id.getIdentity(), location));
    }
  }

  /**
   * Add the task to the estimate map at the given key and cancel any old task found
   */
  private void addEstimate(EstimateMapKey key, LoadEstimateTask task) {
    LoadEstimateTask oldTask;
    oldTask = estimateMap.put(key, task);
    if (oldTask != null) {
      oldTask.cancel();
    }
  }

  /**
   * Remove the task from the estimate map at the given key.
   *
   * @return true it task was removed; false if it was not the task mapped to key
   */
  private boolean removeIfPresentEstimate(EstimateMapKey key, LoadEstimateTask task) {
    // no need to cancel task; it already fired
    return estimateMap.remove(key, task);
  }

  /**
   * Remove and cancel any task estimate mapped to the given key.
   */
  private void removeAndCancelEstimate(EstimateMapKey key) {
    LoadEstimateTask oldTask;
    oldTask = estimateMap.remove(key);
    if (oldTask != null) {
      oldTask.cancel();
    }
  }

  /**
   * Used as a key on the estimateMap. These keys are made up of the identity of the client and
   * server that will be connected by the resource (e.g. queue) that we are trying to create.
   */
  private static class EstimateMapKey {
    private final ClientProxyMembershipID.Identity clientId;

    private final ServerLocation serverId;

    EstimateMapKey(ClientProxyMembershipID.Identity clientId, ServerLocation serverId) {
      this.clientId = clientId;
      this.serverId = serverId;
    }

    @Override
    public int hashCode() {
      return clientId.hashCode() ^ serverId.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof EstimateMapKey)) {
        return false;
      }
      EstimateMapKey that = (EstimateMapKey) obj;
      return clientId.equals(that.clientId) && serverId.equals(that.serverId);
    }
  }

  private class LoadEstimateTask implements Runnable {
    private final EstimateMapKey key;

    private final LoadHolder lh;

    private ScheduledFuture future;

    LoadEstimateTask(EstimateMapKey key, LoadHolder lh) {
      this.key = key;
      this.lh = lh;
      lh.addEstimate();
    }

    @Override
    public void run() {
      if (removeIfPresentEstimate(key, this)) {
        decEstimate();
      }
    }

    public void setFuture(ScheduledFuture future) {
      // Note this is always called once and only once
      // and always before cancel can be called.
      this.future = future;
    }

    public void cancel() {
      future.cancel(false);
      decEstimate();
    }

    private void decEstimate() {
      synchronized (LocatorLoadSnapshot.this) {
        lh.removeEstimate();
      }
    }
  }

  @VisibleForTesting
  static class LoadHolder {
    private float load;

    private float loadPerConnection;

    private int estimateCount;

    private final ServerLocation location;

    private final long loadPollInterval;

    LoadHolder(ServerLocation location, float load, float loadPerConnection,
        long loadPollInterval) {
      this.location = location;
      this.load = load;
      this.loadPerConnection = loadPerConnection;
      this.loadPollInterval = loadPollInterval;
    }

    void setLoad(float load, float loadPerConnection) {
      this.loadPerConnection = loadPerConnection;
      this.load = load + (estimateCount * loadPerConnection);
    }

    void incConnections() {
      load += loadPerConnection;
    }

    void addEstimate() {
      estimateCount++;
      incConnections();
    }

    void removeEstimate() {
      estimateCount--;
      decConnections();
    }

    void decConnections() {
      load -= loadPerConnection;
    }

    public float getLoad() {
      return load;
    }

    public float getLoadPerConnection() {
      return loadPerConnection;
    }

    public ServerLocation getLocation() {
      return location;
    }

    public long getLoadPollInterval() {
      return loadPollInterval;
    }

    @Override
    public String toString() {
      return "LoadHolder[" + getLoad() + ", " + getLocation() + ", loadPollInterval="
          + getLoadPollInterval()
          + ((estimateCount != 0) ? (", estimates=" + estimateCount) : "") + ", "
          + loadPerConnection + "]";
    }
  }
}
