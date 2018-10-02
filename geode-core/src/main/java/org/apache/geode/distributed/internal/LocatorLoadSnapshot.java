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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
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

import org.apache.geode.cache.server.ServerLoad;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.logging.LoggingExecutors;

/**
 * A data structure used to hold load information for a locator
 *
 * @since GemFire 5.7
 *
 */
public class LocatorLoadSnapshot {

  public static final String LOAD_IMBALANCE_THRESHOLD_PROPERTY_NAME =
      "gemfire.locator-load-imbalance-threshold";

  public static final float DEFAULT_LOAD_IMBALANCE_THRESHOLD = 10;

  private final Map/* <ServerLocation, String[]> */ serverGroupMap = new HashMap();

  private final Map/* <String(server group), Map<ServerLocation, LoadHolder> */
  connectionLoadMap = new HashMap();

  private final Map/* <String(server group), Map<ServerLocation, LoadHolder> */
  queueLoadMap = new HashMap();

  private final ConcurrentMap/* <EstimateMapKey, LoadEstimateTask> */
  estimateMap = new ConcurrentHashMap();

  /**
   * when replacing a client's current server we do not move a client from a highly loaded server to
   * a less loaded server until imbalance reaches this threshold. Then we aggressively move clients
   * until balance is achieved.
   */
  private float loadImbalanceThreshold;

  /**
   * when the loadImbalanceThreshold is hit this variable will be true and it will remain true until
   * balance is achieved.
   */
  private boolean rebalancing;

  private final ScheduledExecutorService estimateTimeoutProcessor =
      LoggingExecutors.newScheduledThreadPool("loadEstimateTimeoutProcessor", 1, false);

  public LocatorLoadSnapshot() {
    connectionLoadMap.put(null, new HashMap());
    queueLoadMap.put(null, new HashMap());
    String property = System.getProperty(LOAD_IMBALANCE_THRESHOLD_PROPERTY_NAME);
    if (property != null) {
      loadImbalanceThreshold = Float.parseFloat(property);
    } else {
      loadImbalanceThreshold = DEFAULT_LOAD_IMBALANCE_THRESHOLD;
    }
  }

  public void addServer(ServerLocation location, String[] groups, ServerLoad initialLoad) {
    addServer(location, groups, initialLoad, 30000);
  }

  /**
   * Add a new server to the load snapshot.
   */
  public synchronized void addServer(ServerLocation location, String[] groups,
      ServerLoad initialLoad, long loadPollInterval) {
    serverGroupMap.put(location, groups);
    LoadHolder connectionLoad = new LoadHolder(location, initialLoad.getConnectionLoad(),
        initialLoad.getLoadPerConnection(), loadPollInterval);
    addGroups(connectionLoadMap, groups, connectionLoad);
    LoadHolder queueLoad = new LoadHolder(location, initialLoad.getSubscriptionConnectionLoad(),
        initialLoad.getLoadPerSubscriptionConnection(), loadPollInterval);
    addGroups(queueLoadMap, groups, queueLoad);
    updateLoad(location, initialLoad);
  }

  /**
   * Remove a server from the load snapshot.
   */
  public synchronized void removeServer(ServerLocation location) {
    String[] groups = (String[]) serverGroupMap.remove(location);
    /*
     * Adding null check for #41522 - we were getting a remove from a BridgeServer that was shutting
     * down and the ServerLocation wasn't in this map. The root cause isn't 100% clear but it might
     * be a race from profile add / remove from different channels.
     */
    if (groups != null) {
      removeFromMap(connectionLoadMap, groups, location);
      removeFromMap(queueLoadMap, groups, location);
    }
  }

  public void updateLoad(ServerLocation location, ServerLoad newLoad) {
    updateLoad(location, newLoad, null);
  }

  /**
   * Update the load information for a server that was previously added.
   */
  public synchronized void updateLoad(ServerLocation location, ServerLoad newLoad,
      List/* <ClientProxyMembershipID> */ clientIds) {
    String[] groups = (String[]) serverGroupMap.get(location);
    // the server was asynchronously removed, so don't do anything.
    if (groups == null) {
      return;
    }

    if (clientIds != null) {
      for (Iterator itr = clientIds.iterator(); itr.hasNext();) {
        cancelClientEstimate((ClientProxyMembershipID) itr.next(), location);
      }
    }

    updateMap(connectionLoadMap, location, newLoad.getConnectionLoad(),
        newLoad.getLoadPerConnection());
    updateMap(queueLoadMap, location, newLoad.getSubscriptionConnectionLoad(),
        newLoad.getLoadPerSubscriptionConnection());
  }

  public synchronized boolean hasBalancedConnections(String group) {
    if ("".equals(group)) {
      group = null;
    }

    Map groupServers = (Map) connectionLoadMap.get(group);
    return isBalanced(groupServers);
  }

  private synchronized boolean isBalanced(Map<ServerLocation, LoadHolder> groupServers) {
    return isBalanced(groupServers, false);
  }

  private synchronized boolean isBalanced(Map<ServerLocation, LoadHolder> groupServers,
      boolean withThresholdCheck) {
    if (groupServers == null || groupServers.isEmpty()) {
      return true;
    }

    float bestLoad = Float.MAX_VALUE;
    float largestLoadPerConnection = Float.MIN_VALUE;
    float worstLoad = Float.MIN_VALUE;

    for (Entry<ServerLocation, LoadHolder> loadHolderEntry : groupServers.entrySet()) {
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
  synchronized boolean thresholdCheck(float bestLoad, float worstLoad,
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
  public synchronized ServerLocation getServerForConnection(String group, Set excludedServers) {
    if ("".equals(group)) {
      group = null;
    }

    Map groupServers = (Map) connectionLoadMap.get(group);
    if (groupServers == null || groupServers.isEmpty()) {
      return null;
    }

    {
      List bestLHs = findBestServers(groupServers, excludedServers, 1);
      if (bestLHs == null || bestLHs.isEmpty()) {
        return null;
      }
      LoadHolder lh = (LoadHolder) bestLHs.get(0);
      lh.incConnections();
      return lh.getLocation();
    }
  }

  /**
   * Get the least loaded servers for the given groups as a map. If the given array of groups is
   * null then returns the least loaded servers for all the known groups.
   */
  @SuppressWarnings("unchecked")
  public synchronized Map<String, ServerLocation> getServersForConnection(Collection<String> groups,
      Set excludedServers) {
    final HashMap<String, ServerLocation> loadMap = new HashMap<String, ServerLocation>();
    if (groups == null) {
      groups = this.connectionLoadMap.keySet();
    }
    for (String group : groups) {
      loadMap.put(group, getServerForConnection(group, excludedServers));
    }
    return loadMap;
  }

  public synchronized ArrayList getServers(String group) {
    if ("".equals(group)) {
      group = null;
    }
    Map groupServers = (Map) connectionLoadMap.get(group);
    if (groupServers == null || groupServers.isEmpty()) {
      return null;
    }
    ArrayList servers = new ArrayList();
    servers.addAll(groupServers.keySet());
    return servers;
  }

  public void shutDown() {
    this.estimateTimeoutProcessor.shutdown();
  }

  /**
   * Pick the least loaded server in the given group if currentServer is the most loaded server. n
   *
   * @param group the group, or null or "" if the client has no server group.
   * @param excludedServers a list of servers to exclude as choices
   * @return currentServer if it is not the most loaded, null if there are no servers that aren't
   *         excluded, otherwise the least loaded server in the group.
   */
  public synchronized ServerLocation getReplacementServerForConnection(ServerLocation currentServer,
      String group, Set excludedServers) {
    if ("".equals(group)) {
      group = null;
    }

    Map<ServerLocation, LoadHolder> groupServers = (Map) connectionLoadMap.get(group);
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
      List bestLHs = findBestServers(groupServers, excludedServers, 1);
      if (bestLHs == null || bestLHs.isEmpty()) {
        return null;
      }
      LoadHolder bestLH = (LoadHolder) bestLHs.get(0);
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
  public List getServersForQueue(String group, Set excludedServers, int count) {
    return getServersForQueue(null/* no id */, group, excludedServers, count);
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
  public synchronized List getServersForQueue(ClientProxyMembershipID id, String group,
      Set excludedServers, int count) {
    if ("".equals(group)) {
      group = null;
    }

    Map groupServers = (Map) queueLoadMap.get(group);

    if (groupServers == null || groupServers.isEmpty()) {
      return Collections.EMPTY_LIST;
    }
    {
      List/* <LoadHolder> */ bestLHs = findBestServers(groupServers, excludedServers, count);
      ArrayList/* <ServerLocation> */ result = new ArrayList(bestLHs.size());

      if (id != null) {
        ClientProxyMembershipID.Identity actualId = id.getIdentity();
        for (Iterator itr = bestLHs.iterator(); itr.hasNext();) {
          LoadHolder load = (LoadHolder) itr.next();
          EstimateMapKey key = new EstimateMapKey(actualId, load.getLocation());
          LoadEstimateTask task = new LoadEstimateTask(key, load);
          try {
            final long MIN_TIMEOUT = 60000; // 1 minute
            long timeout = load.getLoadPollInterval() * 2;
            if (timeout < MIN_TIMEOUT) {
              timeout = MIN_TIMEOUT;
            }
            task.setFuture(
                this.estimateTimeoutProcessor.schedule(task, timeout, TimeUnit.MILLISECONDS));
            addEstimate(key, task);
          } catch (RejectedExecutionException e) {
            // ignore, the timer has been cancelled, which means we're shutting
            // down.
          }
          result.add(load.getLocation());
        }
      } else {
        for (Iterator itr = bestLHs.iterator(); itr.hasNext();) {
          LoadHolder load = (LoadHolder) itr.next();
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
    Map connectionMap = (Map) connectionLoadMap.get(null);
    Map queueMap = (Map) queueLoadMap.get(null);
    Map result = new HashMap();

    for (Iterator itr = connectionMap.entrySet().iterator(); itr.hasNext();) {
      Map.Entry next = (Entry) itr.next();
      ServerLocation location = (ServerLocation) next.getKey();
      LoadHolder connectionLoad = (LoadHolder) next.getValue();
      LoadHolder queueLoad = (LoadHolder) queueMap.get(location);
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

  private void addGroups(Map map, String[] groups, LoadHolder holder) {
    for (int i = 0; i < groups.length; i++) {
      Map groupMap = (Map) map.get(groups[i]);
      if (groupMap == null) {
        groupMap = new HashMap();
        map.put(groups[i], groupMap);
      }
      groupMap.put(holder.getLocation(), holder);
    }
    // Special case for GatewayReceiver where we don't put those serverlocation against
    // holder
    if (!(groups.length > 0 && groups[0].equals(GatewayReceiver.RECEIVER_GROUP))) {
      Map groupMap = (Map) map.get(null);
      if (groupMap == null) {
        groupMap = new HashMap();
        map.put(null, groupMap);
      }
      groupMap.put(holder.getLocation(), holder);
    }
  }

  private void removeFromMap(Map map, String[] groups, ServerLocation location) {
    for (int i = 0; i < groups.length; i++) {
      Map groupMap = (Map) map.get(groups[i]);
      if (groupMap != null) {
        groupMap.remove(location);
        if (groupMap.size() == 0) {
          map.remove(groupMap);
        }
      }
    }
    Map groupMap = (Map) map.get(null);
    groupMap.remove(location);
  }

  private void updateMap(Map map, ServerLocation location, float load, float loadPerConnection) {
    Map groupMap = (Map) map.get(null);
    LoadHolder holder = (LoadHolder) groupMap.get(location);
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
  private List /* <LoadHolder> */ findBestServers(Map<ServerLocation, LoadHolder> groupServers,
      Set excludedServers, int count) {

    TreeSet bestEntries = new TreeSet(new Comparator() {
      public int compare(Object o1, Object o2) {
        LoadHolder l1 = (LoadHolder) o1;
        LoadHolder l2 = (LoadHolder) o2;
        int difference = Float.compare(l1.getLoad(), l2.getLoad());
        if (difference != 0) {
          return difference;
        }
        ServerLocation sl1 = l1.getLocation();
        ServerLocation sl2 = l2.getLocation();
        return sl1.compareTo(sl2);
      }
    });

    boolean retainAll = (count < 0);
    float lastBestLoad = Float.MAX_VALUE;

    for (Map.Entry<ServerLocation, LoadHolder> loadEntry : groupServers.entrySet()) {
      ServerLocation location = loadEntry.getKey();
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
        LoadHolder lastBestHolder = (LoadHolder) bestEntries.last();
        lastBestLoad = lastBestHolder.getLoad();
      }
    }

    return new ArrayList(bestEntries);
  }

  /**
   * If it is most loaded then return its LoadHolder; otherwise return null;
   */
  private LoadHolder isCurrentServerMostLoaded(ServerLocation currentServer,
      Map<ServerLocation, LoadHolder> groupServers) {
    final LoadHolder currentLH = groupServers.get(currentServer);
    if (currentLH == null)
      return null;
    final float currentLoad = currentLH.getLoad();
    for (Map.Entry<ServerLocation, LoadHolder> loadEntry : groupServers.entrySet()) {
      ServerLocation location = loadEntry.getKey();
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
    LoadEstimateTask oldTask = null;
    oldTask = (LoadEstimateTask) this.estimateMap.put(key, task);
    if (oldTask != null) {
      oldTask.cancel();
    }
  }

  /**
   * Remove the task from the estimate map at the given key.
   *
   * @return true it task was removed; false if it was not the task mapped to key
   */
  protected boolean removeIfPresentEstimate(EstimateMapKey key, LoadEstimateTask task) {
    // no need to cancel task; it already fired
    return estimateMap.remove(key, task);
  }

  /**
   * Remove and cancel any task estimate mapped to the given key.
   */
  private void removeAndCancelEstimate(EstimateMapKey key) {
    LoadEstimateTask oldTask = null;
    oldTask = (LoadEstimateTask) this.estimateMap.remove(key);
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

    public EstimateMapKey(ClientProxyMembershipID.Identity clientId, ServerLocation serverId) {
      this.clientId = clientId;
      this.serverId = serverId;
    }

    @Override
    public int hashCode() {
      return this.clientId.hashCode() ^ this.serverId.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if ((obj == null) || !(obj instanceof EstimateMapKey)) {
        return false;
      }
      EstimateMapKey that = (EstimateMapKey) obj;
      return this.clientId.equals(that.clientId) && this.serverId.equals(that.serverId);
    }
  }

  private class LoadEstimateTask implements Runnable {
    private final EstimateMapKey key;

    private final LoadHolder lh;

    private ScheduledFuture future;

    public LoadEstimateTask(EstimateMapKey key, LoadHolder lh) {
      this.key = key;
      this.lh = lh;
      lh.addEstimate();
    }

    public void run() {
      if (removeIfPresentEstimate(this.key, this)) {
        decEstimate();
      }
    }

    public void setFuture(ScheduledFuture future) {
      // Note this is always called once and only once
      // and always before cancel can be called.
      this.future = future;
    }

    public void cancel() {
      this.future.cancel(false);
      decEstimate();
    }

    private void decEstimate() {
      synchronized (LocatorLoadSnapshot.this) {
        this.lh.removeEstimate();
      }
    }
  }

  private static class LoadHolder {
    private float load;

    private float loadPerConnection;

    private int estimateCount;

    private final ServerLocation location;

    private final long loadPollInterval;

    public LoadHolder(ServerLocation location, float load, float loadPerConnection,
        long loadPollInterval) {
      this.location = location;
      this.load = load;
      this.loadPerConnection = loadPerConnection;
      this.loadPollInterval = loadPollInterval;
    }

    public void setLoad(float load, float loadPerConnection) {
      this.loadPerConnection = loadPerConnection;
      this.load = load + (this.estimateCount * loadPerConnection);
    }

    public void incConnections() {
      this.load += loadPerConnection;
    }

    public void addEstimate() {
      this.estimateCount++;
      incConnections();
    }

    public void removeEstimate() {
      this.estimateCount--;
      decConnections();
    }

    public void decConnections() {
      this.load -= loadPerConnection;
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
      return this.loadPollInterval;
    }

    @Override
    public String toString() {
      return "LoadHolder[" + getLoad() + ", " + getLocation() + ", loadPollInterval="
          + getLoadPollInterval()
          + ((this.estimateCount != 0) ? (", estimates=" + this.estimateCount) : "") + ", "
          + loadPerConnection + "]";
    }
  }
}
