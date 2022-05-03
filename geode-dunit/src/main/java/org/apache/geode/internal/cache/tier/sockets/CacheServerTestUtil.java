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
package org.apache.geode.internal.cache.tier.sockets;

import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_ID;
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_TIMEOUT;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache.util.CqListenerAdapter;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.PoolFactoryImpl;
import org.apache.geode.internal.cache.PoolFactoryImpl.PoolAttributes;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.util.internal.GeodeGlossary;


/**
 * @deprecated Please use {@link DistributedRule} and Geode User APIs or {@link ClusterStartupRule}
 *             instead.
 */
@Deprecated
public class CacheServerTestUtil extends JUnit4DistributedTestCase {

  private static Cache cache = null;
  private static IgnoredException expected;

  private static PoolImpl pool = null;

  protected static final int TYPE_CREATE = 0;
  protected static final int TYPE_UPDATE = 1;
  protected static final int TYPE_INVALIDATE = 2;
  protected static final int TYPE_DESTROY = 3;

  public static void createCacheClient(Pool poolAttr, String regionName) throws Exception {
    createCacheClient(poolAttr, regionName, getClientProperties(), Boolean.FALSE);
  }

  public static void createCacheClient(Pool poolAttr, String regionName, Properties dsProperties)
      throws Exception {
    createCacheClient(poolAttr, regionName, dsProperties, Boolean.FALSE);
  }

  public static void createClientCache(Pool poolAttr, String regionName) throws Exception {
    createClientCache(poolAttr, regionName, getClientProperties());
  }

  public static void createClientCache(Pool poolAttr, String regionName, Properties dsProperties) {
    ClientCacheFactory ccf = new ClientCacheFactory(dsProperties);
    if (poolAttr != null) {
      ccf.setPoolFreeConnectionTimeout(poolAttr.getFreeConnectionTimeout())
          .setPoolServerConnectionTimeout(poolAttr.getServerConnectionTimeout())
          .setPoolLoadConditioningInterval(poolAttr.getLoadConditioningInterval())
          .setPoolSocketBufferSize(poolAttr.getSocketBufferSize())
          .setPoolMinConnections(poolAttr.getMinConnections())
          .setPoolMaxConnections(poolAttr.getMaxConnections())
          .setPoolIdleTimeout(poolAttr.getIdleTimeout())
          .setPoolPingInterval(poolAttr.getPingInterval())
          .setPoolStatisticInterval(poolAttr.getStatisticInterval())
          .setPoolRetryAttempts(poolAttr.getRetryAttempts())
          .setPoolReadTimeout(poolAttr.getReadTimeout())
          .setPoolSubscriptionEnabled(poolAttr.getSubscriptionEnabled())
          .setPoolPRSingleHopEnabled(poolAttr.getPRSingleHopEnabled())
          .setPoolSubscriptionRedundancy(poolAttr.getSubscriptionRedundancy())
          .setPoolSubscriptionMessageTrackingTimeout(
              poolAttr.getSubscriptionMessageTrackingTimeout())
          .setPoolSubscriptionTimeoutMultiplier(poolAttr.getSubscriptionTimeoutMultiplier())
          .setPoolSubscriptionAckInterval(poolAttr.getSubscriptionAckInterval())
          .setPoolServerGroup(poolAttr.getServerGroup())
          .setPoolMultiuserAuthentication(poolAttr.getMultiuserAuthentication());
      for (InetSocketAddress locator : poolAttr.getLocators()) {
        ccf.addPoolLocator(locator.getHostName(), locator.getPort());
      }
      for (InetSocketAddress server : poolAttr.getServers()) {
        ccf.addPoolServer(server.getHostName(), server.getPort());
      }
    }
    new CacheServerTestUtil().createClientCache(dsProperties, ccf);
    ClientCache cc = (ClientCache) cache;
    cc.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create(regionName);
    pool = (PoolImpl) cc.getDefaultPool();
  }

  public static void createPool(PoolAttributes poolAttr) {
    PoolFactoryImpl pf = (PoolFactoryImpl) PoolManager.createFactory();
    pf.init(poolAttr);
    pool = (PoolImpl) pf.create("CacheServerTestUtil");
  }

  public static void createCacheClient(Pool poolAttr, String regionName, Properties dsProperties,
      Boolean addControlListener) throws Exception {
    createCacheClient(poolAttr, regionName, dsProperties, addControlListener, null);
  }

  public static void createCacheClient(Pool poolAttr, String regionName, Properties dsProperties,
      Boolean addControlListener, Properties javaSystemProperties) {
    new CacheServerTestUtil().createCache(dsProperties);
    IgnoredException.addIgnoredException("java.net.ConnectException||java.net.SocketException");

    if (javaSystemProperties != null && javaSystemProperties.size() > 0) {
      Enumeration<?> e = javaSystemProperties.propertyNames();

      while (e.hasMoreElements()) {
        String key = (String) e.nextElement();
        System.setProperty(key, javaSystemProperties.getProperty(key));
      }
    }

    PoolFactoryImpl pf = (PoolFactoryImpl) PoolManager.createFactory();
    pf.init(poolAttr);
    PoolImpl p = (PoolImpl) pf.create("CacheServerTestUtil");
    AttributesFactory<Object, Object> factory = new AttributesFactory<>();
    factory.setScope(Scope.LOCAL);
    factory.setPoolName(p.getName());
    if (addControlListener) {
      factory.addCacheListener(new ControlListener());
    }
    RegionAttributes<Object, Object> attrs = factory.create();
    cache.createRegion(regionName, attrs);
    pool = p;
  }

  public static void unsetJavaSystemProperties(Properties javaSystemProperties) {
    if (javaSystemProperties != null && javaSystemProperties.size() > 0) {
      Enumeration<?> e = javaSystemProperties.propertyNames();

      while (e.hasMoreElements()) {
        String key = (String) e.nextElement();
        System.clearProperty(key);
      }
    }
  }

  public static void createCacheClient(Pool poolAttr, String regionName1, String regionName2) {
    new CacheServerTestUtil().createCache(getClientProperties());
    PoolFactoryImpl pf = (PoolFactoryImpl) PoolManager.createFactory();
    pf.init(poolAttr);
    PoolImpl p = (PoolImpl) pf.create("CacheServerTestUtil");
    AttributesFactory<Object, Object> factory = new AttributesFactory<>();
    factory.setScope(Scope.LOCAL);
    factory.setPoolName(p.getName());
    RegionAttributes<Object, Object> attrs = factory.create();
    cache.createRegion(regionName1, attrs);
    cache.createRegion(regionName2, attrs);
    pool = p;
  }

  public static void createCacheClientFromXmlN(URL url, String poolName, String durableClientId,
      int timeout, Boolean addControlListener) {
    ClientCacheFactory ccf = new ClientCacheFactory();
    try {
      File cacheXmlFile = new File(url.toURI().getPath());
      ccf.set(CACHE_XML_FILE, cacheXmlFile.toURI().getPath());

    } catch (URISyntaxException e) {
      throw new ExceptionInInitializerError(e);
    }
    ccf.set(MCAST_PORT, "0");
    ccf.set(DURABLE_CLIENT_ID, durableClientId);
    ccf.set(DURABLE_CLIENT_TIMEOUT, String.valueOf(timeout));
    ccf.set(LOG_FILE, "abs_client_system.log");
    ccf.set(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
    cache = (Cache) ccf.create();
    expected = IgnoredException
        .addIgnoredException("java.net.ConnectionException||java.net.SocketException");
    pool = (PoolImpl) PoolManager.find(poolName);

  }

  public static void createCacheClientFromXml(URL url, String poolName, String durableClientId,
      int timeout, Boolean addControlListener) {
    ClientCacheFactory ccf = new ClientCacheFactory();
    try {
      File cacheXmlFile = new File(url.toURI().getPath());
      ccf.set(CACHE_XML_FILE, cacheXmlFile.toURI().getPath());

    } catch (URISyntaxException e) {
      throw new ExceptionInInitializerError(e);
    }
    ccf.set(MCAST_PORT, "0");
    ccf.set(DURABLE_CLIENT_ID, durableClientId);
    ccf.set(DURABLE_CLIENT_TIMEOUT, String.valueOf(timeout));
    cache = (Cache) ccf.create();
    expected = IgnoredException
        .addIgnoredException("java.net.ConnectionException||java.net.SocketException");
    pool = (PoolImpl) PoolManager.find(poolName);

  }

  public static Integer createCacheServerFromXmlN(URL url) {
    CacheFactory ccf = new CacheFactory();
    try {
      File cacheXmlFile = new File(url.toURI().getPath());
      ccf.set(CACHE_XML_FILE, cacheXmlFile.toURI().getPath());
      ccf.set(MCAST_PORT, "0");
      ccf.set(LOCATORS, "localhost[" + DistributedTestUtils.getDUnitLocatorPort() + "]");
      ccf.set(LOG_FILE, "abs_server_system.log");
      ccf.set(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
    } catch (URISyntaxException e) {
      throw new ExceptionInInitializerError(e);
    }
    cache = ccf.create();
    return cache.getCacheServers().get(0).getPort();
  }

  public static Integer createCacheServerFromXml(URL url) {
    CacheFactory ccf = new CacheFactory();
    try {
      File cacheXmlFile = new File(url.toURI().getPath());
      ccf.set(CACHE_XML_FILE, cacheXmlFile.toURI().getPath());
      ccf.set(MCAST_PORT, "0");
      ccf.set(LOCATORS, "localhost[" + DistributedTestUtils.getDUnitLocatorPort() + "]");
    } catch (URISyntaxException e) {
      throw new ExceptionInInitializerError(e);
    }
    cache = ccf.create();
    return cache.getCacheServers().get(0).getPort();
  }

  /**
   * Create client regions
   */
  public static void createCacheClients(Pool poolAttr, String regionName1, String regionName2,
      Properties dsProperties) {
    new CacheServerTestUtil().createCache(dsProperties);

    // Initialize region1
    PoolFactoryImpl pf = (PoolFactoryImpl) PoolManager.createFactory();
    pf.init(poolAttr);
    Pool p = pf.create("CacheServerTestUtil1");
    AttributesFactory<Object, Object> factory1 = new AttributesFactory<>();
    factory1.setScope(Scope.LOCAL);
    factory1.setPoolName(p.getName());
    cache.createRegion(regionName1, factory1.create());

    // Initialize region2
    p = pf.create("CacheServerTestUtil2");
    AttributesFactory<Object, Object> factory2 = new AttributesFactory<>();
    factory2.setScope(Scope.LOCAL);
    factory2.setPoolName(p.getName());
    cache.createRegion(regionName2, factory2.create());
  }

  private static Properties getClientProperties() {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    return props;
  }

  private static Properties getClientProperties(boolean durable) {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    return props;
  }

  public static Integer createCacheServer(String regionName, Boolean notifyBySubscription)
      throws Exception {
    int port = getRandomAvailableTCPPort();
    createCacheServer(regionName, notifyBySubscription, port);

    return port;
  }

  public static Integer[] createCacheServerReturnPorts(String regionName,
      Boolean notifyBySubscription) throws Exception {
    int port = createCacheServer(regionName, notifyBySubscription);
    return new Integer[] {port, 0};
  }

  public static void createCacheServer(String regionName, Boolean notifyBySubscription,
      Integer serverPort) throws Exception {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + DistributedTestUtils.getDUnitLocatorPort() + "]");
    new CacheServerTestUtil().createCache(props);
    AttributesFactory<Object, Object> factory = new AttributesFactory<>();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setEnableBridgeConflation(true);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes<Object, Object> attrs = factory.create();
    cache.createRegion(regionName, attrs);
    CacheServer server = cache.addCacheServer();
    server.setPort(serverPort);
    server.setNotifyBySubscription(notifyBySubscription);
    server.start();
  }

  public static Integer createCacheServer(String regionName1, String regionName2,
      Boolean notifyBySubscription) throws Exception {
    new CacheServerTestUtil().createCache(new Properties());
    AttributesFactory<Object, Object> factory = new AttributesFactory<>();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setEnableBridgeConflation(true);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes<Object, Object> attrs = factory.create();
    if (!regionName1.equals("")) {
      cache.createRegion(regionName1, attrs);
    }
    if (!regionName2.equals("")) {
      cache.createRegion(regionName2, attrs);
    }
    CacheServer server1 = cache.addCacheServer();
    int port = getRandomAvailableTCPPort();
    server1.setPort(port);
    server1.setNotifyBySubscription(notifyBySubscription);
    server1.start();
    return server1.getPort();
  }

  private void createCache(Properties props) {
    DistributedSystem ds = getSystem(props);
    assertThat(ds).isNotNull();
    ds.disconnect();
    ds = getSystem(props);
    cache = CacheFactory.create(ds);
    assertThat(cache).isNotNull();
  }

  private void createClientCache(Properties props, ClientCacheFactory ccf) {
    DistributedSystem ds = getSystem(props);
    assertThat(ds).isNotNull();
    ds.disconnect();
    ClientCache cc = ccf.create();
    setSystem(props, cc.getDistributedSystem());
    cache = (Cache) cc;
    assertThat(cache).isNotNull();
    expected = IgnoredException
        .addIgnoredException("java.net.ConnectionException||java.net.SocketException");
  }

  public static void closeCache() {
    if (expected != null) {
      expected.remove();
      expected = null;
    }
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  public static void closeCache(boolean keepalive) {
    if (expected != null) {
      expected.remove();
      expected = null;
    }
    if (cache != null && !cache.isClosed()) {
      cache.close(keepalive);
      cache.getDistributedSystem().disconnect();
    }
  }

  public static void clearCacheReference() {
    cache = null;
  }

  public static void setClientCrash(boolean crashOnClose) {
    org.apache.geode.cache.client.internal.ConnectionImpl
        .setTEST_DURABLE_CLIENT_CRASH(crashOnClose);
  }

  public static Cache getCache() {
    return cache;
  }

  public static ClientCache getClientCache() {
    return (ClientCache) cache;
  }

  public static PoolImpl getPool() {
    return pool;
  }

  /**
   * Disables the shuffling of endpoints for a client
   */
  public static void disableShufflingOfEndpoints() {
    // TODO DISABLE_RANDOM doesn't seem to be used anywhere
    System.setProperty(GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.DISABLE_RANDOM", "true");
    System.setProperty(GeodeGlossary.GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints",
        "true");
  }

  /**
   * Enables the shuffling of endpoints for a client
   *
   * @since GemFire 5.7
   */
  public static void enableShufflingOfEndpoints() {
    // TODO DISABLE_RANDOM doesn't seem to be used anywhere
    System.setProperty(GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.DISABLE_RANDOM", "false");
    System.setProperty(GeodeGlossary.GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints",
        "false");
  }

  /**
   * Resets the 'disableShufflingOfEndpoints' flag
   */
  public static void resetDisableShufflingOfEndpointsFlag() {
    System.setProperty(GeodeGlossary.GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints",
        "false");
  }

  public static class EventWrapper {
    public final EntryEvent event;
    public final Object key;
    public final Object val;
    public final Object arg;
    public final int type;

    public EventWrapper(EntryEvent ee, int type) {
      event = ee;
      key = ee.getKey();
      val = ee.getNewValue();
      arg = ee.getCallbackArgument();
      this.type = type;
    }

    public boolean isCreate() {
      return type == TYPE_CREATE;
    }

    public String toString() {
      return "EventWrapper: event=" + event + ", type=" + type;
    }
  }

  public static class ControlListener extends CacheListenerAdapter implements Declarable {
    public final LinkedList<EventWrapper> events = new LinkedList<>();
    public final LinkedList<EntryEvent> createEvents = new LinkedList<>();
    public final LinkedList<EntryEvent> updateEvents = new LinkedList<>();
    public final LinkedList<EntryEvent> destroyEvents = new LinkedList<>();
    public final Object CONTROL_LOCK = new Object();

    // added to test creation of cache from xml
    @Override
    public void init(Properties props) {}

    public boolean waitWhileNotEnoughEvents(long sleepMs, int eventCount) {
      return waitWhileNotEnoughEvents(sleepMs, eventCount, -1);
    }

    public boolean waitWhileNotEnoughEvents(long sleepMs, int eventCount, int eventType) {
      return waitWhileNotEnoughEvents(sleepMs, eventCount, getEvents(eventType));
    }


    /**
     * This method is not implemented to test event count matches the eventsToCheck.size() which is
     * confusing. It will wait and return if it got something in the eventsToCheck or not.
     */
    public boolean waitWhileNotEnoughEvents(long sleepMs, int eventCount, List eventsToCheck) {
      long maxMillis = System.currentTimeMillis() + sleepMs;
      synchronized (CONTROL_LOCK) {
        try {
          while (eventsToCheck.size() < eventCount) {
            long waitMillis = maxMillis - System.currentTimeMillis();
            if (waitMillis < 10) {
              break;
            }
            CONTROL_LOCK.wait(waitMillis);
          } // while
        } catch (InterruptedException abort) {
          fail("interrupted");
        }
        return !eventsToCheck.isEmpty();
      } // synchronized
    }

    public List getEvents(int eventType) {
      List eventsToCheck;
      switch (eventType) {
        case TYPE_CREATE:
          eventsToCheck = createEvents;
          break;
        case TYPE_UPDATE:
          eventsToCheck = updateEvents;
          break;
        case TYPE_DESTROY:
          eventsToCheck = destroyEvents;
          break;
        default:
          eventsToCheck = events;
      }
      return eventsToCheck;
    }

    @Override
    public void afterCreate(EntryEvent e) {
      synchronized (CONTROL_LOCK) {
        events.add(new EventWrapper(e, TYPE_CREATE));
        createEvents.add(e);
        CONTROL_LOCK.notifyAll();
      }
    }

    @Override
    public void afterUpdate(EntryEvent e) {
      synchronized (CONTROL_LOCK) {
        events.add(new EventWrapper(e, TYPE_UPDATE));
        updateEvents.add(e);
        CONTROL_LOCK.notifyAll();
      }
    }

    @Override
    public void afterInvalidate(EntryEvent e) {
      synchronized (CONTROL_LOCK) {
        events.add(new EventWrapper(e, TYPE_INVALIDATE));
        CONTROL_LOCK.notifyAll();
      }
    }

    @Override
    public void afterDestroy(EntryEvent e) {
      synchronized (CONTROL_LOCK) {
        events.add(new EventWrapper(e, TYPE_DESTROY));
        destroyEvents.add(e);
        CONTROL_LOCK.notifyAll();
      }
    }
  }

  public static class ControlCqListener extends CqListenerAdapter {
    public final LinkedList events = new LinkedList();
    public final Object CONTROL_LOCK = new Object();

    public boolean waitWhileNotEnoughEvents(long sleepMs, int eventCount) {
      long maxMillis = System.currentTimeMillis() + sleepMs;
      synchronized (CONTROL_LOCK) {
        try {
          while (events.size() < eventCount) {
            long waitMillis = maxMillis - System.currentTimeMillis();
            if (waitMillis < 10) {
              break;
            }
            CONTROL_LOCK.wait(waitMillis);
          } // while
        } catch (InterruptedException abort) {
          fail("interrupted");
        }
        return !events.isEmpty();
      } // synchronized
    }

    @Override
    public void onEvent(CqEvent aCqEvent) {
      synchronized (CONTROL_LOCK) {
        events.add(aCqEvent);
        CONTROL_LOCK.notifyAll();
      }
    }
  }
}
