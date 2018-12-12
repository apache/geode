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
package org.apache.geode.internal.cache.xmlcache;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.DynamicRegionFactory;
import org.apache.geode.cache.GatewayException;
import org.apache.geode.cache.InterestPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.RegionService;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.SubscriptionAttributes;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InitialImageOperation;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.PoolFactoryImpl;

/**
 * Represents a {@link ClientCache} that is created declaratively. Notice that it implements the
 * {@link ClientCache} interface so that this class must be updated when {@link ClientCache} is
 * modified. This class is public for testing purposes.
 *
 * @since GemFire 6.5
 */
@SuppressWarnings("deprecation")
public class ClientCacheCreation extends CacheCreation implements ClientCache {

  /**
   * Creates a new {@code ClientCacheCreation} with no root regions
   */
  public ClientCacheCreation() {
    this(false);
  }

  /**
   * @param forParsing if true then this creation is used for parsing xml; if false then it is used
   *        for generating xml.
   * @since GemFire 5.7
   */
  ClientCacheCreation(boolean forParsing) {
    super(forParsing);
  }

  private static final RegionAttributes clientDefaults = createClientDefaults();

  private static RegionAttributes createClientDefaults() {
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);
    // af.setIgnoreJTA(true); In 6.6 and later releases client regions support JTA
    af.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
    return af.create();
  }

  @Override
  RegionAttributes getDefaultAttributes() {
    return clientDefaults;
  }

  @Override
  protected void initializeRegionShortcuts() {
    GemFireCacheImpl.initializeClientRegionShortcuts(this);
  }

  @Override
  public QueryService getQueryService(String poolName) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public QueryService getLocalQueryService() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void determineDefaultPool() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  /**
   * @since GemFire 6.5
   */
  @Override
  public <K, V> ClientRegionFactory<K, V> createClientRegionFactory(ClientRegionShortcut atts) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  /**
   * @since GemFire 6.5
   */
  @Override
  public <K, V> ClientRegionFactory<K, V> createClientRegionFactory(String regionAttributesId) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public RegionService createAuthenticatedView(Properties userSecurityProperties, String poolName) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public RegionService createAuthenticatedView(Properties userSecurityProperties) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void setLockTimeout(int seconds) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void setLockLease(int seconds) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void setSearchTimeout(int seconds) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void setMessageSyncInterval(int seconds) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public CacheServer addCacheServer() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void setIsServer(boolean isServer) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public void addBackup(File backup) {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  /**
   * Fills in the contents of a {@link Cache} based on this creation object's state.
   */
  @Override
  void create(InternalCache cache)
      throws TimeoutException, CacheWriterException, GatewayException, RegionExistsException {
    cache.setDeclarativeCacheConfig(this.getCacheConfig());
    if (!cache.isClient()) {
      throw new IllegalStateException(
          "You must use ClientCacheFactory when the cache.xml uses client-cache.");
    }

    initializeDeclarablesMap(cache);

    if (hasFunctionService()) {
      getFunctionServiceCreation().create();
    }

    // create connection pools
    Map<String, Pool> pools = getPools();
    if (!pools.isEmpty()) {
      for (final Pool cp : pools.values()) {
        PoolFactoryImpl poolFactory = (PoolFactoryImpl) PoolManager.createFactory();
        poolFactory.init(cp);
        poolFactory.create(cp.getName());
      }
    }

    if (hasResourceManager()) {
      // moved this up to fix bug 42128
      getResourceManager().configure(cache.getResourceManager());
    }

    DiskStoreAttributesCreation pdxRegDSC = initializePdxDiskStore(cache);

    cache.initializePdxRegistry();

    for (DiskStore diskStore : listDiskStores()) {
      DiskStoreAttributesCreation creation = (DiskStoreAttributesCreation) diskStore;
      if (creation != pdxRegDSC) {
        createDiskStore(creation, cache);
      }
    }
    for (DiskStore diskStore : listDiskStores()) {
      DiskStoreAttributesCreation creation = (DiskStoreAttributesCreation) diskStore;

      // Don't let the DiskStoreAttributesCreation escape to the user
      DiskStoreFactory factory = cache.createDiskStoreFactory(creation);
      factory.create(creation.getName());
    }

    if (hasDynamicRegionFactory()) {
      DynamicRegionFactory.get().open(getDynamicRegionFactoryConfig());
    }
    if (hasCopyOnRead()) {
      cache.setCopyOnRead(getCopyOnRead());
    }

    if (this.txMgrCreation != null && this.txMgrCreation.getListeners().length > 0
        && cache.getCacheTransactionManager() != null) {
      cache.getCacheTransactionManager().initListeners(this.txMgrCreation.getListeners());
    }

    if (this.txMgrCreation != null && cache.getCacheTransactionManager() != null
        && this.txMgrCreation.getWriter() != null) {
      throw new IllegalStateException(
          "A TransactionWriter cannot be registered on a client");
    }

    cache.initializePdxRegistry();

    for (String id : this.regionAttributesNames) {
      RegionAttributesCreation creation = (RegionAttributesCreation) getRegionAttributes(id);
      creation.inheritAttributes(cache, false);

      // Don't let the RegionAttributesCreation escape to the user
      AttributesFactory factory = new AttributesFactory(creation);
      RegionAttributes attrs = factory.createRegionAttributes();

      cache.setRegionAttributes(id, attrs);
    }

    for (final Region<?, ?> region : this.roots.values()) {
      RegionCreation regionCreation = (RegionCreation) region;
      regionCreation.createRoot(cache);
    }

    cache.readyDynamicRegionFactory();
    runInitializer(cache);
  }

  public String getDefaultPoolName() {
    String result = null;
    Map<String, Pool> pools = getPools();
    if (pools.size() == 1) {
      Pool pool = pools.values().iterator().next();
      result = pool.getName();
    } else if (pools.isEmpty()) {
      result = "DEFAULT";
    }
    return result;
  }

  @Override
  public Pool getDefaultPool() {
    return getPools().get(getDefaultPoolName());
  }

  @Override
  public boolean getPdxReadSerialized() {
    return false;
  }

  @Override
  public Set<InetSocketAddress> getCurrentServers() {
    return Collections.emptySet();
  }

  @Override
  public void invokeRegionEntrySynchronizationListenersAfterSynchronization(
      InternalDistributedMember sender, InternalRegion region,
      List<InitialImageOperation.Entry> entriesToSynchronize) {
    throw new UnsupportedOperationException("Should not be invoked");
  }
}
