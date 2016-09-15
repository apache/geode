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
package org.apache.geode.internal.cache.xmlcache;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.DynamicRegionFactory;
import org.apache.geode.cache.GatewayException;
import org.apache.geode.cache.InterestPolicy;
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
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.PoolFactoryImpl;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.pdx.internal.TypeRegistry;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Represents a {@link ClientCache} that is created declaratively.  Notice
 * that it implements the {@link ClientCache} interface so that this class
 * must be updated when {@link ClientCache} is modified.  This class is
 * public for testing purposes.
 *
 *
 * @since GemFire 6.5
 */
@SuppressWarnings("deprecation")
public class ClientCacheCreation extends CacheCreation implements ClientCache {

  ////////////////////////  Constructors  ////////////////////////

  /**
   * Creates a new <code>ClientCacheCreation</code> with no root regions
   */
  public ClientCacheCreation() {
    this(false);
  }

  /**
   * @param forParsing if true then this creation is used for parsing xml;
   *   if false then it is used for generating xml.
   * @since GemFire 5.7
   */
  public ClientCacheCreation(boolean forParsing) {
    super(forParsing);
  }
  
  //////////////////////  Instance Methods  //////////////////////

  static final private RegionAttributes clientDefaults;
  static {
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);
//    af.setIgnoreJTA(true);  In 6.6 and later releases client regions support JTA
    af.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
    clientDefaults = af.create();
  }

  @Override
  RegionAttributes getDefaultAttributes() {
    return clientDefaults;
  }

  @Override
  protected void initializeRegionShortcuts() {
    GemFireCacheImpl.initializeClientRegionShortcuts(this);
  }

  public org.apache.geode.cache.query.QueryService getQueryService(String poolName) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }
  public org.apache.geode.cache.query.QueryService getLocalQueryService() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }
  /**
   * @since GemFire 6.5
   */
  public <K,V> ClientRegionFactory<K,V> createClientRegionFactory(ClientRegionShortcut atts) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  /**
   * @since GemFire 6.5
   */
  public <K,V> ClientRegionFactory<K,V> createClientRegionFactory(String regionAttributesId) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }
  
  public RegionService createAuthenticatedView(Properties properties,
      String poolName) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  public RegionService createAuthenticatedView(
      Properties userSecurityProperties) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public void setLockTimeout(int seconds) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }
  
  @Override
  public void setLockLease(int seconds) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public void setSearchTimeout(int seconds) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public void setMessageSyncInterval(int seconds) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public CacheServer addCacheServer() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }
  @Override
  public void setIsServer(boolean isServer) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }
  @Override
  public void addBackup(File backup) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  /**
   * Fills in the contents of a {@link Cache} based on this creation
   * object's state.
   *
   * @throws TimeoutException
   * @throws CacheWriterException
   * @throws RegionExistsException
   * @throws GatewayException
   */
  @Override
  void create(GemFireCacheImpl cache)
    throws TimeoutException, CacheWriterException,
           GatewayException,
           RegionExistsException {
    cache.setDeclarativeCacheConfig(this.getCacheConfig());
    if (!cache.isClient()) {
      throw new IllegalStateException("You must use ClientCacheFactory when the cache.xml uses client-cache.");
    }
    { // create connection pools
      Map m = getPools();
      if (!m.isEmpty()) {
        boolean setDefault = m.size() == 1;
        Iterator it = m.values().iterator();
        while (it.hasNext()) {
          Pool cp = (Pool)it.next();
          PoolFactoryImpl f;
          f = (PoolFactoryImpl)PoolManager.createFactory();
          f.init(cp);
          PoolImpl p = (PoolImpl)f.create(cp.getName());
        }
      }
    }

    cache.determineDefaultPool();

    if (hasResourceManager()) {
      // moved this up to fix bug 42128
      getResourceManager().configure(cache.getResourceManager());
    }

    DiskStoreAttributesCreation pdxRegDSC = initializePdxDiskStore(cache);

    cache.initializePdxRegistry();

    for (Iterator iter = listDiskStores().iterator(); iter.hasNext();) {
      DiskStoreAttributesCreation creation = (DiskStoreAttributesCreation) iter.next();
      if (creation != pdxRegDSC) {
        createDiskStore(creation, cache);
      }
    }
    for (Iterator iter = listDiskStores().iterator(); iter.hasNext();) {
      DiskStoreAttributesCreation creation = (DiskStoreAttributesCreation) iter.next();

      // It's GemFireCache
      GemFireCacheImpl gfc = (GemFireCacheImpl)cache;
      // Don't let the DiskStoreAttributesCreation escape to the user
      DiskStoreFactory factory = gfc.createDiskStoreFactory(creation);
      DiskStore ds = factory.create(creation.getName());
    }
    
    if (hasDynamicRegionFactory()) {
      DynamicRegionFactory.get().open(getDynamicRegionFactoryConfig());
    }
    if (hasCopyOnRead()) {
      cache.setCopyOnRead(getCopyOnRead());
    }

    if (this.txMgrCreation != null &&
        this.txMgrCreation.getListeners().length > 0 &&
        cache.getCacheTransactionManager()!=null) {
      cache.getCacheTransactionManager().initListeners(this.txMgrCreation.getListeners());
    }
    
    if (this.txMgrCreation != null &&
        cache.getCacheTransactionManager()!=null && this.txMgrCreation.getWriter() != null) {
      throw new IllegalStateException(LocalizedStrings.TXManager_NO_WRITER_ON_CLIENT.toLocalizedString());
    }
    
    cache.initializePdxRegistry();

    for (Iterator iter = this.regionAttributesNames.iterator();
         iter.hasNext(); ) {
      String id = (String) iter.next();
      RegionAttributesCreation creation =
        (RegionAttributesCreation) getRegionAttributes(id);
      creation.inheritAttributes(cache, false);

      RegionAttributes attrs;
      // Don't let the RegionAttributesCreation escape to the user
      AttributesFactory factory = new AttributesFactory(creation);
      attrs = factory.createRegionAttributes();

      cache.setRegionAttributes(id, attrs);
    }

    Iterator it = this.roots.values().iterator();
    while (it.hasNext()) {
      RegionCreation r = (RegionCreation)it.next();
      r.createRoot(cache);
    }

    cache.readyDynamicRegionFactory();
    runInitializer();
  }

  public String getDefaultPoolName() {
    String result = null;
    Map m = getPools();
    if (m.size() == 1) {
      Pool p = (Pool)m.values().iterator().next();
      result = p.getName();
    } else if (m.isEmpty()) {
      result = "DEFAULT";
    }
    return result;
  }
  
  public Pool getDefaultPool() {
    return (Pool) getPools().get(getDefaultPoolName());
  }
  
  /* (non-Javadoc)
   * @see org.apache.geode.cache.client.CacheCreation#getPdxReadSerialized()
   */
  @Override
  public boolean getPdxReadSerialized() {
      return false;
  }


  /* (non-Javadoc)
   * @see org.apache.geode.cache.client.ClientCache#getCurrentServers()
   */
  public Set<InetSocketAddress> getCurrentServers() {
    return Collections.EMPTY_SET;
  }


}
