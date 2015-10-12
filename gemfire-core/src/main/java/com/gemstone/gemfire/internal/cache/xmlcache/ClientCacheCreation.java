/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.xmlcache;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.DynamicRegionFactory;
import com.gemstone.gemfire.cache.GatewayException;
import com.gemstone.gemfire.cache.InterestPolicy;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache.RegionService;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.SubscriptionAttributes;
import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientRegionFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PoolFactoryImpl;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.pdx.internal.TypeRegistry;

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
 * @author darrel
 *
 * @since 6.5
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
   * @since 5.7
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

  public com.gemstone.gemfire.cache.query.QueryService getQueryService(String poolName) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }
  public com.gemstone.gemfire.cache.query.QueryService getLocalQueryService() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }
  /**
   * @since 6.5
   */
  public <K,V> ClientRegionFactory<K,V> createClientRegionFactory(ClientRegionShortcut atts) {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  /**
   * @since 6.5
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
   * @see com.gemstone.gemfire.cache.client.CacheCreation#getPdxReadSerialized()
   */
  @Override
  public boolean getPdxReadSerialized() {
      return false;
  }


  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.client.ClientCache#getCurrentServers()
   */
  public Set<InetSocketAddress> getCurrentServers() {
    return Collections.EMPTY_SET;
  }


}
