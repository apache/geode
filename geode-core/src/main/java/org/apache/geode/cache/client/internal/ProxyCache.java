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
package org.apache.geode.cache.client.internal;

import java.util.ArrayList;
import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionService;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.internal.ProxyQueryService;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;
import org.apache.geode.pdx.internal.PdxInstanceFactoryImpl;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

/**
 * A wrapper class over an actual Cache instance. This is used when the
 * multiuser-authentication attribute is set to true. Application must use
 * its {@link #getRegion(String)} API instead that of actual Cache instance for
 * getting a reference to Region instances, to perform operations on server.
 * 
 * TODO Avoid creating multiple instances of ProxyCache for a single user.
 * 
 * @see ClientCache#createAuthenticatedView(Properties)
 * @see ProxyQueryService
 * @see ProxyRegion
 * @since GemFire 6.5
 */
public class ProxyCache implements RegionService {
  
  private final GemFireCacheImpl cache;
  private UserAttributes userAttributes;
  private ProxyQueryService proxyQueryService;
  private boolean isClosed = false;
  private final Stopper stopper = new Stopper();

  public ProxyCache(Properties properties, GemFireCacheImpl cache, PoolImpl pool) {
    this.userAttributes = new UserAttributes(properties, pool);
    this.cache = cache;
  }

  public void close() {
    close(false);
  }
  
  public void close(boolean keepAlive) {
    if (this.isClosed) {
      return;
    }
    // It should go to all the servers it has authenticated itself on and ask
    // them to clean-up its entry from their auth-data structures.
    try {
      if (this.proxyQueryService != null) {
        this.proxyQueryService.closeCqs(keepAlive);
      }
      UserAttributes.userAttributes.set(this.userAttributes);
      Iterator<ServerLocation> iter = this.userAttributes.getServerToId()
          .keySet().iterator();
      while (iter.hasNext()) {
        ProxyCacheCloseOp.executeOn(iter.next(), (PoolImpl)this.userAttributes.getPool(),
            this.userAttributes.getCredentials(), keepAlive);
      }
      ArrayList<ProxyCache> proxyCache = ((PoolImpl)this.userAttributes.getPool()).getProxyCacheList();
      synchronized (proxyCache) {
        proxyCache.remove(this);
      }
    } finally {
      // @todo I think some NPE will be caused by this code.
      // It would be safer to not null things out.
      // It is really bad that we null out and then set isClosed true.
      this.isClosed = true;
      this.proxyQueryService = null;
      this.userAttributes.setCredentials(null);
      this.userAttributes = null;
      UserAttributes.userAttributes.set(null);
    }
  }

  // TODO remove this method
  public String getName() {
    return this.cache.getName();
  }

  public QueryService getQueryService() {
    preOp();
    if (this.proxyQueryService == null) {
      this.proxyQueryService = new ProxyQueryService(this, userAttributes
          .getPool().getQueryService());
    }
    return this.proxyQueryService;
  }

  public <K, V> Region<K, V> getRegion(String path) {
    preOp();
    // TODO Auto-generated method stub
    // ProxyRegion region = this.proxyRegionList.get(path);
    // if (region != null) {
    //   return region;
    // }
    // else {
    if (this.cache.getRegion(path) == null) {
      return null;
    } else {
      if (!this.cache.getRegion(path).getAttributes().getDataPolicy().isEmpty()) {
        throw new IllegalStateException(
            "Region's data-policy must be EMPTY when multiuser-authentication is true");
      }
      return new ProxyRegion(this, this.cache.getRegion(path));
    }
    // }
  }

  public boolean isClosed() {
    return this.isClosed;
  }

  public void setProperties(Properties properties) {
    preOp();
    this.userAttributes.setCredentials(properties);
  }

  public Properties getProperties() {
    preOp();
    return this.userAttributes.getCredentials();
  }

  public void setUserAttributes(UserAttributes userAttributes) {
    preOp();
    this.userAttributes = userAttributes;
  }

  public UserAttributes getUserAttributes() {
    preOp();
    return this.userAttributes;
  }

  public Object getUserId(Object key) {
    preOp();
    if (!(key instanceof ServerLocation)) {
      throw new IllegalArgumentException(
          "Key must be of type ServerLocation, but is " + key.getClass());
    }
    return this.userAttributes.getServerToId().get(key);
  }

  private void preOp() {
    this.stopper.checkCancelInProgress(null);
  }

  protected class Stopper extends CancelCriterion {
    /* (non-Javadoc)
     * @see org.apache.geode.CancelCriterion#cancelInProgress()
     */
    @Override
    public String cancelInProgress() {
      String reason = cache.getCancelCriterion().cancelInProgress();
      if (reason != null) {
        return reason;
      }
      if (isClosed()) {
        return "Authenticated cache view is closed for this user.";
      }
      return null;
    }

    /* (non-Javadoc)
     * @see org.apache.geode.CancelCriterion#generateCancelledException(java.lang.Throwable)
     */
    @Override
    public RuntimeException generateCancelledException(Throwable e) {
      String reason = cancelInProgress();
      if (reason == null) {
        return null;
      }
      RuntimeException result = cache.getCancelCriterion().generateCancelledException(e);
      if (result != null) {
        return result;
      }
      if (e == null) {
        // Caller did not specify  any root cause, so just use our own.
        return new CacheClosedException(reason);
      }

      try {
        return new CacheClosedException(reason, e);
      }
      catch (IllegalStateException e2) {
        // Bug 39496 (Jrockit related)  Give up.  The following
        // error is not entirely sane but gives the correct general picture.
        return new CacheClosedException(reason);
      }
    }
  }
  
  public CancelCriterion getCancelCriterion() {
    return this.stopper;
  }

  /* (non-Javadoc)
   * @see org.apache.geode.cache.RegionService#rootRegions()
   */
  public Set<Region<?, ?>> rootRegions() {
    preOp();
    Set<Region<?, ?>> rRegions = new HashSet<Region<?,?>>(); 
    Iterator<LocalRegion> it = this.cache.rootRegions().iterator();
    while (it.hasNext()) {
      LocalRegion lr = it.next();
      if (!lr.getAttributes().getDataPolicy().withStorage()) {
        rRegions.add(new ProxyRegion(this, lr));
      }
    }
    return Collections.unmodifiableSet(rRegions);
  }

  public PdxInstanceFactory createPdxInstanceFactory(String className) {
    return PdxInstanceFactoryImpl.newCreator(className, true);
  }
  public PdxInstanceFactory createPdxInstanceFactory(String className, boolean b) {
    return PdxInstanceFactoryImpl.newCreator(className, b);
  }
  public PdxInstance createPdxEnum(String className, String enumName, int enumOrdinal) {
    return PdxInstanceFactoryImpl.createPdxEnum(className, enumName, enumOrdinal, this.cache);
  }
}
