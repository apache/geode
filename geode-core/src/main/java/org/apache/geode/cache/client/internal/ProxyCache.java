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
package org.apache.geode.cache.client.internal;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionService;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.internal.ProxyQueryService;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.pdx.JSONFormatter;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;
import org.apache.geode.pdx.internal.PdxInstanceFactoryImpl;

/**
 * A wrapper class over an actual Cache instance. This is used when the multiuser-authentication
 * attribute is set to true. Application must use its {@link #getRegion(String)} API instead that of
 * actual Cache instance for getting a reference to Region instances, to perform operations on
 * server.
 *
 * TODO Avoid creating multiple instances of ProxyCache for a single user.
 *
 * @see ClientCache#createAuthenticatedView(Properties)
 * @see ProxyQueryService
 * @see ProxyRegion
 * @since GemFire 6.5
 */
public class ProxyCache implements RegionService {

  /**
   * package-private to avoid synthetic accessor
   * <p>
   * TODO: if this is only in inside client then this should be InternalClientCache
   */
  final InternalCache cache;

  private UserAttributes userAttributes;
  private ProxyQueryService proxyQueryService;
  private boolean isClosed = false;
  private final Stopper stopper = new Stopper();

  public ProxyCache(Properties properties, InternalCache cache, PoolImpl pool) {
    userAttributes = new UserAttributes(properties, pool);
    this.cache = cache;
  }

  @Override
  public void close() {
    close(false);
  }

  public void close(boolean keepAlive) {
    if (isClosed) {
      return;
    }
    // It should go to all the servers it has authenticated itself on and ask
    // them to clean-up its entry from their auth-data structures.
    try {
      if (proxyQueryService != null) {
        proxyQueryService.closeCqs(keepAlive);
      }
      UserAttributes.userAttributes.set(userAttributes);
      for (final ServerLocation serverLocation : userAttributes.getServerToId().keySet()) {
        ProxyCacheCloseOp.executeOn(serverLocation, (ExecutablePool) userAttributes.getPool(),
            userAttributes.getCredentials(), keepAlive);
      }
      List<ProxyCache> proxyCache = ((PoolImpl) userAttributes.getPool()).getProxyCacheList();
      synchronized (proxyCache) {
        proxyCache.remove(this);
      }
    } finally {
      // TODO: I think some NPE will be caused by this code.
      // It would be safer to not null things out.
      // It is really bad that we null out and then set isClosed true.
      isClosed = true;
      proxyQueryService = null;
      userAttributes.setCredentials(null);
      userAttributes = null;
      UserAttributes.userAttributes.set(null);
    }
  }

  @Override
  public QueryService getQueryService() {
    preOp();
    if (proxyQueryService == null) {
      proxyQueryService =
          new ProxyQueryService(this, userAttributes.getPool().getQueryService());
    }
    return proxyQueryService;
  }

  @Override
  public JSONFormatter getJsonFormatter() {
    return new JSONFormatter(this);
  }

  @Override
  public <K, V> Region<K, V> getRegion(String path) {
    preOp();
    if (cache.getRegion(path) == null) {
      return null;
    } else {
      if (!cache.getRegion(path).getAttributes().getDataPolicy().isEmpty()) {
        throw new IllegalStateException(
            "Region's data-policy must be EMPTY when multiuser-authentication is true");
      }
      return new ProxyRegion(this, cache.getRegion(path), cache.getStatisticsClock());
    }
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }

  public void setProperties(Properties properties) {
    preOp();
    userAttributes.setCredentials(properties);
  }

  public Properties getProperties() {
    preOp();
    return userAttributes.getCredentials();
  }

  public void setUserAttributes(UserAttributes userAttributes) {
    preOp();
    this.userAttributes = userAttributes;
  }

  public UserAttributes getUserAttributes() {
    preOp();
    return userAttributes;
  }

  private void preOp() {
    stopper.checkCancelInProgress(null);
  }

  protected class Stopper extends CancelCriterion {
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
        // Caller did not specify any root cause, so just use our own.
        return cache.getCacheClosedException(reason);
      }

      try {
        return cache.getCacheClosedException(reason, e);
      } catch (IllegalStateException ignore) {
        // Bug 39496 (Jrockit related) Give up. The following
        // error is not entirely sane but gives the correct general picture.
        return new CacheClosedException(reason);
      }
    }
  }

  @Override
  public CancelCriterion getCancelCriterion() {
    return stopper;
  }

  @Override
  public Set<Region<?, ?>> rootRegions() {
    preOp();
    Set<Region<?, ?>> rootRegions = new HashSet<>();
    for (Region<?, ?> region : cache.rootRegions()) {
      if (!region.getAttributes().getDataPolicy().withStorage()) {
        rootRegions.add(new ProxyRegion(this, region, cache.getStatisticsClock()));
      }
    }
    return Collections.unmodifiableSet(rootRegions);
  }

  @Override
  public PdxInstanceFactory createPdxInstanceFactory(String className) {
    return PdxInstanceFactoryImpl.newCreator(className, true, cache);
  }

  public PdxInstanceFactory createPdxInstanceFactory(String className, boolean expectDomainClass) {
    return PdxInstanceFactoryImpl.newCreator(className, expectDomainClass, cache);
  }

  @Override
  public PdxInstance createPdxEnum(String className, String enumName, int enumOrdinal) {
    return PdxInstanceFactoryImpl.createPdxEnum(className, enumName, enumOrdinal, cache);
  }

  /** return a CacheClosedException with the given reason */
  public CacheClosedException getCacheClosedException(String reason) {
    return cache.getCacheClosedException(reason);
  }
}
