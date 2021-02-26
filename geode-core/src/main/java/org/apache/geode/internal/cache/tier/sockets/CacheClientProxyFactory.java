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

import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;

import java.lang.reflect.InvocationTargetException;
import java.net.Socket;

import org.apache.shiro.subject.Subject;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.CacheException;
import org.apache.geode.internal.classloader.ClassPathLoader;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.statistics.StatisticsClock;

/**
 * Creates instances of CacheClientProxy.
 *
 * <p>
 * CacheClientProxyFactory delegates to InternalCacheClientProxyFactory which can be specified by
 * System Property {@code gemfire.CacheClientProxyFactory.INTERNAL_FACTORY}. This allows tests to
 * customize CacheClientProxy and/or MessageDispatcher using Property-Injection. See
 * DeltaPropagationDUnitTest for an example.
 */
public class CacheClientProxyFactory {

  @Immutable
  private static final InternalCacheClientProxyFactory DEFAULT = CacheClientProxy::new;

  @Immutable
  @VisibleForTesting
  public static final String INTERNAL_FACTORY_PROPERTY =
      "gemfire.CacheClientProxyFactory.INTERNAL_FACTORY";

  private static InternalCacheClientProxyFactory factory() {
    String proxyClassName = System.getProperty(INTERNAL_FACTORY_PROPERTY);
    if (proxyClassName == null || proxyClassName.isEmpty()) {
      return DEFAULT;
    }
    try {
      Class<InternalCacheClientProxyFactory> proxyClass =
          uncheckedCast(ClassPathLoader.getLatest().forName(proxyClassName));
      return proxyClass.getConstructor().newInstance();
    } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException
        | IllegalAccessException | InvocationTargetException e) {
      return DEFAULT;
    }
  }

  private final InternalCacheClientProxyFactory internalFactory;

  CacheClientProxyFactory() {
    this(factory());
  }

  private CacheClientProxyFactory(InternalCacheClientProxyFactory internalFactory) {
    this.internalFactory = internalFactory;
  }

  public CacheClientProxy create(CacheClientNotifier notifier, Socket socket,
      ClientProxyMembershipID proxyId, boolean isPrimary, byte clientConflation,
      KnownVersion clientVersion, long acceptorId, boolean notifyBySubscription,
      SecurityService securityService, Subject subject, StatisticsClock statisticsClock)
      throws CacheException {
    return internalFactory.create(notifier, socket, proxyId, isPrimary, clientConflation,
        clientVersion, acceptorId, notifyBySubscription, securityService, subject, statisticsClock);
  }

  @FunctionalInterface
  @VisibleForTesting
  public interface InternalCacheClientProxyFactory {
    CacheClientProxy create(CacheClientNotifier notifier, Socket socket,
        ClientProxyMembershipID proxyId, boolean isPrimary, byte clientConflation,
        KnownVersion clientVersion, long acceptorId, boolean notifyBySubscription,
        SecurityService securityService, Subject subject, StatisticsClock statisticsClock)
        throws CacheException;
  }
}
