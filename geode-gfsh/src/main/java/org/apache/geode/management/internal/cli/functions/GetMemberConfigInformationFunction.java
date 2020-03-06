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
package org.apache.geode.management.internal.cli.functions;

import static org.apache.geode.distributed.ConfigurationProperties.SOCKET_BUFFER_SIZE;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.ConfigSource;
import org.apache.geode.internal.cache.CacheConfig;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.ha.HARegionQueue;
import org.apache.geode.internal.util.ArgumentRedactor;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.internal.cli.domain.MemberConfigurationInfo;
import org.apache.geode.management.internal.functions.CliFunctionResult;

public class GetMemberConfigInformationFunction extends CliFunction<Boolean> {
  private static final long serialVersionUID = 1L;


  @Override
  @SuppressWarnings("deprecation")
  public CliFunctionResult executeFunction(FunctionContext<Boolean> context) {
    boolean hideDefaults = context.getArguments();

    Cache cache = context.getCache();
    InternalDistributedSystem system = (InternalDistributedSystem) cache.getDistributedSystem();
    DistributionConfig config = system.getConfig();

    DistributionConfigImpl distConfigImpl = ((DistributionConfigImpl) config);
    MemberConfigurationInfo memberConfigInfo = new MemberConfigurationInfo();
    memberConfigInfo.setJvmInputArguments(getJvmInputArguments());
    memberConfigInfo
        .setGfePropsRuntime(distConfigImpl.getConfigPropsFromSource(ConfigSource.runtime()));
    memberConfigInfo
        .setGfePropsSetUsingApi(distConfigImpl.getConfigPropsFromSource(ConfigSource.api()));

    if (!hideDefaults)
      memberConfigInfo.setGfePropsSetWithDefaults(distConfigImpl.getConfigPropsFromSource(null));

    memberConfigInfo.setGfePropsSetFromFile(distConfigImpl.getConfigPropsDefinedUsingFiles());

    // CacheAttributes
    Map<String, String> cacheAttributes = new HashMap<>();

    cacheAttributes.put("copy-on-read", Boolean.toString(cache.getCopyOnRead()));
    cacheAttributes.put("is-server", Boolean.toString(cache.isServer()));
    cacheAttributes.put("lock-timeout", Integer.toString(cache.getLockTimeout()));
    cacheAttributes.put("lock-lease", Integer.toString(cache.getLockLease()));
    cacheAttributes.put("message-sync-interval", Integer.toString(cache.getMessageSyncInterval()));
    cacheAttributes.put("search-timeout", Integer.toString(cache.getSearchTimeout()));

    if (cache.getPdxDiskStore() == null) {
      cacheAttributes.put("pdx-disk-store", "");
    } else {
      cacheAttributes.put("pdx-disk-store", cache.getPdxDiskStore());
    }

    cacheAttributes.put("pdx-ignore-unread-fields",
        Boolean.toString(cache.getPdxIgnoreUnreadFields()));
    cacheAttributes.put("pdx-persistent", Boolean.toString(cache.getPdxPersistent()));
    cacheAttributes.put("pdx-read-serialized", Boolean.toString(cache.getPdxReadSerialized()));

    if (hideDefaults) {
      removeDefaults(cacheAttributes, getCacheAttributesDefaultValues());
    }

    memberConfigInfo.setCacheAttributes(cacheAttributes);

    List<Map<String, String>> cacheServerAttributesList = new ArrayList<>();
    List<CacheServer> cacheServers = cache.getCacheServers();

    if (cacheServers != null)
      for (CacheServer cacheServer : cacheServers) {
        Map<String, String> cacheServerAttributes = new HashMap<>();

        cacheServerAttributes.put("bind-address", cacheServer.getBindAddress());
        cacheServerAttributes.put("hostname-for-clients", cacheServer.getHostnameForClients());
        cacheServerAttributes.put("max-connections",
            Integer.toString(cacheServer.getMaxConnections()));
        cacheServerAttributes.put("maximum-message-count",
            Integer.toString(cacheServer.getMaximumMessageCount()));
        cacheServerAttributes.put("maximum-time-between-pings",
            Integer.toString(cacheServer.getMaximumTimeBetweenPings()));
        cacheServerAttributes.put("max-threads", Integer.toString(cacheServer.getMaxThreads()));
        cacheServerAttributes.put("message-time-to-live",
            Integer.toString(cacheServer.getMessageTimeToLive()));
        cacheServerAttributes.put("notify-by-subscription",
            Boolean.toString(cacheServer.getNotifyBySubscription()));
        cacheServerAttributes.put("port", Integer.toString(cacheServer.getPort()));
        cacheServerAttributes.put(SOCKET_BUFFER_SIZE,
            Integer.toString(cacheServer.getSocketBufferSize()));
        cacheServerAttributes.put("load-poll-interval",
            Long.toString(cacheServer.getLoadPollInterval()));
        cacheServerAttributes.put("tcp-no-delay", Boolean.toString(cacheServer.getTcpNoDelay()));

        if (hideDefaults)
          removeDefaults(cacheServerAttributes, getCacheServerAttributesDefaultValues());

        cacheServerAttributesList.add(cacheServerAttributes);
      }

    memberConfigInfo.setCacheServerAttributes(cacheServerAttributesList);

    return new CliFunctionResult(context.getMemberName(), memberConfigInfo);
  }

  /****
   * Gets the default values for the cache attributes
   *
   * @return a map containing the cache attributes - default values
   */
  private Map<String, String> getCacheAttributesDefaultValues() {
    Map<String, String> cacheAttributesDefault = new HashMap<>();
    cacheAttributesDefault.put("pdx-disk-store", "");
    cacheAttributesDefault.put("pdx-read-serialized",
        Boolean.toString(CacheConfig.DEFAULT_PDX_READ_SERIALIZED));
    cacheAttributesDefault.put("pdx-ignore-unread-fields",
        Boolean.toString(CacheConfig.DEFAULT_PDX_IGNORE_UNREAD_FIELDS));
    cacheAttributesDefault.put("pdx-persistent",
        Boolean.toString(CacheConfig.DEFAULT_PDX_PERSISTENT));
    cacheAttributesDefault.put("copy-on-read",
        Boolean.toString(GemFireCacheImpl.DEFAULT_COPY_ON_READ));
    cacheAttributesDefault.put("lock-timeout",
        Integer.toString(GemFireCacheImpl.DEFAULT_LOCK_TIMEOUT));
    cacheAttributesDefault.put("lock-lease", Integer.toString(GemFireCacheImpl.DEFAULT_LOCK_LEASE));
    cacheAttributesDefault.put("message-sync-interval",
        Integer.toString(HARegionQueue.DEFAULT_MESSAGE_SYNC_INTERVAL));
    cacheAttributesDefault.put("search-timeout",
        Integer.toString(GemFireCacheImpl.DEFAULT_SEARCH_TIMEOUT));
    cacheAttributesDefault.put("is-server", Boolean.toString(false));

    return cacheAttributesDefault;
  }

  /***
   * Gets the default values for the cache attributes
   *
   * @return a map containing the cache server attributes - default values
   */
  private Map<String, String> getCacheServerAttributesDefaultValues() {
    Map<String, String> csAttributesDefault = new HashMap<>();
    csAttributesDefault.put("bind-address", CacheServer.DEFAULT_BIND_ADDRESS);
    csAttributesDefault.put("hostname-for-clients", CacheServer.DEFAULT_HOSTNAME_FOR_CLIENTS);
    csAttributesDefault.put("max-connections",
        Integer.toString(CacheServer.DEFAULT_MAX_CONNECTIONS));
    csAttributesDefault.put("maximum-message-count",
        Integer.toString(CacheServer.DEFAULT_MAXIMUM_MESSAGE_COUNT));
    csAttributesDefault.put("maximum-time-between-pings",
        Integer.toString(CacheServer.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS));
    csAttributesDefault.put("max-threads", Integer.toString(CacheServer.DEFAULT_MAX_THREADS));
    csAttributesDefault.put("message-time-to-live",
        Integer.toString(CacheServer.DEFAULT_MESSAGE_TIME_TO_LIVE));
    csAttributesDefault.put("notify-by-subscription",
        Boolean.toString(CacheServer.DEFAULT_NOTIFY_BY_SUBSCRIPTION));
    csAttributesDefault.put("port", Integer.toString(CacheServer.DEFAULT_PORT));
    csAttributesDefault.put(SOCKET_BUFFER_SIZE,
        Integer.toString(CacheServer.DEFAULT_SOCKET_BUFFER_SIZE));
    csAttributesDefault.put("load-poll-interval",
        Long.toString(CacheServer.DEFAULT_LOAD_POLL_INTERVAL));
    return csAttributesDefault;

  }

  /****
   * Removes the default values from the attributesMap based on defaultAttributesMap
   *
   */
  private void removeDefaults(Map<String, String> attributesMap,
      Map<String, String> defaultAttributesMap) {
    // Make a copy to avoid the CME's
    Set<String> attributesSet = new HashSet<>(attributesMap.keySet());

    for (String attribute : attributesSet) {
      String attributeValue = attributesMap.get(attribute);
      String defaultValue = defaultAttributesMap.get(attribute);

      if (attributeValue != null) {
        if (attributeValue.equals(defaultValue)) {
          attributesMap.remove(attribute);
        }
      } else {
        if (defaultValue == null || defaultValue.equals("")) {
          attributesMap.remove(attribute);
        }
      }
    }
  }

  private List<String> getJvmInputArguments() {
    RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
    return ArgumentRedactor.redactEachInList(runtimeBean.getInputArguments());
  }
}
