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

package org.apache.geode.management.client;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

import org.springframework.http.client.ClientHttpRequestFactory;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.internal.ClusterManagementServiceFactory;

/**
 * Top-level entry point for client interaction with the {@link ClusterManagementService}. A user
 * can create an instance of the {@code ClusterManagementService} (CMS for short) in several ways,
 * each providing various level of control or expediency.
 * <p/>
 * Calling {@code getServiceFactory(context)} will return an explicit instance of {@link
 * ClusterManagementServiceFactory}. Methods on this factory can then be called to create or
 * retrieve instances of a CMS. New {@link ClusterManagementServiceProvider}s can be written if
 * specific customization or parameterization is required.
 * <p/>
 * A note about contexts. A context is simply a unique string which identifies a specific instance
 * of CMS that will be returned. Contexts map to the different uses of a CMS. Currently, the
 * following contexts are provided:
 * <ul>
 * <li>
 * {@code JAVA_CLIENT_CONTEXT} ("java-client") - would be used to retrieve a CMS instance on a pure
 * Java client - i.e. an app that is not running in the context of a Geode client or server. This
 * context is available when using the <i>geode-management</i> module.
 * </li>
 * <li>{@code GEODE_CONTEXT} ("geode") - would be used to retrieve a CMS instance
 * from a JVM where either a {@code Cache} or {@code ClientCache} exists. This context is available
 * when using the <i>geode-core</i> module.
 * </li>
 * </ul>
 * If the URL of the Cluster Management Service is known, the {@code getService(url)} method can be
 * called. This implicitly uses the {@code JAVA_CLIENT_CONTEXT}. For further control the CMS can be
 * configured with a {@link ClientHttpRequestFactory} by calling {@code
 * getService(requestFactory)}.
 * <p/>
 * Finally, the simplest way to create a CMS instance is simply to call {@code getService()}. This
 * method will attempt to infer the context and use an appropriate service provider to create a CMS
 * instance.
 */
@Experimental
public class ClusterManagementServiceProvider {

  public static final String JAVA_CLIENT_CONTEXT = "java-client";
  public static final String GEODE_CONTEXT = "geode";

  private static Map<String, ClusterManagementServiceFactory> serviceFactories = null;

  public static ClusterManagementService getService() {
    return getServiceFactory(GEODE_CONTEXT).create();
  }

  public static ClusterManagementService getService(String clusterUrl) {
    return getServiceFactory(JAVA_CLIENT_CONTEXT).create(clusterUrl);
  }

  public static ClusterManagementService getService(ClientHttpRequestFactory requestFactory) {
    return getServiceFactory(JAVA_CLIENT_CONTEXT).create(requestFactory);
  }

  private static synchronized ClusterManagementServiceFactory getServiceFactory(String context) {
    if (serviceFactories == null) {
      loadClusterManagementServiceFactories();
    }
    ClusterManagementServiceFactory factory = serviceFactories.get(context);
    if (factory == null) {
      throw new IllegalArgumentException("Did not find provider for context: " + context);
    }
    return factory;
  }

  private static void loadClusterManagementServiceFactories() {
    serviceFactories = new HashMap<>();

    for (ClusterManagementServiceFactory factory : ServiceLoader
        .load(ClusterManagementServiceFactory.class)) {
      serviceFactories.put(factory.getContext(), factory);
    }
  }
}
