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

package org.apache.geode.modules.session.internal.filter;

import java.util.Enumeration;
import java.util.UUID;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.naming.InitialContext;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.modules.session.bootstrap.AbstractCache;
import org.apache.geode.modules.session.bootstrap.ClientServerCache;
import org.apache.geode.modules.session.bootstrap.LifecycleTypeAdapter;
import org.apache.geode.modules.session.bootstrap.PeerToPeerCache;
import org.apache.geode.modules.session.internal.common.CacheProperty;
import org.apache.geode.modules.session.internal.common.ClientServerSessionCache;
import org.apache.geode.modules.session.internal.common.PeerToPeerSessionCache;
import org.apache.geode.modules.session.internal.common.SessionCache;
import org.apache.geode.modules.session.internal.filter.attributes.AbstractSessionAttributes;
import org.apache.geode.modules.session.internal.filter.attributes.DeltaQueuedSessionAttributes;
import org.apache.geode.modules.session.internal.filter.attributes.DeltaSessionAttributes;
import org.apache.geode.modules.session.internal.filter.util.TypeAwareMap;
import org.apache.geode.modules.session.internal.jmx.SessionStatistics;
import org.apache.geode.modules.util.RegionHelper;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * This class implements the session management using a Gemfire distributedCache as a persistent
 * store for the session objects
 */
public class GemfireSessionManager implements SessionManager {

  private final Logger LOG;

  /**
   * Prefix of init param string used to set gemfire properties
   */
  private static final String GEMFIRE_PROPERTY = GeodeGlossary.GEMFIRE_PREFIX + "property.";

  /**
   * Prefix of init param string used to set gemfire distributedCache setting
   */
  private static final String GEMFIRE_CACHE = GeodeGlossary.GEMFIRE_PREFIX + "cache.";

  private static final String INIT_PARAM_CACHE_TYPE = "cache-type";
  private static final String CACHE_TYPE_CLIENT_SERVER = "client-server";
  private static final String CACHE_TYPE_PEER_TO_PEER = "peer-to-peer";
  private static final String INIT_PARAM_SESSION_COOKIE_NAME = "session-cookie-name";
  private static final String INIT_PARAM_JVM_ID = "jvm-id";
  private static final String DEFAULT_JVM_ID = "default";

  private SessionCache sessionCache = null;

  /**
   * Reference to the distributed system
   */
  private AbstractCache distributedCache = null;

  /**
   * Boolean indicating whether the manager is shutting down
   */
  private boolean isStopping = false;

  /**
   * Boolean indicating whether this manager is defined in the same context (war / classloader) as
   * the filter.
   */
  private boolean isolated = false;

  /**
   * MBean for statistics
   */
  private SessionStatistics mbean;

  /**
   * This CL is used to compare against the class loader of attributes getting pulled out of the
   * cache. This variable should be set to the CL of the filter running everything.
   */
  private ClassLoader referenceClassLoader;

  private String sessionCookieName = "JSESSIONID";

  /**
   * Give this JVM a unique identifier.
   */
  private String jvmId = "default";

  /**
   * Set up properties with default values
   */
  private final TypeAwareMap<CacheProperty, Object> properties =
      new TypeAwareMap<CacheProperty, Object>(CacheProperty.class) {
        {
          put(CacheProperty.REGION_NAME, RegionHelper.NAME + "_sessions");
          put(CacheProperty.ENABLE_GATEWAY_DELTA_REPLICATION, Boolean.FALSE);
          put(CacheProperty.ENABLE_GATEWAY_REPLICATION, Boolean.FALSE);
          put(CacheProperty.ENABLE_DEBUG_LISTENER, Boolean.FALSE);
          put(CacheProperty.STATISTICS_NAME, "gemfire_statistics");
          put(CacheProperty.SESSION_DELTA_POLICY, "delta_queued");
          put(CacheProperty.REPLICATION_TRIGGER, "set");
          /*
           * For REGION_ATTRIBUTES_ID and ENABLE_LOCAL_CACHE the default is different for
           * ClientServerCache and PeerToPeerCache so those values are set in the relevant
           * constructors when these properties are passed in to them.
           */
        }
      };

  public GemfireSessionManager() {
    LOG = LoggerFactory.getLogger(GemfireSessionManager.class.getName());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void start(Object conf, ClassLoader loader) {
    referenceClassLoader = loader;
    FilterConfig config = (FilterConfig) conf;

    startDistributedSystem(config);
    initializeSessionCache(config);

    // Register MBean
    registerMBean();

    if (distributedCache.getClass().getClassLoader() == loader) {
      isolated = true;
    }

    String sessionCookieName = config.getInitParameter(INIT_PARAM_SESSION_COOKIE_NAME);
    if (sessionCookieName != null && !sessionCookieName.isEmpty()) {
      this.sessionCookieName = sessionCookieName;
      LOG.info("Session cookie name set to: {}", this.sessionCookieName);
    }

    jvmId = config.getInitParameter(INIT_PARAM_JVM_ID);
    if (jvmId == null || jvmId.isEmpty()) {
      jvmId = DEFAULT_JVM_ID;
    }

    LOG.info("Started GemfireSessionManager (isolated={}, jvmId={})", isolated, jvmId);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void stop() {
    isStopping = true;

    if (isolated) {
      if (distributedCache != null) {
        LOG.info("Closing distributed cache - assuming isolated cache");
        distributedCache.close();
      }
    } else {
      LOG.info("Not closing distributed cache - assuming common cache");
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public HttpSession getSession(String id) {
    GemfireHttpSession session = (GemfireHttpSession) sessionCache.getOperatingRegion().get(id);

    if (session != null) {
      if (session.justSerialized()) {
        session.setManager(this);
        LOG.debug("Recovered serialized session {} (jvmId={})", id, session.getJvmOwnerId());
      }
      LOG.debug("Retrieved session id {}", id);
    } else {
      LOG.debug("Session id {} not found", id);
    }
    return session;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public HttpSession wrapSession(ServletContext context, int maxInactiveInterval) {
    String id = generateId();
    GemfireHttpSession session = new GemfireHttpSession(id, context);

    // Set up the attribute container depending on how things are configured
    Object sessionPolicy = properties.get(CacheProperty.SESSION_DELTA_POLICY);
    AbstractSessionAttributes attributes;
    if ("delta_queued".equals(sessionPolicy) || "queued".equals(sessionPolicy)) {
      attributes = new DeltaQueuedSessionAttributes();
      ((DeltaQueuedSessionAttributes) attributes)
          .setReplicationTrigger((String) properties.get(CacheProperty.REPLICATION_TRIGGER));
    } else if ("delta_immediate".equals(sessionPolicy) || "immediate".equals(sessionPolicy)) {
      attributes = new DeltaSessionAttributes();
    } else {
      attributes = new DeltaSessionAttributes();
      LOG.warn("No session delta policy specified - using default of 'delta_immediate'");
    }

    attributes.setSession(session);
    attributes.setJvmOwnerId(jvmId);
    attributes.setMaxInactiveInterval(maxInactiveInterval);
    attributes.setCreationTime(System.currentTimeMillis());

    session.setManager(this);
    session.setAttributes(attributes);

    LOG.debug("Creating new session {}", id);
    sessionCache.getOperatingRegion().put(id, session);

    mbean.incActiveSessions();

    return session;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void destroySession(String id) {
    if (!isStopping) {
      try {
        GemfireHttpSession session = (GemfireHttpSession) sessionCache.getOperatingRegion().get(id);
        if (session != null && session.getJvmOwnerId().equals(jvmId)) {
          LOG.debug("Destroying session {}", id);
          sessionCache.getOperatingRegion().destroy(id);
          mbean.decActiveSessions();
        }
      } catch (EntryNotFoundException ignored) {
      }
    } else {
      if (sessionCache.isClientServer()) {
        LOG.debug("Destroying session {}", id);
        try {
          sessionCache.getOperatingRegion().localDestroy(id);
        } catch (EntryNotFoundException | CacheClosedException nex) {
          // Ignored
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void putSession(HttpSession session) {
    sessionCache.getOperatingRegion().put(session.getId(), session);
    mbean.incRegionUpdates();
  }

  ClassLoader getReferenceClassLoader() {
    return referenceClassLoader;
  }

  @Override
  public String getSessionCookieName() {
    return sessionCookieName;
  }

  @Override
  public String getJvmId() {
    return jvmId;
  }


  ///////////////////////////////////////////////////////////////////////
  // Private methods

  /**
   * Start the underlying distributed system
   *
   */
  private void startDistributedSystem(FilterConfig config) {
    // Get the distributedCache type
    final String cacheType = config.getInitParameter(INIT_PARAM_CACHE_TYPE);
    if (CACHE_TYPE_CLIENT_SERVER.equals(cacheType)) {
      distributedCache = ClientServerCache.getInstance();
    } else if (CACHE_TYPE_PEER_TO_PEER.equals(cacheType)) {
      distributedCache = PeerToPeerCache.getInstance();
    } else {
      LOG.error("No 'cache-type' initialization param set. " + "Cache will not be started");
      return;
    }

    if (!distributedCache.isStarted()) {
      // Process all the init params and see if any apply to the distributed system.
      for (Enumeration<String> e = config.getInitParameterNames(); e.hasMoreElements();) {
        String param = e.nextElement();
        if (!param.startsWith(GEMFIRE_PROPERTY)) {
          continue;
        }

        String gemfireProperty = param.substring(GEMFIRE_PROPERTY.length());
        LOG.info("Setting gemfire property: {} = {}", gemfireProperty,
            config.getInitParameter(param));
        distributedCache.setProperty(gemfireProperty, config.getInitParameter(param));
      }

      distributedCache.lifecycleEvent(LifecycleTypeAdapter.START);
    }
  }

  /**
   * Initialize the distributedCache
   */
  private void initializeSessionCache(FilterConfig config) {
    // Retrieve the distributedCache
    GemFireCacheImpl cache = (GemFireCacheImpl) CacheFactory.getAnyInstance();
    if (cache == null) {
      throw new IllegalStateException(
          "No cache exists. Please configure " + "either a PeerToPeerCacheLifecycleListener or "
              + "ClientServerCacheLifecycleListener in the " + "server.xml file.");
    }

    // Process all the init params and see if any apply to the distributedCache
    ResourceManager rm = cache.getResourceManager();
    for (Enumeration<String> e = config.getInitParameterNames(); e.hasMoreElements();) {
      String param = e.nextElement();

      // Uggh - don't like this non-generic stuff
      if (param.equalsIgnoreCase("criticalHeapPercentage")) {
        float val = Float.parseFloat(config.getInitParameter(param));
        rm.setCriticalHeapPercentage(val);
      }

      if (param.equalsIgnoreCase("evictionHeapPercentage")) {
        float val = Float.parseFloat(config.getInitParameter(param));
        rm.setEvictionHeapPercentage(val);
      }


      if (!param.startsWith(GEMFIRE_CACHE)) {
        continue;
      }

      String gemfireWebParam = param.substring(GEMFIRE_CACHE.length());
      LOG.info("Setting cache parameter: {} = {}", gemfireWebParam, config.getInitParameter(param));
      properties.put(CacheProperty.valueOf(gemfireWebParam.toUpperCase()),
          config.getInitParameter(param));
    }

    // Create the appropriate session distributedCache
    sessionCache = cache.isClient() ? new ClientServerSessionCache(cache, properties)
        : new PeerToPeerSessionCache(cache, properties);

    // Initialize the session distributedCache
    sessionCache.initialize();
  }

  /**
   * Register a bean for statistic gathering purposes
   */
  private void registerMBean() {
    mbean = new SessionStatistics();

    try {
      InitialContext ctx = new InitialContext();
      MBeanServer mbs = (MBeanServer) ctx.lookup("java:comp/env/jmx/runtime");
      ObjectName oname = new ObjectName(Constants.SESSION_STATISTICS_MBEAN_NAME);

      mbs.registerMBean(mbean, oname);
    } catch (Exception ex) {
      LOG.warn("Unable to register statistics MBean. Error: {}", ex.getMessage());
    }
  }


  /**
   * Generate an ID string
   */
  private String generateId() {
    return UUID.randomUUID().toString().toUpperCase() + "-GF";
  }

  AbstractCache getCache() {
    return distributedCache;
  }
}
