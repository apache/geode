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
package org.apache.geode.modules.session.catalina;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.http.HttpSession;

import org.apache.catalina.Container;
import org.apache.catalina.Context;
import org.apache.catalina.Lifecycle;
import org.apache.catalina.Loader;
import org.apache.catalina.Pipeline;
import org.apache.catalina.Session;
import org.apache.catalina.Valve;
import org.apache.catalina.session.ManagerBase;
import org.apache.catalina.session.StandardSession;
import org.apache.catalina.util.CustomObjectInputStream;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.modules.session.catalina.internal.DeltaSessionStatistics;
import org.apache.geode.modules.util.ContextMapper;
import org.apache.geode.modules.util.RegionConfiguration;
import org.apache.geode.modules.util.RegionHelper;

public abstract class DeltaSessionManager<CommitSessionValveT extends AbstractCommitSessionValve<?>>
    extends ManagerBase
    implements Lifecycle, PropertyChangeListener, SessionManager, DeltaSessionManagerConfiguration {

  static final String catalinaBaseSystemProperty = "catalina.base";
  static final String javaTempDirSystemProperty = "java.io.tmpdir";
  static final String fileSeparatorSystemProperty = "file.separator";
  /**
   * The number of rejected sessions.
   */
  private final AtomicInteger rejectedSessions;

  /**
   * The maximum number of active Sessions allowed, or -1 for no limit.
   */
  private int maxActiveSessions = -1;

  /**
   * Has this <code>Manager</code> been started?
   */
  protected AtomicBoolean started = new AtomicBoolean(false);

  /**
   * The name of this <code>Manager</code>
   */
  protected String name;

  private Valve jvmRouteBinderValve;

  private CommitSessionValveT commitSessionValve;

  private SessionCache sessionCache;

  private static final String DEFAULT_REGION_NAME = RegionHelper.NAME + "_sessions";

  private static final boolean DEFAULT_ENABLE_GATEWAY_REPLICATION = false;

  private static final boolean DEFAULT_ENABLE_DEBUG_LISTENER = false;

  private static final boolean DEFAULT_ENABLE_COMMIT_VALVE = true;

  private static final boolean DEFAULT_ENABLE_COMMIT_VALVE_FAILFAST = false;

  /**
   * @deprecated No replacement. Always prefer deserialized form.
   */
  @Deprecated
  private static final boolean DEFAULT_PREFER_DESERIALIZED_FORM = true;

  /*
   * This *MUST* only be assigned during start/startInternal otherwise it will be associated with
   * the incorrect context class loader.
   */
  private Log LOGGER;

  protected String regionName = DEFAULT_REGION_NAME;

  private String regionAttributesId; // the default is different for client-server and
  // peer-to-peer

  private Boolean enableLocalCache; // the default is different for client-server and peer-to-peer

  private boolean enableCommitValve = DEFAULT_ENABLE_COMMIT_VALVE;

  private boolean enableCommitValveFailfast = DEFAULT_ENABLE_COMMIT_VALVE_FAILFAST;

  private boolean enableGatewayReplication = DEFAULT_ENABLE_GATEWAY_REPLICATION;

  private boolean enableDebugListener = DEFAULT_ENABLE_DEBUG_LISTENER;

  /**
   * @deprecated No replacement. Always prefer deserialized form.
   */
  @Deprecated
  private boolean preferDeserializedForm = DEFAULT_PREFER_DESERIALIZED_FORM;

  private Timer timer;

  private final Set<String> sessionsToTouch;

  private static final long TIMER_TASK_PERIOD =
      Long.getLong("gemfiremodules.sessionTimerTaskPeriod", 10000);

  private static final long TIMER_TASK_DELAY =
      Long.getLong("gemfiremodules.sessionTimerTaskDelay", 10000);

  public DeltaSessionManager() {
    rejectedSessions = new AtomicInteger(0);
    // Create the set to store sessions to be touched after get attribute requests
    sessionsToTouch = Collections.newSetFromMap(new ConcurrentHashMap<>());
  }

  @Override
  public String getRegionName() {
    return regionName;
  }

  @Override
  public void setRegionName(String regionName) {
    this.regionName = regionName;
  }

  @Override
  public void setMaxInactiveInterval(final int interval) {
    super.setMaxInactiveInterval(interval);
  }

  @Override
  public String getRegionAttributesId() {
    // This property will be null if it hasn't been set in the context.xml file.
    // Since its default is dependent on the session cache, get the default from
    // the session cache.
    if (regionAttributesId == null) {
      regionAttributesId = getSessionCache().getDefaultRegionAttributesId();
    }
    return regionAttributesId;
  }

  @Override
  public void setRegionAttributesId(String regionType) {
    regionAttributesId = regionType;
  }

  @Override
  public boolean getEnableLocalCache() {
    // This property will be null if it hasn't been set in the context.xml file.
    // Since its default is dependent on the session cache, get the default from
    // the session cache.
    if (enableLocalCache == null) {
      enableLocalCache = getSessionCache().getDefaultEnableLocalCache();
    }
    return enableLocalCache;
  }

  @Override
  public void setEnableLocalCache(boolean enableLocalCache) {
    this.enableLocalCache = enableLocalCache;
  }

  @Override
  public int getMaxActiveSessions() {
    return maxActiveSessions;
  }

  @Override
  public void setMaxActiveSessions(int maxActiveSessions) {
    int oldMaxActiveSessions = this.maxActiveSessions;
    this.maxActiveSessions = maxActiveSessions;
    support.firePropertyChange("maxActiveSessions", new Integer(oldMaxActiveSessions),
        new Integer(this.maxActiveSessions));
  }

  @Override
  public boolean getEnableGatewayDeltaReplication() {
    // return this.enableGatewayDeltaReplication;
    return false; // disabled
  }

  @Override
  public void setEnableGatewayDeltaReplication(boolean enableGatewayDeltaReplication) {
    // this.enableGatewayDeltaReplication = enableGatewayDeltaReplication;
    // Disabled. Keeping the method for backward compatibility.
  }

  @Override
  public boolean getEnableGatewayReplication() {
    return enableGatewayReplication;
  }

  @Override
  public void setEnableGatewayReplication(boolean enableGatewayReplication) {
    this.enableGatewayReplication = enableGatewayReplication;
  }

  @Override
  public boolean getEnableDebugListener() {
    return enableDebugListener;
  }

  @Override
  public void setEnableDebugListener(boolean enableDebugListener) {
    this.enableDebugListener = enableDebugListener;
  }

  @Override
  public boolean isCommitValveEnabled() {
    return enableCommitValve;
  }

  @Override
  public void setEnableCommitValve(boolean enable) {
    enableCommitValve = enable;
  }

  @Override
  public boolean isCommitValveFailfastEnabled() {
    return enableCommitValveFailfast;
  }

  @Override
  public void setEnableCommitValveFailfast(boolean enable) {
    enableCommitValveFailfast = enable;
  }

  @Override
  public boolean isBackingCacheAvailable() {
    return sessionCache.isBackingCacheAvailable();
  }

  /**
   * @deprecated No replacement. Always prefer deserialized form.
   */
  @Deprecated
  @Override
  public void setPreferDeserializedForm(boolean enable) {
    log.warn("Use of deprecated preferDeserializedForm property to be removed in future release.");
    if (!enable) {
      log.warn(
          "Use of HttpSessionAttributeListener may result in serialized form in HttpSessionBindingEvent.");
    }
    preferDeserializedForm = enable;
  }

  /**
   * @deprecated No replacement. Always prefer deserialized form.
   */
  @Deprecated
  @Override
  public boolean getPreferDeserializedForm() {
    return preferDeserializedForm;
  }

  @Override
  public String getStatisticsName() {
    return getContextName().replace("/", "");
  }

  @Override
  public Log getLogger() {
    if (LOGGER == null) {
      LOGGER = LogFactory.getLog(DeltaSessionManager.class);
    }
    return LOGGER;
  }

  public SessionCache getSessionCache() {
    return sessionCache;
  }

  public DeltaSessionStatistics getStatistics() {
    return getSessionCache().getStatistics();
  }

  boolean isPeerToPeer() {
    return getSessionCache().isPeerToPeer();
  }

  public boolean isClientServer() {
    return getSessionCache().isClientServer();
  }

  /**
   * This method was taken from StandardManager to set the default maxInactiveInterval based on the
   * container (to 30 minutes).
   * <p>
   * Set the Container with which this Manager has been associated. If it is a Context (the usual
   * case), listen for changes to the session timeout property.
   *
   * @param container The associated Container
   */
  @Override
  public void setContainer(Container container) {
    // De-register from the old Container (if any)
    if ((this.container != null) && (this.container instanceof Context)) {
      this.container.removePropertyChangeListener(this);
    }

    // Default processing provided by our superclass
    super.setContainer(container);

    // Register with the new Container (if any)
    if ((this.container != null) && (this.container instanceof Context)) {
      // Overwrite the max inactive interval with the context's session timeout.
      setMaxInactiveInterval(((Context) this.container).getSessionTimeout() * 60);
      this.container.addPropertyChangeListener(this);
    }
  }

  @Override
  public Session findSession(String id) {
    if (id == null) {
      return null;
    }
    if (getLogger().isDebugEnabled()) {
      getLogger().debug(
          this + ": Finding session " + id + " in " + getSessionCache().getOperatingRegionName());
    }
    DeltaSessionInterface session = (DeltaSessionInterface) getSessionCache().getSession(id);
    /*
     * Check that the context name for this session is the same as this manager's. This comes into
     * play when multiple versions of a webapp are deployed and active at the same time; the context
     * name will contain an embedded version number; something like /test###2.
     */
    if (session != null && !session.getContextName().isEmpty()
        && !getContextName().equals(session.getContextName())) {
      getLogger()
          .info(this + ": Session " + id + " rejected as container name and context do not match: "
              + getContextName() + " != " + session.getContextName());
      session = null;
    }

    if (session == null) {
      if (getLogger().isDebugEnabled()) {
        getLogger().debug(this + ": Did not find session " + id + " in "
            + getSessionCache().getOperatingRegionName());
      }
    } else {
      if (getLogger().isDebugEnabled()) {
        getLogger().debug(this + ": Found session " + id + " in "
            + getSessionCache().getOperatingRegionName() + ": " + session);
      }
      // The session was previously stored. Set new to false.
      session.setNew(false);

      // Check the manager.
      // If the manager is null, the session was replicated and this is a
      // failover situation. Reset the manager and activate the session.
      if (session.getManager() == null) {
        session.setOwner(this);
        session.activate();
      }
    }

    return session;
  }

  protected void initializeSessionCache() {
    // Retrieve the cache
    GemFireCacheImpl cache = (GemFireCacheImpl) getAnyCacheInstance();
    if (cache == null) {
      throw new IllegalStateException(
          "No cache exists. Please configure either a PeerToPeerCacheLifecycleListener or ClientServerCacheLifecycleListener in the server.xml file.");
    }

    // Create the appropriate session cache
    sessionCache = cache.isClient() ? new ClientServerSessionCache(this, cache)
        : new PeerToPeerSessionCache(this, cache);

    // Initialize the session cache
    initSessionCache();
  }

  void initSessionCache() {
    sessionCache.initialize();
  }

  Cache getAnyCacheInstance() {
    return CacheFactory.getAnyInstance();
  }

  @Override
  protected StandardSession getNewSession() {
    return new DeltaSession(this);
  }

  @Override
  public void remove(Session session) {
    // Remove the session from the region if necessary.
    // It will have already been removed if it expired implicitly.
    DeltaSessionInterface ds = (DeltaSessionInterface) session;
    if (ds.getExpired()) {
      if (getLogger().isDebugEnabled()) {
        getLogger().debug(this + ": Expired session " + session.getId() + " from "
            + getSessionCache().getOperatingRegionName());
      }
    } else {
      if (getLogger().isDebugEnabled()) {
        getLogger().debug(this + ": Destroying session " + session.getId() + " from "
            + getSessionCache().getOperatingRegionName());
      }
      getSessionCache().destroySession(session.getId());
      if (getLogger().isDebugEnabled()) {
        getLogger().debug(this + ": Destroyed session " + session.getId() + " from "
            + getSessionCache().getOperatingRegionName());
      }
    }
  }

  @Override
  public void add(Session session) {
    // super.add(session);
    if (getLogger().isDebugEnabled()) {
      getLogger().debug(this + ": Storing session " + session.getId() + " into "
          + getSessionCache().getOperatingRegionName());
    }
    getSessionCache().putSession(session);
    if (getLogger().isDebugEnabled()) {
      getLogger().debug(this + ": Stored session " + session.getId() + " into "
          + getSessionCache().getOperatingRegionName());
    }
    getSessionCache().getStatistics().incSessionsCreated();
  }

  @Override
  public int getRejectedSessions() {
    return rejectedSessions.get();
  }

  @Override
  public void setRejectedSessions(int rejectedSessions) {
    this.rejectedSessions.set(rejectedSessions);
  }

  /**
   * Returns the number of active sessions
   *
   * @return number of sessions active
   */
  @Override
  public int getActiveSessions() {
    return getSessionCache().size();
  }

  /**
   * For debugging: return a list of all session ids currently active
   */
  @Override
  public String listSessionIds() {
    StringBuilder builder = new StringBuilder();
    Iterator<String> sessionIds = getSessionCache().keySet().iterator();
    while (sessionIds.hasNext()) {
      builder.append(sessionIds.next());
      if (sessionIds.hasNext()) {
        builder.append(" ");
      }
    }
    return builder.toString();
  }

  /*
   * If local caching is enabled, add the session to the set of sessions to be touched. A timer task
   * will be periodically invoked to get the session in the session region to update its last
   * accessed time. This prevents the session from expiring in the case where the application is
   * only getting attributes from the session and never putting attributes into the session. If
   * local caching is disabled. the session's last accessed time would already have been updated
   * properly in the sessions region.
   *
   * Note: Due to issues in GemFire expiry, sessions are always asynchronously touched using a
   * function regardless whether or not local caching is enabled. This prevents premature
   * expiration.
   */
  void addSessionToTouch(String sessionId) {
    sessionsToTouch.add(sessionId);
  }

  protected Set<String> getSessionsToTouch() {
    return sessionsToTouch;
  }

  void removeTouchedSession(String sessionId) {
    sessionsToTouch.remove(sessionId);
  }

  protected void scheduleTimerTasks() {
    // Create the timer
    timer = new Timer("Timer for " + toString(), true);

    // Schedule the task to handle sessions to be touched
    scheduleTouchSessionsTask();

    // Schedule the task to maintain the maxActive sessions
    scheduleDetermineMaxActiveSessionsTask();
  }

  private void scheduleTouchSessionsTask() {
    TimerTask task = new TimerTask() {
      @Override
      public void run() {
        // Get the sessionIds to touch and clear the set inside synchronization
        Set<String> sessionIds;
        sessionIds = new HashSet<>(getSessionsToTouch());
        getSessionsToTouch().clear();

        // Touch the sessions we currently have
        if (!sessionIds.isEmpty()) {
          getSessionCache().touchSessions(sessionIds);
          if (getLogger().isDebugEnabled()) {
            getLogger().debug(DeltaSessionManager.this + ": Touched sessions: " + sessionIds);
          }
        }
      }
    };
    timer.schedule(task, TIMER_TASK_DELAY, TIMER_TASK_PERIOD);
  }

  protected void cancelTimer() {
    if (timer != null) {
      timer.cancel();
    }
  }

  private void scheduleDetermineMaxActiveSessionsTask() {
    TimerTask task = new TimerTask() {
      @Override
      public void run() {
        int currentActiveSessions = getSessionCache().size();
        if (currentActiveSessions > getMaxActive()) {
          setMaxActive(currentActiveSessions);
          if (getLogger().isDebugEnabled()) {
            getLogger().debug(
                DeltaSessionManager.this + ": Set max active sessions: " + currentActiveSessions);
          }
        }
      }
    };

    timer.schedule(task, TIMER_TASK_DELAY, TIMER_TASK_PERIOD);
  }

  @Override
  public void load() throws ClassNotFoundException, IOException {
    doLoad();
    ContextMapper.addContext(getContextName(), this);
  }

  @Override
  public void unload() throws IOException {
    doUnload();
    ContextMapper.removeContext(getContextName());
  }

  protected void registerJvmRouteBinderValve() {
    if (getLogger().isDebugEnabled()) {
      getLogger().debug(this + ": Registering JVM route binder valve");
    }
    jvmRouteBinderValve = new JvmRouteBinderValve();
    getPipeline().addValve(jvmRouteBinderValve);
  }

  Pipeline getPipeline() {
    return getContainer().getPipeline();
  }

  protected void unregisterJvmRouteBinderValve() {
    if (getLogger().isDebugEnabled()) {
      getLogger().debug(this + ": Unregistering JVM route binder valve");
    }
    if (jvmRouteBinderValve != null) {
      getPipeline().removeValve(jvmRouteBinderValve);
    }
  }

  protected void registerCommitSessionValve() {
    if (getLogger().isDebugEnabled()) {
      getLogger().debug(this + ": Registering CommitSessionValve");
    }
    commitSessionValve = createCommitSessionValve();
    getPipeline().addValve(commitSessionValve);
  }

  protected abstract CommitSessionValveT createCommitSessionValve();

  protected void unregisterCommitSessionValve() {
    if (getLogger().isDebugEnabled()) {
      getLogger().debug(this + ": Unregistering CommitSessionValve");
    }
    if (commitSessionValve != null) {
      getPipeline().removeValve(commitSessionValve);
    }
  }

  // ------------------------------ Lifecycle Methods

  /**
   * Process property change events from our associated Context.
   * <p>
   * Part of this method implementation was taken from StandardManager. The sessionTimeout can be
   * changed in the web.xml which is processed after the context.xml. The context (and the default
   * session timeout) would already have been set in this Manager. This is the way to get the new
   * session timeout value specified in the web.xml.
   * <p>
   * The precedence order for setting the session timeout value is:
   * <p>
   * <ol>
   * <li>the max inactive interval is set based on the Manager defined in the context.xml
   * <li>the max inactive interval is then overwritten by the value of the Context's session timeout
   * when setContainer is called
   * <li>the max inactive interval is then overwritten by the value of the session-timeout specified
   * in the web.xml (if any)
   * </ol>
   *
   * @param event The property change event that has occurred
   */
  @Override
  public void propertyChange(PropertyChangeEvent event) {

    // Validate the source of this event
    if (!(event.getSource() instanceof Context)) {
      return;
    }

    // Process a relevant property change
    if (event.getPropertyName().equals("sessionTimeout")) {
      try {
        int interval = (Integer) event.getNewValue();
        if (interval < RegionConfiguration.DEFAULT_MAX_INACTIVE_INTERVAL) {
          getLogger().warn("The configured session timeout of " + interval
              + " minutes is invalid. Using the original value of " + event.getOldValue()
              + " minutes.");
          interval = (Integer) event.getOldValue();
        }
        // StandardContext.setSessionTimeout passes -1 if the configured timeout
        // is 0; otherwise it passes the value set in web.xml. If the interval
        // parameter equals the default, set the max inactive interval to the
        // default (no expiration); otherwise set it in seconds.
        setMaxInactiveInterval(interval == RegionConfiguration.DEFAULT_MAX_INACTIVE_INTERVAL
            ? RegionConfiguration.DEFAULT_MAX_INACTIVE_INTERVAL : interval * 60);
      } catch (NumberFormatException e) {
        getLogger()
            .error(sm.getString("standardManager.sessionTimeout", event.getNewValue().toString()));
      }
    }
  }

  /**
   * Save any currently active sessions in the appropriate persistence mechanism, if any. If
   * persistence is not supported, this method returns without doing anything.
   *
   * @throws IOException if an input/output error occurs
   */
  private void doUnload() throws IOException {
    QueryService querySvc = getSessionCache().getCache().getQueryService();
    Context context = getTheContext();

    if (context == null) {
      return;
    }

    String regionName;
    if (getRegionName().startsWith(SEPARATOR)) {
      regionName = getRegionName();
    } else {
      regionName = SEPARATOR + getRegionName();
    }

    Query query = querySvc.newQuery("select s.id from " + regionName
        + " as s where s.contextName = '" + context.getPath() + "'");

    if (getLogger().isDebugEnabled()) {
      getLogger().debug("Query: " + query.getQueryString());
    }

    SelectResults<String> results;
    try {
      results = uncheckedCast(query.execute());
    } catch (Exception ex) {
      getLogger().error("Unable to perform query during doUnload", ex);
      return;
    }

    if (results.isEmpty()) {
      getLogger().debug("No sessions to unload for context " + context.getPath());
      return; // nothing to do
    }

    // Open an output stream to the specified pathname, if any
    File store = sessionStore(context.getPath());
    if (store == null) {
      return;
    }
    if (getLogger().isDebugEnabled()) {
      getLogger().debug("Unloading sessions to " + store.getAbsolutePath());
    }
    FileOutputStream fos = null;
    BufferedOutputStream bos = null;
    final ObjectOutputStream oos;
    boolean error = false;
    try {
      fos = getFileOutputStream(store);
      bos = getBufferedOutputStream(fos);
      oos = getObjectOutputStream(bos);
    } catch (IOException e) {
      error = true;
      getLogger().error("Exception unloading sessions", e);
      throw e;
    } finally {
      if (error) {
        if (bos != null) {
          try {
            bos.close();
          } catch (IOException ioe) {
            // Ignore
          }
        }
        if (fos != null) {
          try {
            fos.close();
          } catch (IOException ioe) {
            // Ignore
          }
        }
      }
    }

    ArrayList<DeltaSessionInterface> list = new ArrayList<>();
    for (final String id : results) {
      DeltaSessionInterface session = (DeltaSessionInterface) findSession(id);
      if (session != null) {
        list.add(session);
      }
    }

    // Write the number of active sessions, followed by the details
    if (getLogger().isDebugEnabled()) {
      getLogger().debug("Unloading " + list.size() + " sessions");
    }
    try {
      writeToObjectOutputStream(oos, list);
      for (DeltaSessionInterface session : list) {
        if (session instanceof StandardSession) {
          StandardSession standardSession = (StandardSession) session;
          standardSession.passivate();
          standardSession.writeObjectData(oos);
        } else {
          // All DeltaSessionInterfaces as of Geode 1.0 should be based on StandardSession
          throw new IOException("Session should be of type StandardSession");
        }
      }
    } catch (IOException e) {
      getLogger().error("Exception unloading sessions", e);
      try {
        oos.close();
      } catch (IOException f) {
        // Ignore
      }
      throw e;
    }

    // Flush and close the output stream
    try {
      oos.flush();
    } finally {
      try {
        oos.close();
      } catch (IOException f) {
        // Ignore
      }
    }

    // Locally destroy the sessions we just wrote
    if (getSessionCache().isClientServer()) {
      for (DeltaSessionInterface session : list) {
        if (getLogger().isDebugEnabled()) {
          getLogger().debug("Locally destroying session " + session.getId());
        }
        try {
          getSessionCache().getOperatingRegion().localDestroy(session.getId());
        } catch (EntryNotFoundException ex) {
          // This can be thrown if an entry is evicted during or immediately after a session is
          // written
          // to disk. This isn't a problem, but the resulting exception messages can be confusing in
          // testing
        }
      }
    }

    if (getLogger().isDebugEnabled()) {
      getLogger().debug("Unloading complete");
    }
  }

  /**
   * Load any currently active sessions that were previously unloaded to the appropriate persistence
   * mechanism, if any. If persistence is not supported, this method returns without doing
   * anything.
   *
   * @throws ClassNotFoundException if a serialized class cannot be found during the reload
   * @throws IOException if an input/output error occurs
   */
  private void doLoad() throws ClassNotFoundException, IOException {
    Context context = getTheContext();
    if (context == null) {
      return;
    }

    // Open an input stream to the specified pathname, if any
    File store = sessionStore(context.getPath());
    if (store == null) {
      getLogger().debug("No session store file found");
      return;
    }
    if (getLogger().isDebugEnabled()) {
      getLogger().debug("Loading sessions from " + store.getAbsolutePath());
    }
    FileInputStream fis = null;
    BufferedInputStream bis = null;
    ObjectInputStream ois;
    Loader loader = null;
    ClassLoader classLoader = null;
    try {
      fis = getFileInputStream(store);
      bis = getBufferedInputStream(fis);
      if (getTheContext() != null) {
        loader = getTheContext().getLoader();
      }
      if (loader != null) {
        classLoader = loader.getClassLoader();
      }
      if (classLoader != null) {
        if (getLogger().isDebugEnabled()) {
          getLogger().debug("Creating custom object input stream for class loader");
        }
        ois = new CustomObjectInputStream(bis, classLoader);
      } else {
        if (getLogger().isDebugEnabled()) {
          getLogger().debug("Creating standard object input stream");
        }
        ois = getObjectInputStream(bis);
      }
    } catch (FileNotFoundException e) {
      if (getLogger().isDebugEnabled()) {
        getLogger().debug("No persisted data file found");
      }
      return;
    } catch (IOException e) {
      getLogger().error("Exception loading sessions", e);
      try {
        fis.close();
      } catch (IOException f) {
        // Ignore
      }
      try {
        bis.close();
      } catch (IOException f) {
        // Ignore
      }
      throw e;
    }

    // Load the previously unloaded active sessions
    try {
      int n = getSessionCountFromObjectInputStream(ois);
      if (getLogger().isDebugEnabled()) {
        getLogger().debug("Loading " + n + " persisted sessions");
      }
      for (int i = 0; i < n; i++) {
        StandardSession session = getNewSession();
        session.readObjectData(ois);
        session.setManager(this);

        final Region<String, HttpSession> region = getSessionCache().getOperatingRegion();
        final DeltaSessionInterface existingSession =
            (DeltaSessionInterface) region.get(session.getId());
        // Check whether the existing session is newer
        if (existingSession != null
            && existingSession.getLastAccessedTime() > session.getLastAccessedTime()) {
          if (getLogger().isDebugEnabled()) {
            getLogger().debug("Loaded session " + session.getId() + " is older than cached copy");
          }
          continue;
        }

        // Check whether the new session has already expired
        if (!session.isValid()) {
          if (getLogger().isDebugEnabled()) {
            getLogger().debug("Loaded session " + session.getId() + " is invalid");
          }
          continue;
        }

        getLogger().debug("Loading session " + session.getId());
        session.activate();
        add(session);
      }
    } catch (ClassNotFoundException | IOException e) {
      getLogger().error(e);
      try {
        ois.close();
      } catch (IOException f) {
        // Ignore
      }
      throw e;
    } finally {
      // Close the input stream
      try {
        ois.close();
      } catch (IOException f) {
        // ignored
      }

      // Delete the persistent storage file
      if (store.exists()) {
        if (!store.delete()) {
          getLogger().warn("Couldn't delete persistent storage file " + store.getAbsolutePath());
        }
      }
    }
  }

  /**
   * Return a File object representing the pathname to our persistence file, if any.
   */
  private File sessionStore(String ctxPath) {
    String storeDir = getSystemPropertyValue(catalinaBaseSystemProperty);
    if (storeDir == null || storeDir.isEmpty()) {
      storeDir = getSystemPropertyValue(javaTempDirSystemProperty);
    } else {
      storeDir += getSystemPropertyValue(fileSeparatorSystemProperty) + "temp";
    }

    return getFileAtPath(storeDir, ctxPath);
  }

  String getSystemPropertyValue(String propertyKey) {
    return System.getProperty(propertyKey);
  }

  File getFileAtPath(String storeDir, String ctxPath) {
    return (new File(storeDir, ctxPath.replaceAll("/", "_") + ".sessions.ser"));
  }

  FileInputStream getFileInputStream(File file) throws FileNotFoundException {
    return new FileInputStream(file.getAbsolutePath());
  }

  BufferedInputStream getBufferedInputStream(FileInputStream fis) {
    return new BufferedInputStream(fis);
  }

  ObjectInputStream getObjectInputStream(BufferedInputStream bis) throws IOException {
    return new ObjectInputStream(bis);
  }

  FileOutputStream getFileOutputStream(File file) throws FileNotFoundException {
    return new FileOutputStream(file.getAbsolutePath());
  }

  BufferedOutputStream getBufferedOutputStream(FileOutputStream fos) {
    return new BufferedOutputStream(fos);
  }

  ObjectOutputStream getObjectOutputStream(BufferedOutputStream bos) throws IOException {
    return new ObjectOutputStream(bos);
  }

  void writeToObjectOutputStream(ObjectOutputStream oos, List<?> listToWrite) throws IOException {
    oos.writeObject(listToWrite.size());
  }

  int getSessionCountFromObjectInputStream(ObjectInputStream ois)
      throws IOException, ClassNotFoundException {
    return (Integer) ois.readObject();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "[" + "container="
        + getTheContext() + "; regionName=" + regionName
        + "; regionAttributesId=" + regionAttributesId + "]";
  }

  String getContextName() {
    return getTheContext().getName();
  }

  public Context getTheContext() {
    if (getContainer() instanceof Context) {
      return (Context) getContainer();
    } else {
      getLogger().error("Unable to unload sessions - container is of type "
          + getContainer().getClass().getName() + " instead of StandardContext");
      return null;
    }
  }
}
