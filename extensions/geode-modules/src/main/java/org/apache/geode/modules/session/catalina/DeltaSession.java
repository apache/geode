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


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.servlet.http.HttpSession;

import org.apache.catalina.Manager;
import org.apache.catalina.ha.session.SerializablePrincipal;
import org.apache.catalina.realm.GenericPrincipal;
import org.apache.catalina.security.SecurityUtil;
import org.apache.catalina.session.StandardSession;
import org.apache.juli.logging.Log;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.Delta;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.size.Sizeable;
import org.apache.geode.internal.util.BlobHelper;
import org.apache.geode.modules.gatewaydelta.GatewayDelta;
import org.apache.geode.modules.gatewaydelta.GatewayDeltaEvent;
import org.apache.geode.modules.session.catalina.internal.DeltaSessionAttributeEvent;
import org.apache.geode.modules.session.catalina.internal.DeltaSessionAttributeEventBatch;
import org.apache.geode.modules.session.catalina.internal.DeltaSessionDestroyAttributeEvent;
import org.apache.geode.modules.session.catalina.internal.DeltaSessionUpdateAttributeEvent;

@SuppressWarnings("serial")
public class DeltaSession extends StandardSession
    implements DataSerializable, Delta, GatewayDelta, Sizeable, DeltaSessionInterface {

  private transient Region<String, HttpSession> operatingRegion;

  private String sessionRegionName;

  private String contextName;

  private boolean hasDelta;

  private boolean applyRemotely;

  private boolean enableGatewayDeltaReplication;

  private final transient Object changeLock = new Object();

  private final ArrayList<DeltaSessionAttributeEvent> eventQueue = new ArrayList<>();

  private transient GatewayDeltaEvent currentGatewayDeltaEvent;

  private transient boolean expired = false;

  /**
   * @deprecated No replacement. Always prefer deserialized form.
   */
  @Deprecated
  private transient boolean preferDeserializedForm = true;

  private byte[] serializedPrincipal;

  private static final Field cachedField;

  static {
    try {
      cachedField = StandardSession.class.getDeclaredField("attributes");
      cachedField.setAccessible(true);
    } catch (NoSuchFieldException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Construct a new <code>Session</code> associated with no <code>Manager</code>. The
   * <code>Manager</code> will be assigned later using {@link #setOwner(Object)}.
   */
  public DeltaSession() {
    super(null);
  }

  /**
   * Construct a new Session associated with the specified Manager.
   *
   * @param manager The manager with which this Session is associated
   */
  public DeltaSession(Manager manager) {
    super(manager);
    setOwner(manager);
  }

  /**
   * Return the <code>HttpSession</code> for which this object is the facade.
   */
  @Override
  public HttpSession getSession() {
    if (facade == null) {
      if (isPackageProtectionEnabled()) {
        final DeltaSession fsession = this;
        facade = getNewFacade(fsession);
      } else {
        facade = new DeltaSessionFacade(this);
      }
    }
    return (facade);
  }

  @Override
  public Principal getPrincipal() {
    final DeltaSessionManager<?> deltaSessionManager = getDeltaSessionManager();

    if (principal == null && serializedPrincipal != null) {
      final Log logger = deltaSessionManager.getLogger();

      final SerializablePrincipal sp;
      try {
        sp = (SerializablePrincipal) BlobHelper.deserializeBlob(serializedPrincipal);
      } catch (Exception e) {
        logger.warn(this
            + ": Serialized principal contains a byte[] that cannot be deserialized due to the following exception",
            e);
        return null;
      }

      principal = sp.getPrincipal(deltaSessionManager.getTheContext().getRealm());

      if (logger.isDebugEnabled()) {
        logger.debug(this + ": Deserialized principal: " + principal);
      }
    }

    return principal;
  }

  @Override
  public void setPrincipal(Principal principal) {
    super.setPrincipal(principal);

    // Put the session into the region to serialize the principal
    if (getManager() != null) {
      // TODO convert this to a delta
      getManager().add(this);
      final Log logger = getDeltaSessionManager().getLogger();
      if (logger.isDebugEnabled()) {
        logger.debug(this + ": Cached principal: " + principal);
      }
    }
  }

  private byte[] getSerializedPrincipal() {
    if (serializedPrincipal == null) {
      if (principal != null && principal instanceof GenericPrincipal) {
        GenericPrincipal gp = (GenericPrincipal) principal;
        SerializablePrincipal sp = SerializablePrincipal.createPrincipal(gp);
        serializedPrincipal = serialize(sp);
        if (manager != null) {
          final Log logger = getDeltaSessionManager().getLogger();
          if (logger.isDebugEnabled()) {
            logger.debug(this + ": Serialized principal: " + sp);
          }
        }
      }
    }
    return serializedPrincipal;
  }

  private Region<String, HttpSession> getOperatingRegion() {
    // This region shouldn't be null when it is needed.
    // It should have been set by the setOwner method.
    return operatingRegion;
  }

  boolean isCommitEnabled() {
    return getDeltaSessionManager().isCommitValveEnabled();
  }

  @Override
  public GatewayDeltaEvent getCurrentGatewayDeltaEvent() {
    return currentGatewayDeltaEvent;
  }

  @Override
  public void setCurrentGatewayDeltaEvent(GatewayDeltaEvent currentGatewayDeltaEvent) {
    this.currentGatewayDeltaEvent = currentGatewayDeltaEvent;
  }

  @Override
  public void setOwner(Object manager) {
    if (manager instanceof DeltaSessionManager) {
      DeltaSessionManager<?> sessionManager = (DeltaSessionManager<?>) manager;
      this.manager = sessionManager;
      initializeRegion(sessionManager);
      hasDelta = false;
      applyRemotely = false;
      enableGatewayDeltaReplication = sessionManager.getEnableGatewayDeltaReplication();
      setOwnerDeprecated(sessionManager);

      // Initialize transient variables
      if (listeners == null) {
        listeners = new ArrayList<>();
      }

      if (notes == null) {
        notes = new Hashtable<>();
      }

      contextName = sessionManager.getContextName();
    } else {
      throw new IllegalArgumentException(this + ": The Manager must be an AbstractManager");
    }
  }

  @SuppressWarnings("deprecation")
  private void setOwnerDeprecated(DeltaSessionManager<?> sessionManager) {
    preferDeserializedForm = sessionManager.getPreferDeserializedForm();
  }

  private void checkBackingCacheAvailable() {
    if (!((SessionManager) getManager()).isBackingCacheAvailable()) {
      throw new IllegalStateException("No backing cache server is available.");
    }
  }

  @Override
  public void setAttribute(String name, Object value, boolean notify) {

    checkBackingCacheAvailable();

    synchronized (changeLock) {
      // Serialize the value
      byte[] serializedValue = serialize(value);

      // Store the attribute locally
      if (preferDeserializedForm) {
        if (notify) {
          deserializeAttributeInternal(name);
        }
        super.setAttribute(name, value, true);
      } else {
        super.setAttribute(name, serializedValue, true);
      }

      if (serializedValue == null) {
        return;
      }

      // Create the update attribute message
      DeltaSessionAttributeEvent event =
          new DeltaSessionUpdateAttributeEvent(name, serializedValue);
      queueAttributeEvent(event, true);

      // Distribute the update
      if (!isCommitEnabled()) {
        putInRegion(getOperatingRegion(), true, null);
      }
    }
  }

  @Override
  public void removeAttribute(String name, boolean notify) {
    checkBackingCacheAvailable();
    if (expired) {
      return;
    }
    synchronized (changeLock) {
      if (notify && preferDeserializedForm) {
        deserializeAttributeInternal(name);
      }

      // Remove the attribute locally
      super.removeAttribute(name, notify);

      // Create the destroy attribute message
      DeltaSessionAttributeEvent event = new DeltaSessionDestroyAttributeEvent(name);
      queueAttributeEvent(event, true);

      // Distribute the update
      if (!isCommitEnabled()) {
        putInRegion(getOperatingRegion(), true, null);
      }
    }
  }

  @Override
  protected void removeAttributeInternal(String name, boolean notify) {
    if (notify && preferDeserializedForm) {
      deserializeAttributeInternal(name);
    }

    super.removeAttributeInternal(name, notify);
  }

  protected Object getAttributeInternal(final String name) {
    if (null == name) {
      return null;
    }
    return getAttributes().get(name);
  }

  protected void setAttributeInternal(String name, Object value) {
    if (null == name) {
      return;
    }
    getAttributes().put(name, value);
  }

  @Override
  public Object getAttribute(String name) {
    checkBackingCacheAvailable();
    Object value = deserializeAttribute(name, super.getAttribute(name), preferDeserializedForm);

    // Touch the session region if necessary. This is an asynchronous operation
    // that prevents the session region from prematurely expiring a session that
    // is only getting attributes.
    getDeltaSessionManager().addSessionToTouch(getId());

    return value;
  }

  protected void deserializeAttributeInternal(final String name) {
    deserializeAttribute(name, getAttributeInternal(name), true);
  }

  private Object deserializeAttribute(final String name, final Object value, final boolean store) {
    // If the attribute is a byte[] (meaning it came from the server),
    // deserialize it and add it to attributes map before returning it.
    if (value instanceof byte[]) {
      try {
        final Object deserialized = BlobHelper.deserializeBlob((byte[]) value);
        if (store) {
          setAttributeInternal(name, deserialized);
        }
        return deserialized;
      } catch (final Exception e) {
        getDeltaSessionManager().getLogger().warn(
            this + ": Attribute named " + name
                + " contains a byte[] that cannot be deserialized due to the following exception",
            e);
      }
    }

    return value;
  }

  private DeltaSessionManager<?> getDeltaSessionManager() {
    return (DeltaSessionManager<?>) getManager();
  }

  Object getAttributeWithoutDeserialize(String name) {
    return super.getAttribute(name);
  }

  @Override
  public void invalidate() {
    super.invalidate();
    getDeltaSessionManager().getStatistics().incSessionsInvalidated();
  }

  @Override
  public void processExpired() {
    DeltaSessionManager<?> manager = getDeltaSessionManager();
    if (manager != null && manager.getLogger() != null && manager.getLogger().isDebugEnabled()) {
      getDeltaSessionManager().getLogger().debug(this + ": Expired");
    }

    // Set expired (so region.destroy is not called again)
    setExpired(true);

    // Do expire processing
    super.expire(true);

    // Update statistics
    if (manager != null) {
      manager.getStatistics().incSessionsExpired();
    }
  }

  @Override
  public void expire(boolean notify) {
    if (notify) {
      getOperatingRegion().destroy(getId(), this);
    } else {
      super.expire(false);
    }
  }

  @Override
  public void setMaxInactiveInterval(int interval) {
    super.setMaxInactiveInterval(interval);

    if (!isCommitEnabled() && id != null) {
      putInRegion(getOperatingRegion(), true, null);
    }
  }

  @Override
  public void localUpdateAttribute(String name, Object value) {
    if (manager == null) {
      // Name cannot be null
      if (name == null) {
        throw new IllegalArgumentException(sm.getString("standardSession.setAttribute.namenull"));
      }

      // Null value is the same as removeAttribute()
      if (value == null) {
        removeAttribute(name);
        return;
      }

      // Validate our current state
      if (!isValidInternal()) {
        throw new IllegalStateException(
            sm.getString("standardSession.setAttribute.ise", getIdInternal()));
      }

      // Replace or add this attribute
      getAttributes().put(name, value);
    } else {
      // don't do notification since this is a replication
      super.setAttribute(name, value, false);
    }
  }

  @Override
  public void localDestroyAttribute(String name) {
    super.removeAttribute(name, false); // don't do notification since this is a replication
  }

  @Override
  public void applyAttributeEvents(Region<String, DeltaSessionInterface> region,
      List<DeltaSessionAttributeEvent> events) {
    for (DeltaSessionAttributeEvent event : events) {
      event.apply(this);
      queueAttributeEvent(event, false);
    }

    putInRegion(region, false, true);
  }

  private void initializeRegion(DeltaSessionManager<?> sessionManager) {
    // Get the session region name
    sessionRegionName = sessionManager.getRegionName();

    // Get the operating region.
    // If a P2P manager is used, then this will be a local region fronting the
    // session region if local cache is enabled; otherwise, it will be the
    // session region itself.
    // If a CS manager is used, it will be the session proxy region.
    operatingRegion = sessionManager.getSessionCache().getOperatingRegion();
    if (sessionManager.getLogger().isDebugEnabled()) {
      sessionManager.getLogger().debug(this + ": Set operating region: " + operatingRegion);
    }
  }

  private void queueAttributeEvent(DeltaSessionAttributeEvent event,
      boolean checkAddToCurrentGatewayDelta) {
    // Add to current gateway delta if necessary
    if (checkAddToCurrentGatewayDelta) {
      // If the manager has enabled gateway delta replication and is a P2P
      // manager, the GatewayDeltaForwardCacheListener will be invoked in this
      // VM. Add the event to the currentDelta.
      if (enableGatewayDeltaReplication && getDeltaSessionManager().isPeerToPeer()) {
        // If commit is not enabled, add the event to the current batch; else,
        // the current batch will be initialized to the events in the queue will
        // be added at commit time.
        if (!isCommitEnabled()) {
          List<DeltaSessionAttributeEvent> events = new ArrayList<>();
          events.add(event);
          currentGatewayDeltaEvent =
              new DeltaSessionAttributeEventBatch(sessionRegionName, id, events);
        }
      }
    }
    addEventToEventQueue(event);
  }

  private void putInRegion(Region region, boolean applyRemotely,
      Object callbackArgument) {
    hasDelta = true;
    this.applyRemotely = applyRemotely;
    region.put(id, this, callbackArgument);
    eventQueue.clear();
  }

  @Override
  public void commit() {
    if (!isValidInternal()) {
      throw new IllegalStateException("commit: Session " + getId() + " already invalidated");
    }
    // (STRING_MANAGER.getString("deltaSession.commit.ise", getId()));

    synchronized (changeLock) {
      // Jens - there used to be a check to only perform this if the queue is
      // empty, but we want this to always run so that the lastAccessedTime
      // will be updated even when no attributes have been changed.
      if (enableGatewayDeltaReplication && getDeltaSessionManager().isPeerToPeer()) {
        setCurrentGatewayDeltaEvent(
            new DeltaSessionAttributeEventBatch(sessionRegionName, id, eventQueue));
      }
      hasDelta = true;
      applyRemotely = true;
      putInRegion(getOperatingRegion(), true, null);
      eventQueue.clear();
    }
  }

  @Override
  public void abort() {
    synchronized (changeLock) {
      eventQueue.clear();
    }
  }

  private void setExpired(boolean expired) {
    this.expired = expired;
  }

  @Override
  public boolean getExpired() {
    return expired;
  }

  @Override
  public String getContextName() {
    return contextName;
  }

  @Override
  public boolean hasDelta() {
    return hasDelta;
  }

  @Override
  public void toDelta(DataOutput out) throws IOException {
    // Write whether to apply the changes to another DS if necessary
    out.writeBoolean(applyRemotely);

    // Write the events
    DataSerializer.writeArrayList(eventQueue, out);

    out.writeLong(lastAccessedTime);
    out.writeInt(maxInactiveInterval);
  }

  @Override
  public void fromDelta(DataInput in) throws IOException, InvalidDeltaException {
    // Read whether to apply the changes to another DS if necessary
    applyRemotely = in.readBoolean();

    // Read the events
    List<DeltaSessionAttributeEvent> events;
    try {
      events = DataSerializer.readArrayList(in);
    } catch (ClassNotFoundException e) {
      throw new InvalidDeltaException(e);
    }

    // This allows for backwards compatibility with 2.1 clients
    if (((InputStream) in).available() > 0) {
      lastAccessedTime = in.readLong();
      maxInactiveInterval = in.readInt();
    }

    // Iterate and apply the events
    for (DeltaSessionAttributeEvent event : events) {
      event.apply(this);
    }

    // Add the events to the gateway delta region if necessary
    if (enableGatewayDeltaReplication && applyRemotely) {
      setCurrentGatewayDeltaEvent(
          new DeltaSessionAttributeEventBatch(sessionRegionName, id, events));
    }

    // Access it to set the last accessed time. End access it to set not new.
    access();
    endAccess();
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    // Write the StandardSession state
    DataSerializer.writeString(id, out);
    out.writeLong(creationTime);
    out.writeLong(lastAccessedTime);
    out.writeLong(thisAccessedTime);
    out.writeInt(maxInactiveInterval);
    out.writeBoolean(isNew);
    out.writeBoolean(isValid);
    DataSerializer.writeObject(getSerializedAttributes(), out);
    DataSerializer.writeByteArray(getSerializedPrincipal(), out);

    // Write the DeltaSession state
    out.writeBoolean(enableGatewayDeltaReplication);
    DataSerializer.writeString(sessionRegionName, out);

    DataSerializer.writeString(contextName, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    // Read the StandardSession state
    id = DataSerializer.readString(in);
    creationTime = in.readLong();
    lastAccessedTime = in.readLong();
    thisAccessedTime = in.readLong();
    maxInactiveInterval = in.readInt();
    isNew = in.readBoolean();
    isValid = in.readBoolean();
    readInAttributes(in);
    serializedPrincipal = DataSerializer.readByteArray(in);

    // Read the DeltaSession state
    enableGatewayDeltaReplication = in.readBoolean();
    sessionRegionName = DataSerializer.readString(in);

    // This allows for backwards compatibility with 2.1 clients
    if (((InputStream) in).available() > 0) {
      contextName = DataSerializer.readString(in);
    }

    // Initialize the transients if necessary
    if (listeners == null) {
      listeners = new ArrayList<>();
    }

    if (notes == null) {
      notes = new Hashtable<>();
    }
  }

  private void readInAttributes(DataInput in) throws IOException, ClassNotFoundException {
    ConcurrentHashMap<Object, Object> map = DataSerializer.readObject(in);
    try {
      Field field = getAttributesFieldObject();
      field.set(this, map);
    } catch (IllegalAccessException e) {
      logError(e);
      throw new IllegalStateException(e);
    }
  }

  private Field getAttributesFieldObject() {
    return cachedField;
  }

  private void logError(Exception e) {
    final DeltaSessionManager<?> deltaSessionManager = getDeltaSessionManager();
    if (deltaSessionManager != null) {
      deltaSessionManager.getLogger().error(e);
    }
  }

  @Override
  public int getSizeInBytes() {
    int size = 0;
    @SuppressWarnings("unchecked")
    Enumeration<String> attributeNames = getAttributeNames();
    while (attributeNames.hasMoreElements()) {
      // Don't use getAttribute() because we don't want to deserialize the value.
      Object value = getAttributeWithoutDeserialize(attributeNames.nextElement());
      if (value instanceof byte[]) {
        size += ((byte[]) value).length;
      }
    }

    return size;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private Map<String, byte[]> getSerializedAttributes() {
    // Iterate the values and serialize them if necessary before sending them to the server. This
    // makes the application classes unnecessary on the server.
    Map<String, byte[]> serializedAttributes = new ConcurrentHashMap<>();
    for (Object o : getAttributes().entrySet()) {
      Map.Entry<String, Object> entry = (Map.Entry<String, Object>) o;
      Object value = entry.getValue();
      byte[] serializedValue = value instanceof byte[] ? (byte[]) value : serialize(value);
      serializedAttributes.put(entry.getKey(), serializedValue);
    }
    return serializedAttributes;
  }

  protected Map getAttributes() {
    try {
      Field field = getAttributesFieldObject();
      return (Map) field.get(this);
    } catch (IllegalAccessException e) {
      logError(e);
    }
    throw new IllegalStateException("Unable to access attributes field");
  }

  byte[] serialize(Object obj) {
    byte[] serializedValue = null;
    try {
      serializedValue = serializeViaBlobHelper(obj);
    } catch (IOException e) {
      String builder = this + ": Object " + obj
          + " cannot be serialized due to the following exception";
      getDeltaSessionManager().getLogger().warn(
          builder, e);
    }
    return serializedValue;
  }

  byte[] serializeViaBlobHelper(Object obj) throws IOException {
    return BlobHelper.serializeToBlob(obj);
  }


  @Override
  public String toString() {
    return "DeltaSession[" + "id=" + getId()
        + "; context=" + contextName + "; sessionRegionName="
        + sessionRegionName + "; operatingRegionName="
        + (getOperatingRegion() == null ? "unset" : getOperatingRegion().getFullPath())
        + "]";
  }

  // Helper methods to enable better unit testing
  DeltaSessionFacade getNewFacade(DeltaSessionInterface fSession) {
    return AccessController.doPrivileged(
        (PrivilegedAction<DeltaSessionFacade>) () -> new DeltaSessionFacade(fSession));
  }

  boolean isPackageProtectionEnabled() {
    return SecurityUtil.isPackageProtectionEnabled();
  }

  void addEventToEventQueue(DeltaSessionAttributeEvent event) {
    eventQueue.add(event);
  }
}
