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

  private final List<DeltaSessionAttributeEvent> eventQueue = new ArrayList<>();

  private transient GatewayDeltaEvent currentGatewayDeltaEvent;

  private transient boolean expired = false;

  private transient boolean preferDeserializedForm = true;

  private byte[] serializedPrincipal;

  private static Field cachedField;

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
  @SuppressWarnings("unchecked")
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
    if (this.principal == null && this.serializedPrincipal != null) {
      SerializablePrincipal sp;
      try {
        sp = (SerializablePrincipal) BlobHelper.deserializeBlob(this.serializedPrincipal);
      } catch (Exception e) {
        String builder = this
            + ": Serialized principal contains a byte[] that cannot be deserialized due to the following exception";
        ((DeltaSessionManager) getManager()).getLogger().warn(builder, e);
        return null;
      }
      this.principal =
          sp.getPrincipal(((DeltaSessionManager) this.manager).getTheContext().getRealm());
      if (getManager() != null) {
        DeltaSessionManager mgr = (DeltaSessionManager) getManager();
        if (mgr.getLogger().isDebugEnabled()) {
          mgr.getLogger().debug(this + ": Deserialized principal: " + this.principal);
          // mgr.logCurrentStack();
        }
      }
    }
    return this.principal;
  }

  @Override
  public void setPrincipal(Principal principal) {
    super.setPrincipal(principal);

    // Put the session into the region to serialize the principal
    if (getManager() != null) {
      // TODO convert this to a delta
      getManager().add(this);
      DeltaSessionManager mgr = (DeltaSessionManager) getManager();
      if (mgr.getLogger().isDebugEnabled()) {
        mgr.getLogger().debug(this + ": Cached principal: " + principal);
        // mgr.logCurrentStack();
      }
    }
  }

  private byte[] getSerializedPrincipal() {
    if (this.serializedPrincipal == null) {
      if (this.principal != null && this.principal instanceof GenericPrincipal) {
        GenericPrincipal gp = (GenericPrincipal) this.principal;
        SerializablePrincipal sp = SerializablePrincipal.createPrincipal(gp);
        this.serializedPrincipal = serialize(sp);
        if (manager != null) {
          DeltaSessionManager mgr = (DeltaSessionManager) getManager();
          if (mgr.getLogger().isDebugEnabled()) {
            mgr.getLogger().debug(this + ": Serialized principal: " + sp);
            // mgr.logCurrentStack();
          }
        }
      }
    }
    return this.serializedPrincipal;
  }

  private Region<String, HttpSession> getOperatingRegion() {
    // This region shouldn't be null when it is needed.
    // It should have been set by the setOwner method.
    return this.operatingRegion;
  }

  boolean isCommitEnabled() {
    DeltaSessionManager mgr = (DeltaSessionManager) getManager();
    return mgr.isCommitValveEnabled();
  }

  @Override
  public GatewayDeltaEvent getCurrentGatewayDeltaEvent() {
    return this.currentGatewayDeltaEvent;
  }

  @Override
  public void setCurrentGatewayDeltaEvent(GatewayDeltaEvent currentGatewayDeltaEvent) {
    this.currentGatewayDeltaEvent = currentGatewayDeltaEvent;
  }

  @Override
  public void setOwner(Object manager) {
    if (manager instanceof DeltaSessionManager) {
      DeltaSessionManager sessionManager = (DeltaSessionManager) manager;
      this.manager = sessionManager;
      initializeRegion(sessionManager);
      this.hasDelta = false;
      this.applyRemotely = false;
      this.enableGatewayDeltaReplication = sessionManager.getEnableGatewayDeltaReplication();
      this.preferDeserializedForm = sessionManager.getPreferDeserializedForm();

      // Initialize transient variables
      if (this.listeners == null) {
        this.listeners = new ArrayList();
      }

      if (this.notes == null) {
        this.notes = new Hashtable();
      }

      contextName = ((DeltaSessionManager) manager).getContextName();
    } else {
      throw new IllegalArgumentException(this + ": The Manager must be an AbstractManager");
    }
  }

  private void checkBackingCacheAvailable() {
    if (!((SessionManager) getManager()).isBackingCacheAvailable()) {
      throw new IllegalStateException("No backing cache server is available.");
    }
  }

  @Override
  public void setAttribute(String name, Object value, boolean notify) {

    checkBackingCacheAvailable();

    synchronized (this.changeLock) {
      // Serialize the value
      byte[] serializedValue = serialize(value);

      // Store the attribute locally
      if (this.preferDeserializedForm) {
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
    synchronized (this.changeLock) {
      // Remove the attribute locally
      super.removeAttribute(name, true);

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
  public Object getAttribute(String name) {
    checkBackingCacheAvailable();
    Object value = super.getAttribute(name);

    // If the attribute is a byte[] (meaning it came from the server),
    // deserialize it and add it to attributes map before returning it.
    if (value instanceof byte[]) {
      try {
        value = BlobHelper.deserializeBlob((byte[]) value);
      } catch (Exception e) {
        String builder = this + ": Attribute named " + name
            + " contains a byte[] that cannot be deserialized due to the following exception";
        ((DeltaSessionManager) getManager()).getLogger().warn(
            builder, e);
      }
      if (this.preferDeserializedForm) {
        localUpdateAttribute(name, value);
      }
    }

    // Touch the session region if necessary. This is an asynchronous operation
    // that prevents the session region from prematurely expiring a session that
    // is only getting attributes.
    ((DeltaSessionManager) getManager()).addSessionToTouch(getId());

    return value;
  }

  Object getAttributeWithoutDeserialize(String name) {
    return super.getAttribute(name);
  }

  @Override
  public void invalidate() {
    super.invalidate();
    // getOperatingRegion().destroy(this.id, true); // already done in super (remove)
    ((DeltaSessionManager) getManager()).getStatistics().incSessionsInvalidated();
  }

  @Override
  public void processExpired() {
    DeltaSessionManager manager = (DeltaSessionManager) getManager();
    if (manager != null && manager.getLogger() != null && manager.getLogger().isDebugEnabled()) {
      ((DeltaSessionManager) getManager()).getLogger().debug(this + ": Expired");
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
      getOperatingRegion().destroy(this.getId(), this);
    } else {
      super.expire(false);
    }
  }

  @Override
  public void setMaxInactiveInterval(int interval) {
    super.setMaxInactiveInterval(interval);
  }

  @Override
  public void localUpdateAttribute(String name, Object value) {
    super.setAttribute(name, value, false); // don't do notification since this is a replication
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

  private void initializeRegion(DeltaSessionManager sessionManager) {
    // Get the session region name
    this.sessionRegionName = sessionManager.getRegionName();

    // Get the operating region.
    // If a P2P manager is used, then this will be a local region fronting the
    // session region if local cache is enabled; otherwise, it will be the
    // session region itself.
    // If a CS manager is used, it will be the session proxy region.
    this.operatingRegion = sessionManager.getSessionCache().getOperatingRegion();
    if (sessionManager.getLogger().isDebugEnabled()) {
      sessionManager.getLogger().debug(this + ": Set operating region: " + this.operatingRegion);
    }
  }

  private void queueAttributeEvent(DeltaSessionAttributeEvent event,
      boolean checkAddToCurrentGatewayDelta) {
    // Add to current gateway delta if necessary
    if (checkAddToCurrentGatewayDelta) {
      // If the manager has enabled gateway delta replication and is a P2P
      // manager, the GatewayDeltaForwardCacheListener will be invoked in this
      // VM. Add the event to the currentDelta.
      DeltaSessionManager mgr = (DeltaSessionManager) this.manager;
      if (this.enableGatewayDeltaReplication && mgr.isPeerToPeer()) {
        // If commit is not enabled, add the event to the current batch; else,
        // the current batch will be initialized to the events in the queue will
        // be added at commit time.
        if (!isCommitEnabled()) {
          List<DeltaSessionAttributeEvent> events = new ArrayList<>();
          events.add(event);
          this.currentGatewayDeltaEvent =
              new DeltaSessionAttributeEventBatch(this.sessionRegionName, this.id, events);
        }
      }
    }
    addEventToEventQueue(event);
  }

  @SuppressWarnings("unchecked")
  private void putInRegion(Region region, boolean applyRemotely, Object callbackArgument) {
    this.hasDelta = true;
    this.applyRemotely = applyRemotely;
    region.put(this.id, this, callbackArgument);
    this.eventQueue.clear();
  }

  @Override
  public void commit() {
    if (!isValidInternal())
      throw new IllegalStateException("commit: Session " + getId() + " already invalidated");
    // (STRING_MANAGER.getString("deltaSession.commit.ise", getId()));

    synchronized (this.changeLock) {
      // Jens - there used to be a check to only perform this if the queue is
      // empty, but we want this to always run so that the lastAccessedTime
      // will be updated even when no attributes have been changed.
      DeltaSessionManager mgr = (DeltaSessionManager) this.manager;
      if (this.enableGatewayDeltaReplication && mgr.isPeerToPeer()) {
        setCurrentGatewayDeltaEvent(
            new DeltaSessionAttributeEventBatch(this.sessionRegionName, this.id, this.eventQueue));
      }
      this.hasDelta = true;
      this.applyRemotely = true;
      putInRegion(getOperatingRegion(), true, null);
      this.eventQueue.clear();
    }
  }

  @Override
  public void abort() {
    synchronized (this.changeLock) {
      this.eventQueue.clear();
    }
  }

  private void setExpired(boolean expired) {
    this.expired = expired;
  }

  @Override
  public boolean getExpired() {
    return this.expired;
  }

  @Override
  public String getContextName() {
    return contextName;
  }

  @Override
  public boolean hasDelta() {
    return this.hasDelta;
  }

  @Override
  public void toDelta(DataOutput out) throws IOException {
    // Write whether to apply the changes to another DS if necessary
    out.writeBoolean(this.applyRemotely);

    // Write the events
    DataSerializer.writeArrayList((ArrayList) this.eventQueue, out);

    out.writeLong(this.lastAccessedTime);
    out.writeInt(this.maxInactiveInterval);
  }

  @Override
  public void fromDelta(DataInput in) throws IOException, InvalidDeltaException {
    // Read whether to apply the changes to another DS if necessary
    this.applyRemotely = in.readBoolean();

    // Read the events
    List<DeltaSessionAttributeEvent> events;
    try {
      events = DataSerializer.readArrayList(in);
    } catch (ClassNotFoundException e) {
      throw new InvalidDeltaException(e);
    }

    // This allows for backwards compatibility with 2.1 clients
    if (((InputStream) in).available() > 0) {
      this.lastAccessedTime = in.readLong();
      this.maxInactiveInterval = in.readInt();
    }

    // Iterate and apply the events
    for (DeltaSessionAttributeEvent event : events) {
      event.apply(this);
    }

    // Add the events to the gateway delta region if necessary
    if (this.enableGatewayDeltaReplication && this.applyRemotely) {
      setCurrentGatewayDeltaEvent(
          new DeltaSessionAttributeEventBatch(this.sessionRegionName, this.id, events));
    }

    // Access it to set the last accessed time. End access it to set not new.
    access();
    endAccess();
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    // Write the StandardSession state
    DataSerializer.writeString(this.id, out);
    out.writeLong(this.creationTime);
    out.writeLong(this.lastAccessedTime);
    out.writeLong(this.thisAccessedTime);
    out.writeInt(this.maxInactiveInterval);
    out.writeBoolean(this.isNew);
    out.writeBoolean(this.isValid);
    DataSerializer.writeObject(getSerializedAttributes(), out);
    DataSerializer.writeByteArray(getSerializedPrincipal(), out);

    // Write the DeltaSession state
    out.writeBoolean(this.enableGatewayDeltaReplication);
    DataSerializer.writeString(this.sessionRegionName, out);

    DataSerializer.writeString(this.contextName, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    // Read the StandardSession state
    this.id = DataSerializer.readString(in);
    this.creationTime = in.readLong();
    this.lastAccessedTime = in.readLong();
    this.thisAccessedTime = in.readLong();
    this.maxInactiveInterval = in.readInt();
    this.isNew = in.readBoolean();
    this.isValid = in.readBoolean();
    readInAttributes(in);
    this.serializedPrincipal = DataSerializer.readByteArray(in);

    // Read the DeltaSession state
    this.enableGatewayDeltaReplication = in.readBoolean();
    this.sessionRegionName = DataSerializer.readString(in);

    // This allows for backwards compatibility with 2.1 clients
    if (((InputStream) in).available() > 0) {
      this.contextName = DataSerializer.readString(in);
    }

    // Initialize the transients if necessary
    if (this.listeners == null) {
      this.listeners = new ArrayList();
    }

    if (this.notes == null) {
      this.notes = new Hashtable();
    }
  }

  private void readInAttributes(DataInput in) throws IOException, ClassNotFoundException {
    ConcurrentHashMap map = DataSerializer.readObject(in);
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
    if (getManager() != null) {
      DeltaSessionManager mgr = (DeltaSessionManager) getManager();
      mgr.getLogger().error(e);
    }
  }

  @Override
  public int getSizeInBytes() {
    int size = 0;
    @SuppressWarnings("unchecked")
    Enumeration<String> attributeNames = (Enumeration<String>) getAttributeNames();
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
      ((DeltaSessionManager) getManager()).getLogger().warn(
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
        + "; context=" + this.contextName + "; sessionRegionName="
        + this.sessionRegionName + "; operatingRegionName="
        + (getOperatingRegion() == null ? "unset" : getOperatingRegion().getFullPath())
        + "]";
  }

  // Helper methods to enable better unit testing
  DeltaSessionFacade getNewFacade(DeltaSessionInterface fSession) {
    return (DeltaSessionFacade) AccessController.doPrivileged(
        (PrivilegedAction) () -> new DeltaSessionFacade(fSession));
  }

  boolean isPackageProtectionEnabled() {
    return SecurityUtil.isPackageProtectionEnabled();
  }

  void addEventToEventQueue(DeltaSessionAttributeEvent event) {
    eventQueue.add(event);
  }
}
