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
package org.apache.geode.modules.session.catalina;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.security.AccessController;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.servlet.http.HttpSession;

import org.apache.catalina.Manager;
import org.apache.catalina.realm.GenericPrincipal;
import org.apache.catalina.security.SecurityUtil;
import org.apache.catalina.session.StandardSession;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.Delta;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.lru.Sizeable;
import org.apache.geode.internal.util.BlobHelper;
import org.apache.geode.modules.gatewaydelta.GatewayDelta;
import org.apache.geode.modules.gatewaydelta.GatewayDeltaEvent;
import org.apache.geode.modules.session.catalina.internal.DeltaSessionAttributeEvent;
import org.apache.geode.modules.session.catalina.internal.DeltaSessionAttributeEventBatch;
import org.apache.geode.modules.session.catalina.internal.DeltaSessionDestroyAttributeEvent;
import org.apache.geode.modules.session.catalina.internal.DeltaSessionUpdateAttributeEvent;

@SuppressWarnings("serial")
public class DeltaSession7 extends StandardSession implements DataSerializable, Delta, GatewayDelta, Sizeable, DeltaSessionInterface {

  private transient Region<String, HttpSession> operatingRegion;

  private String sessionRegionName;

  private String contextName;

  private boolean hasDelta;

  private boolean applyRemotely;

  private boolean enableGatewayDeltaReplication;

  private transient final Object changeLock = new Object();

  private final List<DeltaSessionAttributeEvent> eventQueue = new ArrayList<DeltaSessionAttributeEvent>();

  private transient GatewayDeltaEvent currentGatewayDeltaEvent;

  private transient boolean expired = false;

  private transient boolean preferDeserializedForm = true;

  private byte[] serializedPrincipal;

  private final Log LOG = LogFactory.getLog(DeltaSession7.class.getName());

  /**
   * The string manager for this package.
   */
//  protected static StringManager STRING_MANAGER =
//    StringManager.getManager("org.apache.geode.modules.session.catalina");

  /**
   * Construct a new <code>Session</code> associated with no <code>Manager</code>. The <code>Manager</code> will be
   * assigned later using {@link #setOwner(Object)}.
   */
  public DeltaSession7() {
    super(null);
  }

  /**
   * Construct a new Session associated with the specified Manager.
   *
   * @param manager The manager with which this Session is associated
   */
  public DeltaSession7(Manager manager) {
    super(manager);
    setOwner(manager);
  }

  /**
   * Return the <code>HttpSession</code> for which this object is the facade.
   */
  @SuppressWarnings("unchecked")
  public HttpSession getSession() {
    if (facade == null) {
      if (SecurityUtil.isPackageProtectionEnabled()) {
        final DeltaSession7 fsession = this;
        facade = (DeltaSessionFacade) AccessController.doPrivileged(new PrivilegedAction() {
          public Object run() {
            return new DeltaSessionFacade(fsession);
          }
        });
      } else {
        facade = new DeltaSessionFacade(this);
      }
    }
    return (facade);
  }

  public Principal getPrincipal() {
    if (this.principal == null && this.serializedPrincipal != null) {
      Principal sp = null;
      try {
        sp = (Principal) BlobHelper.deserializeBlob(this.serializedPrincipal);
      } catch (Exception e) {
        StringBuilder builder = new StringBuilder();
        builder.append(this)
            .append(
                ": Serialized principal contains a byte[] that cannot be deserialized due to the following exception");
        ((DeltaSessionManager) getManager()).getLogger().warn(builder.toString(), e);
        return null;
      }
      this.principal = sp;
      if (getManager() != null) {
        DeltaSessionManager mgr = (DeltaSessionManager) getManager();
        if (mgr.getLogger().isDebugEnabled()) {
          mgr.getLogger().debug(this + ": Deserialized principal: " + this.principal);
        }
      }
    }
    return this.principal;
  }

  public void setPrincipal(Principal principal) {
    super.setPrincipal(principal);

    // Put the session into the region to serialize the principal
    if (getManager() != null) {
      // TODO convert this to a delta
      getManager().add(this);
      DeltaSessionManager mgr = (DeltaSessionManager) getManager();
      if (mgr.getLogger().isDebugEnabled()) {
        mgr.getLogger().debug(this + ": Cached principal: " + principal);
      }
    }
  }

  private byte[] getSerializedPrincipal() {
    if (this.serializedPrincipal == null) {
      if (this.principal != null && this.principal instanceof GenericPrincipal) {
        GenericPrincipal gp = (GenericPrincipal) this.principal;
        this.serializedPrincipal = serialize(gp);
        if (manager != null) {
          DeltaSessionManager mgr = (DeltaSessionManager) getManager();
          if (mgr.getLogger().isDebugEnabled()) {
            mgr.getLogger().debug(this + ": Serialized principal: " + gp);
          }
        }
      }
    }
    return this.serializedPrincipal;
  }

  protected Region<String, HttpSession> getOperatingRegion() {
    // This region shouldn't be null when it is needed.
    // It should have been set by the setOwner method.
    return this.operatingRegion;
  }

  public boolean isCommitEnabled() {
    DeltaSessionManager mgr = (DeltaSessionManager) getManager();
    return mgr.isCommitValveEnabled();
  }

  public GatewayDeltaEvent getCurrentGatewayDeltaEvent() {
    return this.currentGatewayDeltaEvent;
  }

  public void setCurrentGatewayDeltaEvent(GatewayDeltaEvent currentGatewayDeltaEvent) {
    this.currentGatewayDeltaEvent = currentGatewayDeltaEvent;
  }

  @SuppressWarnings("unchecked")
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
      DeltaSessionAttributeEvent event = new DeltaSessionUpdateAttributeEvent(name, serializedValue);
      queueAttributeEvent(event, true);

      // Distribute the update
      if (!isCommitEnabled()) {
        putInRegion(getOperatingRegion(), true, null);
      }
    }
  }

  public void removeAttribute(String name, boolean notify) {
    checkBackingCacheAvailable();
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

  public Object getAttribute(String name) {
    checkBackingCacheAvailable();
    Object value = super.getAttribute(name);

    // If the attribute is a byte[] (meaning it came from the server),
    // deserialize it and add it to attributes map before returning it.
    if (value instanceof byte[]) {
      try {
        value = BlobHelper.deserializeBlob((byte[]) value);
      } catch (Exception e) {
        StringBuilder builder = new StringBuilder();
        builder.append(this)
            .append(": Attribute named ")
            .append(name)
            .append(" contains a byte[] that cannot be deserialized due to the following exception");
        ((DeltaSessionManager) getManager()).getLogger().warn(builder.toString(), e);
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

  public void invalidate() {
    super.invalidate();
    //getOperatingRegion().destroy(this.id, true); // already done in super (remove)
    ((DeltaSessionManager) getManager()).getStatistics().incSessionsInvalidated();
  }

  public void processExpired() {
    DeltaSessionManager manager = (DeltaSessionManager) getManager();
    if (manager != null && manager.getLogger() != null && manager.getLogger().isDebugEnabled()) {
      ((DeltaSessionManager) getManager()).getLogger().debug(this + ": Expired");
    }

    // Set expired (so region.destroy is not called again)
    setExpired(true);

    // Do expire processing
    expire();

    // Update statistics
    if (manager != null) {
      manager.getStatistics().incSessionsExpired();
    }
  }

  public void setMaxInactiveInterval(int interval) {
    super.setMaxInactiveInterval(interval);
  }

  public void localUpdateAttribute(String name, Object value) {
    super.setAttribute(name, value, false); // don't do notification since this is a replication
  }

  public void localDestroyAttribute(String name) {
    super.removeAttribute(name, false); // don't do notification since this is a replication
  }

  public void applyAttributeEvents(Region<String, DeltaSessionInterface> region, List<DeltaSessionAttributeEvent> events) {
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

  private void queueAttributeEvent(DeltaSessionAttributeEvent event, boolean checkAddToCurrentGatewayDelta) {
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
          List<DeltaSessionAttributeEvent> events = new ArrayList<DeltaSessionAttributeEvent>();
          events.add(event);
          this.currentGatewayDeltaEvent = new DeltaSessionAttributeEventBatch(this.sessionRegionName, this.id, events);
        }
      }
    }
    this.eventQueue.add(event);
  }

  @SuppressWarnings("unchecked")
  private void putInRegion(Region region, boolean applyRemotely, Object callbackArgument) {
    this.hasDelta = true;
    this.applyRemotely = applyRemotely;
    region.put(this.id, this, callbackArgument);
    this.eventQueue.clear();
  }

  public void commit() {
    if (!isValidInternal()) throw new IllegalStateException("commit: Session " + getId() +
        " already invalidated");

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

  public void abort() {
    synchronized (this.changeLock) {
      this.eventQueue.clear();
    }
  }

  private void setExpired(boolean expired) {
    this.expired = expired;
  }

  public boolean getExpired() {
    return this.expired;
  }

  public String getContextName() {
    return contextName;
  }

  public boolean hasDelta() {
    return this.hasDelta;
  }

  public void toDelta(DataOutput out) throws IOException {
    // Write whether to apply the changes to another DS if necessary
    out.writeBoolean(this.applyRemotely);

    // Write the events
    DataSerializer.writeArrayList((ArrayList) this.eventQueue, out);

    out.writeLong(this.lastAccessedTime);
    out.writeInt(this.maxInactiveInterval);
  }

  public void fromDelta(DataInput in) throws IOException, InvalidDeltaException {
    // Read whether to apply the changes to another DS if necessary
    this.applyRemotely = in.readBoolean();

    // Read the events
    List<DeltaSessionAttributeEvent> events = null;
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
      setCurrentGatewayDeltaEvent(new DeltaSessionAttributeEventBatch(this.sessionRegionName, this.id, events));
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
    this.attributes = readInAttributes(in);
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
      this.listeners = new ArrayList<>();
    }

    if (this.notes == null) {
      this.notes = new Hashtable<>();
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  protected ConcurrentMap readInAttributes(final DataInput in) throws IOException, ClassNotFoundException {
    return DataSerializer.readObject(in);
  }

  @Override
  public int getSizeInBytes() {
    int size = 0;
    for (Enumeration<String> e = getAttributeNames(); e.hasMoreElements(); ) {
      // Don't use this.getAttribute() because we don't want to deserialize
      // the value.
      Object value = super.getAttribute(e.nextElement());
      if (value instanceof byte[]) {
        size += ((byte[]) value).length;
      }
    }

    return size;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  protected ConcurrentMap<String, byte[]> getSerializedAttributes() {
    // Iterate the values and serialize them if necessary before sending them to the server. This makes the application classes unnecessary on the server.
    ConcurrentMap<String, byte[]> serializedAttributes = new ConcurrentHashMap<String, byte[]>();
    for (Iterator i = this.attributes.entrySet().iterator(); i.hasNext(); ) {
      Map.Entry<String, Object> entry = (Map.Entry<String, Object>) i.next();
      Object value = entry.getValue();
      byte[] serializedValue = value instanceof byte[] ? (byte[]) value : serialize(value);
      serializedAttributes.put(entry.getKey(), serializedValue);
    }
    return serializedAttributes;
  }

  protected byte[] serialize(Object obj) {
    byte[] serializedValue = null;
    try {
      serializedValue = BlobHelper.serializeToBlob(obj);
    } catch (IOException e) {
      StringBuilder builder = new StringBuilder();
      builder.append(this)
          .append(": Object ")
          .append(obj)
          .append(" cannot be serialized due to the following exception");
      ((DeltaSessionManager) getManager()).getLogger().warn(builder.toString(), e);
    }
    return serializedValue;
  }

  @Override
  public String toString() {
    return new StringBuilder().append("DeltaSession[")
        .append("id=")
        .append(getId())
        .append("; context=")
        .append(this.contextName)
        .append("; sessionRegionName=")
        .append(this.sessionRegionName)
        .append("; operatingRegionName=")
        .append(getOperatingRegion() == null ? "unset" : getOperatingRegion().getFullPath())
        .append("]")
        .toString();
  }
}
