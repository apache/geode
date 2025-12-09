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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.servlet.ServletContext;
import jakarta.servlet.http.HttpSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.Delta;
import org.apache.geode.Instantiator;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.modules.session.internal.filter.attributes.AbstractSessionAttributes;
import org.apache.geode.modules.session.internal.filter.attributes.SessionAttributes;
import org.apache.geode.modules.util.ClassLoaderObjectInputStream;

/**
 * Class which implements a Gemfire persisted {@code HttpSession}
 *
 * <p>
 * <b>Jakarta EE 10 Migration Changes:</b>
 * <ul>
 * <li>Removed deprecated {@code HttpSessionContext} methods (removed from Jakarta Servlet API)</li>
 * <li>Removed deprecated session value methods: getValue(), getValueNames(), putValue(),
 * removeValue()</li>
 * <li>Added generics to getAttributeNames() return type: Enumeration →
 * Enumeration&lt;String&gt;</li>
 * <li>Removed @SuppressWarnings("deprecation") - no longer needed after deprecated API removal</li>
 * </ul>
 */
public class GemfireHttpSession implements HttpSession, DataSerializable, Delta {

  private static final transient Logger LOG =
      LoggerFactory.getLogger(GemfireHttpSession.class.getName());

  /**
   * Serial id
   */
  private static final long serialVersionUID = 238915238964017823L;

  /**
   * Id for the session
   */
  private String id;

  /**
   * Attributes really hold the essence of persistence.
   */
  private SessionAttributes attributes;

  private transient SessionManager manager;

  private ServletContext context;

  /**
   * A session becomes invalid if it is explicitly invalidated or if it expires.
   */
  private boolean isValid = true;

  private boolean isNew = true;

  private boolean isDirty = false;

  /**
   * This is set during serialization and then reset by the SessionManager when it is retrieved from
   * the attributes.
   */
  private final AtomicBoolean serialized = new AtomicBoolean(false);

  // Register ourselves for de-serialization
  static {
    registerInstantiator();
  }

  public static void registerInstantiator() {
    Instantiator.register(new Instantiator(GemfireHttpSession.class, 27315) {
      @Override
      public DataSerializable newInstance() {
        return new GemfireHttpSession();
      }
    });
  }

  /**
   * Constructor used for de-serialization
   */
  private GemfireHttpSession() {}

  /**
   * Constructor
   */
  GemfireHttpSession(String id, ServletContext context) {
    this();
    this.id = id;
    this.context = context;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Object getAttribute(String name) {
    if (!isValid) {
      throw new IllegalStateException("Session is already invalidated");
    }
    Object obj = attributes.getAttribute(name);

    if (obj != null) {
      Object tmpObj = null;
      ClassLoader loader = ((GemfireSessionManager) manager).getReferenceClassLoader();

      if (obj.getClass().getClassLoader() != loader) {
        LOG.debug("Attribute '{}' needs to be reconstructed with a new classloader", name);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
          ObjectOutputStream oos = new ObjectOutputStream(baos);
          oos.writeObject(obj);
          oos.close();

          // Create filter from user configuration for secure deserialization
          String filterPattern = getServletContext()
              .getInitParameter("serializable-object-filter");
          ObjectInputFilter filter = filterPattern != null
              ? ObjectInputFilter.Config.createFilter(filterPattern)
              : null;

          ObjectInputStream ois = new ClassLoaderObjectInputStream(
              new ByteArrayInputStream(baos.toByteArray()), loader, filter);
          tmpObj = ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
          LOG.error("Exception while recreating attribute '" + name + "'", e);
        }
        if (tmpObj != null) {
          setAttribute(name, tmpObj);
          obj = tmpObj;
        }
      }
    }

    return obj;
  }

  /**
   * {@inheritDoc}
   *
   * <p>
   * <b>Jakarta Servlet API change:</b> Return type now includes generics
   * (Enumeration&lt;String&gt;)
   * instead of raw Enumeration type. This matches Jakarta Servlet 6.0 specification.
   */
  @Override
  public Enumeration<String> getAttributeNames() {
    checkValid();
    return Collections.enumeration(attributes.getAttributeNames());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getCreationTime() {
    checkValid();
    return attributes.getCreationTime();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getId() {
    return id;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getLastAccessedTime() {
    if (!isValid) {
      throw new IllegalStateException("Session is already invalidated");
    }
    return attributes.getLastAccessedTime();
  }

  public void setServletContext(ServletContext context) {
    this.context = context;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ServletContext getServletContext() {
    return context;
  }

  // Jakarta Servlet API removed deprecated methods (removed from interface):
  // - getSessionContext() - deprecated since Servlet 2.1
  // - getValue(String) - replaced by getAttribute(String)
  // - getValueNames() - replaced by getAttributeNames()
  // - putValue(String, Object) - replaced by setAttribute(String, Object)
  // - removeValue(String) - replaced by removeAttribute(String)

  /**
   * {@inheritDoc}
   */
  @Override
  public void invalidate() {
    manager.destroySession(id);
    isValid = false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isNew() {
    if (!isValid) {
      throw new IllegalStateException("Session is already invalidated");
    }
    return isNew;
  }

  public void setIsNew(boolean isNew) {
    this.isNew = isNew;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setMaxInactiveInterval(int interval) {
    attributes.setMaxInactiveInterval(interval);
    isDirty = true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getMaxInactiveInterval() {
    return attributes.getMaxIntactiveInterval();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void removeAttribute(final String name) {
    checkValid();
    LOG.debug("Session {} removing attribute {}", getId(), name);
    attributes.removeAttribute(name);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setAttribute(final String name, final Object value) {
    checkValid();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Session {} setting attribute {} = '{}'", id, name, value);
    }

    isDirty = true;
    if (value == null) {
      removeAttribute(name);
    } else {
      attributes.putAttribute(name, value);
    }
  }

  private void checkValid() {
    if (!isValid()) {
      throw new IllegalStateException("Session is invalid");
    }
  }

  /**
   * Gemfire serialization {@inheritDoc}
   */
  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(id, out);
    DataSerializer.writeObject(attributes, out);
  }

  /**
   * Gemfire de-serialization {@inheritDoc}
   */
  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    id = DataSerializer.readString(in);
    attributes = DataSerializer.readObject(in);
    // Explicit sets
    serialized.set(true);
    attributes.setSession(this);
  }

  /**
   * These three methods handle delta propagation and are deferred to the attribute object.
   */
  @Override
  public boolean hasDelta() {
    return isDirty;
  }

  @Override
  public void toDelta(DataOutput out) throws IOException {
    if (attributes instanceof Delta) {
      ((Delta) attributes).toDelta(out);
    } else {
      toData(out);
    }
  }

  @Override
  public void fromDelta(DataInput in) throws IOException, InvalidDeltaException {
    if (attributes instanceof Delta) {
      ((Delta) attributes).fromDelta(in);
    } else {
      try {
        fromData(in);
      } catch (ClassNotFoundException cex) {
        throw new IOException("Unable to forward fromDelta() call " + "to fromData()", cex);
      }
    }
  }

  @Override
  public String toString() {
    return "[id=" + id + ", isNew=" + isNew + ", isValid="
        + isValid + ", hasDelta=" + hasDelta() + ", lastAccessedTime="
        + attributes.getLastAccessedTime() + ", jvmOwnerId="
        + attributes.getJvmOwnerId()
        + "]";
  }

  /**
   * Flush the session object to the region
   */
  public void putInRegion() {

    manager.putSession(this);
    isDirty = false;
  }

  /**
   * Determine whether the session is still valid or whether it has expired.
   *
   * @return true or false
   */
  public boolean isValid() {
    if (!isValid) {
      return false;
    }

    if (getMaxInactiveInterval() >= 0) {
      long now = System.currentTimeMillis();
      return now - attributes.getLastAccessedTime() < getMaxInactiveInterval() * 1000;
    }

    return true;
  }

  public void setManager(SessionManager manager) {
    this.manager = manager;
  }


  /**
   * Update the last accessed time
   */
  public void updateAccessTime() {
    attributes.setLastAccessedTime(System.currentTimeMillis());
  }

  /**
   * The {@code SessionManager} injects this when creating a new session.
   *
   */
  public void setAttributes(AbstractSessionAttributes attributes) {
    this.attributes = attributes;
  }

  /**
   * This is called on deserialization. You can only call it once to get a meaningful value as it
   * resets the serialized state. In other words, this call is not idempotent.
   *
   * @return whether this object has just been serialized
   */
  boolean justSerialized() {
    return serialized.getAndSet(false);
  }

  /**
   * Called when the session is about to go out of scope. If the session has been defined to use
   * async queued attributes then they will be written out at this point.
   */
  public void commit() {
    attributes.setJvmOwnerId(manager.getJvmId());
    attributes.flush();
  }

  String getJvmOwnerId() {
    if (attributes != null) {
      return attributes.getJvmOwnerId();
    }

    return null;
  }
}
