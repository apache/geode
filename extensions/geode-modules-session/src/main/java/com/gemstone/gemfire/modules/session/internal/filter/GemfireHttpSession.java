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

package com.gemstone.gemfire.modules.session.internal.filter;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.Delta;
import com.gemstone.gemfire.Instantiator;
import com.gemstone.gemfire.InvalidDeltaException;
import com.gemstone.gemfire.modules.session.internal.filter.attributes.AbstractSessionAttributes;
import com.gemstone.gemfire.modules.session.internal.filter.attributes.SessionAttributes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpSessionContext;

import com.gemstone.gemfire.modules.util.ClassLoaderObjectInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class which implements a Gemfire persisted {@code HttpSession}
 */
public class GemfireHttpSession implements
    HttpSession, DataSerializable, Delta {

  private static transient final Logger LOG =
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

  private HttpSession nativeSession = null;

  /**
   * A session becomes invalid if it is explicitly invalidated or if it
   * expires.
   */
  private boolean isValid = true;

  private boolean isNew = true;

  private boolean isDirty = false;

  /**
   * This is set during serialization and then reset by the SessionManager when
   * it is retrieved from the attributes.
   */
  private AtomicBoolean serialized = new AtomicBoolean(false);

  /**
   * Register ourselves for de-serialization
   */
  static {
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
  private GemfireHttpSession() {
  }

  /**
   * Constructor
   */
  public GemfireHttpSession(String id, HttpSession nativeSession) {
    this();
    this.id = id;
    this.nativeSession = nativeSession;
    if (nativeSession != null) {
      attributes.setMaxInactiveInterval(nativeSession.getMaxInactiveInterval());
    }
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
        LOG.debug(
            "Attribute '{}' needs to be reconstructed with a new classloader",
            name);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
          ObjectOutputStream oos = new ObjectOutputStream(baos);
          oos.writeObject(obj);
          oos.close();

          ObjectInputStream ois = new ClassLoaderObjectInputStream(
              new ByteArrayInputStream(baos.toByteArray()),
              loader);
          tmpObj = ois.readObject();
        } catch (IOException e) {
          LOG.error("Exception while recreating attribute '" + name +
              "'", e);
        } catch (ClassNotFoundException e) {
          LOG.error("Exception while recreating attribute '" + name +
              "'", e);
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
   */
  @Override
  public Enumeration getAttributeNames() {
    if (!isValid) {
      throw new IllegalStateException("Session is already invalidated");
    }
    return Collections.enumeration(attributes.getAttributeNames());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getCreationTime() {
    if (nativeSession != null) {
      return nativeSession.getCreationTime();
    } else {
      return 0;
    }
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

  /**
   * {@inheritDoc}
   */
  @Override
  public ServletContext getServletContext() {
    if (nativeSession != null) {
      return nativeSession.getServletContext();
    } else {
      return null;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public HttpSessionContext getSessionContext() {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Object getValue(String name) {
    return getAttribute(name);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String[] getValueNames() {
    return attributes.getAttributeNames().toArray(new String[0]);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void invalidate() {
    nativeSession.invalidate();
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
    if (nativeSession != null) {
      nativeSession.setMaxInactiveInterval(interval);
    }
    attributes.setMaxInactiveInterval(interval);
    isDirty = true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getMaxInactiveInterval() {
    if (nativeSession != null) {
      return nativeSession.getMaxInactiveInterval();
    } else {
      return attributes.getMaxIntactiveInterval();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void putValue(String name, Object value) {
    setAttribute(name, value);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void removeAttribute(final String name) {
    LOG.debug("Session {} removing attribute {}", getId(), name);
    nativeSession.removeAttribute(name);
    attributes.removeAttribute(name);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void removeValue(String name) {
    removeAttribute(name);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setAttribute(final String name, final Object value) {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Session {} setting attribute {} = '{}'",
          new Object[]{id, name, value});
    }

    isDirty = true;
    nativeSession.setAttribute(name, value);
    if (value == null) {
      removeAttribute(name);
    } else {
      attributes.putAttribute(name, value);
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
  public void fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    id = DataSerializer.readString(in);
    attributes = DataSerializer.readObject(in);
    if (getNativeSession() != null) {
      for (String s : attributes.getAttributeNames()) {
        getNativeSession().setAttribute(s, attributes.getAttribute(s));
      }
    }

    // Explicit sets
    serialized.set(true);
    attributes.setSession(this);
  }

  /**
   * These three methods handle delta propagation and are deferred to the
   * attribute object.
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
  public void fromDelta(DataInput in) throws IOException,
      InvalidDeltaException {
    if (attributes instanceof Delta) {
      ((Delta) attributes).fromDelta(in);
    } else {
      try {
        fromData(in);
      } catch (ClassNotFoundException cex) {
        throw new IOException("Unable to forward fromDelta() call "
            + "to fromData()", cex);
      }
    }
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("[id=").append(id)
        .append(", isNew=").append(isNew)
        .append(", isValid=").append(isValid)
        .append(", hasDelta=").append(hasDelta())
        .append(", lastAccessedTime=").append(attributes.getLastAccessedTime())
        .append(", jvmOwnerId=").append(attributes.getJvmOwnerId());
    builder.append("]");
    return builder.toString();
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
      if (now - attributes.getLastAccessedTime() >= getMaxInactiveInterval() * 1000) {
        return false;
      }
    }
    return true;
  }

  /**
   * Is this session dirty and should it be written to cache
   */
  public boolean isDirty() {
    return isDirty;
  }

  public void setManager(SessionManager manager) {
    this.manager = manager;
  }

  /**
   * For testing allow retrieval of the wrapped, native session.
   */
  public HttpSession getNativeSession() {
    return nativeSession;
  }


  public void setNativeSession(HttpSession session) {
    this.nativeSession = session;
  }

  /**
   * Handle the process of failing over the session to a new native session
   * object.
   *
   * @param session
   */
  public void failoverSession(HttpSession session) {
    LOG.debug("Failing over session {} to {}", getId(), session.getId());
    setNativeSession(session);
    for (String name : attributes.getAttributeNames()) {
      LOG.debug("Copying '{}' => {}", name, attributes.getAttribute(name));
      session.setAttribute(name, attributes.getAttribute(name));
    }
    session.setMaxInactiveInterval(attributes.getMaxIntactiveInterval());
    manager.putSession(this);
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
   * @param attributes
   */
  public void setAttributes(AbstractSessionAttributes attributes) {
    this.attributes = attributes;
  }

  /**
   * This is called on deserialization. You can only call it once to get a
   * meaningful value as it resets the serialized state. In other words, this
   * call is not idempotent.
   *
   * @return whether this object has just been serialized
   */
  public boolean justSerialized() {
    return serialized.getAndSet(false);
  }

  /**
   * Called when the session is about to go out of scope. If the session has
   * been defined to use async queued attributes then they will be written out
   * at this point.
   */
  public void commit() {
    attributes.setJvmOwnerId(manager.getJvmId());
    attributes.flush();
  }

  public String getJvmOwnerId() {
    if (attributes != null) {
      return attributes.getJvmOwnerId();
    }

    return null;
  }
}

