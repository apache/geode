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
package org.apache.geode.admin.jmx.internal;

import java.io.IOException;
import java.io.Serializable;

import org.apache.logging.log4j.Level;

import org.apache.geode.SystemFailure;
import org.apache.geode.admin.UnmodifiableConfigurationException;

/**
 * Provides MBean support for managing accessing a ConfigurationParameter.
 * <p>
 * Implements java.io.Serializable because several MBeans have attributes of type
 * ConfigurationParameter. This means that calls to getMBeanInfo which may be serialized for remote
 * clients will be broken unless those attributes support serialization.
 * <p>
 * TODO: refactor to implement ConfigurationParameter and delegate to ConfigurationParameterImpl.
 * Wrap all delegate calls w/ e.printStackTrace() since the HttpAdaptor devours them
 *
 * @since GemFire 3.5
 *
 */
public class ConfigurationParameterJmxImpl
    extends org.apache.geode.admin.internal.ConfigurationParameterImpl implements Serializable {

  private static final long serialVersionUID = -7822171853906772375L;
  private boolean deserialized = false;

  // -------------------------------------------------------------------------
  // Constructor(s)
  // -------------------------------------------------------------------------

  protected ConfigurationParameterJmxImpl(String name, String description, Object value, Class type,
      boolean userModifiable) {
    super(name, description, value, type, userModifiable);
  }

  protected ConfigurationParameterJmxImpl(String name, Object value) {
    super(name, value);
  }

  /** Constructor to allow serialization */
  protected ConfigurationParameterJmxImpl() {
    super();
  }

  @Override
  public void setValue(Object value) throws UnmodifiableConfigurationException {
    if (deserialized) {
      throw new UnsupportedOperationException(
          "Remote mutation of ConfigurationParameter is currently unsupported.");
    }
    try {
      super.setValue(value);
    } catch (UnmodifiableConfigurationException e) {
      MBeanUtil.logStackTrace(Level.WARN, e);
      throw e;
    } catch (java.lang.RuntimeException e) {
      MBeanUtil.logStackTrace(Level.WARN, e);
      throw e;
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error. We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (java.lang.Error e) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above). However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      MBeanUtil.logStackTrace(Level.ERROR, e);
      throw e;
    }
  }

  // -------------------------------------------------------------------------
  // HACK
  // -------------------------------------------------------------------------
  public void setJmxValue(Integer value) throws UnmodifiableConfigurationException {
    setValue(value);
  }

  public void setJmxValue(String value) throws UnmodifiableConfigurationException {
    setValue(value);
  }

  public void setJmxValue(java.io.File value) throws UnmodifiableConfigurationException {
    setValue(value);
  }

  public void setJmxValue(Boolean value) throws UnmodifiableConfigurationException {
    setValue(value);
  }

  public Class getJmxValueType() {
    if (isInetAddress() || isFile() || isOctal()) {
      return java.lang.String.class;
    }
    return getValueType();
  }

  public Object getJmxValue() {
    if (isInetAddress() || isFile() || isOctal()) {
      return getValueAsString();
    }
    return getValue();
  }

  /**
   * Override writeObject which is used in serialization. This class is serialized when JMX client
   * acquires MBeanInfo for ConfigurationParameter MBean. Super class is not serializable.
   */
  private void writeObject(java.io.ObjectOutputStream out) throws IOException {
    out.writeObject(this.name);
    out.writeObject(this.description);
    out.writeObject(this.value);
    out.writeObject(this.type);
    out.writeBoolean(this.userModifiable);
  }

  /**
   * Override readObject which is used in serialization. Customize serialization of this exception
   * to avoid escape of InternalRole which is not Serializable.
   */
  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    String inName = (String) in.readObject();
    String inDescription = (String) in.readObject();
    Object inValue = in.readObject();
    Class inClass = (Class) in.readObject();
    boolean inUserModifiable = in.readBoolean();

    this.deserialized = true;
    this.name = inName;
    setInternalState(inDescription, inValue, inClass, inUserModifiable);
  }

}
