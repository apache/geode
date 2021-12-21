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

package org.apache.geode.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.InternalDataSerializer;

/**
 * Immutable parameter object for accessing and setting the attributes associated with
 * <code>timeToLive</code> and <code>idleTimeout</code>. If the expiration action is not specified,
 * it defaults to <code>ExpirationAction.INVALIDATE</code>. If the timeout is not specified, it
 * defaults to zero (which means to never timeout).
 *
 *
 *
 * @see AttributesFactory
 * @see RegionAttributes
 * @see AttributesMutator
 * @since GemFire 3.0
 */
public class ExpirationAttributes implements DataSerializable {
  private static final long serialVersionUID = 5956885652945706394L;
  /** convenience constant for a default instance */
  @Immutable
  public static final ExpirationAttributes DEFAULT = new ExpirationAttributes();

  /** The number of seconds since this value or region was created before it expires. */
  private int timeout;

  /**
   * The action that should take place when this object or region expires.
   */
  private ExpirationAction action;

  /**
   * Constructs a default <code>ExpirationAttributes</code>, which indicates no expiration will take
   * place.
   */
  public ExpirationAttributes() {
    timeout = 0;
    action = ExpirationAction.INVALIDATE;
  }

  /**
   * Constructs an <code>ExpirationAttributes</code> with the specified expiration time and the
   * default expiration action <code>ExpirationAction.INVALIDATE</code>.
   *
   * @param expirationTime The number of seconds before expiration
   * @throws IllegalArgumentException if expirationTime is nonpositive
   */
  public ExpirationAttributes(int expirationTime) {
    timeout = expirationTime;
    action = ExpirationAction.INVALIDATE;
  }

  /**
   * Constructs an <code>ExpirationAttributes</code> with the specified expiration time and
   * expiration action.
   *
   * @param expirationTime The number of seconds for a value to live before it expires. If this
   *        parameter is negative, the expiration time will be set to 0, indicating no expiration.
   * @param expirationAction the action to take when the value expires
   */
  public ExpirationAttributes(int expirationTime, ExpirationAction expirationAction) {
    if (expirationTime < 0) {
      timeout = 0;
    } else {
      timeout = expirationTime;
    }
    if (expirationAction == null) {
      action = ExpirationAction.INVALIDATE;
    } else {
      action = expirationAction;
    }
  }

  /**
   * Returns the number of seconds before a region or value expires.
   *
   * @return the relative number of seconds before a region or value expires or zero if it will
   *         never expire
   */
  public int getTimeout() {
    return timeout;
  }

  /**
   * Returns the action that should take place when this value or region expires.
   *
   * @return the action to take when expiring
   */
  public ExpirationAction getAction() {
    return action;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ExpirationAttributes)) {
      return false;
    }
    ExpirationAttributes ea = (ExpirationAttributes) obj;
    return timeout == ea.timeout && action == ea.action;
  }

  @Override
  public int hashCode() {
    return timeout ^ action.hashCode();
  }

  /**
   * Returns a string representation of this <code>ExpirationAttributes</code>. If the timeout is
   * zero, returns <code>"NO EXPIRATION"</code>.
   *
   * @return the String representation of this expiration attribute
   */
  @Override
  public String toString() {
    if (timeout == 0) {
      return "NO EXPIRATION";
    }
    return "timeout: " + timeout + ";action: " + action;
  }

  public static ExpirationAttributes createFromData(DataInput in)
      throws IOException, ClassNotFoundException {
    ExpirationAttributes result = new ExpirationAttributes();
    InternalDataSerializer.invokeFromData(result, in);
    return result;
  }


  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    timeout = in.readInt();
    action = DataSerializer.readObject(in);

  }

  @Override
  public void toData(DataOutput out) throws IOException {
    out.writeInt(timeout);
    DataSerializer.writeObject(action, out);
  }

  public boolean isDefault() {
    return (action == null || action == ExpirationAction.INVALIDATE)
        && (timeout == 0);
  }
}
