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

import java.io.ObjectStreamException;
import java.io.Serializable;

import org.apache.geode.annotations.Immutable;

/**
 * Enumerated type for expiration actions.
 *
 *
 *
 * @since GemFire 3.0
 */
public class ExpirationAction implements Serializable {
  private static final long serialVersionUID = 658925707882047900L;

  /** When the region or cached object expires, it is invalidated. */
  @Immutable
  public static final ExpirationAction INVALIDATE = new ExpirationAction("INVALIDATE");
  /** When expired, invalidated locally only. Not supported for partitioned regions. */
  @Immutable
  public static final ExpirationAction LOCAL_INVALIDATE = new ExpirationAction("LOCAL_INVALIDATE");

  /** When the region or cached object expires, it is destroyed. */
  @Immutable
  public static final ExpirationAction DESTROY = new ExpirationAction("DESTROY");
  /**
   * When expired, destroyed locally only. Not supported for partitioned regions. Use DESTROY
   * instead.
   */
  @Immutable
  public static final ExpirationAction LOCAL_DESTROY = new ExpirationAction("LOCAL_DESTROY");

  /** The name of this action */
  private final transient String name;

  /**
   * Creates a new instance of ExpirationAction.
   *
   * @param name the name of the expiration action
   * @see #toString
   */
  private ExpirationAction(String name) {
    this.name = name;
  }

  /**
   * Returns whether this is the action for distributed invalidate.
   *
   * @return true if this in INVALIDATE
   */
  public boolean isInvalidate() {
    return this == INVALIDATE;
  }

  /**
   * Returns whether this is the action for local invalidate.
   *
   * @return true if this is LOCAL_INVALIDATE
   */
  public boolean isLocalInvalidate() {
    return this == LOCAL_INVALIDATE;
  }

  /**
   * Returns whether this is the action for distributed destroy.
   *
   * @return true if this is DESTROY
   */
  public boolean isDestroy() {
    return this == DESTROY;
  }

  /**
   * Returns whether this is the action for local destroy.
   *
   * @return true if thisis LOCAL_DESTROY
   */
  public boolean isLocalDestroy() {
    return this == LOCAL_DESTROY;
  }

  /**
   * Returns whether this action is local.
   *
   * @return true if this is LOCAL_INVALIDATE or LOCAL_DESTROY
   */
  public boolean isLocal() {
    return this == LOCAL_INVALIDATE || this == LOCAL_DESTROY;
  }

  /**
   * Returns whether this action is distributed.
   *
   * @return true if this is INVALIDATE or DESTROY
   */
  public boolean isDistributed() {
    return !isLocal();
  }

  /**
   * Returns a string representation for this action
   *
   * @return the name of this action
   */
  @Override
  public String toString() {
    return this.name;
  }

  /**
   * converts to strings used in cache.xml
   *
   * @return strings used in cache.xml
   */
  public String toXmlString() {
    switch (this.name) {
      case "INVALIDATE":
        return "invalidate";
      case "DESTROY":
        return "destroy";
      case "LOCAL_DESTROY":
        return "local-destroy";
      case "LOCAL_INVALIDATE":
        return "local-invalidate";
      default:
        return null;
    }
  }

  /**
   * converts allowed values in cache.xml into ExpirationAction
   *
   * @param xmlValue the values allowed are: invalidate, destroy, local-invalidate, local-destroy
   * @return the corresponding ExpirationAction
   * @throws IllegalArgumentException for all other invalid strings.
   */
  public static ExpirationAction fromXmlString(String xmlValue) {
    switch (xmlValue) {
      case "invalidate":
        return INVALIDATE;
      case "destroy":
        return DESTROY;
      case "local-destroy":
        return LOCAL_DESTROY;
      case "local-invalidate":
        return LOCAL_INVALIDATE;
      default:
        throw new IllegalArgumentException("invalid expiration action: " + xmlValue);
    }
  }

  // The 4 declarations below are necessary for serialization
  @Immutable
  private static int nextOrdinal = 0;
  public final int ordinal = nextOrdinal++;
  @Immutable
  private static final ExpirationAction[] VALUES =
      {INVALIDATE, LOCAL_INVALIDATE, DESTROY, LOCAL_DESTROY};

  private Object readResolve() throws ObjectStreamException {
    return fromOrdinal(ordinal); // Canonicalize
  }

  /** Return the ExpirationAction represented by specified ordinal */
  public static ExpirationAction fromOrdinal(int ordinal) {
    return VALUES[ordinal];
  }

}
