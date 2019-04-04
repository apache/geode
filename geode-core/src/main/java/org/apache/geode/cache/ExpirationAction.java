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

import java.util.Arrays;

/**
 * Enumerated type for expiration actions.
 *
 * @since GemFire 3.0
 */
public enum ExpirationAction {
  /** When the region or cached object expires, it is invalidated. */
  INVALIDATE("invalidate"),
  /** When expired, invalidated locally only. Not supported for partitioned regions. */
  LOCAL_INVALIDATE("local-invalidate"),

  /** When the region or cached object expires, it is destroyed. */
  DESTROY("destroy"),
  /**
   * When expired, destroyed locally only. Not supported for partitioned regions. Use DESTROY
   * instead.
   */
  LOCAL_DESTROY("local-destroy");

  private static final long serialVersionUID = 658925707882047900L;

  private final transient String xmlName;

  /**
   * Creates a new instance of ExpirationAction.
   *
   * @param xmlName the name of the expiration action
   */
  ExpirationAction(String xmlName) {
    this.xmlName = xmlName;
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
   * converts to strings used in cache.xml
   *
   * @return strings used in cache.xml
   */
  public String toXmlString() {
    return xmlName;
  }

  /**
   * converts allowed values in cache.xml into ExpirationAction
   *
   * @param xmlValue the values allowed are: invalidate, destroy, local-invalidate, local-destroy
   * @return the corresponding ExpirationAction
   * @throws IllegalArgumentException for all other invalid strings.
   */
  public static ExpirationAction fromXmlString(String xmlValue) {
    return Arrays.stream(ExpirationAction.values()).filter(e -> e.xmlName.equals(xmlValue))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("invalid expiration action: " + xmlValue));
  }
}
