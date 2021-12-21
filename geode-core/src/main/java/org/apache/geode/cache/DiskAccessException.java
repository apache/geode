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

import java.io.IOException;

/**
 * Indicates that an <code>IOException</code> during a disk region operation.
 *
 * @since GemFire 3.2
 */
public class DiskAccessException extends CacheRuntimeException {
  private static final long serialVersionUID = 5799100281147167888L;

  private transient boolean isRemote;

  /**
   * Constructs a new <code>DiskAccessException</code>.
   */
  public DiskAccessException() {
    super();
  }

  /**
   * Constructs a new <code>DiskAccessException</code> with a message string.
   *
   * @param msg a message string
   * @param r The Region for which the disk operation failed
   */
  public DiskAccessException(String msg, Region r) {
    this(msg, null, r == null ? null : r.getFullPath());
  }

  /**
   * Constructs a new <code>DiskAccessException</code> with a message string.
   *
   * @param msg a message string
   * @param regionName The name of the Region for which the disk operation failed
   * @since GemFire 6.5
   */
  public DiskAccessException(String msg, String regionName) {
    this(msg, null, regionName);
  }

  /**
   * Constructs a new <code>DiskAccessException</code> with a message string.
   *
   * @param msg a message string
   * @param ds The disk store for which the disk operation failed
   * @since GemFire 6.5
   */
  public DiskAccessException(String msg, DiskStore ds) {
    this(msg, null, ds);
  }

  /**
   * Constructs a new <code>DiskAccessException</code> with a message string and a cause.
   *
   * @param msg the message string
   * @param cause a causal Throwable
   * @param regionName The name of the Region for which the disk operation failed
   * @since GemFire 6.5
   */
  public DiskAccessException(String msg, Throwable cause, String regionName) {
    super((regionName != null ? "For Region: " + regionName + ": " : "") + msg, cause);
  }

  /**
   * Constructs a new <code>DiskAccessException</code> with a message string and a cause.
   *
   * @param msg the message string
   * @param cause a causal Throwable
   * @param ds The disk store for which the disk operation failed
   * @since GemFire 6.5
   */
  public DiskAccessException(String msg, Throwable cause, DiskStore ds) {
    super((ds != null ? "For DiskStore: " + ds.getName() + ": " : "") + msg, cause);
  }

  /**
   * Constructs a new <code>DiskAccessException</code> with a cause.
   *
   * @param cause a causal Throwable
   */
  public DiskAccessException(Throwable cause) {
    super(cause);
  }

  /**
   * Constructs a new <code>DiskAccessException</code> with a message string and a cause.
   *
   * @param msg the message string
   * @param cause a causal Throwable
   * @since GemFire 8.0
   */
  public DiskAccessException(String msg, Throwable cause) {
    super(msg, cause);
  }

  /**
   * Constructs a new <code>DiskAccessException</code> with a message string.
   *
   * @param msg the message string
   */
  public DiskAccessException(String msg) {
    super(msg);
  }

  /**
   * Returns true if this exception originated from a remote node.
   */
  public boolean isRemote() {
    return isRemote;
  }

  // Overrides to set "isRemote" flag after deserialization

  private synchronized void writeObject(final java.io.ObjectOutputStream out) throws IOException {
    getStackTrace(); // Ensure that stackTrace field is initialized.
    out.defaultWriteObject();
  }

  private void readObject(final java.io.ObjectInputStream in)
      throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    isRemote = true;
  }
}
