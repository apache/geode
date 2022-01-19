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

import org.apache.geode.annotations.Immutable;

/**
 * Enumerated type for region mirroring.
 *
 *
 *
 * @see AttributesFactory#setMirrorType
 * @see RegionAttributes#getMirrorType
 *
 * @deprecated as of GemFire 5.0, use {@link DataPolicy} instead.
 *
 * @since GemFire 3.0
 */
@Deprecated
@Immutable
public class MirrorType implements java.io.Serializable {
  private static final long serialVersionUID = -6632651349646672540L;

  /**
   * New entries created in other caches for this region are not automatically propagated to this
   * region in this cache.
   *
   * @deprecated as of GemFire 5.0, use {@link DataPolicy#NORMAL} instead.
   */
  @Deprecated
  @Immutable
  public static final MirrorType NONE = new MirrorType("NONE", DataPolicy.NORMAL, 0);

  /**
   * New entries created in other caches for this region are propagated to this region in this
   * cache, but the value is not necessarily copied to this cache with the key.
   *
   * @deprecated as of GemFire 5.0, use {@link DataPolicy#REPLICATE} instead.
   */
  @Deprecated
  @Immutable
  public static final MirrorType KEYS = new MirrorType("KEYS", DataPolicy.REPLICATE, 1);

  /**
   * New entries created in other caches for this region are propagated to this region in this cache
   * and the value is also copied to this cache.
   *
   * @deprecated as of GemFire 5.0, use {@link DataPolicy#REPLICATE} instead.
   */
  @Deprecated
  @Immutable
  public static final MirrorType KEYS_VALUES = new MirrorType("KEYS_VALUES", DataPolicy.REPLICATE,
      2);


  /** The name of this mirror type. */
  private final transient String name;

  /**
   * The data policy that corresponds to this mirror type.
   */
  private final transient DataPolicy dataPolicy;

  // The 4 declarations below are necessary for serialization
  /** int used as ordinal to represent this Scope */
  public final int ordinal;

  @Immutable
  private static final MirrorType[] VALUES = {NONE, KEYS, KEYS_VALUES};

  private Object readResolve() throws ObjectStreamException {
    return VALUES[ordinal]; // Canonicalize
  }


  /** Creates a new instance of MirrorType. */
  private MirrorType(String name, DataPolicy dataPolicy, int ordinal) {
    this.name = name;
    this.dataPolicy = dataPolicy;
    this.ordinal = ordinal;
  }

  /** Return the MirrorType represented by specified ordinal */
  public static MirrorType fromOrdinal(int ordinal) {
    return VALUES[ordinal];
  }


  /**
   * Returns the {@link DataPolicy} that corresponds to this mirror type.
   *
   * @since GemFire 5.0
   */
  public DataPolicy getDataPolicy() {
    return dataPolicy;
  }

  /** Return whether this is <code>KEYS</code>. */
  public boolean isKeys() {
    return this == KEYS;
  }

  /** Return whether this is <code>KEYS_VALUES</code>. */
  public boolean isKeysValues() {
    return this == KEYS_VALUES;
  }

  /** Return whether this is <code>NONE</code>. */
  public boolean isNone() {
    return this == NONE;
  }

  /**
   * Return whether this indicates a mirrored type.
   *
   * @return true if <code>KEYS</code> or <code>KEYS_VALUES</code>
   */
  public boolean isMirrored() {
    return this != NONE;
  }

  /**
   * Returns a string representation for this mirror type.
   *
   * @return the name of this mirror type
   */
  @Override
  public String toString() {
    return name;
  }
}
