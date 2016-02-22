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


package com.gemstone.gemfire.cache;
import java.io.*;

/**
 * Enumerated type for region subscription interest policy.
 * The interest policy specifies what data a subscriber is interested in having
 * in it's region.
 *
 *
 *
 * @see SubscriptionAttributes
 *
 * @since 5.0
 */
public class InterestPolicy implements java.io.Serializable {
  private static final long serialVersionUID = 1567179436331385968L;

  private static byte nextOrdinal = 0;
    
  private static final InterestPolicy[] VALUES = new InterestPolicy[2];

  /**
   * This subscriber is interested in all data.
   * More specifically operations done in this cache and
   * distributed operations done in remote caches.
   * <p>
   * When combined with {@link DataPolicy#EMPTY} this region will receive
   * events for every distributed operation but will not store the data.
   * <p>
   * When combined with {@link DataPolicy#NORMAL} or
   * {@link DataPolicy#PRELOADED} this region will accept
   * {@link Region#create(Object, Object)} operations done remotely. Without
   * the <code>ALL</code> interest policy, <code>NORMAL</code> and
   * <code>PRELOADED</code> ignore <code>creates</code> that the region
   * does  not have an existing entry for.
   * <p>
   * When combined with the {@link DataPolicy#withReplication replication
   * policies} this interest has no effect.
   * <p>
   * When combined with {@link DataPolicy#PARTITION} this interest policy
   * causes cache listeners to be notified of changes regardless of the
   * physical location of the data affected.  That is, a listener in a VM
   * using this policy will receive notification of all changes to the
   * partitioned region.
   */
  public static final InterestPolicy ALL = new InterestPolicy("ALL");

  /**
   * This subscriber is interested in data that is already in its cache.
   * More specifically operations done in this cache and
   * distributed operations done in remote caches.
   * <p>
   * When combined with {@link DataPolicy#EMPTY} this region will never
   * receive events for distributed operations since its content is always
   * empty.  It will continue to get events for operations done locally.
   * <p>
   * When combined with {@link DataPolicy#NORMAL} or
   * {@link DataPolicy#PRELOADED} this region will accept remote operations
   * done to entries it already has in its cache.
   * <p>
   * When combined with the {@link DataPolicy#withReplication replication
   * policies} * this interest has no effect.
   * <p>
   * When combined with {@link DataPolicy#PARTITION} this interest policy
   * causes cache listeners to be notified in the VM holding the affected data.
   *  That is, listeners are only notified if the affected* key-value pair is
   * in the same process as the listener.
   */
  public static final InterestPolicy CACHE_CONTENT = new InterestPolicy("CACHE_CONTENT");

  /**
   * The interest policy used by default; it is {@link #CACHE_CONTENT}.
   */
  public static final InterestPolicy DEFAULT = CACHE_CONTENT;

    
  /** The name of this mirror type. */
  private final transient String name;
    
  /** used as ordinal to represent this InterestPolicy */
  public final byte ordinal;

  private Object readResolve() throws ObjectStreamException {
    return VALUES[ordinal];  // Canonicalize
  }
    
    
  /** Creates a new instance of InterestPolicy. */
  private InterestPolicy(String name) {
    this.name = name;
    this.ordinal = nextOrdinal++;
    VALUES[this.ordinal] = this;
  }
    
  /** Return the InterestPolicy represented by specified ordinal */
  public static InterestPolicy fromOrdinal(byte ordinal) {
    return VALUES[ordinal];
  }
    
    
  /**
   * Return true if this policy is {@link #ALL}.
   * @return true if this policy is {@link #ALL}.
   */
  public boolean isAll() {
    return this == ALL;
  }
  /**
   * Return true if this policy is {@link #CACHE_CONTENT}.
   * @return true if this policy is {@link #CACHE_CONTENT}.
   */
  public boolean isCacheContent() {
    return this == CACHE_CONTENT;
  }
  /**
   * Return true if this policy is the default.
   * @return true if this policy is the default.
   */
  public boolean isDefault() {
    return this == DEFAULT;
  }
  
  /** Returns a string representation for this interest policy.
     * @return the name of this interest policy.
     */
  @Override
  public String toString() {
    return this.name;
  }
}
