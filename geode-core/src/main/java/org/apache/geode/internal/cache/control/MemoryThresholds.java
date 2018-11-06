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
package org.apache.geode.internal.cache.control;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.LowMemoryException;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.distributed.internal.DistributionConfig;

/**
 * Stores eviction and critical thresholds for memory as well as the logic for determining how
 * memory transitions between states.
 *
 * @since Geode 1.0
 */
public class MemoryThresholds {

  public enum MemoryState {
    DISABLED, // Both eviction and critical disabled
    EVICTION_DISABLED, // Eviction disabled, critical enabled, critical threshold not exceeded
    EVICTION_DISABLED_CRITICAL, // Eviction disabled, critical enabled, critical threshold exceeded
    CRITICAL_DISABLED, // Critical disabled, eviction enabled, eviction threshold not exceeded
    EVICTION_CRITICAL_DISABLED, // Critical disabled, eviction enabled, eviction threshold exceeded
    NORMAL, // Both eviction and critical enabled, neither threshold exceeded
    EVICTION, // Both eviction and critical enabled, eviction threshold exceeded
    CRITICAL, // Both eviction and critical enabled, critical threshold exceeded
    EVICTION_CRITICAL; // Both eviction and critical enabled, both thresholds exceeded

    public static MemoryState fromData(DataInput in) throws IOException {
      return MemoryState.values()[in.readInt()];
    }

    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeInteger(this.ordinal(), out);
    }

    public boolean isEvictionDisabled() {
      return (this == DISABLED || this == EVICTION_DISABLED_CRITICAL || this == EVICTION_DISABLED);
    }

    public boolean isCriticalDisabled() {
      return (this == DISABLED || this == EVICTION_CRITICAL_DISABLED || this == CRITICAL_DISABLED);
    }

    public boolean isNormal() {
      return (this == NORMAL || this == EVICTION_DISABLED || this == CRITICAL_DISABLED);
    }

    public boolean isEviction() {
      return (this == EVICTION || this == EVICTION_CRITICAL_DISABLED || this == EVICTION_CRITICAL);
    }

    public boolean isCritical() {
      return (this == CRITICAL || this == EVICTION_DISABLED_CRITICAL || this == EVICTION_CRITICAL);
    }
  }

  /**
   * When this property is set to true, a {@link LowMemoryException} is not thrown, even when usage
   * crosses the critical threshold.
   */
  private static final boolean DISABLE_LOW_MEM_EXCEPTION =
      Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "disableLowMemoryException");

  /**
   * The default percent of memory at which the VM is considered in a critical state.
   */
  public static final float DEFAULT_CRITICAL_PERCENTAGE =
      ResourceManager.DEFAULT_CRITICAL_PERCENTAGE;

  /**
   * The default percent of memory at which the VM should begin evicting data. Note that if a LRU is
   * created and the eviction percentage has not been set then it will default to <code>80.0</code>
   * unless the critical percentage has been set in which case it will default to a value
   * <code>5.0</code> less than the critical percentage.
   */
  public static final float DEFAULT_EVICTION_PERCENTAGE =
      ResourceManager.DEFAULT_EVICTION_PERCENTAGE;

  /**
   * Memory usage must fall below THRESHOLD-THRESHOLD_THICKNESS before we deliver a down event
   */
  private static final double THRESHOLD_THICKNESS = Double.parseDouble(
      System.getProperty(DistributionConfig.GEMFIRE_PREFIX + "thresholdThickness", "2.00"));

  /**
   * Memory usage must fall below THRESHOLD-THRESHOLD_THICKNESS_EVICT before we deliver an eviction
   * down event
   */
  private static final double THRESHOLD_THICKNESS_EVICT = Double.parseDouble(
      System.getProperty(DistributionConfig.GEMFIRE_PREFIX + "eviction-thresholdThickness",
          Double.toString(THRESHOLD_THICKNESS)));

  private final long maxMemoryBytes;

  // Percent of available memory at which point a critical state is entered
  private final float criticalThreshold;

  // Number of bytes used at which point memory will enter the critical state
  private final long criticalThresholdBytes;

  // Percent of available memory at which point an eviction state is entered
  private final float evictionThreshold;

  // Number of bytes used at which point memory will enter the eviction state
  private final long evictionThresholdBytes;

  // Number of bytes used below which memory will leave the critical state
  private final long criticalThresholdClearBytes;

  // Number of bytes used below which memory will leave the eviction state
  private final long evictionThresholdClearBytes;

  MemoryThresholds(long maxMemoryBytes) {
    this(maxMemoryBytes, DEFAULT_CRITICAL_PERCENTAGE, DEFAULT_EVICTION_PERCENTAGE);
  }

  /**
   * Public for testing.
   */
  public MemoryThresholds(long maxMemoryBytes, float criticalThreshold, float evictionThreshold) {
    if (criticalThreshold > 100.0f || criticalThreshold < 0.0f) {
      throw new IllegalArgumentException(
          "Critical percentage must be greater than 0.0 and less than or equal to 100.0.");
    }

    if (evictionThreshold > 100.0f || evictionThreshold < 0.0f) {
      throw new IllegalArgumentException(
          "Eviction percentage must be greater than 0.0 and less than or equal to 100.0.");
    }

    if (evictionThreshold != 0 && criticalThreshold != 0
        && evictionThreshold >= criticalThreshold) {
      throw new IllegalArgumentException(
          "Critical percentage must be greater than the eviction percentage.");
    }

    this.maxMemoryBytes = maxMemoryBytes;

    this.criticalThreshold = criticalThreshold;
    this.criticalThresholdBytes = (long) (criticalThreshold * 0.01 * maxMemoryBytes);
    this.criticalThresholdClearBytes =
        (long) (this.criticalThresholdBytes - (0.01 * THRESHOLD_THICKNESS * this.maxMemoryBytes));

    this.evictionThreshold = evictionThreshold;
    this.evictionThresholdBytes = (long) (evictionThreshold * 0.01 * maxMemoryBytes);
    this.evictionThresholdClearBytes = (long) (this.evictionThresholdBytes
        - (0.01 * THRESHOLD_THICKNESS_EVICT * this.maxMemoryBytes));
  }

  public static boolean isLowMemoryExceptionDisabled() {
    return DISABLE_LOW_MEM_EXCEPTION;
  }

  public MemoryState computeNextState(final MemoryState oldState, final long bytesUsed) {
    assert oldState != null;
    assert bytesUsed >= 0;

    // Are both eviction and critical thresholds enabled?
    if (this.evictionThreshold != 0 && this.criticalThreshold != 0) {
      if (bytesUsed < this.evictionThresholdClearBytes
          || (!oldState.isEviction() && bytesUsed < this.evictionThresholdBytes)) {
        return MemoryState.NORMAL;
      }
      if (bytesUsed < this.criticalThresholdClearBytes
          || (!oldState.isCritical() && bytesUsed < this.criticalThresholdBytes)) {
        return MemoryState.EVICTION;
      }
      return MemoryState.EVICTION_CRITICAL;
    }

    // Are both eviction and critical thresholds disabled?
    if (this.evictionThreshold == 0 && this.criticalThreshold == 0) {
      return MemoryState.DISABLED;
    }

    // Is just critical threshold enabled?
    if (this.evictionThreshold == 0) {
      if (bytesUsed < this.criticalThresholdClearBytes
          || (!oldState.isCritical() && bytesUsed < this.criticalThresholdBytes)) {
        return MemoryState.EVICTION_DISABLED;
      }
      return MemoryState.EVICTION_DISABLED_CRITICAL;
    }

    // Just the eviction threshold is enabled
    if (bytesUsed < this.evictionThresholdClearBytes
        || (!oldState.isEviction() && bytesUsed < this.evictionThresholdBytes)) {
      return MemoryState.CRITICAL_DISABLED;
    }

    return MemoryState.EVICTION_CRITICAL_DISABLED;
  }

  @Override
  public String toString() {
    return new StringBuilder().append("MemoryThresholds@[").append(System.identityHashCode(this))
        .append(" maxMemoryBytes:" + this.maxMemoryBytes)
        .append(", criticalThreshold:" + this.criticalThreshold)
        .append(", criticalThresholdBytes:" + this.criticalThresholdBytes)
        .append(", criticalThresholdClearBytes:" + this.criticalThresholdClearBytes)
        .append(", evictionThreshold:" + this.evictionThreshold)
        .append(", evictionThresholdBytes:" + this.evictionThresholdBytes)
        .append(", evictionThresholdClearBytes:" + this.evictionThresholdClearBytes).append("]")
        .toString();
  }

  public long getMaxMemoryBytes() {
    return this.maxMemoryBytes;
  }

  public float getCriticalThreshold() {
    return this.criticalThreshold;
  }

  public long getCriticalThresholdBytes() {
    return this.criticalThresholdBytes;
  }

  public long getCriticalThresholdClearBytes() {
    return this.criticalThresholdClearBytes;
  }

  public boolean isCriticalThresholdEnabled() {
    return this.criticalThreshold > 0.0f;
  }

  public float getEvictionThreshold() {
    return this.evictionThreshold;
  }

  public long getEvictionThresholdBytes() {
    return this.evictionThresholdBytes;
  }

  public long getEvictionThresholdClearBytes() {
    return this.evictionThresholdClearBytes;
  }

  public boolean isEvictionThresholdEnabled() {
    return this.evictionThreshold > 0.0f;
  }

  /**
   * Generate a Thresholds object from data available from the DataInput
   *
   * @param in DataInput from which to read the data
   * @return a new instance of Thresholds
   */
  public static MemoryThresholds fromData(DataInput in) throws IOException {
    long maxMemoryBytes = in.readLong();
    float criticalThreshold = in.readFloat();
    float evictionThreshold = in.readFloat();
    return new MemoryThresholds(maxMemoryBytes, criticalThreshold, evictionThreshold);
  }

  /**
   * Write the state of this to the DataOutput
   *
   * @param out DataOutput on which to write internal state
   */
  public void toData(DataOutput out) throws IOException {
    out.writeLong(this.maxMemoryBytes);
    out.writeFloat(this.criticalThreshold);
    out.writeFloat(this.evictionThreshold);
  }
}
