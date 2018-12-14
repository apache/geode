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
package org.apache.geode.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.EvictionAttributesMutator;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.InternalDataSerializer;

/**
 * Defines the attributes for configuring the eviction controller associated with a
 * <code>Region</code>. Eviction, as defined here, is the process of removing an Entry from the
 * Region and potentially placing it elsewhere, as defined by the {@link EvictionAction}. The
 * algorithms used to determine when to perform the <code>EvictionAction</code> are enumerated in
 * the {@link EvictionAlgorithm} class.
 *
 * @see org.apache.geode.cache.EvictionAlgorithm
 * @see org.apache.geode.cache.AttributesFactory
 * @see org.apache.geode.cache.RegionAttributes
 * @see org.apache.geode.cache.AttributesMutator
 *
 * @since GemFire 5.0
 */
public class EvictionAttributesImpl extends EvictionAttributes {
  private static final long serialVersionUID = -6404395520499379715L;

  private EvictionAlgorithm algorithm = EvictionAlgorithm.NONE;

  private ObjectSizer sizer;

  private volatile int maximum;

  private EvictionAction action = EvictionAction.NONE;

  public EvictionAttributesImpl() {}

  /**
   * copy constructor
   */
  public EvictionAttributesImpl(EvictionAttributes other) {
    this.algorithm = other.getAlgorithm();
    this.sizer = other.getObjectSizer();
    this.maximum = other.getMaximum();
    this.action = other.getAction();
  }

  public EvictionAttributesImpl setAlgorithm(EvictionAlgorithm algorithm) {
    this.algorithm = algorithm;
    return this;
  }

  public EvictionAttributesImpl setObjectSizer(ObjectSizer memorySizer) {
    this.sizer = memorySizer;
    return this;
  }

  @Override
  public ObjectSizer getObjectSizer() {
    return this.sizer;
  }

  @Override
  public EvictionAlgorithm getAlgorithm() {
    return this.algorithm;
  }

  /**
   * @param maximum parameter for the given {@link EvictionAlgorithm}
   * @return the instance of {@link EvictionAttributesImpl} on which this method was called
   */
  public EvictionAttributesImpl setMaximum(int maximum) {
    this.maximum = maximum;
    return this;
  }

  @Override
  public int getMaximum() {
    if (this.algorithm.isLRUHeap()) {
      return 0;
    }
    return this.maximum;
  }

  /**
   * Sets the {@link EvictionAction} on the {@link EvictionAttributesImpl} that the given
   * {@link EvictionAlgorithm} uses to perform the eviction.
   *
   * @param action the {@link EvictionAction} used by the {@link EvictionAction}
   * @return the instance of {@link EvictionAttributesMutator} on which this method was called
   */
  public EvictionAttributesImpl setAction(EvictionAction action) {
    if (action != null) {
      this.action = action;
    } else {
      this.action = EvictionAction.NONE;
    }
    return this;
  }

  @Override
  public EvictionAction getAction() {
    return this.action;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.maximum);
    DataSerializer.writeObject(this.action, out);
    DataSerializer.writeObject(this.algorithm, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.maximum = in.readInt();
    this.action = DataSerializer.readObject(in);
    this.algorithm = DataSerializer.readObject(in);
  }

  public static EvictionAttributesImpl createFromData(DataInput in)
      throws IOException, ClassNotFoundException {
    EvictionAttributesImpl result = new EvictionAttributesImpl();
    InternalDataSerializer.invokeFromData(result, in);
    return result;
  }

  /**
   * Returns true if this object uses a LIFO algorithm
   *
   * @since GemFire 5.7
   */
  public boolean isLIFO() {
    return this.algorithm.isLIFO();
  }

  public boolean isLIFOEntry() {
    return this.algorithm == EvictionAlgorithm.LIFO_ENTRY;
  }

  public boolean isLIFOMemory() {
    return this.algorithm == EvictionAlgorithm.LIFO_MEMORY;
  }

  public static EvictionAttributesImpl fromConfig(
      RegionAttributesType.EvictionAttributes configAttributes)
      throws ClassCastException, InstantiationException,
      IllegalAccessException {
    EvictionAttributesImpl evictionAttributes = new EvictionAttributesImpl();

    if (configAttributes.getLruHeapPercentage() != null) {
      evictionAttributes.setAlgorithm(EvictionAlgorithm.LRU_HEAP);
    } else if (configAttributes.getLruEntryCount() != null) {
      evictionAttributes.setAlgorithm(EvictionAlgorithm.LRU_ENTRY);
    } else if (configAttributes.getLruMemorySize() != null) {
      evictionAttributes.setAlgorithm(EvictionAlgorithm.LRU_MEMORY);
    } else {
      evictionAttributes.setAlgorithm(EvictionAlgorithm.NONE);
    }

    String sizerClassName = null;
    if (configAttributes.getLruHeapPercentage() != null) {
      sizerClassName = configAttributes.getLruHeapPercentage().getClassName();
    } else if (configAttributes.getLruMemorySize() != null) {
      sizerClassName = configAttributes.getLruMemorySize().getClassName();
    }

    if (sizerClassName != null) {
      ObjectSizer sizer;
      try {
        sizer = (ObjectSizer) ClassPathLoader.getLatest().forName(sizerClassName).newInstance();
      } catch (ClassNotFoundException e) {
        sizer = ObjectSizer.DEFAULT;
      }
      if (sizer != null && !(sizer instanceof Declarable)) {
        throw new ClassCastException();
      }
      evictionAttributes.setObjectSizer(sizer);
    }

    if (configAttributes.getLruMemorySize() != null) {
      evictionAttributes
          .setMaximum(Integer.valueOf(configAttributes.getLruMemorySize().getMaximum()));
    } else if (configAttributes.getLruEntryCount() != null) {
      evictionAttributes
          .setMaximum(Integer.valueOf(configAttributes.getLruEntryCount().getMaximum()));
    } else {
      evictionAttributes.setMaximum(0);
    }

    if (configAttributes.getLruMemorySize() != null) {
      evictionAttributes
          .setAction(EvictionAction.parseAction(configAttributes.getLruMemorySize().getAction()
              .value()));
    } else if (configAttributes.getLruEntryCount() != null) {
      evictionAttributes
          .setAction(EvictionAction.parseAction(configAttributes.getLruEntryCount().getAction()
              .value()));
    } else if (configAttributes.getLruHeapPercentage() != null) {
      evictionAttributes
          .setAction(EvictionAction.parseAction(configAttributes.getLruHeapPercentage().getAction()
              .value()));
    } else {
      evictionAttributes.setAction(EvictionAction.NONE);
    }

    return evictionAttributes;
  }
}
