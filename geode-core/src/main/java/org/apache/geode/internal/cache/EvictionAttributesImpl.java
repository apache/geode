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
package org.apache.geode.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.EvictionAttributesMutator;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.lru.HeapLRUCapacityController;
import org.apache.geode.internal.cache.lru.LRUAlgorithm;
import org.apache.geode.internal.cache.lru.LRUCapacityController;
import org.apache.geode.internal.cache.lru.MemLRUCapacityController;

/**
 * Defines the attributes for configuring the eviction controller associated
 * with a <code>Region</code>. EvictionAttributesImpl were born out of the
 * previoulsy deprecated CapacityController interface. Eviction, as defined
 * here, is the process of removing an Entry from the Region and potentially
 * placing it elsewhere, as defined by the
 * {@link org.apache.geode.cache.EvictionAction}. The algorithms used to
 * determine when to perform the <code>EvictionAction</code> are enumerated in
 * the {@link org.apache.geode.cache.EvictionAlgorithm} class.
 * 
 * @see org.apache.geode.cache.EvictionAlgorithm
 * @see org.apache.geode.cache.AttributesFactory
 * @see org.apache.geode.cache.RegionAttributes
 * @see org.apache.geode.cache.AttributesMutator
 * 
 * @since GemFire 5.0
 */
public final class EvictionAttributesImpl extends EvictionAttributes 
  implements EvictionAttributesMutator
{
  private static final long serialVersionUID = -6404395520499379715L;

  private EvictionAlgorithm algorithm = EvictionAlgorithm.NONE;

  private ObjectSizer sizer;

  private volatile int maximum;

  private EvictionAction action = EvictionAction.NONE;

 /** The Eviction Controller instance generated as a result of processing this instance 
  * Typically used for any mutation operations
  */
  private volatile LRUAlgorithm evictionController;

  public EvictionAttributesImpl() {
  }
  
  /**
   * construct a new EvictionAttributes with the same characteristics
   * as the given attributes, but not sharing the same evictionController.
   *
   * <p>author bruce
   */
  public EvictionAttributesImpl(EvictionAttributesImpl other) {
    this.algorithm = other.algorithm;
    this.sizer = other.sizer;
    this.maximum = other.maximum;
    this.action = other.action;
    //this.evictionController = null;
  }
  
  public EvictionAttributesImpl setAlgorithm(EvictionAlgorithm algorithm)
  {
    this.algorithm = algorithm;
    return this;
  }

  public  EvictionAttributesImpl setObjectSizer(ObjectSizer memorySizer)
  {
    this.sizer = memorySizer;
    return this;
  }

  /* (non-Javadoc)
   * @see org.apache.geode.cache.EvictionAttributes#getObjectSizer()
   */
  @Override
  public ObjectSizer getObjectSizer()
  {
    return this.sizer;
  }

  /* (non-Javadoc)
   * @see org.apache.geode.cache.EvictionAttributes#getAlgorithm()
   */
  @Override
  public EvictionAlgorithm getAlgorithm()
  {
    return this.algorithm;
  }

  public void setMaximum(int maximum)
  {
    this.maximum = maximum;
    if (this.evictionController != null) {
      this.evictionController.setLimit(this.maximum);
    }
  }

  /**
   * @param maximum parameter for the given {@link EvictionAlgorithm}
   * @return the instance of {@link EvictionAttributesImpl} on which this method was
   *         called
   */
  public EvictionAttributesImpl internalSetMaximum(int maximum)
  {
    this.maximum = maximum;
    return this;
  }

  /* (non-Javadoc)
   * @see org.apache.geode.cache.EvictionAttributes#getMaximum()
   */
  @Override
  public int getMaximum()
  {
    if (this.algorithm.isLRUHeap()) {
      throw new UnsupportedOperationException("LRUHeap does not support a maximum");
    }
    return this.maximum;
  }

  /**
   * Sets the {@link EvictionAction} on the {@link EvictionAttributesImpl} that the
   * given {@link EvictionAlgorithm} uses to perform the eviction.
   * 
   * @param action
   *          the {@link EvictionAction} used by the {@link EvictionAction}
   * @return the instance of {@link EvictionAttributesMutator} on which this
   *         method was called
   */
  public EvictionAttributesImpl setAction(EvictionAction action)
  {
    if (action != null) {
      this.action = action;
    } else {
      this.action = EvictionAction.NONE;
    }
    return this;
  }

  /* (non-Javadoc)
   * @see org.apache.geode.cache.EvictionAttributes#getAction()
   */
  @Override
  public EvictionAction getAction()
  {
    return this.action;
  }

  /** 
   * Build the appropriate eviction controller using the attributes provided.
   * 
   * @return the super of the eviction controller or null if no {@link EvictionAction} 
   * is set.
   * 
   * @see EvictionAttributes
   */
  public LRUAlgorithm createEvictionController(Region region, boolean isOffHeap) 
  {
    if (this.algorithm == EvictionAlgorithm.LRU_ENTRY) {
      this.evictionController = new LRUCapacityController(this.maximum, this.action,region); 
    } else if (this.algorithm == EvictionAlgorithm.LRU_HEAP) {
      this.evictionController = new HeapLRUCapacityController(this.sizer,this.action,region);       
    } else if (this.algorithm == EvictionAlgorithm.LRU_MEMORY) {
      this.evictionController = new MemLRUCapacityController(this.maximum, this.sizer, this.action,region, isOffHeap);
    } else if(this.algorithm == EvictionAlgorithm.LIFO_ENTRY){
      this.evictionController = new LRUCapacityController(this.maximum, this.action,region);
    } else if(this.algorithm == EvictionAlgorithm.LIFO_MEMORY){
      this.evictionController = new MemLRUCapacityController(this.maximum, this.sizer, this.action,region, isOffHeap);
    }  else {
      // for all other algorithms, return null
      this.evictionController = null;
    }
    return this.evictionController;
  }
  
  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.maximum);
    DataSerializer.writeObject(this.action, out);
    DataSerializer.writeObject(this.algorithm, out);
  }
  
  public void fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    this.maximum = in.readInt();
    this.action = (EvictionAction)DataSerializer.readObject(in);
    this.algorithm = (EvictionAlgorithm)DataSerializer.readObject(in);
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

  public final boolean isLIFOEntry() {
    return this.algorithm == EvictionAlgorithm.LIFO_ENTRY;
  }

  public final boolean isLIFOMemory() {
    return this.algorithm == EvictionAlgorithm.LIFO_MEMORY;
  }

 
}
