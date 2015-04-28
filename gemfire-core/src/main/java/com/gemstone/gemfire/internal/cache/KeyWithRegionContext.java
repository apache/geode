/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.internal.DataSerializableFixedID;

/**
 * Interface that can be implemented by region keys to allow passing the region
 * after deserialization for any region specific initialization. Note that the
 * {@link LocalRegion#setKeyRequiresRegionContext(boolean)} should also be set
 * for {@link #setRegionContext(LocalRegion)} to be invoked by the GemFire
 * layer. It is required that either all keys of the region implement this
 * interface (and the flag
 * {@link LocalRegion#setKeyRequiresRegionContext(boolean)} is set) or none do.
 * 
 * Currently used by SQLFabric for the optimized
 * <code>CompactCompositeRegionKey</code> key implementations that keeps the key
 * as a reference to the raw row bytes and requires a handle of the table schema
 * to interpret those in hashCode/equals/compareTo methods that have no region
 * context information.
 * 
 * @author swale
 */
public interface KeyWithRegionContext extends DataSerializableFixedID {

  /**
   * Pass the region of the key for setting up of any region specific context
   * for the key. In case of recovery from disk the region may not have been
   * fully initialized yet, so the implementation needs to take that into
   * consideration.
   * 
   * @param region
   *          the region of this key
   */
  public void setRegionContext(LocalRegion region);

  /**
   * Changes required to be done to the key, if any, to optimize serialization
   * for sending across when value is also available.
   * 
   * SQLFabric will make the value bytes as null in the key so as to avoid
   * serializing the row twice.
   */
  public KeyWithRegionContext beforeSerializationWithValue(boolean valueIsToken);

  /**
   * Changes required to be done to the key, if any, to after deserializing the
   * key in reply with value available. The value is required to be provided in
   * deserialized format (e.g. for {@link CachedDeserializable}s the
   * deserialized value being wrapped must be passed).
   * 
   * SQLFabric will restore the value bytes that were set as null in
   * {@link #beforeSerializationWithValue}.
   */
  public KeyWithRegionContext afterDeserializationWithValue(Object val);
}
