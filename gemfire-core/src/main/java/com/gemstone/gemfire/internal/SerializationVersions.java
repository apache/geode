/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal;

/**
 * This interface is extended by DataSerializableFixedID and
 * VersionedDataSerializable in order to furnish version information
 * to the serialization infrastructure for backward compatibility
 * 
 * @author bruces
 */

public interface SerializationVersions {
  /**
   * Returns the versions where this classes serialized form was modified.
   * Versions returned by this method are expected to be in increasing
   * ordinal order from 0 .. N.  For instance,<br>
   * Version.GFE_7_0, Version.GFE_7_0_1, Version.GFE_8_0, Version.GFXD_1_1<br>
   * <p>
   * You are expected to implement toDataPre_GFE_7_0_0_0(), fromDataPre_GFE_7_0_0_0(), ...,
   * toDataPre_GFXD_1_1_0_0, fromDataPre_GFXD_1_1_0_0.
   * <p>
   * The method name is formed with the version's product name and its major,
   * minor, release and patch numbers.
   */
  public Version[] getSerializationVersions();

}
