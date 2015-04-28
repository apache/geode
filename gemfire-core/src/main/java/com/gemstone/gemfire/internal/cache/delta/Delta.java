/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.delta;

import com.gemstone.gemfire.cache.EntryEvent;

/**
 * Represents changes to apply to a region entry instead of a new value.
 * A Delta is passed as the new value in a put operation on a Region
 * and knows how to apply itself to an old value.
 *
 * Internal Note: When an update message carries a Delta as a payload,
 * it makes sure it gets deserialized before being put into the region.
 *
 * @author Eric Zoerner
 * @since 5.5
 * @see com.gemstone.gemfire.internal.cache.UpdateOperation
 */
public interface Delta {

  /**
   * Apply delta to the old value from the provided EntryEvent.
   * If the delta cannot be applied for any reason then an (unchecked)
   * exception should be thrown. If the put is being applied in a
   * distributed-ack scope, then the exception will be propagated back
   * to the originating put call, but will not necessarily cause puts
   * in other servers to fail.
   *
   * @param putEvent the EntryEvent for the put operation, from which
   * the old value can be obtained (as well as other information such
   * as the key and region being operated on)
   *
   * @return the new value to be put into the region
   */
  Object apply(EntryEvent<?, ?> putEvent);

  Object merge(Object toMerge, boolean isCreate);

  Object merge(Object toMerge);

  Object getResultantValue();
}
