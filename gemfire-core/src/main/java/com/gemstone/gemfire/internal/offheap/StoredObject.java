package com.gemstone.gemfire.internal.offheap;

import com.gemstone.gemfire.internal.cache.CachedDeserializable;

/**
 * Represents an object stored in the cache.
 * Currently this interface is only used for values stored in off-heap regions.
 * At some point in the future it may also be used for values stored in heap regions. 
 * 
 * @author darrel
 * @since 9.0
 */
public interface StoredObject extends OffHeapReference, CachedDeserializable {

}
