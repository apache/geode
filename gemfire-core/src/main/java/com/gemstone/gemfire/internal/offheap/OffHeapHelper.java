package com.gemstone.gemfire.internal.offheap;

import com.gemstone.gemfire.internal.cache.CachedDeserializableFactory;
import com.gemstone.gemfire.internal.offheap.annotations.Released;
import com.gemstone.gemfire.internal.offheap.annotations.Retained;
import com.gemstone.gemfire.internal.offheap.annotations.Unretained;

/**
 * Utility class that provides static method to do some common tasks for off-heap references.
 * 
 * @author darrel
 * @since 9.0
 */
public class OffHeapHelper {
  private OffHeapHelper() {
    // no instances allowed
  }
  
  /**
   * If o is off-heap then return its heap form; otherwise return o since it is already on the heap.
   * Note even if o is sqlf off-heap byte[] or byte[][] the heap form will be created.
   */
  public static Object getHeapForm(Object o) {
    if (o instanceof OffHeapReference) {
      return ((OffHeapReference) o).getValueAsDeserializedHeapObject();
    } else {
      return o;
    }
  }

  /**
   * Just like {@link #copyIfNeeded(Object)} except that if off-heap is copied it is also released.
   * @param v If this value is off-heap then the caller must have already retained it.
   * @return the heap copy to use in place of v
   */
  public static Object copyAndReleaseIfNeeded(@Released Object v) {
    if (v instanceof StoredObject) {
      @Unretained StoredObject ohv = (StoredObject) v;
      try {
      if (ohv.isSerialized()) {
        return CachedDeserializableFactory.create(ohv.getSerializedValue());
      } else {
        // it is a byte[]
        return ohv.getDeserializedForReading();
      }
      } finally {
        ohv.release();
      }
    } else {
      return v;
    }
  }

  /**
   * If v is on heap then just return v; no copy needed.
   * Else v is off-heap so copy it to heap and return a reference to the heap copy.
   * Note that unlike {@link #getHeapForm(Object)} if v is a serialized off-heap object it will be copied to the heap as a CachedDeserializable.
   * If you prefer to have the serialized object also deserialized and copied to the heap use {@link #getHeapForm(Object)}.
   * 
   * @param v possible OFF_HEAP_REFERENCE
   * @return v or a heap copy of v
   */
  public static Object copyIfNeeded(@Unretained Object v) {
    if (v instanceof StoredObject) {
      @Unretained StoredObject ohv = (StoredObject) v;
      if (ohv.isSerialized()) {
        v = CachedDeserializableFactory.create(ohv.getSerializedValue());
      } else {
        // it is a byte[]
        v = ohv.getDeserializedForReading();
      }
    }
    return v;
  }
  
  /**
   * @return true if release was done
   */
  public static boolean release(@Released Object o) {
    if (o instanceof MemoryChunkWithRefCount) {
      ((MemoryChunkWithRefCount) o).release();
      return true;
    } else {
      return false;
    }
  }
  /**
   * Just like {@link #release(Object)} but also disable debug tracking of the release.
   * @return true if release was done
   */
  public static boolean releaseWithNoTracking(@Released Object o) {
    if (o instanceof MemoryChunkWithRefCount) {
      SimpleMemoryAllocatorImpl.skipRefCountTracking();
      ((MemoryChunkWithRefCount) o).release();
      SimpleMemoryAllocatorImpl.unskipRefCountTracking();
      return true;
    } else {
      return false;
    }
  }
  
  /**
   * Just like {@link #release(Object)} but also set the owner for debug tracking of the release.
   * @return true if release was done
   */
  public static boolean releaseAndTrackOwner(@Released final Object o, final Object owner) {
    if (o instanceof MemoryChunkWithRefCount) {
      SimpleMemoryAllocatorImpl.setReferenceCountOwner(owner);
      ((MemoryChunkWithRefCount) o).release();
      SimpleMemoryAllocatorImpl.setReferenceCountOwner(null);
      return true;
    } else {
      return false;
    }
  }

}
