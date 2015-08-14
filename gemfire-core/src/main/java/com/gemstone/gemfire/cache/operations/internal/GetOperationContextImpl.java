package com.gemstone.gemfire.cache.operations.internal;

import com.gemstone.gemfire.SerializationException;
import com.gemstone.gemfire.cache.operations.GetOperationContext;
import com.gemstone.gemfire.internal.offheap.Releasable;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.Chunk;
import com.gemstone.gemfire.internal.offheap.StoredObject;
import com.gemstone.gemfire.internal.offheap.annotations.Unretained;

/**
 * This subclass's job is to keep customers from getting a reference to a value
 * that is off-heap. Any access to an off-heap value should appear to the customer
 * as a serialized value.
 * 
 * @author dschneider
 *
 */
public class GetOperationContextImpl extends GetOperationContext implements Releasable {

  private boolean released;
  
  public GetOperationContextImpl(Object key, boolean postOperation) {
    super(key, postOperation);
  }

  /**
   * This method is for internal use and should not be on the public apis.
   */
  public @Unretained Object getRawValue() {
    return super.getValue();
  }
  
  @Override
  public Object getObject() {
    Object result = super.getObject();
    if (result instanceof StoredObject) {
      // For off-heap object act as if they are serialized forcing them to call getSerializedValue or getValue
      result = null;
    }
    return result;
  }

  @Override
  public void setObject(Object value, boolean isObject) {
    this.released = false;
    super.setObject(value, isObject);
  }

  @Override
  public void setValue(Object value, boolean isObject) {
    this.released = false;
    super.setValue(value, isObject);
  }

  private void checkForReleasedOffHeapValue(Object v) {
    // Note that we only care about Chunk (instead of all StoredObject) because it is the only one using a refcount
    if (this.released && v instanceof Chunk) {
      throw new IllegalStateException("Attempt to access off-heap value after the OperationContext callback returned.");
    }
  }
  
  @Override
  public byte[] getSerializedValue() {
    byte[] result = super.getSerializedValue();
    if (result == null) {
      Object v = super.getValue();
      if (v instanceof StoredObject) {
        checkForReleasedOffHeapValue(v);
        result = ((StoredObject) v).getValueAsHeapByteArray();
      }
    }
    return result;
  }

  @Override
  public Object getDeserializedValue() throws SerializationException {
    Object result = super.getDeserializedValue();
    if (result instanceof StoredObject) {
      checkForReleasedOffHeapValue(result);
      result = ((StoredObject) result).getValueAsDeserializedHeapObject();
    }
    return result;
  }

  @Override
  public Object getValue() {
    Object result = super.getValue();
    if (result instanceof StoredObject) {
      checkForReleasedOffHeapValue(result);
      // since they called getValue they don't care if it is serialized or deserialized so return it as serialized
      result = ((StoredObject) result).getValueAsHeapByteArray();
    }
    return result;
  }

  @Override
  public void release() {
    // Note that if the context's value is stored off-heap
    // and release has been called then we do not release
    // our value (since this context did not retain it)
    // but we do make sure that any future attempt to access
    // the off-heap value fails.
    if (super.getValue() instanceof Chunk) {
      this.released = true;
    }
  }

}
