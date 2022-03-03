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
package org.apache.geode.pdx.internal;

import java.nio.ByteBuffer;
import java.util.Date;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.pdx.PdxFieldDoesNotExistException;
import org.apache.geode.pdx.PdxFieldTypeMismatchException;
import org.apache.geode.pdx.WritablePdxInstance;

public class WritablePdxInstanceImpl extends PdxInstanceImpl implements WritablePdxInstance {
  private static final long serialVersionUID = 7398999150097596214L;
  private static final Object NULL_TOKEN = new Object();
  private Object[] dirtyFields = null;

  public WritablePdxInstanceImpl(PdxReaderImpl original) {
    super(original);
  }

  private synchronized void dirtyField(PdxField f, Object value) {
    if (dirtyFields == null) {
      dirtyFields = new Object[getPdxType().getFieldCount()];
    }
    if (value == null) {
      value = NULL_TOKEN;
    }
    dirtyFields[f.getFieldIndex()] = value;
    clearCachedState();
  }

  /**
   * Flush pending writes if the given field is dirty.
   */
  @Override
  protected synchronized PdxReaderImpl getUnmodifiableReader(String fieldName) {
    if (dirtyFields != null) {
      PdxField f = getPdxType().getPdxField(fieldName);
      if (f != null) {
        if (dirtyFields[f.getFieldIndex()] != null) {
          return getUnmodifiableReader();
        }
      }
    }
    return new PdxReaderImpl(this);
  }

  @Override
  public synchronized Object getCachedObject() {
    return super.getCachedObject();
  }

  @Override
  public synchronized int hashCode() {
    return super.hashCode();
  }

  /**
   * Flush any pending writes.
   */
  @Override
  protected synchronized PdxReaderImpl getUnmodifiableReader() {
    if (dirtyFields != null) {
      PdxOutputStream os = new PdxOutputStream(basicSize() + PdxWriterImpl.HEADER_SIZE);
      PdxWriterImpl writer;
      if (getPdxType().getHasDeletedField()) {
        // Need a new type that does not have the deleted field
        PdxType pt = new PdxType(getPdxType().getClassName(), !getPdxType().getNoDomainClass());
        InternalCache cache = GemFireCacheImpl
            .getForPdx("PDX registry is unavailable because the Cache has been closed.");
        TypeRegistry tr = cache.getPdxRegistry();
        writer = new PdxWriterImpl(pt, tr, os);
      } else {
        writer = new PdxWriterImpl(getPdxType(), os);
      }
      for (PdxField f : getPdxType().getFields()) {
        if (f.isDeleted()) {
          continue;
        }
        Object dv = dirtyFields[f.getFieldIndex()];
        if (dv != null) {
          if (dv == NULL_TOKEN) {
            dv = null;
          }
          writer.writeField(f, dv);
        } else {
          writer.writeRawField(f, getRaw(f));
        }
      }
      writer.completeByteStreamGeneration();
      ByteBuffer bb = os.toByteBuffer();
      bb.position(PdxWriterImpl.HEADER_SIZE);
      basicSetBuffer(bb.slice());
      dirtyFields = null;
    }
    return new PdxReaderImpl(this);
  }

  @Override
  public void setField(String fieldName, Object value) {
    PdxField f = getPdxType().getPdxField(fieldName);
    if (f == null) {
      throw new PdxFieldDoesNotExistException(
          "A field named " + fieldName + " does not exist on " + getPdxType());
    }
    if (value != null) {
      switch (f.getFieldType()) {
        case CHAR:
          if (!(value instanceof Character)) {
            throw new PdxFieldTypeMismatchException(
                "Values for this field must be a Character but was a " + value.getClass());
          }
          break;
        case BOOLEAN:
          if (!(value instanceof Boolean)) {
            throw new PdxFieldTypeMismatchException(
                "Values for this field must be a Boolean but was a " + value.getClass());
          }
          break;
        case BYTE:
          if (!(value instanceof Byte)) {
            throw new PdxFieldTypeMismatchException(
                "Values for this field must be a Byte but was a " + value.getClass());
          }
          break;
        case SHORT:
          if (!(value instanceof Short)) {
            throw new PdxFieldTypeMismatchException(
                "Values for this field must be a Short but was a " + value.getClass());
          }
          break;
        case INT:
          if (!(value instanceof Integer)) {
            throw new PdxFieldTypeMismatchException(
                "Values for this field must be a Integer but was a " + value.getClass());
          }
          break;
        case LONG:
          if (!(value instanceof Long)) {
            throw new PdxFieldTypeMismatchException(
                "Values for this field must be a Long but was a " + value.getClass());
          }
          break;
        case FLOAT:
          if (!(value instanceof Float)) {
            throw new PdxFieldTypeMismatchException(
                "Values for this field must be a Float but was a " + value.getClass());
          }
          break;
        case DOUBLE:
          if (!(value instanceof Double)) {
            throw new PdxFieldTypeMismatchException(
                "Values for this field must be a Double but was a " + value.getClass());
          }
          break;
        case STRING:
          if (!(value instanceof String)) {
            throw new PdxFieldTypeMismatchException(
                "Values for this field must be a String but was a " + value.getClass());
          }
          break;
        case BOOLEAN_ARRAY:
          if (!(value instanceof boolean[])) {
            throw new PdxFieldTypeMismatchException(
                "Values for this field must be a boolean[] but was a " + value.getClass());
          }
          break;
        case CHAR_ARRAY:
          if (!(value instanceof char[])) {
            throw new PdxFieldTypeMismatchException(
                "Values for this field must be a char[] but was a " + value.getClass());
          }
          break;
        case BYTE_ARRAY:
          if (!(value instanceof byte[])) {
            throw new PdxFieldTypeMismatchException(
                "Values for this field must be a byte[] but was a " + value.getClass());
          }
          break;
        case SHORT_ARRAY:
          if (!(value instanceof short[])) {
            throw new PdxFieldTypeMismatchException(
                "Values for this field must be a short[] but was a " + value.getClass());
          }
          break;
        case INT_ARRAY:
          if (!(value instanceof int[])) {
            throw new PdxFieldTypeMismatchException(
                "Values for this field must be a int[] but was a " + value.getClass());
          }
          break;
        case LONG_ARRAY:
          if (!(value instanceof long[])) {
            throw new PdxFieldTypeMismatchException(
                "Values for this field must be a long[] but was a " + value.getClass());
          }
          break;
        case FLOAT_ARRAY:
          if (!(value instanceof float[])) {
            throw new PdxFieldTypeMismatchException(
                "Values for this field must be a float[] but was a " + value.getClass());
          }
          break;
        case DOUBLE_ARRAY:
          if (!(value instanceof double[])) {
            throw new PdxFieldTypeMismatchException(
                "Values for this field must be a double[] but was a " + value.getClass());
          }
          break;
        case STRING_ARRAY:
          if (!(value instanceof String[])) {
            throw new PdxFieldTypeMismatchException(
                "Values for this field must be a String[] but was a " + value.getClass());
          }
          break;
        case ARRAY_OF_BYTE_ARRAYS:
          if (!(value instanceof byte[][])) {
            throw new PdxFieldTypeMismatchException(
                "Values for this field must be a byte[][] but was a " + value.getClass());
          }
          break;
        case OBJECT_ARRAY:
          if (!(value instanceof Object[])) {
            throw new PdxFieldTypeMismatchException(
                "Values for this field must be a Object[] but was a " + value.getClass());
          }
          break;
        case OBJECT:
          // no check needed
          break;
        // All of the following classes are not final. We only support the exact class in this case;
        // not subclasses.
        case DATE:
          if (!Date.class.equals(value.getClass())) {
            throw new PdxFieldTypeMismatchException(
                "Values for this field must be a Date but was a " + value.getClass());
          }
          break;
        default:
          throw new InternalGemFireException("Unhandled field type " + f.getFieldType());
      }
    } else {
      switch (f.getFieldType()) {
        case CHAR:
          value = (char) 0;
          break;
        case BOOLEAN:
          value = Boolean.FALSE;
          break;
        case BYTE:
          value = (byte) 0;
          break;
        case SHORT:
          value = (short) 0;
          break;
        case INT:
          value = 0;
          break;
        case FLOAT:
          value = 0.0f;
          break;
        case DOUBLE:
          value = 0.0;
          break;
        case LONG:
          value = 0L;
          break;
        case DATE:
        case STRING:
        case BOOLEAN_ARRAY:
        case CHAR_ARRAY:
        case BYTE_ARRAY:
        case SHORT_ARRAY:
        case INT_ARRAY:
        case LONG_ARRAY:
        case FLOAT_ARRAY:
        case DOUBLE_ARRAY:
        case STRING_ARRAY:
        case ARRAY_OF_BYTE_ARRAYS:
        case OBJECT_ARRAY:
        case OBJECT:
          // null ok
          break;
        default:
          throw new InternalGemFireException("Unhandled field type " + f.getFieldType());
      }
    }
    dirtyField(f, value);
  }

  @Override
  public boolean equals(Object obj) {
    // no need to compare dirtyFields
    // Overriding just to make this clear
    return super.equals(obj);
  }
}
