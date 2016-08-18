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
package com.gemstone.gemfire.pdx.internal;

import java.nio.ByteBuffer;
import java.util.Date;

import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.tcp.ByteBufferInputStream.ByteSourceFactory;
import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.pdx.PdxInstanceFactory;
import com.gemstone.gemfire.pdx.PdxUnreadFields;

/**
 * PdxInstances created with this factory can never be deserialized
 * but you can access their fields just like any other pdx.
 * <p>The current implementation of this interface is meant for internal use only.
 * The way it defines a PdxType is expensive since it can never figure out it is
 * already defined without doing an expensive check in the type registry.
 * We should optimize this before making this a public feature.
 *
 */
public class PdxInstanceFactoryImpl implements
    PdxInstanceFactory {
  
  private final PdxWriterImpl writer;
  private boolean created = false;

  private PdxInstanceFactoryImpl(String name, boolean expectDomainClass) {
    PdxOutputStream os = new PdxOutputStream();
    PdxType pt = new PdxType(name, expectDomainClass);
    GemFireCacheImpl gfc = GemFireCacheImpl.getForPdx("PDX registry is unavailable because the Cache has been closed.");
    TypeRegistry tr = gfc.getPdxRegistry();
    this.writer = new PdxWriterImpl(pt, tr, os);
  }

  public static PdxInstanceFactory newCreator(String name, boolean expectDomainClass) {
    return new PdxInstanceFactoryImpl(name, expectDomainClass);
  }

  public PdxInstance create() {
    if (this.created) {
      throw new IllegalStateException("The create method can only be called once.");
    }
    this.created = true;
    this.writer.completeByteStreamGeneration();
    return this.writer.makePdxInstance();
  }

  public PdxInstanceFactory writeChar(String fieldName, char value) {
    this.writer.writeChar(fieldName, value);
    return this;
  }

  public PdxInstanceFactory writeBoolean(String fieldName, boolean value) {
    this.writer.writeBoolean(fieldName, value);
    return this;
  }

  public PdxInstanceFactory writeByte(String fieldName, byte value) {
    this.writer.writeByte(fieldName, value);
    return this;
  }

  public PdxInstanceFactory writeShort(String fieldName, short value) {
    this.writer.writeShort(fieldName, value);
    return this;
  }

  public PdxInstanceFactory writeInt(String fieldName, int value) {
    this.writer.writeInt(fieldName, value);
    return this;
  }

  public PdxInstanceFactory writeLong(String fieldName, long value) {
    this.writer.writeLong(fieldName, value);
    return this;
  }

  public PdxInstanceFactory writeFloat(String fieldName, float value) {
    this.writer.writeFloat(fieldName, value);
    return this;
  }

  public PdxInstanceFactory writeDouble(String fieldName, double value) {
    this.writer.writeDouble(fieldName, value);
    return this;
  }

  public PdxInstanceFactory writeDate(String fieldName, Date date) {
    this.writer.writeDate(fieldName, date);
    return this;
  }

  public PdxInstanceFactory writeString(String fieldName, String value) {
    this.writer.writeString(fieldName, value);
    return this;
  }

  public PdxInstanceFactory writeObject(String fieldName, Object object) {
    return writeObject(fieldName, object, false);
  }

  public PdxInstanceFactory writeBooleanArray(String fieldName, boolean[] array) {
    this.writer.writeBooleanArray(fieldName, array);
    return this;
  }

  public PdxInstanceFactory writeCharArray(String fieldName, char[] array) {
    this.writer.writeCharArray(fieldName, array);
    return this;
  }

  public PdxInstanceFactory writeByteArray(String fieldName, byte[] array) {
    this.writer.writeByteArray(fieldName, array);
    return this;
  }

  public PdxInstanceFactory writeShortArray(String fieldName, short[] array) {
    this.writer.writeShortArray(fieldName, array);
    return this;
  }

  public PdxInstanceFactory writeIntArray(String fieldName, int[] array) {
    this.writer.writeIntArray(fieldName, array);
    return this;
  }

  public PdxInstanceFactory writeLongArray(String fieldName, long[] array) {
    this.writer.writeLongArray(fieldName, array);
    return this;
  }

  public PdxInstanceFactory writeFloatArray(String fieldName, float[] array) {
    this.writer.writeFloatArray(fieldName, array);
    return this;
  }

  public PdxInstanceFactory writeDoubleArray(String fieldName, double[] array) {
    this.writer.writeDoubleArray(fieldName, array);
    return this;
  }

  public PdxInstanceFactory writeStringArray(String fieldName, String[] array) {
    this.writer.writeStringArray(fieldName, array);
    return this;
  }

  public PdxInstanceFactory writeObjectArray(String fieldName, Object[] array) {
    return writeObjectArray(fieldName, array, false);
  }

  public PdxInstanceFactory writeUnreadFields(PdxUnreadFields unread) {
    this.writer.writeUnreadFields(unread);
    return this;
  }

  public PdxInstanceFactory writeRaw(PdxField field, ByteBuffer rawData) {
    this.writer.writeRawField(field, ByteSourceFactory.create(rawData));
    return this;
  }

  
  public PdxInstanceFactory writeArrayOfByteArrays(String fieldName,
      byte[][] array) {
    this.writer.writeArrayOfByteArrays(fieldName, array);
    return this;
  }

  public <CT, VT extends CT> PdxInstanceFactory writeField(String fieldName, VT fieldValue, Class<CT> fieldType) {
    return writeField(fieldName, fieldValue, fieldType, false);
  }

  public PdxInstanceFactory markIdentityField(String fieldName) {
    this.writer.markIdentityField(fieldName);
    return this;
  }

  public PdxInstanceFactory writeObject(String fieldName, Object value,
      boolean checkPortability) {
    if (InternalDataSerializer.is662SerializationEnabled()) {
      boolean alreadyInProgress = InternalDataSerializer.isPdxSerializationInProgress();
      if (!alreadyInProgress) {
        InternalDataSerializer.setPdxSerializationInProgress(true);
        try {
          this.writer.writeObject(fieldName, value, checkPortability);
        } finally {
          InternalDataSerializer.setPdxSerializationInProgress(false);
        }
      } else {
        this.writer.writeObject(fieldName, value, checkPortability);
      }
    } else {
      this.writer.writeObject(fieldName, value, checkPortability);
    }
    
    return this;
  }

  public PdxInstanceFactory writeObjectArray(String fieldName, Object[] value,
      boolean checkPortability) {
    if (InternalDataSerializer.is662SerializationEnabled()) {
      boolean alreadyInProgress = InternalDataSerializer.isPdxSerializationInProgress();
      if (!alreadyInProgress) {
        InternalDataSerializer.setPdxSerializationInProgress(true);
        try {
          this.writer.writeObjectArray(fieldName, value, checkPortability);
        } finally {
          InternalDataSerializer.setPdxSerializationInProgress(false);
        }
      } else {
        this.writer.writeObjectArray(fieldName, value, checkPortability);
      }
    } else {
      this.writer.writeObjectArray(fieldName, value, checkPortability);
    }
    return this;
  }

  public <CT, VT extends CT> PdxInstanceFactory writeField(String fieldName,
      VT fieldValue, Class<CT> fieldType, boolean checkPortability) {
    if (InternalDataSerializer.is662SerializationEnabled()) {
      boolean alreadyInProgress = InternalDataSerializer.isPdxSerializationInProgress();
      if (!alreadyInProgress) {
        InternalDataSerializer.setPdxSerializationInProgress(true);
        try {
          this.writer.writeField(fieldName, fieldValue, fieldType, checkPortability);
        } finally {
          InternalDataSerializer.setPdxSerializationInProgress(false);
        }
      } else {
        this.writer.writeField(fieldName, fieldValue, fieldType, checkPortability);
      }
    } else {
      this.writer.writeField(fieldName, fieldValue, fieldType, checkPortability);
    }
    return this;
  }

  public static PdxInstance createPdxEnum(String className, String enumName, int enumOrdinal, GemFireCacheImpl gfc) {
    if (className == null) {
      throw new IllegalArgumentException("className must not be null");
    }
    if (enumName == null) {
      throw new IllegalArgumentException("enumName must not be null");
    }
    TypeRegistry tr = gfc.getPdxRegistry();
    EnumInfo ei = new EnumInfo(className, enumName, enumOrdinal);
    return ei.getPdxInstance(tr.defineEnum(ei));
  }

}
