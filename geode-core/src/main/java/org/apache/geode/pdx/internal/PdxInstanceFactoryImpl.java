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

import java.util.Date;

import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;

/**
 * PdxInstances created with this factory can never be deserialized but you can access their fields
 * just like any other pdx.
 * <p>
 * The current implementation of this interface is meant for internal use only. The way it defines a
 * PdxType is expensive since it can never figure out it is already defined without doing an
 * expensive check in the type registry. We should optimize this before making this a public
 * feature.
 */
public class PdxInstanceFactoryImpl implements PdxInstanceFactory {

  private final PdxWriterImpl writer;
  private final PdxType pdxType;

  private boolean created = false;

  private PdxInstanceFactoryImpl(String name, boolean expectDomainClass, TypeRegistry pdxRegistry) {
    if (name == null) {
      throw new IllegalArgumentException(
          "Class name can not be null when creating a PdxInstanceFactory");
    }
    if (name.isEmpty()) {
      expectDomainClass = false;
    }
    PdxOutputStream pdxOutputStream = new PdxOutputStream();
    this.pdxType = new PdxType(name, expectDomainClass);
    this.writer = new PdxWriterImpl(pdxType, pdxRegistry, pdxOutputStream);
  }

  public static PdxInstanceFactory newCreator(String name, boolean expectDomainClass,
      InternalCache cache) {
    return new PdxInstanceFactoryImpl(name, expectDomainClass, cache.getPdxRegistry());
  }

  @Override
  public PdxInstance create() {
    if (this.created) {
      throw new IllegalStateException("The create method can only be called once.");
    }
    this.created = true;
    this.writer.completeByteStreamGeneration();
    return this.writer.makePdxInstance();
  }

  @Override
  public PdxInstanceFactory writeChar(String fieldName, char value) {
    this.writer.writeChar(fieldName, value);
    return this;
  }

  @Override
  public PdxInstanceFactory writeBoolean(String fieldName, boolean value) {
    this.writer.writeBoolean(fieldName, value);
    return this;
  }

  @Override
  public PdxInstanceFactory writeByte(String fieldName, byte value) {
    this.writer.writeByte(fieldName, value);
    return this;
  }

  @Override
  public PdxInstanceFactory writeShort(String fieldName, short value) {
    this.writer.writeShort(fieldName, value);
    return this;
  }

  @Override
  public PdxInstanceFactory writeInt(String fieldName, int value) {
    this.writer.writeInt(fieldName, value);
    return this;
  }

  @Override
  public PdxInstanceFactory writeLong(String fieldName, long value) {
    this.writer.writeLong(fieldName, value);
    return this;
  }

  @Override
  public PdxInstanceFactory writeFloat(String fieldName, float value) {
    this.writer.writeFloat(fieldName, value);
    return this;
  }

  @Override
  public PdxInstanceFactory writeDouble(String fieldName, double value) {
    this.writer.writeDouble(fieldName, value);
    return this;
  }

  @Override
  public PdxInstanceFactory writeDate(String fieldName, Date value) {
    this.writer.writeDate(fieldName, value);
    return this;
  }

  @Override
  public PdxInstanceFactory writeString(String fieldName, String value) {
    this.writer.writeString(fieldName, value);
    return this;
  }

  @Override
  public PdxInstanceFactory writeObject(String fieldName, Object value) {
    return writeObject(fieldName, value, false);
  }

  @Override
  public PdxInstanceFactory writeBooleanArray(String fieldName, boolean[] value) {
    this.writer.writeBooleanArray(fieldName, value);
    return this;
  }

  @Override
  public PdxInstanceFactory writeCharArray(String fieldName, char[] value) {
    this.writer.writeCharArray(fieldName, value);
    return this;
  }

  @Override
  public PdxInstanceFactory writeByteArray(String fieldName, byte[] value) {
    this.writer.writeByteArray(fieldName, value);
    return this;
  }

  @Override
  public PdxInstanceFactory writeShortArray(String fieldName, short[] value) {
    this.writer.writeShortArray(fieldName, value);
    return this;
  }

  @Override
  public PdxInstanceFactory writeIntArray(String fieldName, int[] value) {
    this.writer.writeIntArray(fieldName, value);
    return this;
  }

  @Override
  public PdxInstanceFactory writeLongArray(String fieldName, long[] value) {
    this.writer.writeLongArray(fieldName, value);
    return this;
  }

  @Override
  public PdxInstanceFactory writeFloatArray(String fieldName, float[] value) {
    this.writer.writeFloatArray(fieldName, value);
    return this;
  }

  @Override
  public PdxInstanceFactory writeDoubleArray(String fieldName, double[] value) {
    this.writer.writeDoubleArray(fieldName, value);
    return this;
  }

  @Override
  public PdxInstanceFactory writeStringArray(String fieldName, String[] value) {
    this.writer.writeStringArray(fieldName, value);
    return this;
  }

  @Override
  public PdxInstanceFactory writeObjectArray(String fieldName, Object[] value) {
    return writeObjectArray(fieldName, value, false);
  }

  @Override
  public PdxInstanceFactory writeArrayOfByteArrays(String fieldName, byte[][] value) {
    this.writer.writeArrayOfByteArrays(fieldName, value);
    return this;
  }

  @Override
  public <CT, VT extends CT> PdxInstanceFactory writeField(String fieldName, VT fieldValue,
      Class<CT> fieldType) {
    return writeField(fieldName, fieldValue, fieldType, false);
  }

  @Override
  public PdxInstanceFactory markIdentityField(String fieldName) {
    this.writer.markIdentityField(fieldName);
    return this;
  }

  @Override
  public PdxInstanceFactory writeObject(String fieldName, Object value, boolean checkPortability) {
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

  @Override
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

  @Override
  public <CT, VT extends CT> PdxInstanceFactory writeField(String fieldName, VT fieldValue,
      Class<CT> fieldType, boolean checkPortability) {
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

  public static PdxInstance createPdxEnum(String className, String enumName, int enumOrdinal,
      InternalCache internalCache) {
    if (className == null) {
      throw new IllegalArgumentException("className must not be null");
    }
    if (enumName == null) {
      throw new IllegalArgumentException("enumName must not be null");
    }
    TypeRegistry tr = internalCache.getPdxRegistry();
    EnumInfo ei = new EnumInfo(className, enumName, enumOrdinal);
    return ei.getPdxInstance(tr.defineEnum(ei));
  }

  @Override
  public PdxInstanceFactory neverDeserialize() {
    this.pdxType.setNoDomainClass(true);
    return this;
  }

}
