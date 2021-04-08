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
package org.apache.geode.internal.serialization.internal;

import static org.apache.geode.internal.serialization.DataSerializableFixedID.NO_FIXED_ID;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.NotSerializableException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Map;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.serialization.BasicSerializable;
import org.apache.geode.internal.serialization.DSCODE;
import org.apache.geode.internal.serialization.DSFIDNotFoundException;
import org.apache.geode.internal.serialization.DSFIDSerializer;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.DscodeHelper;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.ObjectDeserializer;
import org.apache.geode.internal.serialization.ObjectSerializer;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.SerializationVersions;
import org.apache.geode.internal.serialization.StaticDeserialization;
import org.apache.geode.internal.serialization.StaticSerialization;

public class DSFIDSerializerImpl implements DSFIDSerializer {

  @Immutable
  private final Constructor<?>[] dsfidMap = new Constructor<?>[256];

  @Immutable("This maybe should be wrapped in an unmodifiableMap?")
  private final Int2ObjectOpenHashMap<Constructor<?>> dsfidMap2 = new Int2ObjectOpenHashMap<>(800);

  private final ObjectSerializer objectSerializer;
  private final ObjectDeserializer objectDeserializer;

  public DSFIDSerializerImpl() {
    objectSerializer = createDefaultObjectSerializer();
    objectDeserializer = createDefaultObjectDeserializer();
  }

  public DSFIDSerializerImpl(ObjectSerializer objectSerializer,
      ObjectDeserializer objectDeserializer) {
    this.objectSerializer =
        objectSerializer == null ? createDefaultObjectSerializer() : objectSerializer;
    this.objectDeserializer =
        objectDeserializer == null ? createDefaultObjectDeserializer() : objectDeserializer;
  }

  private ObjectSerializer createDefaultObjectSerializer() {
    return new ObjectSerializer() {
      @Override
      public void writeObject(Object obj, DataOutput output) throws IOException {
        if (obj == null || obj instanceof BasicSerializable) {
          write((BasicSerializable) obj, output);
        } else {
          throw new NotSerializableException("object with class " + obj.getClass().getName() +
              " is not serializable via DSFIDSerializerImpl");
        }
      }

      @Override
      public void writeInetAddress(InetAddress address, DataOutput out) throws IOException {
        StaticSerialization.writeInetAddress(address, out);
      }

      @Override
      public void writeByteArray(byte[] array, DataOutput out) throws IOException {
        StaticSerialization.writeByteArray(array, out);
      }

      @Override
      public void writeArrayLength(int len, DataOutput out) throws IOException {
        StaticSerialization.writeArrayLength(len, out);
      }

      @Override
      public void writeString(String value, DataOutput out) throws IOException {
        StaticSerialization.writeString(value, out);
      }

      @Override
      public void writeStringArray(String[] array, DataOutput out) throws IOException {
        StaticSerialization.writeStringArray(array, out);
      }

      @Override
      public void writeInteger(Integer value, DataOutput out) throws IOException {
        StaticSerialization.writeInteger(value, out);
      }

      @Override
      public void writeIntArray(int[] array, DataOutput out) throws IOException {
        StaticSerialization.writeIntArray(array, out);
      }

      @Override
      public void writeHashMap(Map<?, ?> map, DataOutput out, SerializationContext context)
          throws IOException {
        StaticSerialization.writeHashMap(map, out, context);
      }

      @Override
      public void invokeToData(Object ds, DataOutput out) throws IOException {
        DSFIDSerializerImpl.this.invokeToData(ds, out);
      }
    };
  }

  private ObjectDeserializer createDefaultObjectDeserializer() {
    return new ObjectDeserializer() {
      @SuppressWarnings("unchecked")
      @Override
      public <T> T readObject(DataInput input) throws IOException, ClassNotFoundException {
        return (T) readDSFID(input);
      }

      @Override
      public String readString(DataInput in) throws IOException {
        return StaticDeserialization.readString(in);
      }

      @Override
      public InetAddress readInetAddress(DataInput in) throws IOException {
        return StaticDeserialization.readInetAddress(in);
      }

      @Override
      public <K, V> HashMap<K, V> readHashMap(DataInput in, DeserializationContext context)
          throws IOException, ClassNotFoundException {
        return StaticDeserialization.readHashMap(in, context);
      }

      @Override
      public int readArrayLength(DataInput in) throws IOException {
        return StaticDeserialization.readArrayLength(in);
      }

      @Override
      public String[] readStringArray(DataInput in) throws IOException {
        return StaticDeserialization.readStringArray(in);
      }

      @Override
      public int[] readIntArray(DataInput in) throws IOException {
        return StaticDeserialization.readIntArray(in);
      }

      @Override
      public byte[] readByteArray(DataInput in) throws IOException {
        return StaticDeserialization.readByteArray(in);
      }

      @Override
      public void invokeFromData(Object ds, DataInput in)
          throws IOException, ClassNotFoundException {
        DSFIDSerializerImpl.this.invokeFromData(ds, in);
      }
    };
  }

  @Override
  public ObjectSerializer getObjectSerializer() {
    return objectSerializer;
  }

  @Override
  public ObjectDeserializer getObjectDeserializer() {
    return objectDeserializer;
  }

  // Writes just the header of a DataSerializableFixedID to out.
  @Override
  public void writeDSFIDHeader(int dsfid, DataOutput out) throws IOException {
    if (dsfid == DataSerializableFixedID.ILLEGAL) {
      throw new IllegalStateException(
          "attempted to serialize ILLEGAL dsfid");
    }
    if (dsfid <= Byte.MAX_VALUE && dsfid >= Byte.MIN_VALUE) {
      out.writeByte(DSCODE.DS_FIXED_ID_BYTE.toByte());
      out.writeByte(dsfid);
    } else if (dsfid <= Short.MAX_VALUE && dsfid >= Short.MIN_VALUE) {
      out.writeByte(DSCODE.DS_FIXED_ID_SHORT.toByte());
      out.writeShort(dsfid);
    } else {
      out.writeByte(DSCODE.DS_FIXED_ID_INT.toByte());
      out.writeInt(dsfid);
    }
  }

  @Override
  public void write(final BasicSerializable bs, final DataOutput out) throws IOException {
    writeMetaData(bs, out);
    if (bs != null) {
      invokeToData(bs, out);
    }
  }

  private void writeMetaData(final BasicSerializable bs, final DataOutput out)
      throws IOException {
    if (bs == null) {
      out.writeByte(DSCODE.NULL.toByte());
      return;
    }
    if (bs instanceof DataSerializableFixedID) {
      final DataSerializableFixedID dsfid = (DataSerializableFixedID) bs;
      final int id = dsfid.getDSFID();
      if (id == NO_FIXED_ID) {
        throw new IllegalArgumentException(
            "NO_FIXED_ID is not supported by BasicDSFIDSerializer - use InternalDataSerializer instead: "
                + dsfid.getClass().getName());
      }
      writeDSFIDHeader(id, out);
    } else {
      out.writeByte(DSCODE.DATA_SERIALIZABLE.toByte());
      final Class<?> c = bs.getClass();
      StaticSerialization.writeClass(c, out);
    }
  }

  /**
   * For backward compatibility this method should be used to invoke toData on a DSFID.
   * It will invoke the correct toData method based on the class's version
   * information. This method does not write information about the class of the object. When
   * deserializing use the method invokeFromData to read the contents of the object.
   *
   * @param ds the object to write
   * @param out the output stream.
   */
  @Override
  public void invokeToData(Object ds, DataOutput out) throws IOException {

    final SerializationContext context = new SerializationContextImpl(out, this);

    boolean isDSFID = ds instanceof DataSerializableFixedID;

    if (!isDSFID) {
      if (ds instanceof BasicSerializable) {
        ((BasicSerializable) ds).toData(out, context);
        return;
      }
      throw new IllegalArgumentException(
          "Expected a DataSerializableFixedID but found " + ds.getClass().getName());
    }

    try {
      boolean invoked = false;
      KnownVersion v = context.getSerializationVersion();

      if (!KnownVersion.CURRENT.equals(v)) {
        // get versions where DataOutput was upgraded
        SerializationVersions sv = (SerializationVersions) ds;
        KnownVersion[] versions = sv.getSerializationVersions();
        // check if the version of the peer or diskstore is different and
        // there has been a change in the message
        if (versions != null) {
          for (KnownVersion version : versions) {
            // if peer version is less than the greatest upgraded version
            if (v.compareTo(version) < 0) {
              ds.getClass().getMethod("toDataPre_" + version.getMethodSuffix(),
                  new Class[] {DataOutput.class, SerializationContext.class})
                  .invoke(ds, out, context);
              invoked = true;
              break;
            }
          }
        }
      }

      if (!invoked) {
        ((DataSerializableFixedID) ds).toData(out, context);
      }
    } catch (NoSuchMethodException | SecurityException | IllegalAccessException
        | InvocationTargetException e) {
      throw new IOException(
          "problem invoking toData method on object of class" + ds.getClass().getName(), e);
    }
  }

  private Object readDSFID(final DataInput in) throws IOException, ClassNotFoundException {
    checkIn(in);
    DSCODE dsHeaderType = DscodeHelper.toDSCODE(in.readByte());
    if (dsHeaderType == DSCODE.NULL) {
      return null;
    }
    return readDSFID(in, dsHeaderType);
  }

  private Object readDSFID(final DataInput in, DSCODE dscode)
      throws IOException, ClassNotFoundException {
    switch (dscode) {
      case DS_FIXED_ID_BYTE:
        return create(in.readByte(), in);
      case DS_FIXED_ID_SHORT:
        return create(in.readShort(), in);
      case DS_NO_FIXED_ID:
        throw new IllegalStateException(
            "DS_NO_FIXED_ID is not supported in readDSFID - use InternalDataSerializer instead");
      case DS_FIXED_ID_INT:
        return create(in.readInt(), in);
      case DATA_SERIALIZABLE:
        return readDataSerializable(in);
      default:
        throw new IllegalStateException("unexpected byte: " + dscode + " while reading dsfid");
    }
  }

  private int readDSFIDHeader(final DataInput in, DSCODE dscode) throws IOException {
    switch (dscode) {
      case DS_FIXED_ID_BYTE:
        return in.readByte();
      case DS_FIXED_ID_SHORT:
        return in.readShort();
      case DS_FIXED_ID_INT:
        return in.readInt();
      default:
        throw new IllegalStateException("unexpected byte: " + dscode + " while reading dsfid");
    }
  }

  @Override
  public int readDSFIDHeader(final DataInput in) throws IOException {
    checkIn(in);
    return readDSFIDHeader(in, DscodeHelper.toDSCODE(in.readByte()));
  }

  /**
   * Checks to make sure a {@code DataInput} is not {@code null}.
   *
   * @throws NullPointerException If {@code in} is {@code null}
   */
  public static void checkIn(DataInput in) {
    if (in == null) {
      throw new NullPointerException("Null DataInput");
    }
  }

  /**
   * For backward compatibility this method should be used to invoke fromData on a DSFID or
   * DataSerializable. It will invoke the correct fromData method based on the class's version
   * information. This method does not read information about the class of the object. When
   * serializing use the method invokeToData to write the contents of the object.
   *
   * @param ds the object to write
   * @param in the input stream.
   */
  @Override
  public void invokeFromData(Object ds, DataInput in)
      throws IOException, ClassNotFoundException {
    DeserializationContextImpl context = new DeserializationContextImpl(in, this);
    try {
      boolean invoked = false;
      KnownVersion v = context.getSerializationVersion();
      if (!KnownVersion.CURRENT.equals(v) && ds instanceof SerializationVersions) {
        // get versions where DataOutput was upgraded
        SerializationVersions vds = (SerializationVersions) ds;
        KnownVersion[] versions = vds.getSerializationVersions();
        // check if the version of the peer or diskstore is different and
        // there has been a change in the message
        if (versions != null) {
          for (KnownVersion version : versions) {
            // if peer version is less than the greatest upgraded version
            if (v.compareTo(version) < 0) {
              ds.getClass().getMethod("fromDataPre" + '_' + version.getMethodSuffix(),
                  new Class[] {DataInput.class, DeserializationContext.class})
                  .invoke(ds, in, context);
              invoked = true;
              break;
            }
          }
        }
      }
      if (!invoked) {
        if (ds instanceof BasicSerializable) {
          ((BasicSerializable) ds).fromData(in, context);
        } else {
          throw new IOException(
              "problem invoking fromData method on object of class" + ds.getClass().getName());
        }
      }
    } catch (EOFException | ClassNotFoundException | SocketException ex) {
      // client went away - ignore
      throw ex;
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception ex) {
      throw new IOException(
          String.format("Could not create an instance of %s .",
              ds.getClass().getName()),
          ex);
    }
  }



  @Override
  public void registerDSFID(int dsfid, Class<?> dsfidClass) {
    try {
      Constructor<?> cons = dsfidClass.getConstructor((Class<Object>[]) null);
      cons.setAccessible(true);
      if (!cons.isAccessible()) {
        throw new IllegalArgumentException(
            "default constructor not accessible " + "for DSFID=" + dsfid + ": " + dsfidClass);
      }
      if (dsfid >= Byte.MIN_VALUE && dsfid <= Byte.MAX_VALUE) {
        dsfidMap[dsfid + Byte.MAX_VALUE + 1] = cons;
      } else {
        dsfidMap2.put(dsfid, cons);
      }
    } catch (NoSuchMethodException nsme) {
      throw new IllegalArgumentException("Unable to find a default constructor for " + dsfidClass,
          nsme);
    }
  }

  public Object create(int dsfid, DataInput in) throws IOException, ClassNotFoundException {
    final Constructor<?> cons;
    if (dsfid >= Byte.MIN_VALUE && dsfid <= Byte.MAX_VALUE) {
      cons = dsfidMap[dsfid + Byte.MAX_VALUE + 1];
    } else {
      cons = dsfidMap2.get(dsfid);
    }
    if (cons != null) {
      try {
        Object ds = cons.newInstance((Object[]) null);
        invokeFromData(ds, in);
        return ds;
      } catch (InstantiationException | IllegalAccessException ie) {
        throw new IOException(ie.getMessage(), ie);
      } catch (InvocationTargetException ite) {
        Throwable targetEx = ite.getTargetException();
        if (targetEx instanceof IOException) {
          throw (IOException) targetEx;
        } else if (targetEx instanceof ClassNotFoundException) {
          throw (ClassNotFoundException) targetEx;
        } else {
          throw new IOException(ite.getMessage(), targetEx);
        }
      }
    }
    throw new DSFIDNotFoundException("Unknown DataSerializableFixedID: " + dsfid, dsfid);
  }

  private Object readDataSerializable(final DataInput in)
      throws IOException, ClassNotFoundException {
    Class<?> c = StaticDeserialization.readClass(in);
    try {
      Constructor<?> init = c.getConstructor();
      init.setAccessible(true);
      Object o = init.newInstance();

      invokeFromData(o, in);

      return o;
    } catch (EOFException | SocketException ex) {
      // client went away - ignore
      throw ex;
    } catch (Exception ex) {
      throw new IOException(
          String.format("Could not create an instance of %s .",
              c.getName()),
          ex);
    }
  }

  public Constructor<?>[] getDsfidmap() {
    return dsfidMap;
  }

  public Int2ObjectOpenHashMap<Constructor<?>> getDsfidmap2() {
    return dsfidMap2;
  }


  @Override
  public SerializationContext createSerializationContext(DataOutput dataOutput) {
    return new SerializationContextImpl(dataOutput, this);
  }

  @Override
  public DeserializationContext createDeserializationContext(DataInput dataInput) {
    return new DeserializationContextImpl(dataInput, this);
  }
}
