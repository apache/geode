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
package org.apache.geode.internal.serialization;

import static org.apache.geode.internal.serialization.DataSerializableFixedID.NO_FIXED_ID;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.SocketException;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.GemFireRethrowable;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.SerializationException;
import org.apache.geode.SystemFailure;
import org.apache.geode.ToDataException;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;

public class DSFIDSerializer {
  private static final Logger logger = LogService.getLogger();

  @Immutable
  private final Constructor<?>[] dsfidMap = new Constructor<?>[256];

  @Immutable("This maybe should be wrapped in an unmodifiableMap?")
  private final Int2ObjectOpenHashMap dsfidMap2 = new Int2ObjectOpenHashMap(800);


  // Writes just the header of a DataSerializableFixedID to out.
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


  public void writeDSFID(DataSerializableFixedID o, DataOutput out) throws IOException {
    int dsfid = o.getDSFID();
    writeDSFID(o, dsfid, out);
  }

  public void writeDSFID(DataSerializableFixedID o, int dsfid, DataOutput out)
      throws IOException {
    if (dsfid == NO_FIXED_ID) {
      throw new IllegalArgumentException(
          "NO_FIXED_ID is not supported by BasicDSFIDSerializer - use InternalDataSerializer instead: "
              + o.getClass().getName());
    }
    writeDSFIDHeader(dsfid, out);
    try {
      invokeToData(o, out);
    } catch (IOException | CancelException | ToDataException | GemFireRethrowable io) {
      // Note: this is not a user code toData but one from our
      // internal code since only GemFire product code implements DSFID

      // Serializing a PDX can result in a cache closed exception. Just rethrow

      throw io;
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error. We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Throwable t) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above). However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      throw new ToDataException("toData failed on dsfid=" + dsfid + " msg:" + t.getMessage(), t);
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
  public void invokeToData(Object ds, DataOutput out) throws IOException {
    boolean isDSFID = ds instanceof DataSerializableFixedID;
    if (!isDSFID) {
      throw new IllegalArgumentException(
          "Expected a DataSerializableFixedID but found " + ds.getClass().getName());
    }
    try {
      boolean invoked = false;
      Version v = InternalDataSerializer.getVersionForDataStreamOrNull(out);

      if (Version.CURRENT != v && v != null) {
        // get versions where DataOutput was upgraded
        SerializationVersions sv = (SerializationVersions) ds;
        SerializationVersion[] versions = sv.getSerializationVersions();
        // check if the version of the peer or diskstore is different and
        // there has been a change in the message
        if (versions != null) {
          for (SerializationVersion version : versions) {
            // if peer version is less than the greatest upgraded version
            if (v.compareTo(version) < 0) {
              ds.getClass().getMethod("toDataPre_" + version.getMethodSuffix(),
                  new Class[] {DataOutput.class}).invoke(ds, out);
              invoked = true;
              break;
            }
          }
        }
      }

      if (!invoked) {
        ((DataSerializableFixedID) ds).toData(out);
      }
    } catch (IOException io) {
      // DSFID serialization expects an IOException but otherwise
      // we want to catch it and transform into a ToDataException
      // since it might be in user code and we want to report it
      // as a problem with the plugin code
      throw io;
    } catch (CancelException | ToDataException | GemFireRethrowable ex) {
      // Serializing a PDX can result in a cache closed exception. Just rethrow
      throw ex;
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error. We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Throwable t) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above). However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      throw new ToDataException(
          "toData failed on DataSerializableFixedID " + null == ds ? "null"
              : ds.getClass().toString(),
          t);
    }
  }

  /**
   * Get the {@link Version} ordinal of the peer or disk store that created this {@link DataOutput}.
   * Returns
   * zero if the version is same as this member's.
   */
  public SerializationVersion getVersionForDataStreamOrNull(DataOutput out) {
    // check if this is a versioned data output
    if (out instanceof VersionedDataStream) {
      return ((VersionedDataStream) out).getVersion();
    } else {
      return null;
    }
  }


  public Object readDSFID(final DataInput in, DSCODE dscode)
      throws IOException, ClassNotFoundException {
    if (logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE)) {
      logger.trace(LogMarker.SERIALIZER_VERBOSE, "readDSFID: header={}", dscode);
    }
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
      default:
        throw new IllegalStateException("unexpected byte: " + dscode + " while reading dsfid");
    }
  }

  public Object readDSFID(final DataInput in) throws IOException, ClassNotFoundException {
    checkIn(in);
    return readDSFID(in, DscodeHelper.toDSCODE(in.readByte()));
  }

  public int readDSFIDHeader(final DataInput in, DSCODE dscode) throws IOException {
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
  public void invokeFromData(Object ds, DataInput in)
      throws IOException, ClassNotFoundException {
    try {
      boolean invoked = false;
      Version v = InternalDataSerializer.getVersionForDataStreamOrNull(in);
      if (Version.CURRENT != v && v != null) {
        // get versions where DataOutput was upgraded
        SerializationVersion[] versions = null;
        SerializationVersions vds = (SerializationVersions) ds;
        versions = vds.getSerializationVersions();
        // check if the version of the peer or diskstore is different and
        // there has been a change in the message
        if (versions != null) {
          for (SerializationVersion version : versions) {
            // if peer version is less than the greatest upgraded version
            if (v.compareTo(version) < 0) {
              ds.getClass().getMethod("fromDataPre" + '_' + version.getMethodSuffix(),
                  new Class[] {DataInput.class}).invoke(ds, in);
              invoked = true;
              break;
            }
          }
        }
      }
      if (!invoked) {
        ((DataSerializableFixedID) ds).fromData(in);

        if (logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE)) {
          logger.trace(LogMarker.SERIALIZER_VERBOSE, "Read DataSerializableFixedId {}",
              ds);
        }
      }
    } catch (EOFException | ClassNotFoundException | CacheClosedException | SocketException ex) {
      // client went away - ignore
      throw ex;
    } catch (Exception ex) {
      throw new SerializationException(
          String.format("Could not create an instance of %s .",
              ds.getClass().getName()),
          ex);
    }
  }



  /** Register the constructor for a fixed ID class. */
  public void registerDSFID(int dsfid, Class dsfidClass) {
    try {
      Constructor<?> cons = dsfidClass.getConstructor((Class[]) null);
      cons.setAccessible(true);
      if (!cons.isAccessible()) {
        throw new InternalGemFireError(
            "default constructor not accessible " + "for DSFID=" + dsfid + ": " + dsfidClass);
      }
      if (dsfid >= Byte.MIN_VALUE && dsfid <= Byte.MAX_VALUE) {
        dsfidMap[dsfid + Byte.MAX_VALUE + 1] = cons;
      } else {
        dsfidMap2.put(dsfid, cons);
      }
    } catch (NoSuchMethodException nsme) {
      throw new InternalGemFireError(nsme);
    }
  }

  /**
   * Creates a DataSerializableFixedID or StreamableFixedID instance by deserializing it from the
   * data input.
   */
  public Object create(int dsfid, DataInput in) throws IOException, ClassNotFoundException {
    final Constructor<?> cons;
    if (dsfid >= Byte.MIN_VALUE && dsfid <= Byte.MAX_VALUE) {
      cons = dsfidMap[dsfid + Byte.MAX_VALUE + 1];
    } else {
      cons = (Constructor<?>) dsfidMap2.get(dsfid);
    }
    if (cons != null) {
      try {
        Object ds = cons.newInstance((Object[]) null);
        invokeFromData(ds, in);
        return ds;
      } catch (InstantiationException ie) {
        throw new IOException(ie.getMessage(), ie);
      } catch (IllegalAccessException iae) {
        throw new IOException(iae.getMessage(), iae);
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


  public Constructor<?>[] getDsfidmap() {
    return dsfidMap;
  }

  public Int2ObjectOpenHashMap getDsfidmap2() {
    return dsfidMap2;
  }



}
