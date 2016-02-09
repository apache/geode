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
package com.gemstone.gemfire.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.NotSerializableException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.UTFDataFormatException;
import java.lang.ref.WeakReference;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.CanonicalInstantiator;
import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.GemFireIOException;
import com.gemstone.gemfire.GemFireRethrowable;
import com.gemstone.gemfire.Instantiator;
import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.SerializationException;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.ToDataException;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.distributed.internal.DMStats;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.LonerDistributionManager;
import com.gemstone.gemfire.distributed.internal.PooledDistributionMessage;
import com.gemstone.gemfire.distributed.internal.SerialDistributionMessage;
import com.gemstone.gemfire.i18n.StringId;
import com.gemstone.gemfire.internal.cache.EnumListenerEvent;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PoolManagerImpl;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientNotifier;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerHelper;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientDataSerializerMessage;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.cache.tier.sockets.Part;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;
import com.gemstone.gemfire.internal.util.concurrent.CopyOnWriteHashMap;
import com.gemstone.gemfire.pdx.NonPortableClassException;
import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.pdx.PdxSerializable;
import com.gemstone.gemfire.pdx.PdxSerializer;
import com.gemstone.gemfire.pdx.internal.AutoSerializableManager;
import com.gemstone.gemfire.pdx.internal.AutoSerializableManager.AutoClassInfo;
import com.gemstone.gemfire.pdx.internal.EnumInfo;
import com.gemstone.gemfire.pdx.internal.PdxInputStream;
import com.gemstone.gemfire.pdx.internal.PdxInstanceEnum;
import com.gemstone.gemfire.pdx.internal.PdxInstanceImpl;
import com.gemstone.gemfire.pdx.internal.PdxOutputStream;
import com.gemstone.gemfire.pdx.internal.PdxReaderImpl;
import com.gemstone.gemfire.pdx.internal.PdxType;
import com.gemstone.gemfire.pdx.internal.PdxWriterImpl;
import com.gemstone.gemfire.pdx.internal.TypeRegistry;

/**
 * Contains static methods for data serializing instances of internal
 * GemFire classes.  It also contains the implementation of the
 * distribution messaging (and shared memory management) needed to
 * support data serialization.
 *
 * @author David Whitlock
 * @since 3.5
 */
public abstract class InternalDataSerializer extends DataSerializer implements DSCODE {
  private static final Logger logger = LogService.getLogger();
  
  private static final Set loggedClasses = new HashSet();
  /**
   * Maps Class names to their DataSerializer.  This is used to
   * find a DataSerializer during serialization.
   */
  private static final ConcurrentHashMap<String, DataSerializer> classesToSerializers = new ConcurrentHashMap<String, DataSerializer>();
  
  // used by sqlFire
  public static ConcurrentHashMap<String, DataSerializer> getClassesToSerializers() {
    return classesToSerializers;
  }
  
  
  private static final String serializationVersionTxt = System.getProperty("gemfire.serializationVersion");
  /**
   * Any time new serialization format is added then a new enum needs to be added here.
   * @author darrel
   * @since 6.6.2
   */
  private static enum SERIALIZATION_VERSION {
    vINVALID,
    v660, // includes 6.6.0.x and 6.6.1.x. Note that no serialization changes were made in 6.6 until 6.6.2
    v662 // 6.6.2.x or later
    // NOTE if you add a new constant make sure and update "latestVersion".
  }
  /**
   * Change this constant to be the last one in SERIALIZATION_VERSION
   */
  private static final SERIALIZATION_VERSION latestVersion = SERIALIZATION_VERSION.v662;
  
  private static SERIALIZATION_VERSION calculateSerializationVersion() {
    if (serializationVersionTxt == null || serializationVersionTxt.equals("")) {
      return latestVersion;
    } else if (serializationVersionTxt.startsWith("6.6.0") || serializationVersionTxt.startsWith("6.6.1")) {
      return SERIALIZATION_VERSION.v660;
    } else if (serializationVersionTxt.startsWith("6.6.2")) {
      return SERIALIZATION_VERSION.v662;
    } else {
      return SERIALIZATION_VERSION.vINVALID;
    }
  }
  private static final SERIALIZATION_VERSION serializationVersion = calculateSerializationVersion();

  public static boolean is662SerializationEnabled() {
    return serializationVersion.ordinal() >= SERIALIZATION_VERSION.v662.ordinal();
  }
  
  public static void checkSerializationVersion() {
    if (serializationVersion == SERIALIZATION_VERSION.vINVALID) {
      throw new IllegalArgumentException("The system property \"gemfire.serializationVersion\" was set to \"" + serializationVersionTxt + "\" which is not a valid serialization version. Valid versions must start with \"6.6.0\", \"6.6.1\", or \"6.6.2\"");
    }
  }

  static {
    initializeWellKnownSerializers();
  }
  private static void initializeWellKnownSerializers() { 
    // ArrayBlockingQueue does not have zero-arg constructor
    // LinkedBlockingQueue does have zero-arg constructor but no way to get capacity
  
    classesToSerializers.put("java.lang.String",
                             new WellKnownPdxDS() {
      @Override
                               public final boolean toData(Object o, DataOutput out)
                                 throws IOException {
                                 try {
                                   writeString((String)o, out);
                                 }
                                 catch (UTFDataFormatException ex) {
                                   // See bug 30428
                                   String s = "While writing a String of length " +
                                     ((String)o).length();
                                   UTFDataFormatException ex2 = new UTFDataFormatException(s);
                                   ex2.initCause(ex);
                                   throw ex2;
                                 }
                                 return true;
                               }});
    classesToSerializers.put("java.net.InetAddress",
                             new WellKnownDS() {
      @Override
                               public final boolean toData(Object o, DataOutput out)
                                 throws IOException {
                                 InetAddress address = (InetAddress) o;
                                 out.writeByte(INET_ADDRESS);
                                 writeInetAddress(address, out);
                                 return true;
                               }});
    classesToSerializers.put("java.net.Inet4Address",
                             new WellKnownDS() {
      @Override
                               public final boolean toData(Object o, DataOutput out)
                                 throws IOException {
                                 InetAddress address = (InetAddress) o;
                                 out.writeByte(INET_ADDRESS);
                                 writeInetAddress(address, out);
                                 return true;
                               }});
    classesToSerializers.put("java.net.Inet6Address",
                             new WellKnownDS() {
      @Override
                               public final boolean toData(Object o, DataOutput out)
                                 throws IOException {
                                 InetAddress address = (InetAddress) o;
                                 out.writeByte(INET_ADDRESS);
                                 writeInetAddress(address, out);
                                 return true;
                               }});
    classesToSerializers.put("java.lang.Class",
                             new WellKnownDS() {
      @Override
                               public final boolean toData(Object o, DataOutput out)
                                 throws IOException {
                                 Class c = (Class) o;
                                 if (c.isPrimitive()) {
                                   writePrimitiveClass(c, out);
                                 }
                                 else {
                                   out.writeByte(CLASS);
                                   writeClass(c, out);
                                 }
                                 return true;
                               }});
    classesToSerializers.put("java.lang.Boolean",
                             new WellKnownPdxDS() {
      @Override
                               public final boolean toData(Object o, DataOutput out)
                                 throws IOException {
                                 Boolean value = (Boolean) o;
                                 out.writeByte(BOOLEAN);
                                 writeBoolean(value, out);
                                 return true;
                               }});
    classesToSerializers.put("java.lang.Character",
                             new WellKnownPdxDS() {
      @Override
                               public final boolean toData(Object o, DataOutput out)
                                 throws IOException {
                                 Character value = (Character) o;
                                 out.writeByte(CHARACTER);
                                 writeCharacter(value, out);
                                 return true;
                               }});
    classesToSerializers.put("java.lang.Byte",
                             new WellKnownPdxDS() {
      @Override
                               public final boolean toData(Object o, DataOutput out)
                                 throws IOException {
                                 Byte value = (Byte) o;
                                 out.writeByte(BYTE);
                                 writeByte(value, out);
                                 return true;
                               }});
    classesToSerializers.put("java.lang.Short",
                             new WellKnownPdxDS() {
      @Override
                               public final boolean toData(Object o, DataOutput out)
                                 throws IOException {
                                 Short value = (Short) o;
                                 out.writeByte(SHORT);
                                 writeShort(value, out);
                                 return true;
                               }});
    classesToSerializers.put("java.lang.Integer",
                             new WellKnownPdxDS() {
      @Override
                               public final boolean toData(Object o, DataOutput out)
                                 throws IOException {
                                 Integer value = (Integer) o;
                                 out.writeByte(INTEGER);
                                 writeInteger(value, out);
                                 return true;
                               }});
    classesToSerializers.put("java.lang.Long",
                             new WellKnownPdxDS() {
      @Override
                               public final boolean toData(Object o, DataOutput out)
                                 throws IOException {
                                 Long value = (Long) o;
                                 out.writeByte(LONG);
                                 writeLong(value, out);
                                 return true;
                               }});
    classesToSerializers.put("java.lang.Float",
                             new WellKnownPdxDS() {
      @Override
                               public final boolean toData(Object o, DataOutput out)
                                 throws IOException {
                                 Float value = (Float) o;
                                 out.writeByte(FLOAT);
                                 writeFloat(value, out);
                                 return true;
                               }});
    classesToSerializers.put("java.lang.Double",
                             new WellKnownPdxDS() {
      @Override
                               public final boolean toData(Object o, DataOutput out)
                                 throws IOException {
                                 Double value = (Double) o;
                                 out.writeByte(DOUBLE);
                                 writeDouble(value, out);
                                 return true;
                               }});
    classesToSerializers.put("[Z", // boolean[]
                             new WellKnownPdxDS() {
      @Override
                               public final boolean toData(Object o, DataOutput out)
                                 throws IOException {
                                 out.writeByte(BOOLEAN_ARRAY);
                                 writeBooleanArray((boolean[]) o, out);
                                 return true;
                               }});
    classesToSerializers.put("[B", // byte[]
                             new WellKnownPdxDS() {
      @Override
                               public final boolean toData(Object o, DataOutput out)
                                 throws IOException {
                                 byte[] array = (byte[]) o;
                                 out.writeByte(BYTE_ARRAY);
                                 writeByteArray(array, out);
                                 return true;
                               }});
    classesToSerializers.put("[C", // char[]
                             new WellKnownPdxDS() {
      @Override
                               public final boolean toData(Object o, DataOutput out)
                                 throws IOException {
                                 out.writeByte(CHAR_ARRAY);
                                 writeCharArray((char[]) o, out);
                                 return true;
                               }});
    classesToSerializers.put("[D", // double[]
                             new WellKnownPdxDS() {
      @Override
                               public final boolean toData(Object o, DataOutput out)
                                 throws IOException {
                                 double[] array = (double[]) o;
                                 out.writeByte(DOUBLE_ARRAY);
                                 writeDoubleArray(array, out);
                                 return true;
                               }});
    classesToSerializers.put("[F", // float[]
                             new WellKnownPdxDS() {
      @Override
                               public final boolean toData(Object o, DataOutput out)
                                 throws IOException {
                                 float[] array = (float[]) o;
                                 out.writeByte(FLOAT_ARRAY);
                                 writeFloatArray(array, out);
                                 return true;
                               }});
    classesToSerializers.put("[I", // int[]
                             new WellKnownPdxDS() {
      @Override
                               public final boolean toData(Object o, DataOutput out)
                                 throws IOException {
                                 int[] array = (int[]) o;
                                 out.writeByte(INT_ARRAY);
                                 writeIntArray(array, out);
                                 return true;
                               }});
    classesToSerializers.put("[J", // long[]
                             new WellKnownPdxDS() {
      @Override
                               public final boolean toData(Object o, DataOutput out)
                                 throws IOException {
                                 long[] array = (long[]) o;
                                 out.writeByte(LONG_ARRAY);
                                 writeLongArray(array, out);
                                 return true;
                               }});
    classesToSerializers.put("[S", // short[]
                             new WellKnownPdxDS() {
      @Override
                               public final boolean toData(Object o, DataOutput out)
                                 throws IOException {
                                 short[] array = (short[]) o;
                                 out.writeByte(SHORT_ARRAY);
                                 writeShortArray(array, out);
                                 return true;
                               }});
    classesToSerializers.put("[Ljava.lang.String;", // String[]
                             new WellKnownPdxDS() {
      @Override
                               public final boolean toData(Object o, DataOutput out)
                                 throws IOException {
                                 String[] array = (String[]) o;
                                 out.writeByte(STRING_ARRAY);
                                 writeStringArray(array, out);
                                 return true;
                               }});
    classesToSerializers.put(TimeUnit.NANOSECONDS.getClass().getName(),
        new WellKnownDS() {
          @Override public final boolean toData(Object o, DataOutput out)
            throws IOException {
            out.writeByte(TIME_UNIT);
            out.writeByte(TIME_UNIT_NANOSECONDS);
            return true;
          }});
    classesToSerializers.put(TimeUnit.MICROSECONDS.getClass().getName(),
        new WellKnownDS() {
          @Override public final boolean toData(Object o, DataOutput out)
            throws IOException {
            out.writeByte(TIME_UNIT);
            out.writeByte(TIME_UNIT_MICROSECONDS);
            return true;
          }});
    classesToSerializers.put(TimeUnit.MILLISECONDS.getClass().getName(),
        new WellKnownDS() {
          @Override public final boolean toData(Object o, DataOutput out)
            throws IOException {
            out.writeByte(TIME_UNIT);
            out.writeByte(TIME_UNIT_MILLISECONDS);
            return true;
          }});
    classesToSerializers.put(TimeUnit.SECONDS.getClass().getName(),
        new WellKnownDS() {
          @Override public final boolean toData(Object o, DataOutput out)
            throws IOException {
            out.writeByte(TIME_UNIT);
            out.writeByte(TIME_UNIT_SECONDS);
            return true;
          }});
    classesToSerializers.put("java.util.Date",
                             new WellKnownPdxDS() {
      @Override
                               public final boolean toData(Object o, DataOutput out)
                                 throws IOException {
                                 Date date = (Date) o;
                                 out.writeByte(DATE);
                                 writeDate(date, out);
                                 return true;
                               }});
    classesToSerializers.put("java.io.File",
                             new WellKnownDS() {
      @Override
                               public final boolean toData(Object o, DataOutput out)
                                 throws IOException {
                                 File file = (File) o;
                                 out.writeByte(FILE);
                                 writeFile(file, out);
                                 return true;
                               }});
    classesToSerializers.put("java.util.ArrayList",
                             new WellKnownPdxDS() {
      @Override
                               public final boolean toData(Object o, DataOutput out)
                                 throws IOException {
                                 ArrayList list = (ArrayList) o;
                                 out.writeByte(ARRAY_LIST);
                                 writeArrayList(list, out);
                                 return true;
                               }});
    classesToSerializers.put("java.util.LinkedList",
                             new WellKnownDS() {
      @Override
                               public final boolean toData(Object o, DataOutput out)
                                 throws IOException {
                                 LinkedList list = (LinkedList) o;
                                 out.writeByte(LINKED_LIST);
                                 writeLinkedList(list, out);
                                 return true;
                               }});
    classesToSerializers.put("java.util.Vector",
                             new WellKnownPdxDS() {
      @Override
                               public final boolean toData(Object o, DataOutput out)
                                 throws IOException {
                                 out.writeByte(VECTOR);
                                 writeVector((Vector) o, out);
                                 return true;
                               }});
    classesToSerializers.put("java.util.Stack",
                             new WellKnownDS() {
      @Override
                               public final boolean toData(Object o, DataOutput out)
                                 throws IOException {
                                 out.writeByte(STACK);
                                 writeStack((Stack) o, out);
                                 return true;
                               }});
    classesToSerializers.put("java.util.HashSet",
                             new WellKnownPdxDS() {
      @Override
                               public final boolean toData(Object o, DataOutput out)
                                 throws IOException {
                                 HashSet list = (HashSet) o;
                                 out.writeByte(HASH_SET);
                                 writeHashSet(list, out);
                                 return true;
                               }});
    classesToSerializers.put("java.util.LinkedHashSet",
                             new WellKnownPdxDS() {
      @Override
                               public final boolean toData(Object o, DataOutput out)
                                 throws IOException {
                                 out.writeByte(LINKED_HASH_SET);
                                 writeLinkedHashSet((LinkedHashSet) o, out);
                                 return true;
                               }});
    classesToSerializers.put("java.util.HashMap",
                             new WellKnownPdxDS() {
      @Override
                               public final boolean toData(Object o, DataOutput out)
                                 throws IOException {
                                 HashMap list = (HashMap) o;
                                 out.writeByte(HASH_MAP);
                                 writeHashMap(list, out);
                                 return true;
                               }});
    classesToSerializers.put("java.util.IdentityHashMap",
                             new WellKnownDS() {
      @Override
                               public final boolean toData(Object o, DataOutput out)
                                 throws IOException {
                                 out.writeByte(IDENTITY_HASH_MAP);
                                 writeIdentityHashMap((IdentityHashMap) o, out);
                                 return true;
                               }});
    classesToSerializers.put("java.util.Hashtable",
                             new WellKnownPdxDS() {
      @Override
                               public final boolean toData(Object o, DataOutput out)
                                 throws IOException {
                                 out.writeByte(HASH_TABLE);
                                 writeHashtable((Hashtable) o, out);
                                 return true;
                               }});
    // We can't add this here because it would cause writeObject to not be compatible with previous releases
//    classesToSerializers.put("java.util.concurrent.ConcurrentHashMap",
//                             new WellKnownDS() {
//                              @Override
//                              public final boolean toData(Object o, DataOutput out)
//                                throws IOException {
//                                out.writeByte(CONCURRENT_HASH_MAP);
//                                writeConcurrentHashMap((ConcurrentHashMap<?, ?>) o, out);
//                                return true;
//                              }});
    classesToSerializers.put("java.util.Properties",
                             new WellKnownDS() {
      @Override
                               public final boolean toData(Object o, DataOutput out)
                                 throws IOException {
                                 Properties props  = (Properties) o;
                                 out.writeByte(PROPERTIES);
                                 writeProperties(props, out);
                                 return true;
                               }});
    classesToSerializers.put("java.util.TreeMap",
                             new WellKnownDS() {
      @Override
                               public final boolean toData(Object o, DataOutput out)
                                 throws IOException {
                                 out.writeByte(TREE_MAP);
                                 writeTreeMap((TreeMap) o, out);
                                 return true;
                               }});
    classesToSerializers.put("java.util.TreeSet",
                             new WellKnownDS() {
      @Override
                               public final boolean toData(Object o, DataOutput out)
                                 throws IOException {
                                 out.writeByte(TREE_SET);
                                 writeTreeSet((TreeSet) o, out);
                                 return true;
                               }});
    if (is662SerializationEnabled()) {
      classesToSerializers.put("java.math.BigInteger",
          new WellKnownDS() {
        @Override
        public final boolean toData(Object o, DataOutput out)
        throws IOException {
          out.writeByte(BIG_INTEGER);
          writeBigInteger((BigInteger) o, out);
          return true;
        }});
      classesToSerializers.put("java.math.BigDecimal",
          new WellKnownDS() {
        @Override
        public final boolean toData(Object o, DataOutput out)
        throws IOException {
          out.writeByte(BIG_DECIMAL);
          writeBigDecimal((BigDecimal) o, out);
          return true;
        }});
      classesToSerializers.put("java.util.UUID",
          new WellKnownDS() {
        @Override
        public final boolean toData(Object o, DataOutput out)
        throws IOException {
          out.writeByte(UUID);
          writeUUID((UUID) o, out);
          return true;
        }});
      classesToSerializers.put("java.sql.Timestamp",
          new WellKnownDS() {
        @Override
        public final boolean toData(Object o, DataOutput out)
        throws IOException {
          out.writeByte(TIMESTAMP);
          writeTimestamp((Timestamp) o, out);
          return true;
        }});
    }
    // @todo add: LinkedHashMap (hard to do because it might not be insertion ordered)
  }
  
  /** Maps the id of a serializer to its <code>DataSerializer</code>.
   */
  private static final ConcurrentMap/*<Integer, DataSerializer|Marker>*/ idsToSerializers = new ConcurrentHashMap();

  /**
   * Contains the classnames of the data serializers (and not the supported
   * classes) not yet loaded into the vm as keys and their corresponding holder
   * instances as values.
   */
  private static final ConcurrentHashMap<String, SerializerAttributesHolder> dsClassesToHolders = new ConcurrentHashMap<String, SerializerAttributesHolder>();

  /**
   * Contains the id of the data serializers not yet loaded into the vm as keys
   * and their corresponding holder instances as values.
   */
  private static final ConcurrentHashMap<Integer, SerializerAttributesHolder> idsToHolders = new ConcurrentHashMap<Integer, SerializerAttributesHolder>();

  /**
   * Contains the classnames of supported classes as keys and their
   * corresponding SerializerAttributesHolder instances as values. This applies
   * only to the data serializers which have not been loaded into the vm.
   */
  private static final ConcurrentHashMap<String, SerializerAttributesHolder> supportedClassesToHolders = new ConcurrentHashMap<String, SerializerAttributesHolder>();

  /** <code>RegistrationListener</code>s that receive callbacks when
   * <code>DataSerializer</code>s and <code>Instantiator</code>s are
   * registered.
   * Note: copy-on-write access used for this set
   */
  private static volatile Set listeners = new HashSet();
  private static final Object listenersSync = new Object();

  ////////////////////  Static Methods  ////////////////////

  /**
   * Convert the given unsigned byte to an int.
   * The returned value will be in the range [0..255] inclusive
   */
  private static final int ubyteToInt(byte ub) {
    return ub & 0xFF;
  }

  /**
   * Instantiates an instance of <code>DataSerializer</code>
   *
   * @throws IllegalArgumentException
   *         If the class can't be instantiated
   *
   * @see DataSerializer#register(Class)
   */
  private static DataSerializer newInstance(Class c) {
    if (!DataSerializer.class.isAssignableFrom(c)) {
      throw new IllegalArgumentException(LocalizedStrings.DataSerializer_0_DOES_NOT_EXTEND_DATASERIALIZER.toLocalizedString(c.getName()));
    }

    Constructor init;
    try {
      init = c.getDeclaredConstructor(new Class[0]);

    } catch (NoSuchMethodException ex) {
      StringId s = LocalizedStrings.DataSerializer_CLASS_0_DOES_NOT_HAVE_A_ZEROARGUMENT_CONSTRUCTOR;
      Object[] args = new Object[] {c.getName()};
      if (c.getDeclaringClass() != null) {
        s = LocalizedStrings.DataSerializer_CLASS_0_DOES_NOT_HAVE_A_ZEROARGUMENT_CONSTRUCTOR_IT_IS_AN_INNER_CLASS_OF_1_SHOULD_IT_BE_A_STATIC_INNER_CLASS;
        args = new Object[] {c.getName(), c.getDeclaringClass()};
      } 
      throw new IllegalArgumentException(s.toLocalizedString(args));
    }

    DataSerializer s;
    try {
      init.setAccessible(true);
      s = (DataSerializer) init.newInstance(new Object[0]);

    } catch (IllegalAccessException ex) {
      throw new IllegalArgumentException(LocalizedStrings.DataSerializer_COULD_NOT_INSTANTIATE_AN_INSTANCE_OF_0.toLocalizedString(c.getName()));

    } catch (InstantiationException ex) {
      RuntimeException ex2 = new IllegalArgumentException(LocalizedStrings.DataSerializer_COULD_NOT_INSTANTIATE_AN_INSTANCE_OF_0.toLocalizedString(c.getName()));
      ex2.initCause(ex);
      throw ex2;

    } catch (InvocationTargetException ex) {
      RuntimeException ex2 = new IllegalArgumentException(LocalizedStrings.DataSerializer_WHILE_INSTANTIATING_AN_INSTANCE_OF_0.toLocalizedString(c.getName()));
      ex2.initCause(ex);
      throw ex2;
    }

    return s;
  }
  
  public static DataSerializer register(Class c, boolean distribute, EventID eventId,
      ClientProxyMembershipID context) {
    DataSerializer s = newInstance(c);
    // This method is only called when server connection and
    // CacheClientUpdaterThread
    s.setEventId(eventId);
    s.setContext(context);
    return _register(s, distribute);
  }

  /**
   * Registers a <code>DataSerializer</code> instance with the data
   * serialization framework.
   *
   * @param distribute
   *        Should the registered <code>DataSerializer</code> be
   *        distributed to other members of the distributed system?
   *
   * @see DataSerializer#register(Class)
   */
  public static DataSerializer register(Class c, boolean distribute) {
    final DataSerializer s = newInstance(c);
    return _register(s, distribute);
  }
  
  public static DataSerializer _register(DataSerializer s,
                                        boolean distribute) {
    final int id = s.getId();
    DataSerializer dsForMarkers = s;
    if (id == 0) {
      throw new IllegalArgumentException(LocalizedStrings.InternalDataSerializer_CANNOT_CREATE_A_DATASERIALIZER_WITH_ID_0.toLocalizedString());
    }
    final Class[] classes = s.getSupportedClasses();
    if (classes == null || classes.length == 0) {
      final StringId msg = LocalizedStrings.InternalDataSerializer_THE_DATASERIALIZER_0_HAS_NO_SUPPORTED_CLASSES_ITS_GETSUPPORTEDCLASSES_METHOD_MUST_RETURN_AT_LEAST_ONE_CLASS;
      throw new IllegalArgumentException(msg.toLocalizedString(s.getClass().getName()));
    }
    {
      for (int i = 0; i < classes.length; i++) {
        if (classes[i] == null) {
          final StringId msg = LocalizedStrings.InternalDataSerializer_THE_DATASERIALIZER_GETSUPPORTEDCLASSES_METHOD_FOR_0_RETURNED_AN_ARRAY_THAT_CONTAINED_A_NULL_ELEMENT;
          throw new IllegalArgumentException(msg.toLocalizedString(s.getClass().getName()));
        } else if (classes[i].isArray()) {
          final StringId msg = LocalizedStrings.InternalDataSerializer_THE_DATASERIALIZER_GETSUPPORTEDCLASSES_METHOD_FOR_0_RETURNED_AN_ARRAY_THAT_CONTAINED_AN_ARRAY_CLASS_WHICH_IS_NOT_ALLOWED_SINCE_ARRAYS_HAVE_BUILTIN_SUPPORT;
          throw new IllegalArgumentException(msg.toLocalizedString(s.getClass().getName()));
        }
      }
    }

    final Integer idx = Integer.valueOf(id);
    boolean retry;
    Marker oldMarker = null;
    final Marker m = new InitMarker();
    do {
      retry = false;
      Object oldSerializer = idsToSerializers.putIfAbsent(idx, m);
      if (oldSerializer != null) {
        if (oldSerializer instanceof Marker) {
          retry = !idsToSerializers.replace(idx, oldSerializer, m);
          if (!retry) {
            oldMarker = (Marker)oldSerializer;
          }
        } else if (oldSerializer.getClass().equals(s.getClass())) {
          // We've already got one of these registered
          if (distribute) {
            sendRegistrationMessage(s);
          }
          return (DataSerializer) oldSerializer;
        } else {
          DataSerializer other = (DataSerializer) oldSerializer;
          throw new IllegalStateException(LocalizedStrings.InternalDataSerializer_A_DATASERIALIZER_OF_CLASS_0_IS_ALREADY_REGISTERED_WITH_ID_1_SO_THE_DATASERIALIZER_OF_CLASS_2_COULD_NOT_BE_REGISTERED.toLocalizedString(new Object[] {other.getClass().getName(), Integer.valueOf(other.getId())}));
        }
      }
    } while (retry);

    try {
      for (int i = 0; i < classes.length; i++) {
        DataSerializer oldS = classesToSerializers.putIfAbsent(classes[i].getName(), s);
        if (oldS != null) {
          if (!s.equals(oldS)) {
            // cleanup the ones we have already added
            for (int j = 0; j < i; j++) {
              classesToSerializers.remove(classes[j].getName(), s);
            }
            dsForMarkers = null;
            String oldMsg;
            if (oldS.getId() == 0) {
              oldMsg = "DataSerializer has built-in support for class ";
            } else {
              oldMsg = "A DataSerializer of class "
                + oldS.getClass().getName()
                + " is already registered to support class ";
            }
            String msg = oldMsg
              + classes[i].getName()
              + " so the DataSerializer of class "
              + s.getClass().getName()
              + " could not be registered.";
            if (oldS.getId() == 0) {
              throw new IllegalArgumentException(msg);
            } else {
              throw new IllegalStateException(msg);
            }
          }
        }
      }
    } finally {
      if (dsForMarkers == null) {
        idsToSerializers.remove(idx, m);
      } else {
        idsToSerializers.replace(idx, m, dsForMarkers);
      }
      if (oldMarker != null) {
        oldMarker.setSerializer(dsForMarkers);
      }
      m.setSerializer(dsForMarkers);
    }

 // if dataserializer is getting registered for first time
    // its EventID will be null, so generate a new event id
    // the the distributed system is connected
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    if (cache != null && s.getEventId() == null) {
      s.setEventId(new EventID(cache.getDistributedSystem()));
    }
    
    if (distribute) {
      // send a message to other peers telling them about a newly-registered
      // dataserializer, it also send event id of the originator along with the
      // dataserializer
      sendRegistrationMessage(s);
      // send it to cache servers if it is a client
      sendRegistrationMessageToServers(s);
    }
    // send it to all cache clients irelevent of distribute
    // bridge servers send it all the clients irelevent of
    // originator VM
    sendRegistrationMessageToClients(s);
    
    fireNewDataSerializer(s);

    return s;
  }

  /**
   * Marks a <code>DataSerializer</code> className for registration with the
   * data serialization framework. Does not necessarily load the classes into
   * this VM.
   * 
   * @param className Name of the DataSerializer class.
   * @param distribute
   *          If true, distribute this data serializer.
   * @param eventId
   *          Event id
   * @param proxyId
   *          proxy id
   * @see DataSerializer#register(Class)
   */
  public static void register(String className, boolean distribute,
      EventID eventId, ClientProxyMembershipID proxyId, int id) {
    register(className, distribute, new SerializerAttributesHolder(className,
        eventId, proxyId, id));
  }

  /**
   * Marks a <code>DataSerializer</code> className for registration with the
   * data serialization framework. Does not necessarily load the classes into
   * this VM.
   * 
   * @param className
   * @param distribute
   *          If true, distribute this data serializer.
   * @see DataSerializer#register(Class)
   */
  public static void register(String className, boolean distribute) {
    register(className, distribute, new SerializerAttributesHolder());
  }

  private static void register(String className, boolean distribute,
      SerializerAttributesHolder holder) {
    if (className == null || className.trim().equals("")) {
      throw new IllegalArgumentException("Class name cannot be null or empty.");
    }

    SerializerAttributesHolder oldValue = dsClassesToHolders.putIfAbsent(
        className, holder);
    if (oldValue != null) {
      if (oldValue.getId() != 0 && holder.getId() != 0
          && oldValue.getId() != holder.getId()) {
        throw new IllegalStateException(
            LocalizedStrings.InternalDataSerializer_A_DATASERIALIZER_OF_CLASS_0_IS_ALREADY_REGISTERED_WITH_ID_1_SO_THE_DATASERIALIZER_OF_CLASS_2_COULD_NOT_BE_REGISTERED
                .toLocalizedString(new Object[] {oldValue.getClass().getName(),
                    Integer.valueOf(oldValue.getId())}));
      }
    }

    idsToHolders.putIfAbsent(holder.getId(), holder);

    Object ds = idsToSerializers.get(holder.getId());
    if (ds instanceof Marker) {
      synchronized (ds) {
        ((Marker)ds).notifyAll();
      }
    }

    if (distribute) {
      sendRegistrationMessageToServers(holder);
    }
  }

  public static void updateSupportedClassesMap(
      HashMap<Integer, ArrayList<String>> map) {
    for (Entry<Integer, ArrayList<String>> e : map.entrySet()) {
      for (String supportedClassName : e.getValue()) {
        supportedClassesToHolders.putIfAbsent(supportedClassName,
            idsToHolders.get(e.getKey()));
      }
    }
  }

  public static void updateSupportedClassesMap(String dsClassName,
      String supportedClassName) {
    supportedClassesToHolders.putIfAbsent(supportedClassName,
        dsClassesToHolders.get(dsClassName));
  }

  public static class SerializerAttributesHolder {
    private String className = "";
    private EventID eventId = null;
    private ClientProxyMembershipID proxyId = null;
    private int id = 0;
    
    public SerializerAttributesHolder () {
    }

    public SerializerAttributesHolder(String name, EventID event,
        ClientProxyMembershipID proxy, int id) {
      this.className = name;
      this.eventId = event;
      this.proxyId = proxy;
      this.id = id;
    }

    /**
     * 
     * @return String the classname of the data serializer this instance
     *         represents.
     */
    public String getClassName() {
      return this.className;
    }

    public EventID getEventId() {
      return this.eventId;
    }

    public ClientProxyMembershipID getProxyId() {
      return this.proxyId;
    }

    public int getId() {
      return this.id;
    }

    public String toString() {
      return "SerializerAttributesHolder[name=" + this.className + ",id=" + this.id + ",eventId=" + this.eventId + "]";
    }
  }

  private static void sendRegistrationMessageToServers(DataSerializer dataSerializer)
  {
    PoolManagerImpl.allPoolsRegisterDataSerializers(dataSerializer);
  }

  private static void sendRegistrationMessageToServers(
      SerializerAttributesHolder holder) {
    PoolManagerImpl.allPoolsRegisterDataSerializers(holder);
  }

  private static void sendRegistrationMessageToClients(DataSerializer dataSerializer)
  {
    Cache cache = GemFireCacheImpl.getInstance();
    if (cache == null) {
      // A cache has not yet been created.
      // we can't propagate it to clients
      return;
    }
    byte[][] serializedDataSerializer = new byte[2][];
    try {
      serializedDataSerializer[0] = CacheServerHelper.serialize(dataSerializer
          .getClass().toString().substring(6));
      {
        byte[] idBytes = new byte[4];
        Part.encodeInt(dataSerializer.getId(), idBytes);
        serializedDataSerializer[1] = idBytes;
      }
    }
    catch (IOException e) {
      if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
        logger.trace(LogMarker.SERIALIZER, "InternalDataSerializer encountered an IOException while serializing DataSerializer :{}", dataSerializer);
      }
    }
    ClientDataSerializerMessage clientDataSerializerMessage = new ClientDataSerializerMessage(
        EnumListenerEvent.AFTER_REGISTER_DATASERIALIZER, serializedDataSerializer,
        (ClientProxyMembershipID)dataSerializer.getContext(),
        (EventID)dataSerializer.getEventId(),
        new Class[][]{dataSerializer.getSupportedClasses()});
    // Deliver it to all the clients
    CacheClientNotifier.routeClientMessage(clientDataSerializerMessage);
  }

  public static EventID generateEventId(){
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    if(cache == null){
      // A cache has not yet created
      return null;
    }
    return new EventID(InternalDistributedSystem.getAnyInstance());
  }
  /**
   * Unregisters a <code>Serializer</code> that was previously
   * registered with the data serialization framework.
   */
  public static void unregister(int id) {
    final Integer idx = Integer.valueOf(id);
    Object o = idsToSerializers.remove(idx);
    if (o != null) {
      if (o instanceof InitMarker) {
        o = ((Marker)o).getSerializer();
      }
    }
    if (o instanceof DataSerializer) {
      DataSerializer s = (DataSerializer)o;
      Class[] classes = s.getSupportedClasses();
      for (int i = 0; i < classes.length; i++) {
        classesToSerializers.remove(classes[i].getName(), s);
        supportedClassesToHolders.remove(classes[i].getName());
      }
      dsClassesToHolders.remove(s.getClass().getName());
      idsToHolders.remove(idx);
    }
  }
  
  // testHook used to clean up any registered DataSerializers
  public static void reinitialize() {
    idsToSerializers.clear();
    classesToSerializers.clear();
    supportedClassesToHolders.clear();
    dsClassesToHolders.clear();
    idsToHolders.clear();
    initializeWellKnownSerializers();
  }

  /**
   * Returns the <code>DataSerializer</code> for the given class.  If
   * no class has been registered, <code>null</code> is returned.
   * Remember that it is okay to return <code>null</code> in this
   * case.  This method is invoked when writing an object.  If a
   * serializer isn't available, then its the user's fault.
   */
  public static DataSerializer getSerializer(Class c) {
    DataSerializer ds = classesToSerializers.get(c.getName());
    if (ds == null) {
      SerializerAttributesHolder sah = supportedClassesToHolders.get(c.getName());
      if (sah != null) {
        Class dsClass = null;
        try {
          dsClass = getCachedClass(sah.getClassName());

          DataSerializer serializer = register(dsClass, false);
          dsClassesToHolders.remove(dsClass.getName());
          idsToHolders.remove(serializer.getId());
          for (Class clazz : serializer.getSupportedClasses()) {
            supportedClassesToHolders.remove(clazz.getName());
          }
          return serializer;
        } catch (ClassNotFoundException cnfe) {
          logger.info(LogMarker.SERIALIZER, LocalizedMessage.create(LocalizedStrings.InternalDataSerializer_COULD_NOT_LOAD_DATASERIALIZER_CLASS_0, dsClass));
        }
      }
    }
    return ds;
  }

  /**
   * Returns the <code>DataSerializer</code> with the given id.
   */
  public static DataSerializer getSerializer(int id) {
    final Integer idx = Integer.valueOf(id);
    final GetMarker marker = new GetMarker();
    DataSerializer result = null;
    boolean timedOut = false;
    SerializerAttributesHolder sah=idsToHolders.get(idx);
    while (result == null && !timedOut && sah == null) {
      Object o = idsToSerializers.putIfAbsent(idx, marker);
      if (o == null) {
        result = marker.getSerializer();
        if (result == null) {
          // timed out
          timedOut = true;
          idsToSerializers.remove(idx, marker);
        }
      } else if (o instanceof Marker) {
        result = ((Marker)o).getSerializer();
      } else {
        result = (DataSerializer) o;
      }
    }
    if (result == null) {
      //SerializerAttributesHolder sah = idsToHolders.get(idx);
      if (sah != null) {
        Class dsClass = null;
        try {
          dsClass = getCachedClass(sah.getClassName());

          DataSerializer ds = register(dsClass, false);
          dsClassesToHolders.remove(sah.getClassName());
          idsToHolders.remove(id);
          for (Class clazz : ds.getSupportedClasses()) {
            supportedClassesToHolders.remove(clazz.getName());
          }
          return ds;
        } catch (ClassNotFoundException cnfe) {
          logger.info(LogMarker.SERIALIZER, LocalizedMessage.create(LocalizedStrings.InternalDataSerializer_COULD_NOT_LOAD_DATASERIALIZER_CLASS_0, dsClass));
        }
      }
    }
    return result;
  }
  /**
   * Returns all of the currently registered serializers
   */
  public static DataSerializer[] getSerializers() {
    final int size = idsToSerializers.size();
    Collection coll = new ArrayList(size);
    Iterator it = idsToSerializers.values().iterator();
    while (it.hasNext()) {
      Object v = it.next();
      if (v instanceof InitMarker) {
        v = ((Marker)v).getSerializer();
      }
      if (v instanceof DataSerializer) {
        coll.add(v);
      }
    }

    Iterator<Entry<String, SerializerAttributesHolder>> iterator = dsClassesToHolders.entrySet().iterator();
    while (iterator.hasNext()) {
      Entry<String, SerializerAttributesHolder> entry = iterator.next();
      String name = entry.getKey();
      SerializerAttributesHolder holder = entry.getValue();
      try {
        Class cl = getCachedClass(name); 
        DataSerializer ds = null;
        if (holder.getEventId() != null) {
          ds = register(cl, false, holder.getEventId(), holder.getProxyId());
        } else {
          ds = register(cl, false);
        }
        coll.add(ds);
        iterator.remove();
        idsToHolders.remove(ds.getId());
        for (Class clazz : ds.getSupportedClasses()) {
          supportedClassesToHolders.remove(clazz.getName());
        }
      } catch (ClassNotFoundException cnfe) {
        logger.info(LogMarker.SERIALIZER, LocalizedMessage.create(LocalizedStrings.InternalDataSerializer_COULD_NOT_LOAD_DATASERIALIZER_CLASS_0, name));
      }
    }

    return (DataSerializer[]) coll.toArray(new DataSerializer[coll.size()]);
  }

  /**
   * Returns all the data serializers in this vm. This method, unlike
   * {@link #getSerializers()}, does not force loading of the data serializers
   * which were not loaded in the vm earlier.
   * 
   * @return Array of {@link SerializerAttributesHolder}
   */
  public static SerializerAttributesHolder[] getSerializersForDistribution() {

    final int size = idsToSerializers.size() + dsClassesToHolders.size();
    Collection<SerializerAttributesHolder> coll = new ArrayList<InternalDataSerializer.SerializerAttributesHolder>(
        size);

    Iterator it = idsToSerializers.values().iterator();
    while (it.hasNext()) {
      Object v = it.next();
      if (v instanceof InitMarker) {
        v = ((Marker)v).getSerializer();
      }
      if (v instanceof DataSerializer) {
        DataSerializer s = (DataSerializer)v;
        coll.add(new SerializerAttributesHolder(s.getClass().getName(),
            (EventID)s.getEventId(), (ClientProxyMembershipID)s.getContext(), s
                .getId()));
      }
    }

    Iterator<Entry<String, SerializerAttributesHolder>> iterator = dsClassesToHolders
        .entrySet().iterator();
    while (iterator.hasNext()) {
      SerializerAttributesHolder v = iterator.next().getValue();
      coll.add(v);
    }

    return coll.toArray(new SerializerAttributesHolder[coll.size()]);
  }

  /**
   * Persist this class's map to out 
   */
  public static void saveRegistrations(DataOutput out) throws IOException {
    Iterator it = idsToSerializers.values().iterator();
    while (it.hasNext()) {
      Object v = it.next();
      if (v instanceof InitMarker) {
        v = ((Marker)v).getSerializer();
      }
      if (v instanceof DataSerializer) {
        DataSerializer ds = (DataSerializer)v;
        out.writeInt(ds.getId()); // since 5.7 an int instead of a byte
        DataSerializer.writeClass(ds.getClass(), out);
      }
    }
    if (!dsClassesToHolders.isEmpty()) {
      Iterator<Entry<String, SerializerAttributesHolder>> iterator = dsClassesToHolders
          .entrySet().iterator();
      Class dsClass = null;
      while (iterator.hasNext()) {
        try {
          dsClass = getCachedClass(iterator.next().getKey());
        } catch (ClassNotFoundException cnfe) {
          logger.info(LogMarker.SERIALIZER, LocalizedMessage.create(LocalizedStrings.InternalDataSerializer_COULD_NOT_LOAD_DATASERIALIZER_CLASS_0, dsClass));
          continue;
        }
        DataSerializer ds = register(dsClass, false);
        iterator.remove();
        idsToHolders.remove(ds.getId());
        for (Class clazz : ds.getSupportedClasses()) {
          supportedClassesToHolders.remove(clazz.getName());
        }

        out.writeInt(ds.getId()); // since 5.7 an int instead of a byte
        DataSerializer.writeClass(ds.getClass(), out);
      }
    }
    // We know that DataSerializer's id must be > 0 so write a zero
    // to mark the end of the ds list.
    out.writeInt(0); // since 5.7 an int instead of a byte
  }

  /**
   * Read the data from in and register it with this class.
   * @throws IllegalArgumentException if a registration fails
   */
  public static void loadRegistrations(DataInput in) throws IOException
  {
    while (in.readInt() != 0) {
      Class dsClass = null;
      boolean skip = false;
      try {
        dsClass = DataSerializer.readClass(in);
      } catch (ClassNotFoundException ex) {
        skip = true;
      }
      if (skip) {
        continue;
      }
      register(dsClass, /*dsId,*/ true);
    }
  }

  /**
   * Adds a <code>RegistrationListener</code> that will receive
   * callbacks when <code>DataSerializer</code>s and
   * <code>Instantiator</code>s are registered.
   */
  public static void addRegistrationListener(RegistrationListener l) {
    synchronized (listenersSync) {
      Set newSet = new HashSet(listeners);
      newSet.add(l);
      listeners = newSet;
    }
  }

  /**
   * Removes a <code>RegistrationListener</code> so that it no longer
   * receives callbacks.
   */
  public static void removeRegistrationListener(RegistrationListener l) {
    synchronized (listenersSync) {
      Set newSet = new HashSet(listeners);
      newSet.remove(l);
      listeners = newSet;
    }
  }

  /**
   * Alerts all <code>RegistrationListener</code>s that a new
   * <code>DataSerializer</code> has been registered
   *
   * @see InternalDataSerializer.RegistrationListener#newDataSerializer
   */
  private static void fireNewDataSerializer(DataSerializer ds) {
    Iterator iter = listeners.iterator();
    while (iter.hasNext()) {
      RegistrationListener listener = (RegistrationListener) iter.next();
      listener.newDataSerializer(ds);
    }
  }

  /**
   * Alerts all <code>RegistrationListener</code>s that a new
   * <code>Instantiator</code> has been registered
   *
   * @see InternalDataSerializer.RegistrationListener#newInstantiator
   */
  static void fireNewInstantiator(Instantiator instantiator) {
    Iterator iter = listeners.iterator();
    while (iter.hasNext()) {
      RegistrationListener listener = (RegistrationListener) iter.next();
      listener.newInstantiator(instantiator);
    }
  }

  /**
   * If we are connected to a distributed system, send a message to
   * other members telling them about a newly-registered serializer.
   */
  private static void sendRegistrationMessage(DataSerializer s) {
    InternalDistributedSystem system = InternalDistributedSystem.getConnectedInstance();
    if (system != null) {
      RegistrationMessage m = new RegistrationMessage(s);
      system.getDistributionManager().putOutgoing(m);
    }
  }

  ///////////////// START DataSerializer Implementation Methods ///////////

  // Writes just the header of a DataSerializableFixedID to out.
  public static final void writeDSFIDHeader(int dsfid, DataOutput out) throws IOException {
    if (dsfid == DataSerializableFixedID.ILLEGAL) {
      throw new IllegalStateException(LocalizedStrings.InternalDataSerializer_ATTEMPTED_TO_SERIALIZE_ILLEGAL_DSFID.toLocalizedString());
    }
   if (dsfid <= Byte.MAX_VALUE && dsfid >= Byte.MIN_VALUE) {
      out.writeByte(DS_FIXED_ID_BYTE);
      out.writeByte(dsfid);
    } else if (dsfid <= Short.MAX_VALUE && dsfid >= Short.MIN_VALUE) {
      out.writeByte(DS_FIXED_ID_SHORT);
      out.writeShort(dsfid);
    } else {
      out.writeByte(DS_FIXED_ID_INT);
      out.writeInt(dsfid);
    }
  }
  
  public static final void writeDSFID(DataSerializableFixedID o, DataOutput out)
    throws IOException
  {
    int dsfid = o.getDSFID();
    if (dsfidToClassMap != null && logger.isTraceEnabled(LogMarker.DEBUG_DSFID)) {
      logger.trace(LogMarker.DEBUG_DSFID, "writeDSFID {} class={}", dsfid, o.getClass());
      if (dsfid != DataSerializableFixedID.NO_FIXED_ID && dsfid != DataSerializableFixedID.ILLEGAL) {
        // consistency check to make sure that the same DSFID is not used
        // for two different classes
        String newClassName = o.getClass().getName();
        String existingClassName = (String)dsfidToClassMap.putIfAbsent(Integer.valueOf(dsfid), newClassName);
        if (existingClassName != null && !existingClassName.equals(newClassName)) {
          logger.trace(LogMarker.DEBUG_DSFID, "dsfid={} is used for class {} and class {}", dsfid, existingClassName, newClassName);
        }
      }
    }
    if (dsfid == DataSerializableFixedID.NO_FIXED_ID) {
      out.writeByte(DS_NO_FIXED_ID);
      DataSerializer.writeClass(o.getClass(), out);
    } else {
      writeDSFIDHeader(dsfid, out);
    }
    try {
      invokeToData(o, out);
    } catch (IOException io) {
      // Note: this is not a user code toData but one from our
      // internal code since only GemFire product code implements DSFID
      throw io;
    } catch (CancelException ex) {
      //Serializing a PDX can result in a cache closed exception. Just rethrow
      throw ex;
    } catch (ToDataException ex) {
      throw ex;
    } catch (GemFireRethrowable ex) {
      throw ex;
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Throwable t) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      throw new ToDataException("toData failed on dsfid=" + dsfid+" msg:"+t.getMessage(),  t);
    }
  }

  /**
   * Data serializes an instance of a well-known class to the given
   * <code>DataOutput</code>.
   *
   * @return <code>true</code> if <code>o</code> was actually
   *         written to <code>out</code>
   */
  public static boolean writeWellKnownObject(Object o,
                                              DataOutput out, boolean ensurePdxCompatibility)
    throws IOException {
    return writeUserObject(o, out, ensurePdxCompatibility);
  }
  /**
   * Data serializes an instance of a "user class" (that is, a class
   * that can be handled by a registered <code>DataSerializer</code>)
   * to the given <code>DataOutput</code>.
   *
   * @return <code>true</code> if <code>o</code> was written to
   *         <code>out</code>.
   */
  private static boolean writeUserObject(Object o, DataOutput out, boolean ensurePdxCompatibility)
    throws IOException {

    final Class<?> c = o.getClass();
    final DataSerializer serializer =
      InternalDataSerializer.getSerializer(c);
    if (serializer != null) {
      int id = serializer.getId();
      if (id != 0) {
        checkPdxCompatible(o, ensurePdxCompatibility);
        // id will be 0 if it is a WellKnowDS
        if (id <= Byte.MAX_VALUE && id >= Byte.MIN_VALUE) {
          out.writeByte(USER_CLASS);
          out.writeByte((byte)id);
        } else if (id <= Short.MAX_VALUE && id >= Short.MIN_VALUE) {
          out.writeByte(USER_CLASS_2);
          out.writeShort(id);
        } else {
          out.writeByte(USER_CLASS_4);
          out.writeInt(id);
        }
      } else {
        if (ensurePdxCompatibility) {
          if (!(serializer instanceof WellKnownPdxDS)) {
            checkPdxCompatible(o, ensurePdxCompatibility);
          }
        }
      }
      boolean toDataResult = false;
      try {
        toDataResult = serializer.toData(o, out);
      } catch (IOException io) {
        if (serializer instanceof WellKnownDS) {
          // this is not user code so throw IOException
          throw io; // see bug 44659
        } else {
          // We no longer rethrow IOException here
          // because if user code throws an IOException we want
          // to create a ToDataException to report it as a problem
          // with the plugin code.
          throw new ToDataException("toData failed on DataSerializer with id=" + id + " for class " + c,  io);
        }
      } catch (ToDataException ex) {
        throw ex;
      } catch (CancelException ex) {
        //Serializing a PDX can result in a cache closed exception. Just rethrow
        throw ex;
      } catch (GemFireRethrowable ex) {
        throw ex;
      } catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error.  We're poisoned
        // now, so don't let this thread continue.
        throw err;
      } catch (Throwable t) {
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above).  However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        throw new ToDataException("toData failed on DataSerializer with id=" + id + " for class " + c,  t);
      }
      if (toDataResult) {
        return true;
      } else {
        throw new ToDataException(
          LocalizedStrings.DataSerializer_SERIALIZER_0_A_1_SAID_THAT_IT_COULD_SERIALIZE_AN_INSTANCE_OF_2_BUT_ITS_TODATA_METHOD_RETURNED_FALSE
          .toLocalizedString(new Object[]{Integer.valueOf(serializer.getId()), serializer.getClass().getName(), o.getClass().getName()}));
      }
      // Do byte[][] and Object[] here to fix bug 44060
    } else if (o instanceof byte[][]) {
      byte[][] byteArrays = (byte[][])o;
      out.writeByte(ARRAY_OF_BYTE_ARRAYS);
      writeArrayOfByteArrays(byteArrays, out);
      return true;
    } else if (o instanceof Object[]) {
      Object[] array = (Object[]) o;
      out.writeByte(OBJECT_ARRAY);
      writeObjectArray(array, out, ensurePdxCompatibility);
      return true;
    } else if (is662SerializationEnabled() && (o.getClass().isEnum()
        /* for bug 52271 */ || (o.getClass().getSuperclass() != null && o.getClass().getSuperclass().isEnum()))) {
      if (isPdxSerializationInProgress()) {
        writePdxEnum((Enum<?>)o, out);
      } else {
        // TODO once .NET is enhanced to support inline enums then it should be compatible.
        checkPdxCompatible(o, ensurePdxCompatibility);
        writeGemFireEnum((Enum<?>)o, out);
      }
      return true;
    } else {
      PdxSerializer pdxSerializer = TypeRegistry.getPdxSerializer();
      if (pdxSerializer != null) {
        return writePdx(out, null, o, pdxSerializer);
      }
//       DSDataOutput out2 = new DSDataOutput(out);

//       for (int i = 0; i < serializers.length; i++) {
//         final DataSerializer myserializer =
//           (DataSerializer) serializers[i];
//         out2.setSerializerId(myserializer.getId());
//         if (myserializer.toData(o, out2)) {
//           if (!out2.hasWritten()) {
//             String s = "Serializer " + serializer + " serialized a " +
//               o.getClass().getName() + ", but it did not write " +
//               "any data";
//             throw new IOException(s);
//           }
//           return true;

//         } else {
//           if (out2.hasWritten()) {
//             String s = "Serializer " + myserializer +
//               " did not serialize a " + o.getClass().getName() +
//               ", but it wrote data";
//             throw new IOException(s);
//           }
//         }
//       }
      return false;
    }
  }


  public static boolean autoSerialized(Object o, DataOutput out) throws IOException {
    AutoSerializableManager asm = TypeRegistry.getAutoSerializableManager();
    if (asm != null) {
      AutoClassInfo aci = asm.getExistingClassInfo(o.getClass());
      if (aci != null) {
        GemFireCacheImpl gfc = GemFireCacheImpl.getForPdx("PDX registry is unavailable because the Cache has been closed.");
        TypeRegistry tr = gfc.getPdxRegistry();

        PdxWriterImpl writer;
        {
          PdxOutputStream os;
          if (out instanceof HeapDataOutputStream) {
            os = new PdxOutputStream((HeapDataOutputStream) out);
          } else {
            os = new PdxOutputStream();
          }
          writer = new PdxWriterImpl(tr, o, aci, os);
        }
        try {
          if (is662SerializationEnabled()) {
            boolean alreadyInProgress = isPdxSerializationInProgress();
            if (!alreadyInProgress) {
              setPdxSerializationInProgress(true);
              try {
                asm.writeData(writer, o, aci);
              } finally {
                setPdxSerializationInProgress(false);
              }
            } else {
              asm.writeData(writer, o, aci);
            }
          } else {
            asm.writeData(writer, o, aci);
          }
        } catch (ToDataException ex) {
          throw ex;
        } catch (CancelException ex) {
          //Serializing a PDX can result in a cache closed exception. Just rethrow
          throw ex;
        } catch (NonPortableClassException ex) {
          throw ex;
        } catch (GemFireRethrowable ex) {
          throw ex;
        } catch (VirtualMachineError err) {
          SystemFailure.initiateFailure(err);
          // If this ever returns, rethrow the error.  We're poisoned
          // now, so don't let this thread continue.
          throw err;
        } catch (Throwable t) {
          // Whenever you catch Error or Throwable, you must also
          // catch VirtualMachineError (see above).  However, there is
          // _still_ a possibility that you are dealing with a cascading
          // error condition, so you also need to check to see if the JVM
          // is still usable:
          SystemFailure.checkFailure();
          throw new ToDataException("PdxSerializer failed when calling toData on " + o.getClass(),  t);
        }
        int bytesWritten = writer.completeByteStreamGeneration();
        getDMStats(gfc).incPdxSerialization(bytesWritten);
        if (!(out instanceof HeapDataOutputStream)) {
          writer.sendTo(out);
        }
        return true;
      }
    }
    return false;
  }

  public static void checkPdxCompatible(Object o, boolean ensurePdxCompatibility) {
    if (ensurePdxCompatibility) {
      throw new NonPortableClassException("Instances of " + o.getClass() + " are not compatible with non-java PDX.");
    }
  }

  /**
   * Test to see if the object is in the gemfire package,
   * to see if we should pass it on to a users custom serializater.
   * 
   * TODO - this is a hack. We should have different flavor of
   * write object for user objects that calls the serializer. Other
   * kinds of write object shouldn't even get to the pdx serializer.
   */
  private static boolean isGemfireObject(Object o) {
    return ((o instanceof Function) // fixes 43691
            || o.getClass().getName().startsWith("com.gemstone."))
      && !(o instanceof PdxSerializerObject);
  }

  /**
   * Reads an object that was serialized by a customer ("user")
   * <code>DataSerializer</code> from the given <code>DataInput</code>.
   *
   * @throws IOException
   *         If the serializer that can deserialize the object is
   *         not registered.
   */
  private static Object readUserObject(DataInput in, int serializerId)
    throws IOException, ClassNotFoundException {
    DataSerializer serializer =
      InternalDataSerializer.getSerializer(serializerId);

    if (serializer == null) {
      throw new IOException(LocalizedStrings.DataSerializer_SERIALIZER_0_IS_NOT_REGISTERED.toLocalizedString(new Object[] { Integer.valueOf(serializerId) }));
    }

    return serializer.fromData(in);
  }

  /**
   * Checks to make sure a <code>DataOutput</code> is not
   * <code>null</code>.
   *
   * @throws NullPointerException
   *         If <code>out</code> is <code>null</code>
   */
  public static void checkOut(DataOutput out) {
    if (out == null) {
      String s = "Null DataOutput";
      throw new NullPointerException(s);
    }
  }
  /**
   * Checks to make sure a <code>DataInput</code> is not
   * <code>null</code>.
   *
   * @throws NullPointerException
   *         If <code>in</code> is <code>null</code>
   */
  public static void checkIn(DataInput in) {
    if (in == null) {
      String s = "Null DataInput";
      throw new NullPointerException(s);
    }
  }



  /**
   * Writes a <code>Set</code> to a <code>DataOutput</code>.
   *
   * <P>
   *
   * This method is internal because its semantics (that is, its
   * ability to write any kind of <code>Set</code>) are different from
   * the <code>write</code>XXX methods of the external
   * <code>DataSerializer</code>. 
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   *
   * @see #readSet
   *
   * @since 4.0
   */
  public static void writeSet(Collection<?> set, DataOutput out)
      throws IOException {

    checkOut(out);

    int size;
    
    if (set == null) {
      size = -1;
    } else {
      size = set.size();
    }
    writeArrayLength(size, out);
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing HashSet with {} elements: {}", size, set);
    }
    if (size > 0) {
      for (Object element : set) {
        writeObject(element, out);
      }
    }
  }

  /**
   * Reads a <code>Set</code> from a <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   * @throws ClassNotFoundException
   *         The class of one of the <Code>HashSet</code>'s
   *         elements cannot be found.
   *
   * @see #writeSet
   *
   * @since 4.0
   */
  public static Set readSet(DataInput in) 
    throws IOException, ClassNotFoundException {
    return readHashSet(in);
  }

  /**
   * Reads a <code>Set</code> from a <code>DataInput</code> into the given
   * non-null collection. Returns true if collection read is non-null else
   * returns false.
   * 
   * @throws IOException
   *           A problem occurs while reading from <code>in</code>
   * @throws ClassNotFoundException
   *           The class of one of the <Code>Set</code>'s elements cannot be
   *           found.
   * 
   * @see #writeSet
   */
  public static <E> boolean readCollection(DataInput in, Collection<E> c)
      throws IOException, ClassNotFoundException {

    checkIn(in);

    final int size = readArrayLength(in);
    if (size >= 0) {
      E element;
      for (int index = 0; index < size; ++index) {
        element = DataSerializer.<E> readObject(in);
        c.add(element);
      }

      if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
        logger.trace(LogMarker.SERIALIZER, "Read Collection with {} elements: {}", size, c);
      }
      return true;
    }
    return false;
  }

  /**
   * write a set of Long objects
   * @param set the set of Long objects
   * @param hasLongIDs if false, write only ints, not longs
   * @param out the output stream
   */
  public static void writeSetOfLongs(Set set, boolean hasLongIDs, DataOutput out) throws IOException {
    if (set == null) {
      out.writeInt(-1);
    } else {
      out.writeInt(set.size());
      out.writeBoolean(hasLongIDs);
      for (Iterator it=set.iterator(); it.hasNext(); ) {
        Long l = (Long)it.next();
        if (hasLongIDs) {
          out.writeLong(l.longValue());
        } else {
          out.writeInt((int)l.longValue());
        }
      }
    }
  }
  
  /** read a set of Long objects */
  public static Set<Long> readSetOfLongs(DataInput in) throws IOException {
    int size = in.readInt();
    if (size < 0) {
      return null;
    } else {
      Set result = new HashSet(size);
      boolean longIDs = in.readBoolean();
      for (int i=0; i<size; i++) {
        long l = longIDs? in.readLong() : in.readInt();
        result.add(Long.valueOf(l));
      }
      return result;
    }
  }

  /**
   * write a set of Long objects
   * @param list the set of Long objects
   * @param hasLongIDs if false, write only ints, not longs
   * @param out the output stream
   */
  public static void writeListOfLongs(List list, boolean hasLongIDs, DataOutput out) throws IOException {
    if (list == null) {
      out.writeInt(-1);
    } else {
      out.writeInt(list.size());
      out.writeBoolean(hasLongIDs);
      for (Iterator it=list.iterator(); it.hasNext(); ) {
        Long l = (Long)it.next();
        if (hasLongIDs) {
          out.writeLong(l.longValue());
        } else {
          out.writeInt((int)l.longValue());
        }
      }
    }
  }
  
  /** read a set of Long objects */
  public static List<Long> readListOfLongs(DataInput in) throws IOException {
    int size = in.readInt();
    if (size < 0) {
      return null;
    } else {
      List result = new LinkedList();
      boolean longIDs = in.readBoolean();
      for (int i=0; i<size; i++) {
        long l = longIDs? in.readLong() : in.readInt();
        result.add(Long.valueOf(l));
      }
      return result;
    }
  }
  

  
  /**
   * Writes the type code for a primitive type Class
   * to <code>DataOutput</code>.
   */
  public static final void writePrimitiveClass(Class c, DataOutput out)
  throws IOException {
    if (c == Boolean.TYPE) {
      out.writeByte(BOOLEAN_TYPE);
    }
    else if (c == Character.TYPE) {
      out.writeByte(CHARACTER_TYPE);
    }
    else if (c == Byte.TYPE) {
      out.writeByte(BYTE_TYPE);
    }
    else if (c == Short.TYPE) {
      out.writeByte(SHORT_TYPE);
    }
    else if (c == Integer.TYPE) {
      out.writeByte(INTEGER_TYPE);
    }
    else if (c == Long.TYPE) {
      out.writeByte(LONG_TYPE);
    }
    else if (c == Float.TYPE) {
      out.writeByte(FLOAT_TYPE);
    }
    else if (c == Double.TYPE) {
      out.writeByte(DOUBLE_TYPE);
    }
    else if (c == Void.TYPE) {
      out.writeByte(VOID_TYPE);
    }
    else if (c == null) {
      out.writeByte(NULL);
    }
    else {
      throw new InternalGemFireError(LocalizedStrings.InternalDataSerializer_UNKNOWN_PRIMITIVE_TYPE_0.toLocalizedString(c.getName()));
    }
  }
  
  public static final Class decodePrimitiveClass(byte typeCode) {
    switch (typeCode) {
      case BOOLEAN_TYPE:
        return Boolean.TYPE;
      case CHARACTER_TYPE:
        return Character.TYPE;
      case BYTE_TYPE:
        return Byte.TYPE;
      case SHORT_TYPE:
        return Short.TYPE;
      case INTEGER_TYPE:
        return Integer.TYPE;
      case LONG_TYPE:
        return Long.TYPE;
      case FLOAT_TYPE:
        return Float.TYPE;
      case DOUBLE_TYPE:
        return Double.TYPE;
      case VOID_TYPE:
        return Void.TYPE;
      case NULL:
        return null;
      default:
        throw new InternalGemFireError(LocalizedStrings.InternalDataSerializer_UNEXPECTED_TYPECODE_0.toLocalizedString(Byte.valueOf(typeCode)));
    }
  }

  private static final byte TIME_UNIT_NANOSECONDS = -1;
  private static final byte TIME_UNIT_MICROSECONDS = -2;
  private static final byte TIME_UNIT_MILLISECONDS = -3;
  private static final byte TIME_UNIT_SECONDS = -4;

  /**
   * Reads a <code>TimeUnit</code> from a <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   */
  public static TimeUnit readTimeUnit(DataInput in)
    throws IOException {

    InternalDataSerializer.checkIn(in);

    byte type = in.readByte();

    TimeUnit unit;
    switch (type) {
    case TIME_UNIT_NANOSECONDS:
      unit = TimeUnit.NANOSECONDS;
      break;
    case TIME_UNIT_MICROSECONDS:
      unit = TimeUnit.MICROSECONDS;
      break;
    case TIME_UNIT_MILLISECONDS:
      unit = TimeUnit.MILLISECONDS;
      break;
    case TIME_UNIT_SECONDS:
      unit = TimeUnit.SECONDS;
      break;
    default:
      throw new IOException(LocalizedStrings.DataSerializer_UNKNOWN_TIMEUNIT_TYPE_0.toLocalizedString(Byte.valueOf(type)));
    }

    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Read TimeUnit: {}", unit);
    }

    return unit;
  }

  public static void writeTimestamp(Timestamp o, DataOutput out) throws IOException {
    InternalDataSerializer.checkOut(out);

    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing Timestamp: {}", o);
    }
    DataSerializer.writePrimitiveLong(o.getTime(), out);
  }
  public static Timestamp readTimestamp(DataInput in) throws IOException {
    InternalDataSerializer.checkIn(in);
    Timestamp result = new Timestamp(DataSerializer.readPrimitiveLong(in));
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Read Timestamp: {}", result);
    }
    return result;
  }

  public static void writeUUID(java.util.UUID o, DataOutput out) throws IOException {
    InternalDataSerializer.checkOut(out);

    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing UUID: {}", o);
    }
    DataSerializer.writePrimitiveLong(o.getMostSignificantBits(), out);
    DataSerializer.writePrimitiveLong(o.getLeastSignificantBits(), out);
  }
  public static UUID readUUID(DataInput in) throws IOException {
    InternalDataSerializer.checkIn(in);
    long mb = DataSerializer.readPrimitiveLong(in);
    long lb = DataSerializer.readPrimitiveLong(in);
    UUID result = new UUID(mb, lb);
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Read UUID: {}", result);
    }
    return result;
  }

  public static void writeBigDecimal(BigDecimal o, DataOutput out) throws IOException {
    InternalDataSerializer.checkOut(out);

    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing BigDecimal: {}", o);
    }
    DataSerializer.writeString(o.toString(), out);
  }
  public static BigDecimal readBigDecimal(DataInput in) throws IOException {
    InternalDataSerializer.checkIn(in);
    BigDecimal result = new BigDecimal(DataSerializer.readString(in));
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Read BigDecimal: {}", result);
    }
    return result;
  }

  public static void writeBigInteger(BigInteger o, DataOutput out) throws IOException {
    InternalDataSerializer.checkOut(out);

    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing BigInteger: {}", o);
    }
    DataSerializer.writeByteArray(o.toByteArray(), out);
  }
  public static BigInteger readBigInteger(DataInput in) throws IOException {
    InternalDataSerializer.checkIn(in);
    BigInteger result = new BigInteger(DataSerializer.readByteArray(in));
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Read BigInteger: {}", result);
    }
    return result;
  }


  //private static final HashSet seenClassNames = DEBUG_DSFID ? new HashSet(): null;
  private static final ConcurrentMap dsfidToClassMap = logger.isTraceEnabled(LogMarker.DEBUG_DSFID) ? new ConcurrentHashMap(): null;
  
  public static final void writeUserDataSerializableHeader(int classId,
                                                           DataOutput out)
    throws IOException
  {
    if (classId <= Byte.MAX_VALUE && classId >= Byte.MIN_VALUE) {
      out.writeByte(USER_DATA_SERIALIZABLE);
      out.writeByte(classId);
    } else if (classId <= Short.MAX_VALUE && classId >= Short.MIN_VALUE) {
      out.writeByte(USER_DATA_SERIALIZABLE_2);
      out.writeShort(classId);
    } else {
      out.writeByte(USER_DATA_SERIALIZABLE_4);
      out.writeInt(classId);
    }
  }

  /**
   * Writes given number of characters from array of <code>char</code>s to a
   * <code>DataOutput</code>.
   * 
   * @throws IOException
   *           A problem occurs while writing to <code>out</code>
   * 
   * @see DataSerializer#readCharArray
   * @since 6.6
   */
  public static void writeCharArray(char[] array, int length, DataOutput out)
      throws IOException {

    checkOut(out);

    if (array == null) {
      length = -1;
    }
    writeArrayLength(length, out);
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing char array of length {}", length);
    }
    if (length > 0) {
      for (int i = 0; i < length; i++) {
        out.writeChar(array[i]);
      }
    }
  }
  
  /**
   * returns true if the byte array is the serialized form of a null reference
   * @param serializedForm the serialized byte array
   */
  public static final boolean isSerializedNull(byte[] serializedForm) {
    return serializedForm.length == 1 && serializedForm[0] == NULL;
  }

  public static final void basicWriteObject(Object o, DataOutput out, boolean ensurePdxCompatibility)
    throws IOException {

    checkOut(out);

    final boolean isDebugEnabled_SERIALIZER = logger.isTraceEnabled(LogMarker.SERIALIZER);
    if (isDebugEnabled_SERIALIZER) {
      logger.trace(LogMarker.SERIALIZER, "basicWriteObject: {}", o);
    }

    // Handle special objects first
    if (o == null) {
      out.writeByte(NULL);

    } else if (o instanceof DataSerializableFixedID) {
      checkPdxCompatible(o, ensurePdxCompatibility);
      DataSerializableFixedID dsfid = (DataSerializableFixedID)o;
      writeDSFID(dsfid, out);
    } else if (autoSerialized(o, out)) {
      // all done
    } else if (o instanceof DataSerializable.Replaceable) {
      // do this first to fix bug 31609
      // do this before DataSerializable
      Object replacement = ((DataSerializable.Replaceable) o).replace();
      basicWriteObject(replacement, out, ensurePdxCompatibility);

    } else if (o instanceof PdxSerializable) {
      writePdx(out, GemFireCacheImpl.getForPdx("PDX registry is unavailable because the Cache has been closed."), o, null);
    } else if (o instanceof DataSerializable) {
      if (isDebugEnabled_SERIALIZER) {
        logger.trace(LogMarker.SERIALIZER, "Writing DataSerializable: {}", o);
      }
      checkPdxCompatible(o, ensurePdxCompatibility);
      // @todo darrel: make a subinterface of DataSerializable
      // named InstantiatedDataSerializable
      // which adds one method which returns the instantiator code.
      // This would allow the serialization side to not need a map lookup (instead they just call the method)
      // which also helps the DataSerializable case to no longer do the lookup.
      // We could also use it to get rid of the need for static register calls
      // but that would mean that when we find one of these that we check to see
      // if that class had been registered and do so if not.
      // So from the customer's point of view this would be easier; just implement
      // a method that returns an int.

      Class c = o.getClass();
      // Is "c" a user class registered with an Instantiator?
      int classId = InternalInstantiator.getClassId(c);
      if (classId != 0) {
        writeUserDataSerializableHeader(classId, out);
      } else {
        out.writeByte(DATA_SERIALIZABLE);
//         if (DEBUG_DSFID) {
//           if (logger.infoEnabled()) {
//             boolean alreadySeen;
//             synchronized (seenClassNames) {
//               alreadySeen = seenClassNames.add(c.getName());
//             }
//             if (alreadySeen) {
//               // this class should be made a DSFID if it is a product class
//               logger.info("DataSerialized class " + c.getName(), new RuntimeException("CALLSTACK"));
//             }
//           }
//         }
        DataSerializer.writeClass(c, out);
      }
      DataSerializable ds = (DataSerializable) o;
      invokeToData(ds, out);

    } else if (o instanceof Sendable) {
      if (!(o instanceof PdxInstance) || o instanceof PdxInstanceEnum) {
        checkPdxCompatible(o, ensurePdxCompatibility);
      }
      ((Sendable)o).sendTo(out);
    } else if (writeWellKnownObject(o, out, ensurePdxCompatibility)) {
      // Nothing more to do...
    } else {
      checkPdxCompatible(o, ensurePdxCompatibility);
      if (logger.isTraceEnabled(LogMarker.DUMP_SERIALIZED)) {
        logger.trace(LogMarker.DUMP_SERIALIZED, "DataSerializer Serializing an instance of {}", o.getClass().getName());
      }

      /* If the (internally known) ThreadLocal named "DataSerializer.DISALLOW_JAVA_SERIALIZATION" is set,
       * then an exception will be thrown if we try to do standard Java Serialization.
       * This is used to catch Java serialization early for the case where the data is being
       * sent to a non-Java client
       */
      if (disallowJavaSerialization() && (o instanceof Serializable)) {
        throw new NotSerializableException(LocalizedStrings.DataSerializer_0_IS_NOT_DATASERIALIZABLE_AND_JAVA_SERIALIZATION_IS_DISALLOWED.toLocalizedString(o.getClass().getName()));
      }

//       if (out instanceof DSDataOutput) {
//         // Unwrap the DSDataOutput to avoid one layer of
//         // delegation.  This also prevents us from having to flush
//         // the ObjectOutputStream.
//         out = ((DSDataOutput) out).out;
//       }
      writeSerializableObject(o, out);
    }
  }
  
  private static boolean disallowJavaSerialization() {
    Boolean v = DISALLOW_JAVA_SERIALIZATION.get();
    return v != null && v;
  }
  
  /**
   * @throws IOException 
   * @since 6.6.2
   */
  private static void writePdxEnum(Enum<?> e, DataOutput out) throws IOException {
    TypeRegistry tr =
      GemFireCacheImpl.getForPdx("PDX registry is unavailable because the Cache has been closed.").getPdxRegistry();
    int eId = tr.getEnumId(e);
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "write PdxEnum id={} enum={}", eId, e);
    }
    writePdxEnumId(eId, out);
  }
  public static void writePdxEnumId(int eId, DataOutput out) throws IOException {
    out.writeByte(PDX_ENUM);
    out.writeByte(eId >> 24);
    writeArrayLength(eId & 0xFFFFFF, out);
  }

  /**
   * @throws IOException
   * since 6.6.2
   */
  private static Object readPdxEnum(DataInput in) throws IOException {
    int dsId = in.readByte();
    int tmp = readArrayLength(in);
    int enumId = (dsId << 24) | (tmp & 0xFFFFFF);
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "read PdxEnum id={}", enumId);
    }
    GemFireCacheImpl gfc = GemFireCacheImpl.getForPdx("PDX registry is unavailable because the Cache has been closed.");
    TypeRegistry tr = gfc.getPdxRegistry();
    
    Object result = tr.getEnumById(enumId);
    if (result instanceof PdxInstance) {
      getDMStats(gfc).incPdxInstanceCreations();
    }
    return result;
  }
  
  private static void writeGemFireEnum(Enum<?> e, DataOutput out) throws IOException {
    boolean isGemFireObject = isGemfireObject(e);
    DataSerializer.writePrimitiveByte(isGemFireObject ? GEMFIRE_ENUM : PDX_INLINE_ENUM, out);
    DataSerializer.writeString(e.getDeclaringClass().getName(), out);
    DataSerializer.writeString(e.name(), out);
    if (!isGemFireObject) {
      InternalDataSerializer.writeArrayLength(e.ordinal(), out);
    }
  }

  @SuppressWarnings("unchecked")
  private static Enum<?> readGemFireEnum(DataInput in) throws IOException, ClassNotFoundException {
    String className = DataSerializer.readString(in);
    String enumName = DataSerializer.readString(in);
    @SuppressWarnings("rawtypes")
    Class c = getCachedClass(className);
    return Enum.valueOf(c, enumName);
  }

  private static Object readPdxInlineEnum(DataInput in) throws IOException, ClassNotFoundException {
    GemFireCacheImpl gfc = GemFireCacheImpl.getInstance();
    if (gfc != null && gfc.getPdxReadSerializedByAnyGemFireServices()) {
      String className = DataSerializer.readString(in);
      String enumName = DataSerializer.readString(in);
      int enumOrdinal = InternalDataSerializer.readArrayLength(in);
      getDMStats(gfc).incPdxInstanceCreations();
      return new PdxInstanceEnum(className, enumName, enumOrdinal);
    } else {
      Enum<?> e = readGemFireEnum(in);
      InternalDataSerializer.readArrayLength(in);
      return e;
    }
  }


  /**
   * write an object in java Serializable form with a SERIALIZABLE DSCODE so
   * that it can be deserialized with DataSerializer.readObject()
   * @param o the object to serialize
   * @param out the data output to serialize to
   * @throws IOException
   */
  public static final void writeSerializableObject(Object o, DataOutput out)
    throws IOException {
    out.writeByte(SERIALIZABLE);
    if (out instanceof ObjectOutputStream) {
      ((ObjectOutputStream)out).writeObject(o);
    } else {
      OutputStream stream;
      if (out instanceof OutputStream) {
        stream = (OutputStream)out;

      } else {
        final DataOutput out2 = out;
        stream = new OutputStream() {
          @Override
            public void write(int b) throws IOException {
              out2.write(b);
            }

            //               public void write(byte[] b) throws IOException {
            //                 out.write(b);
            //               }

            //               public void write(byte[] b, int off, int len)
            //                 throws IOException {
            //                 out.write(b, off, len);
            //               }
          };
      }
      boolean wasDoNotCopy = false;
      if (out instanceof HeapDataOutputStream) {
        // To fix bug 52197 disable doNotCopy mode
        // while serialize with an ObjectOutputStream.
        // The problem is that ObjectOutputStream keeps
        // an internal byte array that it reuses while serializing.
        wasDoNotCopy = ((HeapDataOutputStream) out).setDoNotCopy(false);
      }
      try {
      ObjectOutput oos = new ObjectOutputStream(stream);
      if ( stream instanceof VersionedDataStream ) {
        Version v = ((VersionedDataStream)stream).getVersion();
        if (v != null && v != Version.CURRENT) { 
          oos = new VersionedObjectOutput(oos, v);
        }
      }
      oos.writeObject(o);
      // To fix bug 35568 just call flush. We can't call close because
      // it calls close on the wrapped OutputStream.
      oos.flush();
      } finally {
        if (wasDoNotCopy) {
          ((HeapDataOutputStream) out).setDoNotCopy(true);
        }
      }
    }
  }
  
  /**
   * For backward compatibility this method should be used to invoke
   * toData on a DSFID or DataSerializable.  It will invoke the
   * correct toData method based on the class's version information.
   * This method does not write information about the class of the
   * object.  When deserializing use the method invokeFromData to
   * read the contents of the object.
   * 
   * @param ds the object to write
   * @param out the output stream.
   */
  public static final void invokeToData(Object ds, DataOutput out) throws IOException {
    boolean isDSFID = (ds instanceof DataSerializableFixedID);
    try {
      boolean invoked = false;
      Version v = InternalDataSerializer.getVersionForDataStreamOrNull(out);
      Version[] versions = null;
      
      if (v != null && v != Version.CURRENT) {
        // get versions where DataOutput was upgraded
        if (ds instanceof SerializationVersions) {
          SerializationVersions sv = (SerializationVersions) ds;
          versions = sv.getSerializationVersions();
        }
        // check if the version of the peer or diskstore is different and
        // there has been a change in the message
        if (versions != null && versions.length > 0) {
          for (int i = 0; i < versions.length; i++) {
            // if peer version is less than the greatest upgraded version
            if (v.compareTo(versions[i]) < 0) {
              ds.getClass()
                  .getMethod(
                      "toDataPre_" + versions[i].getMethodSuffix(),
                      new Class[] { DataOutput.class }).invoke(ds, out);
              invoked = true;
              break;
            }
          }
        }
      }
      
      if (!invoked) {
        if (isDSFID) {
          ((DataSerializableFixedID)ds).toData(out);
        } else {
          ((DataSerializable)ds).toData(out);
        }
      }
    } catch (IOException io) {
      // DSFID serialization expects an IOException but otherwise
      // we want to catch it and transform into a ToDataException
      // since it might be in user code and we want to report it
      // as a problem with the plugin code
      if (isDSFID) {
        throw io;
      } else {
        throw new ToDataException("toData failed on DataSerializable " + ds.getClass(),  io);
      }
    } catch (ToDataException ex) {
      throw ex;
    } catch (CancelException ex) {
      //Serializing a PDX can result in a cache closed exception. Just rethrow
      throw ex;
    } catch (GemFireRethrowable ex) {
      throw ex;
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Throwable t) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      throw new ToDataException("toData failed on DataSerializable " + ds.getClass(),  t);
    }
  }
  
  /**
   * For backward compatibility this method should be used to invoke
   * fromData on a DSFID or DataSerializable.  It will invoke the
   * correct fromData method based on the class's version information.
   * This method does not read information about the class of the
   * object.  When serializing use the method invokeToData to
   * write the contents of the object.
   * 
   * @param ds  the object to write
   * @param in  the input stream.
   */
  public static final void invokeFromData(Object ds, DataInput in) throws IOException, ClassNotFoundException {
    try {
      boolean invoked = false;
      Version v = InternalDataSerializer.getVersionForDataStreamOrNull(in);
      Version[] versions = null;
      if (v != null && v != Version.CURRENT) {
        // get versions where DataOutput was upgraded
        if (ds instanceof SerializationVersions) {
          SerializationVersions vds = (SerializationVersions) ds;
          versions = vds.getSerializationVersions();
        }
        // check if the version of the peer or diskstore is different and
        // there has been a change in the message
        if (versions != null && versions.length > 0) {
          for (int i = 0; i < versions.length; i++) {
            // if peer version is less than the greatest upgraded version
            if (v.compareTo(versions[i]) < 0) {
              ds.getClass()
                  .getMethod(
                      "fromDataPre" + "_" + versions[i].getMethodSuffix(),
                      new Class[] { DataInput.class }).invoke(ds, in);
              invoked = true;
              break;
            }
          }
        }
      }
      if (!invoked) {
        if (ds instanceof DataSerializableFixedID) {
          ((DataSerializableFixedID)ds).fromData(in);
        } else {
          ((DataSerializable)ds).fromData(in);
        }
      }
    } catch (EOFException ex) {
      // client went away - ignore
      throw ex;
    } catch (ClassNotFoundException ex) {
      throw ex;
    } catch (CacheClosedException cce) {
      throw cce;
    } catch (Exception ex) {
      SerializationException ex2 = new SerializationException(LocalizedStrings.DataSerializer_COULD_NOT_CREATE_AN_INSTANCE_OF_0.toLocalizedString(ds.getClass().getName()), ex);
      throw ex2;
    }
  }

  
  private static final Object readDataSerializable(final DataInput in)
    throws IOException, ClassNotFoundException
  {
    Class c = readClass(in);
    try {
      Constructor init = c.getConstructor(new Class[0]);
      init.setAccessible(true);
      Object o = init.newInstance(new Object[0]);
      Assert.assertTrue(o instanceof DataSerializable);
      invokeFromData(o, in);

      if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
        logger.trace(LogMarker.SERIALIZER, "Read DataSerializable {}", o);
      }

      return o;

    } catch (EOFException ex) {
      // client went away - ignore
      throw ex;
    } catch (Exception ex) {
      SerializationException ex2 = new SerializationException(LocalizedStrings.DataSerializer_COULD_NOT_CREATE_AN_INSTANCE_OF_0.toLocalizedString(c.getName()), ex);
      throw ex2;
    }
  }
  private static final Object
    readDataSerializableFixedID(final DataInput in)
    throws IOException, ClassNotFoundException
  {
    Class c = readClass(in);
    try {
      Constructor init = c.getConstructor(new Class[0]);
      init.setAccessible(true);
      Object o = init.newInstance(new Object[0]);

      invokeFromData(o, in);
      
      if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
        logger.trace(LogMarker.SERIALIZER, "Read DataSerializableFixedID {}", o);
      }

      return o;

    } catch (Exception ex) {
      SerializationException ex2 = new SerializationException(LocalizedStrings.DataSerializer_COULD_NOT_CREATE_AN_INSTANCE_OF_0.toLocalizedString(c.getName()), ex);
      throw ex2;
    }
  }

  /**
   * Get the {@link Version} of the peer or disk store that created this
   * {@link DataInput}.
   */
  public static final Version getVersionForDataStream(DataInput in) {
    // check if this is a versioned data input
    if (in instanceof VersionedDataStream) {
      final Version v = ((VersionedDataStream)in).getVersion();
      return v != null ? v : Version.CURRENT;
    }
    else {
      // assume latest version
      return Version.CURRENT;
    }
  }

  /**
   * Get the {@link Version} of the peer or disk store that created this
   * {@link DataInput}. Returns null if the version is same as this member's.
   */
  public static final Version getVersionForDataStreamOrNull(DataInput in) {
    // check if this is a versioned data input
    if (in instanceof VersionedDataStream) {
      return ((VersionedDataStream)in).getVersion();
    }
    else {
      // assume latest version
      return null;
    }
  }

  /**
   * Get the {@link Version} of the peer or disk store that created this
   * {@link DataOutput}.
   */
  public static final Version getVersionForDataStream(DataOutput out) {
    // check if this is a versioned data output
    if (out instanceof VersionedDataStream) {
      final Version v = ((VersionedDataStream)out).getVersion();
      return v != null ? v : Version.CURRENT;
    }
    else {
      // assume latest version
      return Version.CURRENT;
    }
  }

  /**
   * Get the {@link Version} of the peer or disk store that created this
   * {@link DataOutput}. Returns null if the version is same as this member's.
   */
  public static final Version getVersionForDataStreamOrNull(DataOutput out) {
    // check if this is a versioned data output
    if (out instanceof VersionedDataStream) {
      return ((VersionedDataStream)out).getVersion();
    }
    else {
      // assume latest version
      return null;
    }
  }

  public static final byte NULL_ARRAY = -1; // array is null
  /**
   * @since 5.7 
   */
  private static final byte SHORT_ARRAY_LEN = -2; // array len encoded as unsigned short in next 2 bytes
  /**
   * @since 5.7
   */
  public static final byte INT_ARRAY_LEN = -3; // array len encoded as int in next 4 bytes
  private static final int MAX_BYTE_ARRAY_LEN = ((byte)-4) & 0xFF; 
  
  public static void writeArrayLength(int len, DataOutput out)
    throws IOException {
    if (len == -1) {
      out.writeByte(NULL_ARRAY);
    } else if (len <= MAX_BYTE_ARRAY_LEN) {
      out.writeByte(len);
    } else if (len <= 0xFFFF) {
      out.writeByte(SHORT_ARRAY_LEN);
      out.writeShort(len);
    } else {
      out.writeByte(INT_ARRAY_LEN);
      out.writeInt(len);
    }
  }
  public static int readArrayLength(DataInput in)
    throws IOException {
    byte code = in.readByte();
    if (code == NULL_ARRAY) {
      return -1;
    } else {
      int result = ubyteToInt(code);
      if (result > MAX_BYTE_ARRAY_LEN) {
        if (code == SHORT_ARRAY_LEN) {
          result = in.readUnsignedShort();
        } else if (code == INT_ARRAY_LEN) {
          result = in.readInt();
        } else {
          throw new IllegalStateException("unexpected array length code=" + code);
        }
      }
      return result;
    }
  }

  /**
   * Serializes a list of Integers.  The argument may be null.  Deserialize with
   * readListOfIntegers().
   */
  public void writeListOfIntegers(List<Integer> list, DataOutput out) throws IOException {
    int size;
    if (list == null) {
      size = -1;
    } else {
      size = list.size();
    }
    InternalDataSerializer.writeArrayLength(size, out);
    if (size > 0) {
      for (int i = 0; i < size; i++) {
        out.writeInt(list.get(i).intValue());
      }
    }
  }

  /**
   * Reads a list of integers serialized by writeListOfIntegers.  This
   * will return null if the object serialized by writeListOfIntegers was null. 
   */
  public List<Integer> readListOfIntegers(DataInput in) throws IOException {
    int size = InternalDataSerializer.readArrayLength(in);
    if (size > 0) {
      List<Integer> list = new ArrayList<Integer>(size);
      for (int i = 0; i < size; i++) {
        list.add(Integer.valueOf(in.readInt()));
      }
      return list;
    }
    else if (size == 0) {
      return Collections.<Integer>emptyList();
    }
    else {
      return null;
    }
  }
  
  /**
   * Reads and discards an array of <code>byte</code>s from a
   * <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   *
   * @see #writeByteArray(byte[], DataOutput)
   */
  public static void skipByteArray(DataInput in)
    throws IOException {

      InternalDataSerializer.checkIn(in);

      int length = InternalDataSerializer.readArrayLength(in);
      if (length != -1) {
        in.skipBytes(length);
        if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
          logger.trace(LogMarker.SERIALIZER, "Skipped byte array of length {}", length);
        }
      }
    }

  public static final Object readDSFID(final DataInput in)
    throws IOException, ClassNotFoundException
  {
    checkIn(in);
    byte header = in.readByte();
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "readDSFID: header={}", header);
    }
    if (header == DS_FIXED_ID_BYTE) {
      return DSFIDFactory.create(in.readByte(), in);
    } else if (header == DS_FIXED_ID_SHORT) {
      return DSFIDFactory.create(in.readShort(), in);
    } else if (header == DS_NO_FIXED_ID) {
      return readDataSerializableFixedID(in);
    } else if (header == DS_FIXED_ID_INT) {
      return DSFIDFactory.create(in.readInt(), in);
    } else {
      throw new IllegalStateException("unexpected byte: " + header + " while reading dsfid");
    }
  }
  
  /**
   * Reads an instance of <code>String</code> from a
   * <code>DataInput</code> given the header byte already being read.
   * The return value may be <code>null</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   *
   * @since 5.7
   */
  public static String readString(DataInput in, byte header) throws IOException {
    if (header == DSCODE.STRING_BYTES) {
      int len = in.readUnsignedShort();
      if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
        logger.trace(LogMarker.SERIALIZER, "Reading STRING_BYTES of len={}", len);
      }
      byte[] buf = new byte[len];
      in.readFully(buf, 0, len);
      return new String(buf, 0);  // intentionally using deprecated constructor
    }
    else if (header == DSCODE.STRING) {
      if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
        logger.trace(LogMarker.SERIALIZER, "Reading utf STRING");
      }
      return in.readUTF();
    }
    else if (header == DSCODE.NULL_STRING) {
      if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
        logger.trace(LogMarker.SERIALIZER, "Reading NULL_STRING");
      }
      return null;
    }
    else if (header == DSCODE.HUGE_STRING_BYTES) {
      int len = in.readInt();
      if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
        logger.trace(LogMarker.SERIALIZER, "Reading HUGE_STRING_BYTES of len={}", len);
      }
      byte[] buf = new byte[len];
      in.readFully(buf, 0, len);
      return new String(buf, 0); // intentionally using deprecated constructor
    }
    else if (header == DSCODE.HUGE_STRING) {
      int len = in.readInt();
      if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
        logger.trace(LogMarker.SERIALIZER, "Reading HUGE_STRING of len={}", len);
      }
      char[] buf = new char[len];
      for (int i=0; i < len; i++) {
        buf[i] = in.readChar();
      }
      return new String(buf);
    }
    else {
      String s = "Unknown String header " + header;
      throw new IOException(s);
    }
  }

  private static DataSerializer dvddeserializer;

  public static void registerDVDDeserializer(DataSerializer dvddeslzr) {
    dvddeserializer = dvddeslzr;
  }
  
  /**
   * Just like readObject but make sure and pdx deserialized is not
   * a PdxInstance. 
   * @since 6.6.2
   */
  public static final <T> T readNonPdxInstanceObject(final DataInput in)
  throws IOException, ClassNotFoundException {
    boolean wouldReadSerialized = PdxInstanceImpl.getPdxReadSerialized();
    if (!wouldReadSerialized) {
      return DataSerializer.<T>readObject(in);
    } else {
      PdxInstanceImpl.setPdxReadSerialized(false);
      try {
        return DataSerializer.<T>readObject(in);
      } finally {
        PdxInstanceImpl.setPdxReadSerialized(true);
      }
    }
  }
  
  public static final Object basicReadObject(final DataInput in)
      throws IOException, ClassNotFoundException {

    checkIn(in);

    // Read the header byte
    byte header = in.readByte();
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "basicReadObject: header={}", header);
    }
    switch (header) {
    case DS_FIXED_ID_BYTE:
      return DSFIDFactory.create(in.readByte(), in);
    case DS_FIXED_ID_SHORT:
      return DSFIDFactory.create(in.readShort(), in);
    case DS_FIXED_ID_INT:
      return DSFIDFactory.create(in.readInt(), in);
    case DS_NO_FIXED_ID:
      return readDataSerializableFixedID(in);
    case SQLF_DVD_ARR:
      return dvddeserializer.fromData(in);
    case NULL:
      return null;
    case NULL_STRING:
    case STRING:
    case HUGE_STRING:
    case STRING_BYTES:
    case HUGE_STRING_BYTES:
      return readString(in, header);
    case CLASS:
      return readClass(in);
    case DATE:
      return readDate(in);
    case FILE:
      return readFile(in);
    case INET_ADDRESS:
      return readInetAddress(in);
    case BOOLEAN:
      return readBoolean(in);
    case CHARACTER:
      return readCharacter(in);
    case BYTE:
      return readByte(in);
    case SHORT:
      return readShort(in);
    case INTEGER:
      return readInteger(in);
    case LONG:
      return readLong(in);
    case FLOAT:
      return readFloat(in);
    case DOUBLE:
      return readDouble(in);
    case BYTE_ARRAY:
      return readByteArray(in);
    case ARRAY_OF_BYTE_ARRAYS:
      return readArrayOfByteArrays(in);  
    case SHORT_ARRAY:
      return readShortArray(in);
    case STRING_ARRAY:
      return readStringArray(in);
    case INT_ARRAY:
      return readIntArray(in);
    case LONG_ARRAY:
      return readLongArray(in);
    case FLOAT_ARRAY:
      return readFloatArray(in);
    case DOUBLE_ARRAY:
      return readDoubleArray(in);
    case BOOLEAN_ARRAY:
      return readBooleanArray(in);
    case CHAR_ARRAY:
      return readCharArray(in);
    case OBJECT_ARRAY:
      return readObjectArray(in);
    case ARRAY_LIST:
      return readArrayList(in);
    case LINKED_LIST:
      return readLinkedList(in);
    case HASH_SET:
      return readHashSet(in);
    case LINKED_HASH_SET:
      return readLinkedHashSet(in);
    case HASH_MAP:
      return readHashMap(in);
    case IDENTITY_HASH_MAP:
      return readIdentityHashMap(in);
    case HASH_TABLE:
      return readHashtable(in);
    case CONCURRENT_HASH_MAP:
      return readConcurrentHashMap(in);
    case PROPERTIES:
      return readProperties(in);
    case TIME_UNIT:
      return readTimeUnit(in);
    case USER_CLASS:
      return readUserObject(in, in.readByte());
    case USER_CLASS_2:
      return readUserObject(in, in.readShort());
    case USER_CLASS_4:
      return readUserObject(in, in.readInt());
    case VECTOR:
      return readVector(in);
    case STACK:
      return readStack(in);
    case TREE_MAP:
      return readTreeMap(in);
    case TREE_SET:
      return readTreeSet(in);
    case BOOLEAN_TYPE:
      return Boolean.TYPE;
    case CHARACTER_TYPE:
      return Character.TYPE;
    case BYTE_TYPE:
      return Byte.TYPE;
    case SHORT_TYPE:
      return Short.TYPE;
    case INTEGER_TYPE:
      return Integer.TYPE;
    case LONG_TYPE:
      return Long.TYPE;
    case FLOAT_TYPE:
      return Float.TYPE;
    case DOUBLE_TYPE:
      return Double.TYPE;
    case VOID_TYPE:
      return Void.TYPE;

    case USER_DATA_SERIALIZABLE:
      return readUserDataSerializable(in, in.readByte());
    case USER_DATA_SERIALIZABLE_2:
      return readUserDataSerializable(in, in.readShort());
    case USER_DATA_SERIALIZABLE_4:
      return readUserDataSerializable(in, in.readInt());

    case DATA_SERIALIZABLE:
      return readDataSerializable(in);

    case SERIALIZABLE: {
      final boolean isDebugEnabled_SERIALIZER = logger.isTraceEnabled(LogMarker.SERIALIZER);
      Object serializableResult = null;
      if (in instanceof DSObjectInputStream) {
        serializableResult = ((DSObjectInputStream)in).readObject();
      } else {
        InputStream stream;
        if (in instanceof InputStream) {
          stream = (InputStream)in;
        } else {
          stream = new InputStream() {
            @Override
              public int read() throws IOException {
                try {
                  return in.readUnsignedByte(); // fix for bug 47249
                } catch (EOFException enfOfStream) {
                  // InputStream.read() should return -1 on EOF
                  return -1;
                }
              }

              //               public int read(byte[] b, int off, int len)
              //                 throws IOException {
              //                 // @todo davidw Do read() and readFully() have the
              //                 // same semantics in this case?
              //                 in.readFully(b, off, len);
              //                 return len;
              //               }

              //               public long skip(long n) throws IOException {
              //                 // @todo davidw Is casting the right thing to do?
              //                 return in.skipBytes((int) n);
              //               }
            };
        }

        ObjectInput ois = new DSObjectInputStream(stream);
        if ( stream instanceof VersionedDataStream ) {
          Version v = ((VersionedDataStream)stream).getVersion();
          if (v != null && v != Version.CURRENT) { 
            ois = new VersionedObjectInput(ois, v);
          }
        }

        serializableResult = ois.readObject();
        
        if (isDebugEnabled_SERIALIZER) {
          logger.trace(LogMarker.SERIALIZER, "Read Serializable object: {}", serializableResult);
        }
      }
      if (isDebugEnabled_SERIALIZER) {
        logger.trace(LogMarker.SERIALIZER, "deserialized instanceof {}", serializableResult.getClass());
      }
      return serializableResult;
    }
      case PDX:
        return readPdxSerializable(in);
      case PDX_ENUM:
        return readPdxEnum(in);
      case GEMFIRE_ENUM:
        return readGemFireEnum(in);
      case PDX_INLINE_ENUM:
        return readPdxInlineEnum(in);
      case BIG_INTEGER:
        return readBigInteger(in);
      case BIG_DECIMAL:
        return readBigDecimal(in);
      case UUID:
        return readUUID(in);
      case TIMESTAMP:
        return readTimestamp(in);
    default:
      String s = "Unknown header byte: " + header;
      throw new IOException(s);
    }

  }

  private static final Object readUserDataSerializable(final DataInput in, int classId)
    throws IOException, ClassNotFoundException {
    Instantiator instantiator =
      InternalInstantiator.getInstantiator(classId);
    if (instantiator == null) {
      logger.error(LogMarker.SERIALIZER, LocalizedMessage.create(LocalizedStrings.DataSerializer_NO_INSTANTIATOR_HAS_BEEN_REGISTERED_FOR_CLASS_WITH_ID_0, classId));
      throw new IOException(LocalizedStrings.DataSerializer_NO_INSTANTIATOR_HAS_BEEN_REGISTERED_FOR_CLASS_WITH_ID_0.toLocalizedString(classId));

    } else {
      try {
        DataSerializable ds;
        if (instantiator instanceof CanonicalInstantiator) {
          CanonicalInstantiator ci = (CanonicalInstantiator)instantiator;
          ds = ci.newInstance(in);
        } else {
          ds = instantiator.newInstance();
        }
        ds.fromData(in);
        return ds;

      } catch (Exception ex) {
        SerializationException ex2 = new SerializationException(LocalizedStrings.DataSerializer_COULD_NOT_DESERIALIZE_AN_INSTANCE_OF_0.toLocalizedString(instantiator.getInstantiatedClass().getName()), ex);
        throw ex2;
      }
    }
  }

  private static final ThreadLocal<Boolean> pdxSerializationInProgress = new ThreadLocal<Boolean>();
  public static boolean isPdxSerializationInProgress() {
    Boolean v = pdxSerializationInProgress.get();
    return v != null && v;
  }
  public static void setPdxSerializationInProgress(boolean v) {
    if (v) {
      pdxSerializationInProgress.set(true);
    } else {
      pdxSerializationInProgress.set(false);
    }
  }
  
  public final static boolean writePdx(DataOutput out, GemFireCacheImpl gfc,
      Object pdx, PdxSerializer pdxSerializer) throws IOException {
    TypeRegistry tr = null;
    if (gfc != null) {
      tr = gfc.getPdxRegistry();
    }

    PdxWriterImpl writer;
    {
      PdxOutputStream os;
      if (out instanceof HeapDataOutputStream) {
        os = new PdxOutputStream((HeapDataOutputStream) out);
      } else {
        os = new PdxOutputStream();
      }
      writer = new PdxWriterImpl(tr, pdx, os);
    }
    try {
      if (pdxSerializer != null) {
        //Hack to make sure we don't pass internal objects to the user's 
        //serializer
        if(isGemfireObject(pdx)) {
          return false;
        }
        if (is662SerializationEnabled()) {
          boolean alreadyInProgress = isPdxSerializationInProgress();
          if (!alreadyInProgress) {
            setPdxSerializationInProgress(true);
            try {
              if (!pdxSerializer.toData(pdx, writer)) {
                return false;
              }
            } finally {
              setPdxSerializationInProgress(false);
            }
          } else {
            if (!pdxSerializer.toData(pdx, writer)) {
              return false;
            }
          }
        } else {
          if (!pdxSerializer.toData(pdx, writer)) {
            return false;
          }
        }
      } else {
        if (is662SerializationEnabled()) {
          boolean alreadyInProgress = isPdxSerializationInProgress();
          if (!alreadyInProgress) {
            setPdxSerializationInProgress(true);
            try {
              ((PdxSerializable) pdx).toData(writer);
            } finally {
              setPdxSerializationInProgress(false);
            }
          } else {
            ((PdxSerializable) pdx).toData(writer);
          }
        } else {
          ((PdxSerializable) pdx).toData(writer);
        }
      }
    } catch (ToDataException ex) {
      throw ex;
    } catch (CancelException ex) {
      //Serializing a PDX can result in a cache closed exception. Just rethrow
      throw ex;
    } catch (NonPortableClassException ex) {
      throw ex;
    } catch (GemFireRethrowable ex) {
      throw ex;
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Throwable t) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      if (pdxSerializer != null) {
        throw new ToDataException("PdxSerializer failed when calling toData on " + pdx.getClass(),  t);
      } else {
        throw new ToDataException("toData failed on PdxSerializable " + pdx.getClass(),  t);
      }
    }
    int bytesWritten = writer.completeByteStreamGeneration();
    getDMStats(gfc).incPdxSerialization(bytesWritten);
    if (!(out instanceof HeapDataOutputStream)) {
      writer.sendTo(out);
    }
    return true;
  }

  public static DMStats getDMStats(GemFireCacheImpl gfc) {
    if (gfc != null) {
      return gfc.getDistributionManager().getStats();
    } else {
      DMStats result = InternalDistributedSystem.getDMStats();
      if (result == null) {
        result = new LonerDistributionManager.DummyDMStats();
      }
      return result;
    }
  }

  private static final Object readPdxSerializable(final DataInput in)
      throws IOException, ClassNotFoundException {

    int len = in.readInt();
    int typeId = in.readInt();
    
    GemFireCacheImpl gfc = GemFireCacheImpl.getForPdx("PDX registry is unavailable because the Cache has been closed.");
    PdxType pdxType = gfc.getPdxRegistry().getType(typeId);
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "readPdxSerializable pdxType={}", pdxType);
    }
    if (pdxType == null) {
      throw new IllegalStateException("Unknown pdx type=" + typeId);
    }
    
    DMStats dmStats = getDMStats(gfc);
    dmStats.incPdxDeserialization(len+9);
    
    // check if PdxInstance needs to be returned.
    if (pdxType.getNoDomainClass() || gfc.getPdxReadSerializedByAnyGemFireServices()) {
//      if (logger.isDebugEnabled()) {
//        gfc.getLogger().info("returning PdxInstance", new Exception("stack trace"));
//      }
      dmStats.incPdxInstanceCreations();
      return new PdxInstanceImpl(pdxType, in, len);
    } else {
//      if (logger.isDebugEnabled()) {
//        gfc.getLogger().info("returning domain object", new Exception("stack trace"));
//      }
      // return domain object.
      PdxReaderImpl pdxReader = new PdxReaderImpl(pdxType, in, len);
      return pdxReader.getObject();
    } 
  }
  /**
   * Reads a PdxInstance from dataBytes and returns it. If the first object
   * read is not pdx encoded returns null.
   */
  public static final PdxInstance readPdxInstance(final byte[] dataBytes, GemFireCacheImpl gfc) {
    try {
      byte type = dataBytes[0];
      if (type == PDX) {
        PdxInputStream in = new PdxInputStream(dataBytes);
        in.readByte(); // throw away the type byte
        int len = in.readInt();
        int typeId = in.readInt();
        PdxType pdxType = gfc.getPdxRegistry().getType(typeId);
        //gfc.getLogger().info("logger.isDebugEnabled(): pdxType="+ pdxType);
        if (pdxType == null) {
          throw new IllegalStateException("Unknown pdx type=" + typeId);
        }

        return new PdxInstanceImpl(pdxType, in, len);
      } else if (type == DSCODE.PDX_ENUM) {
        PdxInputStream in = new PdxInputStream(dataBytes);
        in.readByte(); // throw away the type byte
        int dsId = in.readByte();
        int tmp = readArrayLength(in);
        int enumId = (dsId << 24) | (tmp & 0xFFFFFF);
        TypeRegistry tr = gfc.getPdxRegistry();
        EnumInfo ei = tr.getEnumInfoById(enumId);
        if (ei == null) {
          throw new IllegalStateException("Unknown pdx enum id=" + enumId);
        }
        return ei.getPdxInstance(enumId);
      } else if (type == DSCODE.PDX_INLINE_ENUM) {
        PdxInputStream in = new PdxInputStream(dataBytes);
        in.readByte(); // throw away the type byte
        String className = DataSerializer.readString(in);
        String enumName = DataSerializer.readString(in);
        int enumOrdinal = InternalDataSerializer.readArrayLength(in);
        return new PdxInstanceEnum(className, enumName, enumOrdinal);
      }
    } catch (IOException ignore) {
    }
    return null;
  }

  /////////////////////////////   START Test only methods /////////////////////////////
  public static int getLoadedDataSerializers() {
    return idsToSerializers.size();
  }

  public final static Map getDsClassesToHoldersMap() {
    return dsClassesToHolders;
  }

  public final static Map getIdsToHoldersMap() {
    return idsToHolders;
  }

  public final static Map getSupportedClassesToHoldersMap() {
    return supportedClassesToHolders;
  }
  /////////////////////////////   END Test only methods /////////////////////////////


  ///////////////// END DataSerializer Implementation Methods ///////////

  ///////////////////////  Inner Classes  ///////////////////////

  /**
   * A marker object for <Code>DataSerializer</code>s that have not
   * been registered.  Using this marker object allows us to
   * asynchronously send <Code>DataSerializer</code> registration
   * updates.  If the serialized bytes arrive at a VM before the
   * registration message does, the deserializer will wait an amount
   * of time for the registration message to arrive.
   */
  static abstract class Marker {
    /** The DataSerializer that is filled in upon registration */
    protected DataSerializer serializer = null;
    /** set to true once setSerializer is called. */
    protected boolean hasBeenSet = false;
    
    abstract DataSerializer getSerializer();

    /**
     * Sets the serializer associated with this marker.  It will
     * notify any threads that are waiting for the serializer to be
     * registered.
     */
    void setSerializer(DataSerializer serializer) {
      synchronized (this) {
        this.hasBeenSet = true;
        this.serializer = serializer;
        this.notifyAll();
      }
    }
  }
  /**
   * A marker object for <Code>DataSerializer</code>s that have not
   * been registered.  Using this marker object allows us to
   * asynchronously send <Code>DataSerializer</code> registration
   * updates.  If the serialized bytes arrive at a VM before the
   * registration message does, the deserializer will wait an amount
   * of time for the registration message to arrive.
   * Made public for unit test access.
   * @since 5.7
   */
  public static class GetMarker extends Marker {
    /**
     * Number of milliseconds to wait. Also used by InternalInstantiator.
     * Note that some tests set this to a small amount to speed up failures.
     * Made public for unit test access.
     */
    public static int WAIT_MS = Integer.getInteger("gemfire.InternalDataSerializer.WAIT_MS", 60 * 1000);

    /**
     * Returns the serializer associated with this marker.  If the
     * serializer has not been registered yet, then this method will
     * wait until the serializer is registered.  If this method has to
     * wait for too long, then <code>null</code> is returned.
     */
    @Override
    DataSerializer getSerializer() {
      boolean firstTime = true;
      long endTime = 0;
      synchronized (this) {
        while (!this.hasBeenSet) {
          if (firstTime) {
            firstTime = false;
            endTime = System.currentTimeMillis() + WAIT_MS;
          }
          try {
            long remainingMs = endTime - System.currentTimeMillis();
            if (remainingMs > 0) {
              this.wait(remainingMs); // spurious wakeup ok
//               if (!this.hasBeenSet) {
//                 logger.info("logger.isDebugEnabled() getSerializer had to wait for " + remainingMs + "ms",
//                             new Exception("STACK"));
//               }
            } else {
              // timed out call setSerializer just to make sure that anyone else
              // also waiting on this marker times out also
              setSerializer(null);
              break;
            }
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            // Just return null, let it fail
            return null;
          }
        }
        return this.serializer;
      }
    }
  }

  /**
   * A marker object for <Code>DataSerializer</code>s that is in the process
   * of being registered.
   * It is possible for getSerializer to return <code>null</code>
   * 
   * @since 5.7
   */
  static class InitMarker extends Marker {
    /**
     * Returns the serializer associated with this marker.  If the
     * serializer has not been registered yet, then this method will
     * wait until the serializer is registered.  If this method has to
     * wait for too long, then <code>null</code> is returned.
     */
    /**
     * Returns the serializer associated with this marker.
     * Waits forever (unless interrupted) for it to be initialized.
     * Returns null if this Marker failed to initialize.
     */
    @Override
    DataSerializer getSerializer() {
      synchronized (this) {
        while (!this.hasBeenSet) {
          try {
            this.wait(); // spurious wakeup ok
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            // Just return null, let it fail
            return null;
          }
        }
        return this.serializer;
      }
    }
  }
  /**
   * A distribution message that alerts other members of the
   * distributed cache of a new <code>DataSerializer</code> being
   * registered.
   */
  public static final class RegistrationMessage extends SerialDistributionMessage {
    /** The id of the <code>DataSerializer</code> that was
     * registered
     * since 5.7 an int instead of a byte
     */
    private int id;

    /** The eventId of the <codE>DataSerializer</code> that was
     * registered */
    protected EventID eventId;
    
    /** The name of the <code>DataSerializer</code> class */
    private String className;
    /** The versions in which this message was modified */
    private static final Version[] dsfidVersions = new Version[]{};

    /**
     * Constructor for <code>DataSerializable</code>
     */
    public RegistrationMessage() {

    }

    /**
     * Creates a new <code>RegistrationMessage</code> that broadcasts
     * that the given <code>DataSerializer</code> was registered.
     */
    public RegistrationMessage(DataSerializer s) {
      this.className = s.getClass().getName();
      this.id = s.getId();
      this.eventId = (EventID)s.getEventId();
    }

    public static String getFullMessage(Throwable t) {
      StringBuffer sb = new StringBuffer();
      getFullMessage(sb, t);
      return sb.toString();
    }
    
    private static void getFullMessage(StringBuffer sb, Throwable t) {
      if (t.getMessage() != null) {
        sb.append(t.getMessage());
      } else {
        sb.append(t.getClass());
      }
      if (t.getCause() != null) {
        sb.append(" caused by: ");
        getFullMessage(sb, t.getCause());
      }
    }
    
    
    
    @Override
    protected void process(DistributionManager dm) {
      if (CacheClientNotifier.getInstance() != null) {
        // This is a server so we need to send the dataserializer to clients
        // right away. For that we need to load the class as the constructor of
        // ClientDataSerializerMessage requires list of supported classes.
        Class<?> c = null;
        try {
          c = getCachedClass(this.className); // fix for bug 41206
        } catch (ClassNotFoundException ex) {
          // fixes bug 44112
          logger.warn("Could not load data serializer class {} so both clients of this server and this server will not have this data serializer. Load failed because: {}",
              this.className, getFullMessage(ex));
          return;
        }
        DataSerializer s = null;
        try {
          s = newInstance(c);
        } catch (IllegalArgumentException ex) {
          // fixes bug 44112
          logger.warn("Could not create an instance of data serializer for class {} so both clients of this server and this server will not have this data serializer. Create failed because: {}",
              this.className, getFullMessage(ex));
          return;
        }
        s.setEventId(this.eventId);
        try {
          InternalDataSerializer._register(s, false);
        } catch (IllegalArgumentException ex) {
          logger.warn("Could not register data serializer for class {} so both clients of this server and this server will not have this data serializer. Registration failed because: {}",
              this.className, getFullMessage(ex));
          return;
        } catch (IllegalStateException ex) {
          logger.warn("Could not register data serializer for class {} so both clients of this server and this server will not have this data serializer. Registration failed because: {}",
              this.className, getFullMessage(ex));
          return;
        }
      } else {
        try {
          InternalDataSerializer.register(this.className, false, this.eventId, null, this.id);
        } catch (IllegalArgumentException ex) {
          logger.warn("Could not register data serializer for class {} so it will not be available in this JVM. Registration failed because: {}",
              this.className, getFullMessage(ex));
          return;
        } catch (IllegalStateException ex) {
          logger.warn("Could not register data serializer for class {} so it will not be available in this JVM. Registration failed because: {}",
              this.className, getFullMessage(ex));
          return;
        }
     }
    }

    public int getDSFID() {
      return IDS_REGISTRATION_MESSAGE;
    }
    
    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      DataSerializer.writeNonPrimitiveClassName(this.className, out);
      out.writeInt(this.id);
      DataSerializer.writeObject(this.eventId, out);
    }

    @Override
    public void fromData(DataInput in)
      throws IOException, ClassNotFoundException {

      super.fromData(in);
      InternalDataSerializer.checkIn(in);
      this.className = DataSerializer.readNonPrimitiveClassName(in);
      this.id = in.readInt();
      this.eventId = (EventID)DataSerializer.readObject(in);
    }

    @Override
    public String toString() {
      return LocalizedStrings.InternalDataSerializer_REGISTER_DATASERIALIZER_0_OF_CLASS_1
      .toLocalizedString(new Object[]{Integer.valueOf(this.id), this.className});
    }

    @Override
    public Version[] getSerializationVersions() {
      return dsfidVersions;
    }

  }

  /**
   * A listener whose listener methods are invoked when {@link
   * DataSerializer}s and {@link Instantiator}s are registered.  This
   * is part of the fix for bug 31422.
   *
   * @see InternalDataSerializer#addRegistrationListener
   * @see InternalDataSerializer#removeRegistrationListener
   */
  public interface RegistrationListener {

    /**
     * Invoked when a new <code>Instantiator</code> is {@linkplain
     * Instantiator#register(Instantiator) registered}.
     */
    public void newInstantiator(Instantiator instantiator);

    /**
     * Invoked when a new <code>DataSerializer</code> is {@linkplain
     * DataSerializer#register(Class) registered}.
     */
    public void newDataSerializer(DataSerializer ds);

  }

  /**
   * An <code>ObjectInputStream</code> whose {@link #resolveClass}
   * method loads classes from the current context class loader.
   */
  private static class DSObjectInputStream extends ObjectInputStream {

    /**
     * Creates a new <code>DSObjectInputStream</code> that delegates
     * its behavior to a given <code>InputStream</code>.
     */
    public DSObjectInputStream(InputStream stream) throws IOException {
      super(stream);
    }

    @Override
    protected Class resolveClass(ObjectStreamClass desc)
      throws IOException, ClassNotFoundException {

      String className = desc.getName();
     try {
        return getCachedClass(className);

      } catch (ClassNotFoundException ex) {
        return super.resolveClass(desc);
      }
    }

    @Override
    protected Class resolveProxyClass(String[] interfaces)
      throws IOException, ClassNotFoundException {

      ClassLoader nonPublicLoader = null;
      boolean hasNonPublicInterface = false;

      // define proxy in class loader of non-public
      // interface(s), if any
      Class[] classObjs = new Class[interfaces.length];
      for (int i = 0; i < interfaces.length; i++) {
        Class cl =
          getCachedClass(interfaces[i]);
        if ((cl.getModifiers() & Modifier.PUBLIC) == 0) {
          if (hasNonPublicInterface) {
            if (nonPublicLoader != cl.getClassLoader()) {
              String s =
                "conflicting non-public interface class loaders";
              throw new IllegalAccessError(s);
            }

          } else {
            nonPublicLoader = cl.getClassLoader();
            hasNonPublicInterface = true;
          }
        }
        classObjs[i] = cl;
      }

      try {
        if (hasNonPublicInterface) {
          return Proxy.getProxyClass(nonPublicLoader, classObjs);
        } else {
          return ClassPathLoader.getLatest().getProxyClass(classObjs);
        }
      } catch (IllegalArgumentException e) {
        throw new ClassNotFoundException(null, e);
      }
    }
  }

//   /**
//    * A <code>DataOutput</code> that writes special header information
//    * before it writes any other data.  It is passed to a
//    * <code>DataSerializer</code>'s {@link
//    * DataSerializer#toData(Object, DataOutput)} method to ensure
//    * that the stream has the correct format.
//    */
//   private static class DSDataOutput implements DataOutput {
//     /** Has the header information been written? */
//     private boolean headerWritten = false;

//     /** The id of serializer that is writing to this output */
//     private byte serializerId;

//     /** The output stream to which this DSDataOutput writes */
//     protected DataOutput out;

//     //////////////////////  Constructors  //////////////////////

//     /**
//      * Creates a new <code>DSDataOutput</code> that write to the
//      * given output stream.
//      */
//     DSDataOutput(DataOutput out) {
//       this.out = out;
//     }

//     /////////////////////  Instance Methods  ////////////////////

//     /**
//      * Sets the id of the serializer that will possibly write to
//      * this stream.
//      */
//     void setSerializerId(byte id) {
//       this.serializerId = id;
//     }

//     /**
//      * Returns whether or not any data hass been written to this
//      * stream.
//      */
//     boolean hasWritten() {
//       return this.headerWritten;
//     }

//     /**
//      * Write the {@link #USER_CLASS} "class id" followed by the id
//      * of the serializer.
//      */
//     private void writeHeader() throws IOException {
//       if (!headerWritten) {
//         out.writeByte(USER_CLASS);
//         out.writeByte(serializerId);
//         this.headerWritten = true;
//       }
//     }

//     public void write(int b) throws IOException {
//       writeHeader();
//       out.write(b);
//     }

//     public void write(byte[] b) throws IOException {
//       writeHeader();
//       out.write(b);
//     }

//     public void write(byte[] b, int off, int len)
//       throws IOException {
//       writeHeader();
//       out.write(b, off, len);
//     }

//     public void writeBoolean(boolean v) throws IOException {
//       writeHeader();
//       out.writeBoolean(v);
//     }

//     public void writeByte(int v) throws IOException {
//       writeHeader();
//       out.writeByte(v);
//     }

//     public void writeShort(int v) throws IOException {
//       writeHeader();
//       out.writeShort(v);
//     }

//     public void writeChar(int v) throws IOException {
//       writeHeader();
//       out.writeChar(v);
//     }

//     public void writeInt(int v) throws IOException {
//       writeHeader();
//       out.writeInt(v);
//     }

//     public void writeLong(long v) throws IOException {
//       writeHeader();
//       out.writeLong(v);
//     }

//     public void writeFloat(float v) throws IOException {
//       writeHeader();
//       out.writeFloat(v);
//     }

//     public void writeDouble(double v) throws IOException {
//       writeHeader();
//       out.writeDouble(v);
//     }

//     public void writeBytes(String s) throws IOException {
//       writeHeader();
//       out.writeBytes(s);
//     }

//     public void writeChars(String s) throws IOException {
//       writeHeader();
//       out.writeChars(s);
//     }

//     public void writeUTF(String str) throws IOException {
//       writeHeader();
//       out.writeUTF(str);
//     }

//   }
  /**
   * Used to implement serialization code for the well known classes we support
   * in DataSerializer.
   * @since 5.7
   */
  protected static abstract class WellKnownDS extends DataSerializer {
    @Override
    public final int getId() {
      // illegal for a customer to use but since our WellKnownDS is never registered
      // with this id it gives us one to use
      return 0;
    }
    @Override
    public final Class[] getSupportedClasses() {
      // illegal for a customer to return null but we can do it since we never register
      // this serializer.
      return null;
    }
    @Override
    public final Object fromData(DataInput in) throws IOException, ClassNotFoundException {
      throw new IllegalStateException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
    }
    // subclasses need to implement toData
  }
  /**
   * Just like a WellKnownDS but its type is compatible with PDX.
   * @author darrel
   *
   */
  protected static abstract class WellKnownPdxDS extends WellKnownDS {
    // subclasses need to implement toData
  }
  
  public static void writeObjectArray(Object[] array, DataOutput out, boolean ensureCompatibility)
    throws IOException {
    InternalDataSerializer.checkOut(out);
    int length;
    if (array == null) {
      length = -1;
    } else {
      length = array.length;
    }
    InternalDataSerializer.writeArrayLength(length, out);
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing Object array of length {}", length);
    }
    if (length >= 0) {
      writeClass(array.getClass().getComponentType(), out);
      for (int i = 0; i < length; i++) {
        basicWriteObject(array[i], out, ensureCompatibility);
      }
    }
  }
  
  private static final byte INT_VL = 126; // Variable Length long encoded as int
  // in next 4 bytes
  private static final byte LONG_VL = 127; // Variable Length long encoded as
  // long in next 8 bytes
  private static final int MAX_BYTE_VL = 125;

  /**
   * Write a variable length long the old way (pre 7.0). Use this
   * only in contexts where you might need to communicate with pre 7.0
   * members or files.
   */
  public static void writeVLOld(long data, DataOutput out) throws IOException {
    if(data < 0) {
      Assert.fail("Data expected to be >=0 is " + data);
    }
    if (data <= MAX_BYTE_VL) {
      out.writeByte((byte) data);
    } else if (data <= 0x7FFF) {
      // set the sign bit to indicate a short
      out.write((((int) data >>> 8) | 0x80) & 0xFF);
      out.write(((int) data >>> 0) & 0xFF);
    } else if (data <= Integer.MAX_VALUE) {
      out.writeByte(INT_VL);
      out.writeInt((int) data);
    } else {
      out.writeByte(LONG_VL);
      out.writeLong(data);
    }
  }

  /**
   * Write a variable length long the old way (pre 7.0). Use this
   * only in contexts where you might need to communicate with pre 7.0
   * members or files.
   */
  public static long readVLOld(DataInput in) throws IOException {
    byte code = in.readByte();
    long result;
    if (code < 0) {
      // mask off sign bit
      result = code & 0x7F;
      result <<= 8;
      result |= in.readByte() & 0xFF;
    } else if (code <= MAX_BYTE_VL) {
      result = code;
    } else if (code == INT_VL) {
      result = in.readInt();
    } else if (code == LONG_VL) {
      result = in.readLong();
    } else {
      throw new IllegalStateException("unexpected variable length code=" + code);
    }
    return result;
  }
  
  /**
   * Encode a long as a variable length array. 
   * 
   * This method is appropriate for unsigned integers. For signed integers,
   * negative values will always consume 10 bytes, so it is recommended to
   * use writeSignedVL instead.
   * 
   * This is taken from the varint encoding in protobufs (BSD licensed).
   * See https://developers.google.com/protocol-buffers/docs/encoding
   */
  public static void writeUnsignedVL(long data, DataOutput out) throws IOException {
    while (true) {
      if ((data & ~0x7FL) == 0) {
        out.writeByte((int)data);
        return;
      } else {
        out.writeByte(((int)data & 0x7F) | 0x80);
        data >>>= 7;
      }
    }
  }
  
  /**
   * Decode a long as a variable length array. 
   * 
   * This is taken from the varint encoding in protobufs (BSD licensed).
   * See https://developers.google.com/protocol-buffers/docs/encoding
   */
  public static long readUnsignedVL(DataInput in) throws IOException {
    int shift = 0;
    long result = 0;
    while (shift < 64) {
      final byte b = in.readByte();
      result |= (long)(b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        return result;
      }
      shift += 7;
    }
    throw new GemFireIOException("Malformed variable length integer");
  }
  
  /**
   * Encode a signed long as a variable length array. 
   * 
   * This method is appropriate for signed integers. It uses
   * zig zag encoding to so that negative numbers will be respresented more
   * compactly. For unsigned values, writeUnsignedVL will be more efficient. 
   * 
   */
  public static void writeSignedVL(long data, DataOutput out) throws IOException {
    writeUnsignedVL(encodeZigZag64(data), out);
  }
  
  /**
   * Decode a signed long as a variable length array. 
   * 
   * This method is appropriate for signed integers. It uses
   * zig zag encoding to so that negative numbers will be respresented more
   * compactly. For unsigned values, writeUnsignedVL will be more efficient. 
   * 
   */
  public static long readSignedVL(DataInput in) throws IOException {
    return decodeZigZag64(readUnsignedVL(in));
  }

  /**
   * Decode a ZigZag-encoded 64-bit value.  ZigZag encodes signed integers
   * into values that can be efficiently encoded with varint.  (Otherwise,
   * negative values must be sign-extended to 64 bits to be varint encoded,
   * thus always taking 10 bytes on the wire.)
   *
   * @param n An unsigned 64-bit integer, stored in a signed int because
   *          Java has no explicit unsigned support.
   * @return A signed 64-bit integer.
   * 
   * This is taken from the varint encoding in protobufs (BSD licensed).
   * See https://developers.google.com/protocol-buffers/docs/encoding
   */
  public static long decodeZigZag64(final long n) {
    return (n >>> 1) ^ -(n & 1);
  }

  /**
   * Encode a ZigZag-encoded 64-bit value.  ZigZag encodes signed integers
   * into values that can be efficiently encoded with varint.  (Otherwise,
   * negative values must be sign-extended to 64 bits to be varint encoded,
   * thus always taking 10 bytes on the wire.)
   *
   * @param n A signed 64-bit integer.
   * @return An unsigned 64-bit integer, stored in a signed int because
   *         Java has no explicit unsigned support.
   *         
   * This is taken from the varint encoding in protobufs (BSD licensed).
   * See https://developers.google.com/protocol-buffers/docs/encoding
   */
  public static long encodeZigZag64(final long n) {
    // Note:  the right-shift must be arithmetic
    return (n << 1) ^ (n >> 63);
  }

  /* test only method */
  public static int calculateBytesForTSandDSID(int dsid) {
    HeapDataOutputStream out = new HeapDataOutputStream(4 + 8, Version.CURRENT);
    long now = System.currentTimeMillis();
    try {
      writeUnsignedVL(now, out);
      writeUnsignedVL(InternalDataSerializer.encodeZigZag64(dsid), out);
    } catch (IOException e) {
      return 0;
    }
    return out.size();
  }
  
  public static final boolean LOAD_CLASS_EACH_TIME = Boolean.getBoolean("gemfire.loadClassOnEveryDeserialization");
  private static final CopyOnWriteHashMap<String, WeakReference<Class<?>>> classCache = LOAD_CLASS_EACH_TIME ? null : new CopyOnWriteHashMap<String, WeakReference<Class<?>>>();
  private static final Object cacheAccessLock = new Object();
  
  public static Class<?> getCachedClass(String className) throws ClassNotFoundException {
    if (LOAD_CLASS_EACH_TIME) {
      return ClassPathLoader.getLatest().forName(className);
    } else {
      Class<?> result = getExistingCachedClass(className);
      if (result == null) {
        // Do the forName call outside the sync to fix bug 46172
        result = ClassPathLoader.getLatest().forName(className);
        synchronized (cacheAccessLock) {
          Class<?> cachedClass = getExistingCachedClass(className);
          if (cachedClass == null) {
            classCache.put(className, new WeakReference<Class<?>>(result));
          } else {
            result = cachedClass;
          }
        }
      }
      return result;
    }
  }
  private static Class<?> getExistingCachedClass(String className) {
    WeakReference<Class<?>> wr = classCache.get(className);
    Class<?> result = null;
    if (wr != null) {
      result = wr.get();
    }
    return result;
  }

  public static void flushClassCache() {
    if (classCache != null) {
      // Not locking classCache during clear as doing so causes a deadlock in the JarClassLoader
      classCache.clear();
    }
  }
}
