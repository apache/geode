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
package org.apache.geode.internal;

import static org.apache.geode.internal.serialization.filter.SanctionedSerializables.loadSanctionedClassNames;
import static org.apache.geode.internal.serialization.filter.SanctionedSerializables.loadSanctionedSerializablesServices;

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
import java.net.SocketException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
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
import java.util.Objects;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.CanonicalInstantiator;
import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.GemFireConfigException;
import org.apache.geode.GemFireIOException;
import org.apache.geode.GemFireRethrowable;
import org.apache.geode.Instantiator;
import org.apache.geode.SerializationException;
import org.apache.geode.SystemFailure;
import org.apache.geode.ToDataException;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.annotations.internal.MutableForTesting;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.LonerDistributionManager;
import org.apache.geode.distributed.internal.SerialDistributionMessage;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PoolManagerImpl;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheServerHelper;
import org.apache.geode.internal.cache.tier.sockets.ClientDataSerializerMessage;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.OldClientSupportService;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.classloader.ClassPathLoader;
import org.apache.geode.internal.lang.ClassUtils;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.serialization.BasicSerializable;
import org.apache.geode.internal.serialization.DSCODE;
import org.apache.geode.internal.serialization.DSFIDSerializer;
import org.apache.geode.internal.serialization.DSFIDSerializerFactory;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DataSerializableFixedIdRegistrant;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.DscodeHelper;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.ObjectDeserializer;
import org.apache.geode.internal.serialization.ObjectSerializer;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.SerializationVersions;
import org.apache.geode.internal.serialization.StaticSerialization;
import org.apache.geode.internal.serialization.VersionedDataStream;
import org.apache.geode.internal.serialization.filter.DelegatingObjectInputFilterFactory;
import org.apache.geode.internal.serialization.filter.EmptyObjectInputFilter;
import org.apache.geode.internal.serialization.filter.ObjectInputFilter;
import org.apache.geode.internal.serialization.filter.SanctionedSerializablesFilterPattern;
import org.apache.geode.internal.serialization.filter.SanctionedSerializablesService;
import org.apache.geode.internal.util.concurrent.CopyOnWriteHashMap;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.pdx.NonPortableClassException;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxSerializer;
import org.apache.geode.pdx.internal.AutoSerializableManager;
import org.apache.geode.pdx.internal.AutoSerializableManager.AutoClassInfo;
import org.apache.geode.pdx.internal.EnumInfo;
import org.apache.geode.pdx.internal.PdxInputStream;
import org.apache.geode.pdx.internal.PdxInstanceEnum;
import org.apache.geode.pdx.internal.PdxInstanceImpl;
import org.apache.geode.pdx.internal.PdxOutputStream;
import org.apache.geode.pdx.internal.PdxReaderImpl;
import org.apache.geode.pdx.internal.PdxType;
import org.apache.geode.pdx.internal.PdxWriterImpl;
import org.apache.geode.pdx.internal.TypeRegistry;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * Contains static methods for data serializing instances of internal GemFire classes. It also
 * contains the implementation of the distribution messaging (and shared memory management) needed
 * to support data serialization.
 *
 * @since GemFire 3.5
 */
public abstract class InternalDataSerializer extends DataSerializer {
  public static final boolean LOAD_CLASS_EACH_TIME =
      Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "loadClassOnEveryDeserialization");
  private static final Logger logger = LogService.getLogger();
  /**
   * Maps Class names to their DataSerializer. This is used to find a DataSerializer during
   * serialization.
   */
  @MakeNotStatic
  private static final Map<String, DataSerializer> classesToSerializers = new ConcurrentHashMap<>();
  private static final String serializationVersionTxt =
      System.getProperty(GeodeGlossary.GEMFIRE_PREFIX + "serializationVersion");
  /**
   * Change this constant to be the last one in SERIALIZATION_VERSION
   */
  @Immutable
  private static final SERIALIZATION_VERSION latestVersion = SERIALIZATION_VERSION.v662;
  @Immutable
  private static final SERIALIZATION_VERSION serializationVersion = calculateSerializationVersion();
  /**
   * Maps the id of a serializer to its {@code DataSerializer}.
   */
  @MakeNotStatic
  private static final ConcurrentMap<Integer, Object /* DataSerializer|Marker */> idsToSerializers =
      new ConcurrentHashMap<>();
  /**
   * Contains the classnames of the data serializers (and not the supported classes) not yet loaded
   * into the vm as keys and their corresponding holder instances as values.
   */
  @MakeNotStatic
  private static final ConcurrentHashMap<String, SerializerAttributesHolder> dsClassesToHolders =
      new ConcurrentHashMap<>();
  /**
   * Contains the id of the data serializers not yet loaded into the vm as keys and their
   * corresponding holder instances as values.
   */
  @MakeNotStatic
  private static final ConcurrentHashMap<Integer, SerializerAttributesHolder> idsToHolders =
      new ConcurrentHashMap<>();
  /**
   * Contains the classnames of supported classes as keys and their corresponding
   * SerializerAttributesHolder instances as values. This applies only to the data serializers which
   * have not been loaded into the vm.
   */
  @MakeNotStatic
  private static final ConcurrentHashMap<String, SerializerAttributesHolder> supportedClassesToHolders =
      new ConcurrentHashMap<>();
  private static final Object listenersSync = new Object();

  private static final ThreadLocal<Boolean> pdxSerializationInProgress = new ThreadLocal<>();
  @MakeNotStatic
  private static final CopyOnWriteHashMap<String, WeakReference<Class<?>>> classCache =
      LOAD_CLASS_EACH_TIME ? null : new CopyOnWriteHashMap<>();
  private static final Object cacheAccessLock = new Object();

  private static final String POST_GEODE_190_SERVER_CQIMPL =
      "org.apache.geode.cache.query.cq.internal.ServerCQImpl";
  private static final String PRE_GEODE_190_SERVER_CQIMPL =
      "org.apache.geode.cache.query.internal.cq.ServerCQImpl";

  @Immutable
  private static final ObjectInputFilter defaultSerializationFilter = new EmptyObjectInputFilter();
  /**
   * A deserialization filter for ObjectInputStreams
   */
  @MakeNotStatic
  private static ObjectInputFilter serializationFilter = defaultSerializationFilter;
  /**
   * support for old GemFire clients and WAN sites - needed to enable moving from GemFire to Geode
   */
  @MakeNotStatic
  private static OldClientSupportService oldClientSupportService;

  @MakeNotStatic
  private static final DSFIDSerializer dsfidSerializer;

  @MakeNotStatic
  private static final DSFIDFactory dsfidFactory;

  /**
   * {@code RegistrationListener}s that receive callbacks when {@code DataSerializer}s and {@code
   * Instantiator}s are registered. Note: copy-on-write access used for this set
   */
  @MakeNotStatic
  private static volatile Set<RegistrationListener> listeners = new HashSet<>();

  static {
    dsfidSerializer = new DSFIDSerializerFactory().setObjectSerializer(new ObjectSerializer() {
      @Override
      public void writeObject(Object obj, DataOutput output) throws IOException {
        InternalDataSerializer.writeObject(obj, output);
      }

      @Override
      public void invokeToData(Object ds, DataOutput out) throws IOException {
        InternalDataSerializer.invokeToData(ds, out);
      }

    }).setObjectDeserializer(new ObjectDeserializer() {
      @Override
      public <T> T readObject(DataInput input) throws IOException, ClassNotFoundException {
        return InternalDataSerializer.readObject(input);
      }

      @Override
      public void invokeFromData(Object ds, DataInput in)
          throws IOException, ClassNotFoundException {
        InternalDataSerializer.invokeFromData(ds, in);
      }
    }).create();
    initializeWellKnownSerializers();
    dsfidFactory = new DSFIDFactory(dsfidSerializer);

    ServiceLoader<DataSerializableFixedIdRegistrant> loaders = ServiceLoader.load(
        DataSerializableFixedIdRegistrant.class);
    for (DataSerializableFixedIdRegistrant loader : loaders) {
      try {
        loader.register(dsfidSerializer);
      } catch (Exception ex) {
        logger.warn("Data serializable fixed ID loader '{}' failed",
            loader.getClass().getName(), ex);
      }
    }
  }

  /**
   * For backward compatibility we must swizzle the package of some classes that had to be moved
   * when GemFire was open- sourced. This preserves backward-compatibility.
   *
   * @param nameArg the fully qualified class name
   * @return the name of the class in this implementation
   */
  public static String processIncomingClassName(String nameArg) {
    final String name = StaticSerialization.processIncomingClassName(nameArg);
    // using identity comparison on purpose because we are on the hot path
    if (name != nameArg) {
      return name;
    }
    if (name.equals(PRE_GEODE_190_SERVER_CQIMPL)) {
      return POST_GEODE_190_SERVER_CQIMPL;
    }
    OldClientSupportService svc = getOldClientSupportService();
    if (svc != null) {
      return svc.processIncomingClassName(name);
    }
    return name;
  }

  /**
   * For backward compatibility we must swizzle the package of some classes that had to be moved
   * when GemFire was open- sourced. This preserves backward-compatibility.
   *
   * @param nameArg the fully qualified class name
   * @param out the consumer of the serialized object
   * @return the name of the class in this implementation
   */
  public static String processOutgoingClassName(final String nameArg, DataOutput out) {

    final String name = StaticSerialization.processOutgoingClassName(nameArg);
    // using identity comparison on purpose because we are on the hot path
    if (name != nameArg) {
      return name;
    }

    if (out instanceof VersionedDataStream) {
      VersionedDataStream vout = (VersionedDataStream) out;
      KnownVersion version = vout.getVersion();
      if (null != version) {
        if (version.isOlderThan(KnownVersion.GEODE_1_9_0)) {
          if (name.equals(POST_GEODE_190_SERVER_CQIMPL)) {
            return PRE_GEODE_190_SERVER_CQIMPL;
          }
        }
      }
    }
    OldClientSupportService svc = getOldClientSupportService();
    if (svc != null) {
      return svc.processOutgoingClassName(name, out);
    }
    return name;
  }

  /**
   * Initializes the optional serialization "accept list" if the user has requested it in the
   * DistributionConfig
   *
   * @param distributionConfig the DistributedSystem configuration
   */
  public static void initializeSerializationFilter(DistributionConfig distributionConfig) {
    initializeSerializationFilter(distributionConfig, loadSanctionedSerializablesServices());
  }

  /**
   * Initializes the optional serialization "accept list" if the user has requested it in the
   * DistributionConfig
   *
   * @param distributionConfig the DistributedSystem configuration
   * @param services SanctionedSerializablesService that might have classes to acceptlist
   */
  @VisibleForTesting
  public static void initializeSerializationFilter(DistributionConfig distributionConfig,
      Collection<SanctionedSerializablesService> services) {
    logger.info("initializing InternalDataSerializer with {} services", services.size());
    if (distributionConfig.getValidateSerializableObjects()) {
      if (!ClassUtils.isClassAvailable("sun.misc.ObjectInputFilter")
          && !ClassUtils.isClassAvailable("java.io.ObjectInputFilter")) {
        throw new GemFireConfigException(
            "A serialization filter has been specified but this version of Java does not support serialization filters - ObjectInputFilter is not available");
      }
      // TODO:KIRK: start serializer: if configured: create serial filter on every geode-created
      // InputObjectStream
      String filterPattern = new SanctionedSerializablesFilterPattern()
          .append(distributionConfig.getSerializableObjectFilter())
          .pattern();
      System.out.println("JC debug: filterPattern=" + filterPattern);
      serializationFilter = new DelegatingObjectInputFilterFactory()
          .create(filterPattern, loadSanctionedClassNames(services));
    } else {
      clearSerializationFilter();
    }
  }

  @VisibleForTesting
  static void clearSerializationFilter() {
    serializationFilter = defaultSerializationFilter;
  }

  @VisibleForTesting
  public static ObjectInputFilter getSerializationFilter() {
    return serializationFilter;
  }

  private static SERIALIZATION_VERSION calculateSerializationVersion() {
    if (serializationVersionTxt == null || serializationVersionTxt.isEmpty()) {
      return latestVersion;
    } else if (serializationVersionTxt.startsWith("6.6.0")
        || serializationVersionTxt.startsWith("6.6.1")) {
      return SERIALIZATION_VERSION.v660;
    } else if (serializationVersionTxt.startsWith("6.6.2")) {
      return SERIALIZATION_VERSION.v662;
    } else {
      return SERIALIZATION_VERSION.vINVALID;
    }
  }

  public static boolean is662SerializationEnabled() {
    return serializationVersion.ordinal() >= SERIALIZATION_VERSION.v662.ordinal();
  }

  public static void checkSerializationVersion() {
    if (serializationVersion == SERIALIZATION_VERSION.vINVALID) {
      throw new IllegalArgumentException(
          "The system property \"gemfire.serializationVersion\" was set to \""
              + serializationVersionTxt
              + "\" which is not a valid serialization version. Valid versions must start with \"6.6.0\", \"6.6.1\", or \"6.6.2\"");
    }
  }

  private static void initializeWellKnownSerializers() {
    // ArrayBlockingQueue does not have zero-arg constructor
    // LinkedBlockingQueue does have zero-arg constructor but no way to get capacity

    classesToSerializers.put("java.lang.String", new WellKnownPdxDS() {
      @Override
      public boolean toData(Object o, DataOutput out) throws IOException {
        try {
          writeString((String) o, out);
        } catch (UTFDataFormatException ex) {
          // See bug 30428
          String s = "While writing a String of length " + ((String) o).length();
          UTFDataFormatException ex2 = new UTFDataFormatException(s);
          ex2.initCause(ex);
          throw ex2;
        }
        return true;
      }
    });
    classesToSerializers.put("java.net.InetAddress", new WellKnownDS() {
      @Override
      public boolean toData(Object o, DataOutput out) throws IOException {
        InetAddress address = (InetAddress) o;
        out.writeByte(DSCODE.INET_ADDRESS.toByte());
        writeInetAddress(address, out);
        return true;
      }
    });
    classesToSerializers.put("java.net.Inet4Address", new WellKnownDS() {
      @Override
      public boolean toData(Object o, DataOutput out) throws IOException {
        InetAddress address = (InetAddress) o;
        out.writeByte(DSCODE.INET_ADDRESS.toByte());
        writeInetAddress(address, out);
        return true;
      }
    });
    classesToSerializers.put("java.net.Inet6Address", new WellKnownDS() {
      @Override
      public boolean toData(Object o, DataOutput out) throws IOException {
        InetAddress address = (InetAddress) o;
        out.writeByte(DSCODE.INET_ADDRESS.toByte());
        writeInetAddress(address, out);
        return true;
      }
    });
    classesToSerializers.put("java.lang.Class", new WellKnownDS() {
      @Override
      public boolean toData(Object o, DataOutput out) throws IOException {
        Class c = (Class) o;
        if (c.isPrimitive()) {
          StaticSerialization.writePrimitiveClass(c, out);
        } else {
          out.writeByte(DSCODE.CLASS.toByte());
          writeClass(c, out);
        }
        return true;
      }
    });
    classesToSerializers.put("java.lang.Boolean", new WellKnownPdxDS() {
      @Override
      public boolean toData(Object o, DataOutput out) throws IOException {
        Boolean value = (Boolean) o;
        out.writeByte(DSCODE.BOOLEAN.toByte());
        writeBoolean(value, out);
        return true;
      }
    });
    classesToSerializers.put("java.lang.Character", new WellKnownPdxDS() {
      @Override
      public boolean toData(Object o, DataOutput out) throws IOException {
        Character value = (Character) o;
        out.writeByte(DSCODE.CHARACTER.toByte());
        writeCharacter(value, out);
        return true;
      }
    });
    classesToSerializers.put("java.lang.Byte", new WellKnownPdxDS() {
      @Override
      public boolean toData(Object o, DataOutput out) throws IOException {
        Byte value = (Byte) o;
        out.writeByte(DSCODE.BYTE.toByte());
        writeByte(value, out);
        return true;
      }
    });
    classesToSerializers.put("java.lang.Short", new WellKnownPdxDS() {
      @Override
      public boolean toData(Object o, DataOutput out) throws IOException {
        Short value = (Short) o;
        out.writeByte(DSCODE.SHORT.toByte());
        writeShort(value, out);
        return true;
      }
    });
    classesToSerializers.put("java.lang.Integer", new WellKnownPdxDS() {
      @Override
      public boolean toData(Object o, DataOutput out) throws IOException {
        Integer value = (Integer) o;
        out.writeByte(DSCODE.INTEGER.toByte());
        writeInteger(value, out);
        return true;
      }
    });
    classesToSerializers.put("java.lang.Long", new WellKnownPdxDS() {
      @Override
      public boolean toData(Object o, DataOutput out) throws IOException {
        Long value = (Long) o;
        out.writeByte(DSCODE.LONG.toByte());
        writeLong(value, out);
        return true;
      }
    });
    classesToSerializers.put("java.lang.Float", new WellKnownPdxDS() {
      @Override
      public boolean toData(Object o, DataOutput out) throws IOException {
        Float value = (Float) o;
        out.writeByte(DSCODE.FLOAT.toByte());
        writeFloat(value, out);
        return true;
      }
    });
    classesToSerializers.put("java.lang.Double", new WellKnownPdxDS() {
      @Override
      public boolean toData(Object o, DataOutput out) throws IOException {
        Double value = (Double) o;
        out.writeByte(DSCODE.DOUBLE.toByte());
        writeDouble(value, out);
        return true;
      }
    });
    classesToSerializers.put("[Z", // boolean[]
        new WellKnownPdxDS() {
          @Override
          public boolean toData(Object o, DataOutput out) throws IOException {
            out.writeByte(DSCODE.BOOLEAN_ARRAY.toByte());
            writeBooleanArray((boolean[]) o, out);
            return true;
          }
        });
    classesToSerializers.put("[B", // byte[]
        new WellKnownPdxDS() {
          @Override
          public boolean toData(Object o, DataOutput out) throws IOException {
            byte[] array = (byte[]) o;
            out.writeByte(DSCODE.BYTE_ARRAY.toByte());
            writeByteArray(array, out);
            return true;
          }
        });
    classesToSerializers.put("[C", // char[]
        new WellKnownPdxDS() {
          @Override
          public boolean toData(Object o, DataOutput out) throws IOException {
            out.writeByte(DSCODE.CHAR_ARRAY.toByte());
            writeCharArray((char[]) o, out);
            return true;
          }
        });
    classesToSerializers.put("[D", // double[]
        new WellKnownPdxDS() {
          @Override
          public boolean toData(Object o, DataOutput out) throws IOException {
            double[] array = (double[]) o;
            out.writeByte(DSCODE.DOUBLE_ARRAY.toByte());
            writeDoubleArray(array, out);
            return true;
          }
        });
    classesToSerializers.put("[F", // float[]
        new WellKnownPdxDS() {
          @Override
          public boolean toData(Object o, DataOutput out) throws IOException {
            float[] array = (float[]) o;
            out.writeByte(DSCODE.FLOAT_ARRAY.toByte());
            writeFloatArray(array, out);
            return true;
          }
        });
    classesToSerializers.put("[I", // int[]
        new WellKnownPdxDS() {
          @Override
          public boolean toData(Object o, DataOutput out) throws IOException {
            int[] array = (int[]) o;
            out.writeByte(DSCODE.INT_ARRAY.toByte());
            writeIntArray(array, out);
            return true;
          }
        });
    classesToSerializers.put("[J", // long[]
        new WellKnownPdxDS() {
          @Override
          public boolean toData(Object o, DataOutput out) throws IOException {
            long[] array = (long[]) o;
            out.writeByte(DSCODE.LONG_ARRAY.toByte());
            writeLongArray(array, out);
            return true;
          }
        });
    classesToSerializers.put("[S", // short[]
        new WellKnownPdxDS() {
          @Override
          public boolean toData(Object o, DataOutput out) throws IOException {
            short[] array = (short[]) o;
            out.writeByte(DSCODE.SHORT_ARRAY.toByte());
            writeShortArray(array, out);
            return true;
          }
        });
    classesToSerializers.put("[Ljava.lang.String;", // String[]
        new WellKnownPdxDS() {
          @Override
          public boolean toData(Object o, DataOutput out) throws IOException {
            String[] array = (String[]) o;
            out.writeByte(DSCODE.STRING_ARRAY.toByte());
            writeStringArray(array, out);
            return true;
          }
        });

    WellKnownDS TIME_UNIT_SERIALIZER = new WellKnownDS() {
      @Override
      public boolean toData(Object o, DataOutput out) throws IOException {
        TimeUnit timeUnit = (TimeUnit) o;
        switch (timeUnit) {
          case NANOSECONDS: {
            out.writeByte(DSCODE.TIME_UNIT.toByte());
            out.writeByte(StaticSerialization.TIME_UNIT_NANOSECONDS);
            break;
          }
          case MICROSECONDS: {
            out.writeByte(DSCODE.TIME_UNIT.toByte());
            out.writeByte(StaticSerialization.TIME_UNIT_MICROSECONDS);
            break;
          }
          case MILLISECONDS: {
            out.writeByte(DSCODE.TIME_UNIT.toByte());
            out.writeByte(StaticSerialization.TIME_UNIT_MILLISECONDS);
            break;
          }
          case SECONDS: {
            out.writeByte(DSCODE.TIME_UNIT.toByte());
            out.writeByte(StaticSerialization.TIME_UNIT_SECONDS);
            break;
          }
          // handles all other timeunits
          default: {
            writeGemFireEnum(timeUnit, out);
          }
        }
        return true;
      }
    };

    // in java 9 and above, TimeUnit implementation changes. the class name of these units are the
    // same now.
    if (TimeUnit.NANOSECONDS.getClass().getName().equals(TimeUnit.SECONDS.getClass().getName())) {
      classesToSerializers.put(TimeUnit.class.getName(), TIME_UNIT_SERIALIZER);
    }
    // in java 8 and below
    else {
      classesToSerializers.put(TimeUnit.NANOSECONDS.getClass().getName(), TIME_UNIT_SERIALIZER);
      classesToSerializers.put(TimeUnit.MICROSECONDS.getClass().getName(), TIME_UNIT_SERIALIZER);
      classesToSerializers.put(TimeUnit.MILLISECONDS.getClass().getName(), TIME_UNIT_SERIALIZER);
      classesToSerializers.put(TimeUnit.SECONDS.getClass().getName(), TIME_UNIT_SERIALIZER);
    }
    classesToSerializers.put("java.util.Date", new WellKnownPdxDS() {
      @Override
      public boolean toData(Object o, DataOutput out) throws IOException {
        Date date = (Date) o;
        out.writeByte(DSCODE.DATE.toByte());
        writeDate(date, out);
        return true;
      }
    });
    classesToSerializers.put("java.io.File", new WellKnownDS() {
      @Override
      public boolean toData(Object o, DataOutput out) throws IOException {
        File file = (File) o;
        out.writeByte(DSCODE.FILE.toByte());
        writeFile(file, out);
        return true;
      }
    });
    classesToSerializers.put("java.util.ArrayList", new WellKnownPdxDS() {
      @Override
      public boolean toData(Object o, DataOutput out) throws IOException {
        ArrayList list = (ArrayList) o;
        out.writeByte(DSCODE.ARRAY_LIST.toByte());
        writeArrayList(list, out);
        return true;
      }
    });
    classesToSerializers.put("java.util.LinkedList", new WellKnownDS() {
      @Override
      public boolean toData(Object o, DataOutput out) throws IOException {
        LinkedList list = (LinkedList) o;
        out.writeByte(DSCODE.LINKED_LIST.toByte());
        writeLinkedList(list, out);
        return true;
      }
    });
    classesToSerializers.put("java.util.Vector", new WellKnownPdxDS() {
      @Override
      public boolean toData(Object o, DataOutput out) throws IOException {
        out.writeByte(DSCODE.VECTOR.toByte());
        writeVector((Vector) o, out);
        return true;
      }
    });
    classesToSerializers.put("java.util.Stack", new WellKnownDS() {
      @Override
      public boolean toData(Object o, DataOutput out) throws IOException {
        out.writeByte(DSCODE.STACK.toByte());
        writeStack((Stack) o, out);
        return true;
      }
    });
    classesToSerializers.put("java.util.HashSet", new WellKnownPdxDS() {
      @Override
      public boolean toData(Object o, DataOutput out) throws IOException {
        HashSet list = (HashSet) o;
        out.writeByte(DSCODE.HASH_SET.toByte());
        writeHashSet(list, out);
        return true;
      }
    });
    classesToSerializers.put("java.util.LinkedHashSet", new WellKnownPdxDS() {
      @Override
      public boolean toData(Object o, DataOutput out) throws IOException {
        out.writeByte(DSCODE.LINKED_HASH_SET.toByte());
        writeLinkedHashSet((LinkedHashSet) o, out);
        return true;
      }
    });
    classesToSerializers.put("java.util.HashMap", new WellKnownPdxDS() {
      @Override
      public boolean toData(Object o, DataOutput out) throws IOException {
        HashMap list = (HashMap) o;
        out.writeByte(DSCODE.HASH_MAP.toByte());
        writeHashMap(list, out);
        return true;
      }
    });
    classesToSerializers.put("java.util.IdentityHashMap", new WellKnownDS() {
      @Override
      public boolean toData(Object o, DataOutput out) throws IOException {
        out.writeByte(DSCODE.IDENTITY_HASH_MAP.toByte());
        writeIdentityHashMap((IdentityHashMap) o, out);
        return true;
      }
    });
    classesToSerializers.put("java.util.Hashtable", new WellKnownPdxDS() {
      @Override
      public boolean toData(Object o, DataOutput out) throws IOException {
        out.writeByte(DSCODE.HASH_TABLE.toByte());
        writeHashtable((Hashtable) o, out);
        return true;
      }
    });
    classesToSerializers.put("java.util.Properties", new WellKnownDS() {
      @Override
      public boolean toData(Object o, DataOutput out) throws IOException {
        Properties props = (Properties) o;
        out.writeByte(DSCODE.PROPERTIES.toByte());
        writeProperties(props, out);
        return true;
      }
    });
    classesToSerializers.put("java.util.TreeMap", new WellKnownDS() {
      @Override
      public boolean toData(Object o, DataOutput out) throws IOException {
        out.writeByte(DSCODE.TREE_MAP.toByte());
        writeTreeMap((TreeMap) o, out);
        return true;
      }
    });
    classesToSerializers.put("java.util.TreeSet", new WellKnownDS() {
      @Override
      public boolean toData(Object o, DataOutput out) throws IOException {
        out.writeByte(DSCODE.TREE_SET.toByte());
        writeTreeSet((TreeSet) o, out);
        return true;
      }
    });
    if (is662SerializationEnabled()) {
      classesToSerializers.put("java.math.BigInteger", new WellKnownDS() {
        @Override
        public boolean toData(Object o, DataOutput out) throws IOException {
          out.writeByte(DSCODE.BIG_INTEGER.toByte());
          writeBigInteger((BigInteger) o, out);
          return true;
        }
      });
      classesToSerializers.put("java.math.BigDecimal", new WellKnownDS() {
        @Override
        public boolean toData(Object o, DataOutput out) throws IOException {
          out.writeByte(DSCODE.BIG_DECIMAL.toByte());
          writeBigDecimal((BigDecimal) o, out);
          return true;
        }
      });
      classesToSerializers.put("java.util.UUID", new WellKnownDS() {
        @Override
        public boolean toData(Object o, DataOutput out) throws IOException {
          out.writeByte(DSCODE.UUID.toByte());
          writeUUID((UUID) o, out);
          return true;
        }
      });
      classesToSerializers.put("java.sql.Timestamp", new WellKnownDS() {
        @Override
        public boolean toData(Object o, DataOutput out) throws IOException {
          out.writeByte(DSCODE.TIMESTAMP.toByte());
          writeTimestamp((Timestamp) o, out);
          return true;
        }
      });
    }
  }

  /**
   * Convert the given unsigned byte to an int. The returned value will be in the range [0..255]
   * inclusive
   */
  private static int ubyteToInt(byte ub) {
    return ub & 0xFF;
  }

  public static OldClientSupportService getOldClientSupportService() {
    return oldClientSupportService;
  }

  public static void setOldClientSupportService(final OldClientSupportService svc) {
    oldClientSupportService = svc;
  }

  /**
   * Instantiates an instance of {@code DataSerializer}
   *
   * @throws IllegalArgumentException If the class can't be instantiated
   * @see DataSerializer#register(Class)
   */
  static DataSerializer newInstance(Class<? extends DataSerializer> c)
      throws IllegalArgumentException {
    if (!DataSerializer.class.isAssignableFrom(c)) {
      throw new IllegalArgumentException(
          String.format("%s does not extend DataSerializer.",
              c.getName()));
    }

    Constructor<? extends DataSerializer> init;
    try {
      init = c.getDeclaredConstructor();

    } catch (NoSuchMethodException ignored) {
      if (c.getDeclaringClass() != null) {
        String message = String.format(
            "Class %s does not have a zero-argument constructor. It is an inner class of %s. Should it be a static inner class?",
            c.getName(), c.getDeclaringClass());
        throw new IllegalArgumentException(message);
      }
      String message = String.format("Class %s does not have a zero-argument constructor.",
          c.getName());
      throw new IllegalArgumentException(message);
    }

    DataSerializer s;
    try {
      init.setAccessible(true);
      s = init.newInstance();

    } catch (IllegalAccessException ignored) {
      throw new IllegalArgumentException(
          String.format("Could not instantiate an instance of %s",
              c.getName()));

    } catch (InstantiationException ex) {
      throw new IllegalArgumentException(
          String.format("Could not instantiate an instance of %s",
              c.getName()),
          ex);

    } catch (InvocationTargetException ex) {
      throw new IllegalArgumentException(
          String.format("While instantiating an instance of %s",
              c.getName()),
          ex);
    }

    return s;
  }

  public static DataSerializer register(Class<? extends DataSerializer> c, boolean distribute,
      EventID eventId,
      ClientProxyMembershipID context) {
    DataSerializer s = newInstance(c);
    // This method is only called when server connection and CacheClientUpdaterThread
    s.setEventId(eventId);
    s.setContext(context);
    return _register(s, distribute);
  }

  /**
   * Registers a {@code DataSerializer} instance with the data serialization framework.
   *
   * @param distribute Should the registered {@code DataSerializer} be distributed to other members
   *        of the distributed system?
   * @see DataSerializer#register(Class)
   */
  public static DataSerializer register(Class<? extends DataSerializer> c, boolean distribute) {
    final DataSerializer s = newInstance(c);
    return _register(s, distribute);
  }

  public static DataSerializer _register(DataSerializer s, boolean distribute) {
    final int id = s.getId();
    DataSerializer dsForMarkers = s;
    if (id == 0) {
      throw new IllegalArgumentException(
          "Cannot create a DataSerializer with id 0.");
    }
    final Class[] classes = s.getSupportedClasses();
    if (classes == null || classes.length == 0) {
      final String msg =
          "The DataSerializer %s has no supported classes. It's getSupportedClasses method must return at least one class";
      throw new IllegalArgumentException(String.format(msg, s.getClass().getName()));
    }

    for (Class aClass : classes) {
      if (aClass == null) {
        final String msg =
            "The DataSerializer getSupportedClasses method for %s returned an array that contained a null element.";
        throw new IllegalArgumentException(String.format(msg, s.getClass().getName()));
      } else if (aClass.isArray()) {
        final String msg =
            "The DataSerializer getSupportedClasses method for %s returned an array that contained an array class which is not allowed since arrays have built-in support.";
        throw new IllegalArgumentException(String.format(msg, s.getClass().getName()));
      }
    }

    final Integer idx = id;
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
            oldMarker = (Marker) oldSerializer;
          }
        } else if (oldSerializer.getClass().equals(s.getClass())) {
          // We've already got one of these registered
          if (distribute) {
            sendRegistrationMessage(s);
          }
          return (DataSerializer) oldSerializer;
        } else {
          DataSerializer other = (DataSerializer) oldSerializer;
          throw new IllegalStateException(
              String.format(
                  "A DataSerializer of class %s is already registered with id %s.",
                  other.getClass().getName(), other.getId()));
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
              oldMsg = "A DataSerializer of class " + oldS.getClass().getName()
                  + " is already registered to support class ";
            }
            String msg = oldMsg + classes[i].getName() + " so the DataSerializer of class "
                + s.getClass().getName() + " could not be registered.";
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
    InternalCache cache = getInternalCache();
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
    // send it to all cache clients irrelevant of distribute
    // cache servers send it all the clients irrelevant of
    // originator VM
    sendRegistrationMessageToClients(s);

    fireNewDataSerializer(s);

    return s;
  }

  @SuppressWarnings("deprecation")
  private static InternalCache getInternalCache() {
    return GemFireCacheImpl.getInstance();
  }

  /**
   * Marks a {@code DataSerializer} className for registration with the data serialization framework
   * if and when it is needed. Does not necessarily load the classes into this VM.
   *
   * @param className Name of the DataSerializer class.
   * @param distribute If true, distribute this data serializer.
   * @param eventId Event id
   * @param proxyId proxy id
   * @see DataSerializer#register(Class)
   */
  public static void register(String className, boolean distribute, EventID eventId,
      ClientProxyMembershipID proxyId, int id) {
    register(className, distribute,
        new SerializerAttributesHolder(className, eventId, proxyId, id));
  }

  /**
   * Marks a {@code DataSerializer} className for registration with the data serialization
   * framework. Does not necessarily load the classes into this VM.
   *
   * @param distribute If true, distribute this data serializer.
   * @see DataSerializer#register(Class)
   */
  public static void register(String className, boolean distribute) {
    register(className, distribute, new SerializerAttributesHolder());
  }

  private static void register(String className, boolean distribute,
      SerializerAttributesHolder holder) {
    if (StringUtils.isBlank(className)) {
      throw new IllegalArgumentException("Class name cannot be null or empty.");
    }

    SerializerAttributesHolder oldValue = dsClassesToHolders.putIfAbsent(className, holder);
    if (oldValue != null) {
      if (oldValue.getId() != 0 && holder.getId() != 0 && oldValue.getId() != holder.getId()) {
        throw new IllegalStateException(
            String.format(
                "A DataSerializer of class %s is already registered with id %s.",
                oldValue.getClass().getName(), oldValue.getId()));
      }
    }

    idsToHolders.putIfAbsent(holder.getId(), holder);

    Object ds = idsToSerializers.get(holder.getId());
    if (ds instanceof Marker) {
      synchronized (ds) {
        ((Marker) ds).notifyAll();
      }
    }

    if (distribute) {
      sendRegistrationMessageToServers(holder);
    }
  }

  /**
   * During client/server handshakes the server may send a collection of DataSerializers and the
   * classes they support. The DataSerializers are registered as "holders" to avoid loading the
   * actual classes until they're needed. This method registers the names of classes supported by
   * the DataSerializers
   *
   * @param map The classes returned by DataSerializer.supportedClasses()
   */
  public static void updateSupportedClassesMap(Map<Integer, List<String>> map) {
    for (Entry<Integer, List<String>> e : map.entrySet()) {
      for (String supportedClassName : e.getValue()) {
        SerializerAttributesHolder serializerAttributesHolder = idsToHolders.get(e.getKey());
        if (serializerAttributesHolder != null) {
          supportedClassesToHolders.putIfAbsent(supportedClassName, serializerAttributesHolder);
        }
      }
    }
  }

  public static void updateSupportedClassesMap(String dsClassName, String supportedClassName) {
    SerializerAttributesHolder holder = dsClassesToHolders.get(dsClassName);
    if (holder != null) {
      supportedClassesToHolders.putIfAbsent(supportedClassName, holder);
    }
  }

  private static void sendRegistrationMessageToServers(DataSerializer dataSerializer) {
    PoolManagerImpl.allPoolsRegisterDataSerializers(dataSerializer);
  }

  private static void sendRegistrationMessageToServers(SerializerAttributesHolder holder) {
    PoolManagerImpl.allPoolsRegisterDataSerializers(holder);
  }

  private static void sendRegistrationMessageToClients(DataSerializer dataSerializer) {
    InternalCache cache = getInternalCache();
    if (cache == null) {
      // A cache has not yet been created.
      // we can't propagate it to clients
      return;
    }
    byte[][] serializedDataSerializer = new byte[2][];
    try {
      serializedDataSerializer[0] =
          CacheServerHelper.serialize(dataSerializer.getClass().toString().substring(6));

      byte[] idBytes = new byte[4];
      Part.encodeInt(dataSerializer.getId(), idBytes);
      serializedDataSerializer[1] = idBytes;
    } catch (IOException ignored) {
      if (logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE)) {
        logger.trace(LogMarker.SERIALIZER_VERBOSE,
            "InternalDataSerializer encountered an IOException while serializing DataSerializer :{}",
            dataSerializer);
      }
    }
    ClientDataSerializerMessage clientDataSerializerMessage =
        new ClientDataSerializerMessage(EnumListenerEvent.AFTER_REGISTER_DATASERIALIZER,
            serializedDataSerializer, (ClientProxyMembershipID) dataSerializer.getContext(),
            (EventID) dataSerializer.getEventId(),
            new Class[][] {dataSerializer.getSupportedClasses()});
    // Deliver it to all the clients
    CacheClientNotifier.routeClientMessage(clientDataSerializerMessage);
  }

  public static EventID generateEventId() {
    InternalCache cache = getInternalCache();
    if (cache == null) {
      // A cache has not yet created
      return null;
    }
    return new EventID(InternalDistributedSystem.getAnyInstance());
  }

  /**
   * Unregisters a {@code Serializer} that was previously registered with the data serialization
   * framework.
   */
  public static void unregister(int id) {
    final Integer idx = id;
    Object o = idsToSerializers.remove(idx);
    if (o != null) {
      if (o instanceof InitMarker) {
        o = ((Marker) o).getSerializer();
      }
    }
    if (o instanceof DataSerializer) {
      DataSerializer s = (DataSerializer) o;
      Class[] classes = s.getSupportedClasses();
      for (Class aClass : classes) {
        classesToSerializers.remove(aClass.getName(), s);
        supportedClassesToHolders.remove(aClass.getName());
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
   * Returns the {@code DataSerializer} for the given class. If no class has been registered, {@code
   * null} is returned. Remember that it is okay to return {@code null} in this case. This method is
   * invoked when writing an object. If a serializer isn't available, then its the user's fault.
   */
  private static DataSerializer getSerializer(Class c) {
    DataSerializer ds = classesToSerializers.get(c.getName());
    if (ds == null) {
      SerializerAttributesHolder sah = supportedClassesToHolders.get(c.getName());
      if (sah != null) {
        Class<DataSerializer> dsClass;
        try {
          dsClass = InternalDataSerializer.getCachedClass(sah.getClassName());
          DataSerializer serializer = register(dsClass, false);
          dsClassesToHolders.remove(dsClass.getName());
          idsToHolders.remove(serializer.getId());
          for (Class clazz : serializer.getSupportedClasses()) {
            supportedClassesToHolders.remove(clazz.getName());
          }
          return serializer;
        } catch (ClassNotFoundException ignored) {
          logger.info(LogMarker.SERIALIZER_MARKER, "Could not load DataSerializer class: {}",
              c.getName());
        }
      }
    }
    return ds;
  }

  /**
   * Returns the {@code DataSerializer} with the given id.
   */
  public static DataSerializer getSerializer(int id) {
    final Integer idx = id;
    final GetMarker marker = new GetMarker();
    DataSerializer result = null;
    boolean timedOut = false;
    SerializerAttributesHolder sah = idsToHolders.get(idx);
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
        result = ((Marker) o).getSerializer();
      } else {
        result = (DataSerializer) o;
      }
    }
    if (result == null) {
      if (sah != null) {
        Class<DataSerializer> dsClass;
        try {
          dsClass = getCachedClass(sah.getClassName());
          DataSerializer ds = register(dsClass, false);
          dsClassesToHolders.remove(sah.getClassName());
          idsToHolders.remove(id);
          for (Class clazz : ds.getSupportedClasses()) {
            supportedClassesToHolders.remove(clazz.getName());
          }
          return ds;
        } catch (ClassNotFoundException ignored) {
          logger.info(LogMarker.SERIALIZER_MARKER, "Could not load DataSerializer class: {}",
              sah.getClassName());
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
    List<DataSerializer> coll = new ArrayList<>(size);
    for (Object v : idsToSerializers.values()) {
      if (v instanceof InitMarker) {
        v = ((Marker) v).getSerializer();
      }
      if (v instanceof DataSerializer) {
        coll.add((DataSerializer) v);
      }
    }

    Iterator<Entry<String, SerializerAttributesHolder>> iterator =
        dsClassesToHolders.entrySet().iterator();
    while (iterator.hasNext()) {
      Entry<String, SerializerAttributesHolder> entry = iterator.next();
      String name = entry.getKey();
      SerializerAttributesHolder holder = entry.getValue();
      try {
        Class<? extends DataSerializer> cl = getCachedClass(name);
        DataSerializer ds;
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
      } catch (ClassNotFoundException ignored) {
        logger.info(LogMarker.SERIALIZER_MARKER, "Could not load DataSerializer class: {}", name);
      }
    }

    return coll.toArray(new DataSerializer[0]);
  }

  /**
   * Returns all the data serializers in this vm. This method, unlike {@link #getSerializers()},
   * does not force loading of the data serializers which were not loaded in the vm earlier.
   *
   * @return Array of {@link SerializerAttributesHolder}
   */
  public static SerializerAttributesHolder[] getSerializersForDistribution() {

    final int size = idsToSerializers.size() + dsClassesToHolders.size();
    Collection<SerializerAttributesHolder> coll = new ArrayList<>(size);

    for (Object v : idsToSerializers.values()) {
      if (v instanceof InitMarker) {
        v = ((Marker) v).getSerializer();
      }
      if (v instanceof DataSerializer) {
        DataSerializer s = (DataSerializer) v;
        coll.add(new SerializerAttributesHolder(s.getClass().getName(), (EventID) s.getEventId(),
            (ClientProxyMembershipID) s.getContext(), s.getId()));
      }
    }

    for (final Entry<String, SerializerAttributesHolder> stringSerializerAttributesHolderEntry : dsClassesToHolders
        .entrySet()) {
      SerializerAttributesHolder v = stringSerializerAttributesHolderEntry.getValue();
      coll.add(v);
    }

    return coll.toArray(new SerializerAttributesHolder[0]);
  }

  /**
   * Adds a {@code RegistrationListener} that will receive callbacks when {@code DataSerializer}s
   * and {@code Instantiator}s are registered.
   */
  public static void addRegistrationListener(RegistrationListener l) {
    synchronized (listenersSync) {
      Set<RegistrationListener> newSet = new HashSet<>(listeners);
      newSet.add(l);
      listeners = newSet;
    }
  }

  /**
   * Removes a {@code RegistrationListener} so that it no longer receives callbacks.
   */
  public static void removeRegistrationListener(RegistrationListener l) {
    synchronized (listenersSync) {
      Set<RegistrationListener> newSet = new HashSet<>(listeners);
      newSet.remove(l);
      listeners = newSet;
    }
  }

  /**
   * Alerts all {@code RegistrationListener}s that a new {@code DataSerializer} has been registered
   *
   * @see InternalDataSerializer.RegistrationListener#newDataSerializer
   */
  private static void fireNewDataSerializer(DataSerializer ds) {
    for (RegistrationListener listener : listeners) {
      listener.newDataSerializer(ds);
    }
  }

  /**
   * Alerts all {@code RegistrationListener}s that a new {@code Instantiator} has been registered
   *
   * @see InternalDataSerializer.RegistrationListener#newInstantiator
   */
  static void fireNewInstantiator(Instantiator instantiator) {
    for (RegistrationListener listener : listeners) {
      listener.newInstantiator(instantiator);
    }
  }

  /**
   * If we are connected to a distributed system, send a message to other members telling them about
   * a newly-registered serializer.
   */
  private static void sendRegistrationMessage(DataSerializer s) {
    InternalDistributedSystem system = InternalDistributedSystem.getConnectedInstance();
    if (system != null) {
      RegistrationMessage m = new RegistrationMessage(s);
      system.getDistributionManager().putOutgoing(m);
    }
  }

  // Writes just the header of a DataSerializableFixedID to out.
  public static void writeDSFIDHeader(int dsfid, DataOutput out) throws IOException {
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

  public static void writeDSFID(DataSerializableFixedID o, DataOutput out) throws IOException {
    int dsfid = o.getDSFID();
    try {
      if (dsfid != DataSerializableFixedID.NO_FIXED_ID) {
        dsfidSerializer.write(o, out);
        return;
      }
      out.writeByte(DSCODE.DS_NO_FIXED_ID.toByte());
      DataSerializer.writeClass(o.getClass(), out);
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
   * Data serializes an instance of a well-known class to the given {@code DataOutput}.
   *
   * @return {@code true} if {@code o} was actually written to {@code out}
   */
  private static boolean writeWellKnownObject(Object o, DataOutput out,
      boolean ensurePdxCompatibility) throws IOException {
    return writeUserObject(o, out, ensurePdxCompatibility);
  }

  /**
   * Data serializes an instance of a "user class" (that is, a class that can be handled by a
   * registered {@code DataSerializer}) to the given {@code DataOutput}.
   *
   * @return {@code true} if {@code o} was written to {@code out}.
   */
  private static boolean writeUserObject(Object o, DataOutput out, boolean ensurePdxCompatibility)
      throws IOException {

    final Class<?> c = o.getClass();
    final DataSerializer serializer = InternalDataSerializer.getSerializer(c);
    if (serializer != null) {
      int id = serializer.getId();
      if (id != 0) {
        checkPdxCompatible(o, ensurePdxCompatibility);
        // id will be 0 if it is a WellKnowDS
        if (id <= Byte.MAX_VALUE && id >= Byte.MIN_VALUE) {
          out.writeByte(DSCODE.USER_CLASS.toByte());
          out.writeByte((byte) id);
        } else if (id <= Short.MAX_VALUE && id >= Short.MIN_VALUE) {
          out.writeByte(DSCODE.USER_CLASS_2.toByte());
          out.writeShort(id);
        } else {
          out.writeByte(DSCODE.USER_CLASS_4.toByte());
          out.writeInt(id);
        }
      } else {
        if (ensurePdxCompatibility) {
          if (!(serializer instanceof WellKnownPdxDS)) {
            checkPdxCompatible(o, ensurePdxCompatibility);
          }
        }
      }
      boolean toDataResult;
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
          throw new ToDataException(
              "toData failed on DataSerializer with id=" + id + " for class " + c, io);
        }
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
            "toData failed on DataSerializer with id=" + id + " for class " + c, t);
      }
      if (toDataResult) {
        return true;
      } else {
        throw new ToDataException(
            String.format(
                "Serializer %s (a %s ) said that it could serialize an instance of %s , but its toData() method returned false.",
                serializer.getId(), serializer.getClass().getName(),
                o.getClass().getName()));
      }
      // Do byte[][] and Object[] here to fix bug 44060
    } else if (o instanceof byte[][]) {
      byte[][] byteArrays = (byte[][]) o;
      out.writeByte(DSCODE.ARRAY_OF_BYTE_ARRAYS.toByte());
      writeArrayOfByteArrays(byteArrays, out);
      return true;
    } else if (o instanceof Object[]) {
      Object[] array = (Object[]) o;
      out.writeByte(DSCODE.OBJECT_ARRAY.toByte());
      writeObjectArray(array, out, ensurePdxCompatibility);
      return true;
    } else if (is662SerializationEnabled()
        && (o.getClass().isEnum()/* for bug 52271 */ || (o.getClass().getSuperclass() != null
            && o.getClass().getSuperclass().isEnum()))) {
      if (isPdxSerializationInProgress()) {
        writePdxEnum((Enum<?>) o, out);
      } else {
        checkPdxCompatible(o, ensurePdxCompatibility);
        writeGemFireEnum((Enum<?>) o, out);
      }
      return true;
    } else {
      PdxSerializer pdxSerializer = TypeRegistry.getPdxSerializer();
      return pdxSerializer != null && writePdx(out, null, o, pdxSerializer);
    }
  }

  public static boolean autoSerialized(Object o, DataOutput out) throws IOException {
    AutoSerializableManager asm = TypeRegistry.getAutoSerializableManager();
    if (asm != null) {
      AutoClassInfo aci = asm.getExistingClassInfo(o.getClass());
      if (aci != null) {
        InternalCache internalCache = GemFireCacheImpl
            .getForPdx("PDX registry is unavailable because the Cache has been closed.");
        TypeRegistry tr = internalCache.getPdxRegistry();

        PdxOutputStream os;
        if (out instanceof HeapDataOutputStream) {
          os = new PdxOutputStream((HeapDataOutputStream) out);
        } else {
          os = new PdxOutputStream();
        }
        PdxWriterImpl writer = new PdxWriterImpl(tr, o, aci, os);

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
        } catch (ToDataException | CancelException | NonPortableClassException
            | GemFireRethrowable ex) {
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
          throw new ToDataException("PdxSerializer failed when calling toData on " + o.getClass(),
              t);
        }
        int bytesWritten = writer.completeByteStreamGeneration();
        getDMStats(internalCache).incPdxSerialization(bytesWritten);
        if (!(out instanceof HeapDataOutputStream)) {
          writer.sendTo(out);
        }
        return true;
      }
    }
    return false;
  }

  private static void checkPdxCompatible(Object o, boolean ensurePdxCompatibility) {
    if (ensurePdxCompatibility) {
      throw new NonPortableClassException(
          "Instances of " + o.getClass() + " are not compatible with non-java PDX.");
    }
  }

  /**
   * Test to see if the object is in the gemfire package, to see if we should pass it on to a user's
   * custom serializater.
   */
  static boolean isGemfireObject(Object o) {
    return (o instanceof Function // fixes 43691
        || o.getClass().getName().startsWith("org.apache.geode."))
        && !(o instanceof PdxSerializerObject);
  }

  /**
   * Reads an object that was serialized by a customer ("user") {@code DataSerializer} from the
   * given {@code DataInput}.
   *
   * @throws IOException If the serializer that can deserialize the object is not registered.
   */
  private static Object readUserObject(DataInput in, int serializerId)
      throws IOException, ClassNotFoundException {
    DataSerializer serializer = InternalDataSerializer.getSerializer(serializerId);

    if (serializer == null) {
      throw new IOException(String.format("Serializer with Id %s is not registered", serializerId));
    }

    return serializer.fromData(in);
  }

  /**
   * Checks to make sure a {@code DataOutput} is not {@code null}.
   *
   * @throws NullPointerException If {@code out} is {@code null}
   */
  public static void checkOut(DataOutput out) {
    if (out == null) {
      throw new NullPointerException("Null DataOutput");
    }
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
   * Writes a {@code Set} to a {@code DataOutput}.
   * <P>
   * This method is internal because its semantics (that is, its ability to write any kind of {@code
   * Set}) are different from the {@code write}XXX methods of the external {@code DataSerializer}.
   *
   * @throws IOException A problem occurs while writing to {@code out}
   * @see #readSet
   * @since GemFire 4.0
   */
  public static void writeSet(Collection<?> set, DataOutput out) throws IOException {
    checkOut(out);

    if (set != null) {
      final int size = set.size();
      writeArrayLength(size, out);
      if (logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE)) {
        logger
            .trace(LogMarker.SERIALIZER_VERBOSE, "Writing Set with {} elements: {}", size, set);
      }
      for (Object element : set) {
        writeObject(element, out);
      }
    } else {
      writeArrayLength(-1, out);
      if (logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE)) {
        logger
            .trace(LogMarker.SERIALIZER_VERBOSE, "Writing Set with {} elements: {}", -1, null);
      }
    }
  }

  /**
   * Reads a {@code Set} from a {@code DataInput}.
   *
   * @throws IOException A problem occurs while writing to {@code out}
   * @throws ClassNotFoundException The class of one of the {@code HashSet}'s elements cannot be
   *         found.
   * @see #writeSet
   * @since GemFire 4.0
   */
  public static Set readSet(DataInput in) throws IOException, ClassNotFoundException {
    return readHashSet(in);
  }

  /**
   * write a set of Long objects
   *
   * @param set the set of Long objects
   * @param hasLongIDs if false, write only ints, not longs
   * @param out the output stream
   */
  public static void writeSetOfLongs(Set set, boolean hasLongIDs, DataOutput out)
      throws IOException {
    if (set == null) {
      out.writeInt(-1);
    } else {
      out.writeInt(set.size());
      out.writeBoolean(hasLongIDs);
      for (Object aSet : set) {
        Long l = (Long) aSet;
        if (hasLongIDs) {
          out.writeLong(l);
        } else {
          out.writeInt((int) l.longValue());
        }
      }
    }
  }

  /**
   * read a set of Long objects
   */
  public static Set<Long> readSetOfLongs(DataInput in) throws IOException {
    int size = in.readInt();
    if (size < 0) {
      return null;
    } else {
      Set<Long> result = new HashSet<>(size);
      boolean longIDs = in.readBoolean();
      for (int i = 0; i < size; i++) {
        long l = longIDs ? in.readLong() : in.readInt();
        result.add(l);
      }
      return result;
    }
  }

  /**
   * Reads a {@code TimeUnit} from a {@code DataInput}.
   *
   * @throws IOException A problem occurs while writing to {@code out}
   */
  private static TimeUnit readTimeUnit(DataInput in) throws IOException {
    InternalDataSerializer.checkIn(in);

    byte type = in.readByte();

    TimeUnit unit;
    switch (type) {
      case StaticSerialization.TIME_UNIT_NANOSECONDS:
        unit = TimeUnit.NANOSECONDS;
        break;
      case StaticSerialization.TIME_UNIT_MICROSECONDS:
        unit = TimeUnit.MICROSECONDS;
        break;
      case StaticSerialization.TIME_UNIT_MILLISECONDS:
        unit = TimeUnit.MILLISECONDS;
        break;
      case StaticSerialization.TIME_UNIT_SECONDS:
        unit = TimeUnit.SECONDS;
        break;
      default:
        throw new IOException(
            String.format("Unknown TimeUnit type: %s", type));
    }

    if (logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE)) {
      logger.trace(LogMarker.SERIALIZER_VERBOSE, "Read TimeUnit: {}", unit);
    }

    return unit;
  }

  private static void writeTimestamp(Timestamp o, DataOutput out) throws IOException {
    InternalDataSerializer.checkOut(out);

    if (logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE)) {
      logger.trace(LogMarker.SERIALIZER_VERBOSE, "Writing Timestamp: {}", o);
    }
    DataSerializer.writePrimitiveLong(o.getTime(), out);
  }

  private static Timestamp readTimestamp(DataInput in) throws IOException {
    InternalDataSerializer.checkIn(in);
    Timestamp result = new Timestamp(DataSerializer.readPrimitiveLong(in));
    if (logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE)) {
      logger.trace(LogMarker.SERIALIZER_VERBOSE, "Read Timestamp: {}", result);
    }
    return result;
  }

  private static void writeUUID(java.util.UUID o, DataOutput out) throws IOException {
    InternalDataSerializer.checkOut(out);

    if (logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE)) {
      logger.trace(LogMarker.SERIALIZER_VERBOSE, "Writing UUID: {}", o);
    }
    DataSerializer.writePrimitiveLong(o.getMostSignificantBits(), out);
    DataSerializer.writePrimitiveLong(o.getLeastSignificantBits(), out);
  }

  private static UUID readUUID(DataInput in) throws IOException {
    InternalDataSerializer.checkIn(in);
    long mb = DataSerializer.readPrimitiveLong(in);
    long lb = DataSerializer.readPrimitiveLong(in);
    UUID result = new UUID(mb, lb);
    if (logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE)) {
      logger.trace(LogMarker.SERIALIZER_VERBOSE, "Read UUID: {}", result);
    }
    return result;
  }

  private static void writeBigDecimal(BigDecimal o, DataOutput out) throws IOException {
    InternalDataSerializer.checkOut(out);

    if (logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE)) {
      logger.trace(LogMarker.SERIALIZER_VERBOSE, "Writing BigDecimal: {}", o);
    }
    DataSerializer.writeString(o.toString(), out);
  }

  private static BigDecimal readBigDecimal(DataInput in) throws IOException {
    InternalDataSerializer.checkIn(in);
    BigDecimal result = new BigDecimal(DataSerializer.readString(in));
    if (logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE)) {
      logger.trace(LogMarker.SERIALIZER_VERBOSE, "Read BigDecimal: {}", result);
    }
    return result;
  }

  private static void writeBigInteger(BigInteger o, DataOutput out) throws IOException {
    InternalDataSerializer.checkOut(out);

    if (logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE)) {
      logger.trace(LogMarker.SERIALIZER_VERBOSE, "Writing BigInteger: {}", o);
    }
    DataSerializer.writeByteArray(o.toByteArray(), out);
  }

  private static BigInteger readBigInteger(DataInput in) throws IOException {
    InternalDataSerializer.checkIn(in);
    BigInteger result = new BigInteger(Objects.requireNonNull(DataSerializer.readByteArray(in)));
    if (logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE)) {
      logger.trace(LogMarker.SERIALIZER_VERBOSE, "Read BigInteger: {}", result);
    }
    return result;
  }

  public static void writeUserDataSerializableHeader(int classId, DataOutput out)
      throws IOException {
    if (classId <= Byte.MAX_VALUE && classId >= Byte.MIN_VALUE) {
      out.writeByte(DSCODE.USER_DATA_SERIALIZABLE.toByte());
      out.writeByte(classId);
    } else if (classId <= Short.MAX_VALUE && classId >= Short.MIN_VALUE) {
      out.writeByte(DSCODE.USER_DATA_SERIALIZABLE_2.toByte());
      out.writeShort(classId);
    } else {
      out.writeByte(DSCODE.USER_DATA_SERIALIZABLE_4.toByte());
      out.writeInt(classId);
    }
  }

  /**
   * Writes given number of characters from array of {@code char}s to a {@code DataOutput}.
   *
   * @throws IOException A problem occurs while writing to {@code out}
   * @see DataSerializer#readCharArray
   * @since GemFire 6.6
   */
  public static void writeCharArray(char[] array, DataOutput out) throws IOException {
    checkOut(out);

    if (array == null) {
      writeArrayLength(-1, out);
    } else {
      final int length = array.length;
      if (logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE)) {
        logger.trace(LogMarker.SERIALIZER_VERBOSE, "Writing char array of length {}", length);
      }
      writeArrayLength(length, out);
      for (char character : array) {
        out.writeChar(character);
      }
    }
  }

  /**
   * returns true if the byte array is the serialized form of a null reference
   *
   * @param serializedForm the serialized byte array
   */
  public static boolean isSerializedNull(byte[] serializedForm) {
    return serializedForm.length == 1 && serializedForm[0] == DSCODE.NULL.toByte();
  }

  public static void basicWriteObject(Object o, DataOutput out, boolean ensurePdxCompatibility)
      throws IOException {
    checkOut(out);

    final boolean isDebugEnabled_SERIALIZER = logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE);
    if (isDebugEnabled_SERIALIZER) {
      logger.trace(LogMarker.SERIALIZER_VERBOSE, "basicWriteObject: {}", o);
    }

    // Handle special objects first
    if (o == null) {
      out.writeByte(DSCODE.NULL.toByte());
    } else if (o instanceof BasicSerializable) {
      checkPdxCompatible(o, ensurePdxCompatibility);
      BasicSerializable bs = (BasicSerializable) o;
      dsfidSerializer.write(bs, out);
    } else if (autoSerialized(o, out)) {
      // all done
    } else if (o instanceof DataSerializable.Replaceable) {
      // do this first to fix bug 31609
      // do this before DataSerializable
      Object replacement = ((DataSerializable.Replaceable) o).replace();
      basicWriteObject(replacement, out, ensurePdxCompatibility);

    } else if (o instanceof PdxSerializable) {
      writePdx(out, GemFireCacheImpl
          .getForPdx("PDX registry is unavailable because the Cache has been closed."), o, null);
    } else if (o instanceof DataSerializable) {
      if (isDebugEnabled_SERIALIZER) {
        logger.trace(LogMarker.SERIALIZER_VERBOSE, "Writing DataSerializable: {}", o);
      }
      checkPdxCompatible(o, ensurePdxCompatibility);

      Class c = o.getClass();
      // Is "c" a user class registered with an Instantiator?
      int classId = InternalInstantiator.getClassId(c);
      if (classId != 0) {
        writeUserDataSerializableHeader(classId, out);
      } else {
        out.writeByte(DSCODE.DATA_SERIALIZABLE.toByte());
        DataSerializer.writeClass(c, out);
      }
      invokeToData(o, out);
    } else if (o instanceof Sendable) {
      if (!(o instanceof PdxInstance) || o instanceof PdxInstanceEnum) {
        checkPdxCompatible(o, ensurePdxCompatibility);
      }
      ((Sendable) o).sendTo(out);
    } else if (writeWellKnownObject(o, out, ensurePdxCompatibility)) {
      // Nothing more to do...
    } else {
      checkPdxCompatible(o, ensurePdxCompatibility);
      if (logger.isTraceEnabled(LogMarker.SERIALIZER_ANNOUNCE_TYPE_WRITTEN_VERBOSE)) {
        logger.trace(LogMarker.SERIALIZER_ANNOUNCE_TYPE_WRITTEN_VERBOSE,
            "DataSerializer Serializing an instance of {}", o.getClass().getName());
      }

      /*
       * If the (internally known) ThreadLocal named "DataSerializer.DISALLOW_JAVA_SERIALIZATION" is
       * set, then an exception will be thrown if we try to do standard Java Serialization. This is
       * used to catch Java serialization early for the case where the data is being sent to a
       * non-Java client
       */
      if (disallowJavaSerialization() && o instanceof Serializable) {
        throw new NotSerializableException(
            String.format("%s is not DataSerializable and Java Serialization is disallowed",
                o.getClass().getName()));
      }

      writeSerializableObject(o, out);
    }
  }

  private static boolean disallowJavaSerialization() {
    Boolean v = DISALLOW_JAVA_SERIALIZATION.get();
    return v != null && v;
  }

  /**
   * @since GemFire 6.6.2
   */
  private static void writePdxEnum(Enum<?> e, DataOutput out) throws IOException {
    TypeRegistry tr =
        GemFireCacheImpl.getForPdx("PDX registry is unavailable because the Cache has been closed.")
            .getPdxRegistry();
    int eId = tr.getEnumId(e);
    if (logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE)) {
      logger.trace(LogMarker.SERIALIZER_VERBOSE, "write PdxEnum id={} enum={}", eId, e);
    }
    writePdxEnumId(eId, out);
  }

  public static void writePdxEnumId(int eId, DataOutput out) throws IOException {
    out.writeByte(DSCODE.PDX_ENUM.toByte());
    out.writeByte(eId >> 24);
    writeArrayLength(eId & 0xFFFFFF, out);
  }

  /**
   * @throws IOException since 6.6.2
   */
  private static Object readPdxEnum(DataInput in) throws IOException {
    int dsId = in.readByte();
    int tmp = readArrayLength(in);
    int enumId = dsId << 24 | tmp & 0xFFFFFF;
    if (logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE)) {
      logger.trace(LogMarker.SERIALIZER_VERBOSE, "read PdxEnum id={}", enumId);
    }
    InternalCache internalCache = GemFireCacheImpl
        .getForPdx("PDX registry is unavailable because the Cache has been closed.");
    TypeRegistry tr = internalCache.getPdxRegistry();

    Object result = tr.getEnumById(enumId);
    if (result instanceof PdxInstance) {
      getDMStats(internalCache).incPdxInstanceCreations();
    }
    return result;
  }

  private static void writeGemFireEnum(Enum<?> e, DataOutput out) throws IOException {
    boolean isGemFireObject = isGemfireObject(e);
    DataSerializer.writePrimitiveByte(
        isGemFireObject ? DSCODE.GEMFIRE_ENUM.toByte() : DSCODE.PDX_INLINE_ENUM.toByte(), out);
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
    InternalCache internalCache = getInternalCache();
    if (internalCache != null && internalCache.getPdxReadSerializedByAnyGemFireServices()) {
      String className = DataSerializer.readString(in);
      String enumName = DataSerializer.readString(in);
      int enumOrdinal = InternalDataSerializer.readArrayLength(in);
      getDMStats(internalCache).incPdxInstanceCreations();
      return new PdxInstanceEnum(className, enumName, enumOrdinal);
    } else {
      Enum<?> e = readGemFireEnum(in);
      InternalDataSerializer.readArrayLength(in);
      return e;
    }
  }

  /**
   * write an object in java Serializable form with a SERIALIZABLE DSCODE so that it can be
   * deserialized with DataSerializer.readObject()
   *
   * @param o the object to serialize
   * @param out the data output to serialize to
   */
  public static void writeSerializableObject(Object o, DataOutput out) throws IOException {
    out.writeByte(DSCODE.SERIALIZABLE.toByte());
    if (out instanceof ObjectOutputStream) {
      ((ObjectOutputStream) out).writeObject(o);
    } else {
      OutputStream stream;
      if (out instanceof OutputStream) {
        stream = (OutputStream) out;

      } else {
        final DataOutput out2 = out;
        stream = new OutputStream() {
          @Override
          public void write(int b) throws IOException {
            out2.write(b);
          }

          @Override
          public void write(byte[] b, int off, int len) throws IOException {
            out2.write(b, off, len);
          }
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
        if (stream instanceof VersionedDataStream) {
          KnownVersion v = ((VersionedDataStream) stream).getVersion();
          if (v != null && v != KnownVersion.CURRENT) {
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
   * For backward compatibility this method should be used to invoke toData on a DSFID or
   * DataSerializable. It will invoke the correct toData method based on the class's version
   * information. This method does not write information about the class of the object. When
   * deserializing use the method invokeFromData to read the contents of the object.
   *
   * @param serializableObject the object to write
   * @param out the output stream.
   */
  public static void invokeToData(Object serializableObject, DataOutput out) throws IOException {
    try {
      if (serializableObject instanceof BasicSerializable) {
        dsfidSerializer.invokeToData(serializableObject, out);
        return;
      }
      boolean invoked = false;
      KnownVersion v = StaticSerialization.getVersionForDataStreamOrNull(out);

      if (KnownVersion.CURRENT != v && v != null) {
        // get versions where DataOutput was upgraded
        KnownVersion[] versions = null;
        if (serializableObject instanceof SerializationVersions) {
          SerializationVersions sv = (SerializationVersions) serializableObject;
          versions = sv.getSerializationVersions();
        }
        // check if the version of the peer or diskstore is different and
        // there has been a change in the message
        if (versions != null) {
          for (KnownVersion version : versions) {
            // if peer version is less than the greatest upgraded version
            if (v.compareTo(version) < 0) {
              serializableObject.getClass().getMethod("toDataPre_" + version.getMethodSuffix(),
                  new Class[] {DataOutput.class}).invoke(serializableObject, out);
              invoked = true;
              break;
            }
          }
        }
      }

      if (!invoked) {
        ((DataSerializable) serializableObject).toData(out);
      }
    } catch (IOException io) {
      throw new ToDataException(
          "toData failed on DataSerializable " + serializableObject.getClass(), io);
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
          "toData failed on DataSerializable " + null == serializableObject ? "null"
              : serializableObject.getClass().toString(),
          t);
    }
  }

  /**
   * For backward compatibility this method should be used to invoke fromData on a DSFID or
   * DataSerializable. It will invoke the correct fromData method based on the class's version
   * information. This method does not read information about the class of the object. When
   * serializing use the method invokeToData to write the contents of the object.
   *
   * @param deserializableObject the object to write
   * @param in the input stream.
   */
  public static void invokeFromData(Object deserializableObject, DataInput in)
      throws IOException, ClassNotFoundException {
    if (deserializableObject instanceof BasicSerializable) {
      dsfidSerializer.invokeFromData(deserializableObject, in);
      return;
    }
    try {
      boolean invoked = false;
      KnownVersion v = StaticSerialization.getVersionForDataStreamOrNull(in);
      if (KnownVersion.CURRENT != v && v != null) {
        // get versions where DataOutput was upgraded
        KnownVersion[] versions = null;
        if (deserializableObject instanceof SerializationVersions) {
          SerializationVersions vds = (SerializationVersions) deserializableObject;
          versions = vds.getSerializationVersions();
        }
        // check if the version of the peer or diskstore is different and
        // there has been a change in the message
        if (versions != null) {
          for (KnownVersion version : versions) {
            // if peer version is less than the greatest upgraded version
            if (v.compareTo(version) < 0) {
              deserializableObject.getClass()
                  .getMethod("fromDataPre" + '_' + version.getMethodSuffix(),
                      new Class[] {DataInput.class})
                  .invoke(deserializableObject, in);
              invoked = true;
              break;
            }
          }
        }
      }
      if (!invoked) {
        ((DataSerializable) deserializableObject).fromData(in);

        if (logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE)) {
          logger.trace(LogMarker.SERIALIZER_VERBOSE, "Read DataSerializable {}",
              deserializableObject);
        }
      }
    } catch (EOFException | ClassNotFoundException | CacheClosedException | SocketException ex) {
      // client went away - ignore
      throw ex;
    } catch (Exception ex) {
      throw new SerializationException(
          String.format("Could not create an instance of %s .",
              deserializableObject.getClass().getName()),
          ex);
    }
  }

  @SuppressWarnings("unchecked")
  private static Object readDataSerializable(final DataInput in)
      throws IOException, ClassNotFoundException {
    final Class<?> c = readClass(in);
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
      throw new SerializationException(
          String.format("Could not create an instance of %s .",
              c.getName()),
          ex);
    }
  }

  public static void writeArrayLength(int len, DataOutput out) throws IOException {
    if (len == -1) {
      out.writeByte(StaticSerialization.NULL_ARRAY);
    } else if (len <= StaticSerialization.MAX_BYTE_ARRAY_LEN) {
      out.writeByte(len);
    } else if (len <= 0xFFFF) {
      out.writeByte(StaticSerialization.SHORT_ARRAY_LEN);
      out.writeShort(len);
    } else {
      out.writeByte(StaticSerialization.INT_ARRAY_LEN);
      out.writeInt(len);
    }
  }

  public static int readArrayLength(DataInput in) throws IOException {
    byte code = in.readByte();
    if (code == StaticSerialization.NULL_ARRAY) {
      return -1;
    } else {
      int result = ubyteToInt(code);
      if (result > StaticSerialization.MAX_BYTE_ARRAY_LEN) {
        if (code == StaticSerialization.SHORT_ARRAY_LEN) {
          return in.readUnsignedShort();
        } else if (code == StaticSerialization.INT_ARRAY_LEN) {
          return in.readInt();
        } else {
          throw new IllegalStateException("unexpected array length code=" + code);
        }
      }
      return result;
    }
  }

  private static Object readDSFID(final DataInput in, DSCODE dscode)
      throws IOException, ClassNotFoundException {
    if (logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE)) {
      logger.trace(LogMarker.SERIALIZER_VERBOSE, "readDSFID: header={}", dscode);
    }
    switch (dscode) {
      case DS_FIXED_ID_BYTE:
        return dsfidFactory.create(in.readByte(), in);
      case DS_FIXED_ID_SHORT:
        return dsfidFactory.create(in.readShort(), in);
      case DS_NO_FIXED_ID:
        return readDataSerializable(in);
      case DS_FIXED_ID_INT:
        return dsfidFactory.create(in.readInt(), in);
      default:
        throw new IllegalStateException("unexpected byte: " + dscode + " while reading dsfid");
    }
  }

  public static Object readDSFID(final DataInput in) throws IOException, ClassNotFoundException {
    checkIn(in);
    return readDSFID(in, DscodeHelper.toDSCODE(in.readByte()));
  }

  /**
   * Reads an instance of {@code String} from a {@code DataInput} given the header byte already
   * being read. The return value may be {@code null}.
   *
   * @throws IOException A problem occurs while reading from {@code in}
   * @since GemFire 5.7
   */
  private static String readString(DataInput in, DSCODE dscode) throws IOException {
    switch (dscode) {
      case STRING_BYTES:
        return readStringBytesFromDataInput(in, in.readUnsignedShort());
      case STRING:
        return readStringUTFFromDataInput(in);
      case NULL_STRING: {
        if (logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE)) {
          logger.trace(LogMarker.SERIALIZER_VERBOSE, "Reading NULL_STRING");
        }
        return null;
      }
      case HUGE_STRING_BYTES:
        return readStringBytesFromDataInput(in, in.readInt());
      case HUGE_STRING:
        return readHugeStringFromDataInput(in);
      default:
        throw new IOException("Unknown String header " + dscode);
    }
  }

  private static String readHugeStringFromDataInput(DataInput in) throws IOException {
    int len = in.readInt();
    if (logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE)) {
      logger.trace(LogMarker.SERIALIZER_VERBOSE, "Reading HUGE_STRING of len={}", len);
    }
    char[] buf = new char[len];
    for (int i = 0; i < len; i++) {
      buf[i] = in.readChar();
    }
    return new String(buf);
  }

  private static String readStringUTFFromDataInput(DataInput in) throws IOException {
    if (logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE)) {
      logger.trace(LogMarker.SERIALIZER_VERBOSE, "Reading utf STRING");
    }
    return in.readUTF();
  }

  private static String readStringBytesFromDataInput(DataInput dataInput, int len)
      throws IOException {
    if (logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE)) {
      logger.trace(LogMarker.SERIALIZER_VERBOSE, "Reading STRING_BYTES of len={}", len);
    }
    if (len == 0) {
      return "";
    }
    byte[] buf = StaticSerialization.getThreadLocalByteArray(len);
    dataInput.readFully(buf, 0, len);
    return new String(buf, 0, 0, len); // intentionally using deprecated constructor
  }

  public static String readString(DataInput in, byte header) throws IOException {
    return StaticSerialization.readString(in, header);
  }

  /**
   * Just like readObject but make sure and pdx deserialized is not a PdxInstance.
   *
   * @since GemFire 6.6.2
   */
  public static <T> T readNonPdxInstanceObject(final DataInput in)
      throws IOException, ClassNotFoundException {
    boolean wouldReadSerialized = PdxInstanceImpl.getPdxReadSerialized();
    if (!wouldReadSerialized) {
      return DataSerializer.readObject(in);
    } else {
      PdxInstanceImpl.setPdxReadSerialized(false);
      try {
        return DataSerializer.readObject(in);
      } finally {
        PdxInstanceImpl.setPdxReadSerialized(true);
      }
    }
  }

  public static Object basicReadObject(final DataInput in)
      throws IOException, ClassNotFoundException {
    checkIn(in);

    // Read the header byte
    byte header = in.readByte();
    DSCODE headerDSCode = DscodeHelper.toDSCODE(header);

    if (logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE)) {
      logger.trace(LogMarker.SERIALIZER_VERBOSE, "basicReadObject: header={}", header);
    }

    if (headerDSCode == null) {
      throw new IOException("Unknown header byte: " + header);
    }

    switch (headerDSCode) {
      case DS_FIXED_ID_BYTE:
        return dsfidFactory.create(in.readByte(), in);
      case DS_FIXED_ID_SHORT:
        return dsfidFactory.create(in.readShort(), in);
      case DS_FIXED_ID_INT:
        return dsfidFactory.create(in.readInt(), in);
      case DS_NO_FIXED_ID:
      case DATA_SERIALIZABLE:
        return readDataSerializable(in);
      case NULL:
      case NULL_STRING:
        return null;
      case STRING:
        return readStringUTFFromDataInput(in);
      case HUGE_STRING:
        return readHugeStringFromDataInput(in);
      case STRING_BYTES:
        return readStringBytesFromDataInput(in, in.readUnsignedShort());
      case HUGE_STRING_BYTES:
        return readStringBytesFromDataInput(in, in.readInt());
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
      case SERIALIZABLE:
        return readSerializable(in);
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
        throw new IOException("Unknown header byte: " + header);
    }

  }

  private static Serializable readSerializable(DataInput in)
      throws IOException, ClassNotFoundException {
    final boolean isDebugEnabled_SERIALIZER = logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE);
    Serializable serializableResult;
    if (in instanceof DSObjectInputStream) {
      serializableResult = (Serializable) ((DSObjectInputStream) in).readObject();
    } else {
      InputStream stream;
      if (in instanceof InputStream) {
        stream = (InputStream) in;
      } else {
        stream = new InputStream() {
          @Override
          public int read() throws IOException {
            try {
              return in.readUnsignedByte(); // fix for bug 47249
            } catch (EOFException ignored) {
              return -1;
            }
          }

        };
      }

      ObjectInput ois = new DSObjectInputStream(stream);
      serializationFilter.setFilterOn((ObjectInputStream) ois);
      if (stream instanceof VersionedDataStream) {
        KnownVersion v = ((VersionedDataStream) stream).getVersion();
        if (KnownVersion.CURRENT != v && v != null) {
          ois = new VersionedObjectInput(ois, v);
        }
      }

      serializableResult = (Serializable) ois.readObject();

      if (isDebugEnabled_SERIALIZER) {
        logger.trace(LogMarker.SERIALIZER_VERBOSE, "Read Serializable object: {}",
            serializableResult);
      }
    }
    if (isDebugEnabled_SERIALIZER) {
      logger.trace(LogMarker.SERIALIZER_VERBOSE, "deserialized instanceof {}",
          serializableResult.getClass());
    }
    return serializableResult;
  }

  private static Object readUserDataSerializable(final DataInput in, int classId)
      throws IOException {
    Instantiator instantiator = InternalInstantiator.getInstantiator(classId);
    if (instantiator == null) {
      logger.error(LogMarker.SERIALIZER_MARKER,
          "No Instantiator has been registered for class with id {}",
          classId);
      throw new IOException(
          String.format("No Instantiator has been registered for class with id %s",
              classId));

    } else {
      try {
        DataSerializable ds;
        if (instantiator instanceof CanonicalInstantiator) {
          CanonicalInstantiator ci = (CanonicalInstantiator) instantiator;
          ds = ci.newInstance(in);
        } else {
          ds = instantiator.newInstance();
        }
        ds.fromData(in);
        return ds;

      } catch (Exception ex) {
        throw new SerializationException(
            String.format("Could not deserialize an instance of %s",
                instantiator.getInstantiatedClass().getName()),
            ex);
      }
    }
  }

  public static boolean isPdxSerializationInProgress() {
    Boolean v = pdxSerializationInProgress.get();
    return v != null && v;
  }

  public static void setPdxSerializationInProgress(boolean inProgress) {
    pdxSerializationInProgress.set(inProgress);
  }

  public static boolean writePdx(DataOutput out, InternalCache internalCache, Object pdx,
      PdxSerializer pdxSerializer) throws IOException {

    // Hack to make sure we don't pass internal objects to the user's serializer
    if (pdxSerializer != null &&
        isGemfireObject(pdx)) {
      return false;
    }

    TypeRegistry tr = null;
    if (internalCache != null) {
      tr = internalCache.getPdxRegistry();
    }

    PdxOutputStream os;
    if (out instanceof HeapDataOutputStream) {
      os = new PdxOutputStream((HeapDataOutputStream) out);
    } else {
      os = new PdxOutputStream();
    }
    PdxWriterImpl writer = new PdxWriterImpl(tr, pdx, os);

    try {
      if (pdxSerializer != null) {
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
    } catch (ToDataException | CancelException | NonPortableClassException
        | GemFireRethrowable ex) {
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
      if (pdxSerializer != null) {
        throw new ToDataException("PdxSerializer failed when calling toData on " + pdx.getClass(),
            t);
      } else {
        throw new ToDataException("toData failed on PdxSerializable " + pdx.getClass(), t);
      }
    }
    int bytesWritten = writer.completeByteStreamGeneration();
    getDMStats(internalCache).incPdxSerialization(bytesWritten);
    if (!(out instanceof HeapDataOutputStream)) {
      writer.sendTo(out);
    }
    return true;
  }

  public static DMStats getDMStats(InternalCache internalCache) {
    if (internalCache != null) {
      return internalCache.getDistributionManager().getStats();
    } else {
      DMStats result = InternalDistributedSystem.getDMStats();
      if (result == null) {
        result = new LonerDistributionManager.DummyDMStats();
      }
      return result;
    }
  }

  private static Object readPdxSerializable(final DataInput in)
      throws IOException, ClassNotFoundException {
    int len = in.readInt();
    int typeId = in.readInt();

    InternalCache internalCache = GemFireCacheImpl
        .getForPdx("PDX registry is unavailable because the Cache has been closed.");
    PdxType pdxType = internalCache.getPdxRegistry().getType(typeId);
    if (logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE)) {
      logger.trace(LogMarker.SERIALIZER_VERBOSE, "readPdxSerializable pdxType={}", pdxType);
    }
    if (pdxType == null) {
      throw new IllegalStateException("Unknown pdx type=" + typeId);
    }

    DMStats dmStats = getDMStats(internalCache);
    dmStats.incPdxDeserialization(len + 9);

    // check if PdxInstance needs to be returned.
    if (pdxType.getNoDomainClass() || internalCache.getPdxReadSerializedByAnyGemFireServices()) {
      dmStats.incPdxInstanceCreations();
      return new PdxInstanceImpl(pdxType, in, len);
    } else {
      PdxReaderImpl pdxReader = new PdxReaderImpl(pdxType, in, len);
      return pdxReader.getObject();
    }
  }

  /**
   * Reads a PdxInstance from dataBytes and returns it. If the first object read is not pdx encoded
   * returns null.
   */
  public static PdxInstance readPdxInstance(final byte[] dataBytes, InternalCache internalCache) {
    try {
      byte type = dataBytes[0];
      if (type == DSCODE.PDX.toByte()) {
        PdxInputStream in = new PdxInputStream(dataBytes);
        in.readByte(); // throw away the type byte
        int len = in.readInt();
        int typeId = in.readInt();
        PdxType pdxType = internalCache.getPdxRegistry().getType(typeId);
        if (pdxType == null) {
          throw new IllegalStateException("Unknown pdx type=" + typeId);
        }

        return new PdxInstanceImpl(pdxType, in, len);
      } else if (type == DSCODE.PDX_ENUM.toByte()) {
        try (PdxInputStream in = new PdxInputStream(dataBytes)) {
          in.readByte(); // throw away the type byte
          int dsId = in.readByte();
          int tmp = readArrayLength(in);
          int enumId = dsId << 24 | tmp & 0xFFFFFF;
          TypeRegistry tr = internalCache.getPdxRegistry();
          EnumInfo ei = tr.getEnumInfoById(enumId);
          if (ei == null) {
            throw new IllegalStateException("Unknown pdx enum id=" + enumId);
          }
          return ei.getPdxInstance(enumId);
        }
      } else if (type == DSCODE.PDX_INLINE_ENUM.toByte()) {
        try (PdxInputStream in = new PdxInputStream(dataBytes)) {
          in.readByte(); // throw away the type byte
          String className = DataSerializer.readString(in);
          String enumName = DataSerializer.readString(in);
          int enumOrdinal = InternalDataSerializer.readArrayLength(in);
          return new PdxInstanceEnum(className, enumName, enumOrdinal);
        }
      }
    } catch (IOException ignore) {
    }
    return null;
  }

  public static int getLoadedDataSerializers() {
    return idsToSerializers.size();
  }

  public static Map getDsClassesToHoldersMap() {
    return dsClassesToHolders;
  }

  public static Map getIdsToHoldersMap() {
    return idsToHolders;
  }

  public static Map getSupportedClassesToHoldersMap() {
    return supportedClassesToHolders;
  }

  public static void writeObjectArray(Object[] array, DataOutput out, boolean ensureCompatibility)
      throws IOException {
    InternalDataSerializer.checkOut(out);
    int length = -1;
    if (array != null) {
      length = array.length;
    }
    InternalDataSerializer.writeArrayLength(length, out);
    if (logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE)) {
      logger.trace(LogMarker.SERIALIZER_VERBOSE, "Writing Object array of length {}", length);
    }
    if (length >= 0) {
      writeClass(array.getClass().getComponentType(), out);
      for (int i = 0; i < length; i++) {
        basicWriteObject(array[i], out, ensureCompatibility);
      }
    }
  }

  /**
   * Write a variable length long the old way (pre 7.0). Use this only in contexts where you might
   * need to communicate with pre 7.0 members or files.
   */
  public static void writeVLOld(long data, DataOutput out) throws IOException {
    if (data < 0) {
      Assert.fail("Data expected to be >=0 is " + data);
    }
    if (data <= StaticSerialization.MAX_BYTE_VL) {
      out.writeByte((byte) data);
    } else if (data <= 0x7FFF) {
      // set the sign bit to indicate a short
      out.write(((int) data >>> 8 | 0x80) & 0xFF);
      out.write((int) data & 0xFF);
    } else if (data <= Integer.MAX_VALUE) {
      out.writeByte(StaticSerialization.INT_VL);
      out.writeInt((int) data);
    } else {
      out.writeByte(StaticSerialization.LONG_VL);
      out.writeLong(data);
    }
  }

  /**
   * Write a variable length long the old way (pre 7.0). Use this only in contexts where you might
   * need to communicate with pre 7.0 members or files.
   */
  public static long readVLOld(DataInput in) throws IOException {
    byte code = in.readByte();
    long result;
    if (code < 0) {
      // mask off sign bit
      result = code & 0x7F;
      result <<= 8;
      result |= in.readByte() & 0xFF;
    } else if (code <= StaticSerialization.MAX_BYTE_VL) {
      result = code;
    } else if (code == StaticSerialization.INT_VL) {
      result = in.readInt();
    } else {
      result = in.readLong();
    }
    return result;
  }

  /**
   * Encode a long as a variable length array.
   *
   * This method is appropriate for unsigned integers. For signed integers, negative values will
   * always consume 10 bytes, so it is recommended to use writeSignedVL instead.
   *
   * This is taken from the varint encoding in protobufs (BSD licensed). See
   * https://developers.google.com/protocol-buffers/docs/encoding
   */
  public static void writeUnsignedVL(long data, DataOutput out) throws IOException {
    while (true) {
      if ((data & ~0x7FL) == 0) {
        out.writeByte((int) data);
        return;
      } else {
        out.writeByte((int) data & 0x7F | 0x80);
        data >>>= 7;
      }
    }
  }

  /**
   * Decode a long as a variable length array.
   *
   * This is taken from the varint encoding in protobufs (BSD licensed). See
   * https://developers.google.com/protocol-buffers/docs/encoding
   */
  public static long readUnsignedVL(DataInput in) throws IOException {
    int shift = 0;
    long result = 0;
    while (shift < 64) {
      final byte b = in.readByte();
      result |= (long) (b & 0x7F) << shift;
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
   * This method is appropriate for signed integers. It uses zig zag encoding to so that negative
   * numbers will be represented more compactly. For unsigned values, writeUnsignedVL will be more
   * efficient.
   */
  public static void writeSignedVL(long data, DataOutput out) throws IOException {
    writeUnsignedVL(encodeZigZag64(data), out);
  }

  /**
   * Decode a signed long as a variable length array.
   *
   * This method is appropriate for signed integers. It uses zig zag encoding to so that negative
   * numbers will be represented more compactly. For unsigned values, writeUnsignedVL will be more
   * efficient.
   */
  public static long readSignedVL(DataInput in) throws IOException {
    return decodeZigZag64(readUnsignedVL(in));
  }

  /**
   * Decode a ZigZag-encoded 64-bit value. ZigZag encodes signed integers into values that can be
   * efficiently encoded with varint. (Otherwise, negative values must be sign-extended to 64 bits
   * to be varint encoded, thus always taking 10 bytes on the wire.)
   *
   * @param n An unsigned 64-bit integer, stored in a signed int because Java has no explicit
   *        unsigned support.
   * @return A signed 64-bit integer.
   *
   *         This is taken from the varint encoding in protobufs (BSD licensed). See
   *         https://developers.google.com/protocol-buffers/docs/encoding
   */
  private static long decodeZigZag64(final long n) {
    return n >>> 1 ^ -(n & 1);
  }

  /**
   * Encode a ZigZag-encoded 64-bit value. ZigZag encodes signed integers into values that can be
   * efficiently encoded with varint. (Otherwise, negative values must be sign-extended to 64 bits
   * to be varint encoded, thus always taking 10 bytes on the wire.)
   *
   * @param n A signed 64-bit integer.
   * @return An unsigned 64-bit integer, stored in a signed int because Java has no explicit
   *         unsigned support.
   *
   *         This is taken from the varint encoding in protobufs (BSD licensed). See
   *         https://developers.google.com/protocol-buffers/docs/encoding
   */
  private static long encodeZigZag64(final long n) {
    // Note: the right-shift must be arithmetic
    return n << 1 ^ n >> 63;
  }

  /* test only method */
  public static int calculateBytesForTSandDSID(int dsid) {
    HeapDataOutputStream out = new HeapDataOutputStream(4 + 8, KnownVersion.CURRENT);
    long now = System.currentTimeMillis();
    try {
      writeUnsignedVL(now, out);
      writeUnsignedVL(InternalDataSerializer.encodeZigZag64(dsid), out);
    } catch (IOException ignored) {
      return 0;
    }
    return out.size();
  }

  @SuppressWarnings("unchecked")
  public static <T> Class<T> getCachedClass(String p_className) throws ClassNotFoundException {
    String className = processIncomingClassName(p_className);
    if (LOAD_CLASS_EACH_TIME) {
      return (Class<T>) ClassPathLoader.getLatest().forName(className);
    } else {
      Class<?> result = getExistingCachedClass(className);
      if (result == null) {
        // Do the forName call outside the sync to fix bug 46172
        result = ClassPathLoader.getLatest().forName(className);
        synchronized (cacheAccessLock) {
          Class<?> cachedClass = getExistingCachedClass(className);
          if (cachedClass == null) {
            classCache.put(className, new WeakReference<>(result));
          } else {
            result = cachedClass;
          }
        }
      }
      return (Class<T>) result;
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
      // Not locking classCache during clear as doing so causes a deadlock in the DeployedJar
      classCache.clear();
    }
  }

  public static DSFIDSerializer getDSFIDSerializer() {
    return dsfidSerializer;
  }

  /**
   * shortcut for getDSFIDSerializer().createDeserializationContext(), this should be used
   * when you need to create a deserialization "context" to pass to a fromData method
   * and don't have one available
   */
  public static DeserializationContext createDeserializationContext(DataInput in) {
    return dsfidSerializer.createDeserializationContext(in);
  }

  /**
   * shortcut for getDSFIDSerializer().createSerializationContext(), this should be used
   * when you need to create a deserialization "context" to pass to a toData method and
   * don't have a one available
   */
  public static SerializationContext createSerializationContext(DataOutput out) {
    return dsfidSerializer.createSerializationContext(out);
  }

  public static DSFIDFactory getDSFIDFactory() {
    return dsfidFactory;
  }

  /**
   * Any time new serialization format is added then a new enum needs to be added here.
   *
   * @since GemFire 6.6.2
   */
  private enum SERIALIZATION_VERSION {
    vINVALID,
    // includes 6.6.0.x and 6.6.1.x. Note that no serialization changes were made in 6.6 until 6.6.2
    v660,
    // 6.6.2.x or later NOTE if you add a new constant make sure and update "latestVersion".
    v662
  }

  /**
   * A listener whose listener methods are invoked when {@link DataSerializer}s and {@link
   * Instantiator}s are registered. This is part of the fix for bug 31422.
   *
   * @see InternalDataSerializer#addRegistrationListener
   * @see InternalDataSerializer#removeRegistrationListener
   */
  public interface RegistrationListener {

    /**
     * Invoked when a new {@code Instantiator} is {@linkplain Instantiator#register(Instantiator)
     * registered}.
     */
    void newInstantiator(Instantiator instantiator);

    /**
     * Invoked when a new {@code DataSerializer} is {@linkplain DataSerializer#register(Class)
     * registered}.
     */
    void newDataSerializer(DataSerializer ds);
  }

  /**
   * A SerializerAttributesHolder holds information required to load a DataSerializer and exists to
   * allow client/server connections to be created more quickly than they would if the
   * DataSerializer information downloaded from the server were used to immediately load the
   * corresponding classes.
   */
  public static class SerializerAttributesHolder {
    private String className = "";
    private EventID eventId = null;
    private ClientProxyMembershipID proxyId = null;
    private int id = 0;

    SerializerAttributesHolder() {}

    SerializerAttributesHolder(String name, EventID event, ClientProxyMembershipID proxy, int id) {
      this.className = name;
      this.eventId = event;
      this.proxyId = proxy;
      this.id = id;
    }

    /**
     * @return String the classname of the data serializer this instance represents.
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

    @Override
    public String toString() {
      return "SerializerAttributesHolder[name=" + this.className + ",id=" + this.id + ",eventId="
          + this.eventId + ']';
    }
  }

  /**
   * A marker object for {@code DataSerializer}s that have not been registered. Using this marker
   * object allows us to asynchronously send {@code DataSerializer} registration updates. If the
   * serialized bytes arrive at a VM before the registration message does, the deserializer will
   * wait an amount of time for the registration message to arrive.
   */
  abstract static class Marker {
    /**
     * The DataSerializer that is filled in upon registration
     */
    protected DataSerializer serializer = null;

    /**
     * set to true once setSerializer is called.
     */
    boolean hasBeenSet = false;

    abstract DataSerializer getSerializer();

    /**
     * Sets the serializer associated with this marker. It will notify any threads that are waiting
     * for the serializer to be registered.
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
   * A marker object for {@code DataSerializer}s that have not been registered. Using this marker
   * object allows us to asynchronously send {@code DataSerializer} registration updates. If the
   * serialized bytes arrive at a VM before the registration message does, the deserializer will
   * wait an amount of time for the registration message to arrive. Made public for unit test
   * access.
   *
   * @since GemFire 5.7
   */
  public static class GetMarker extends Marker {
    /**
     * Number of milliseconds to wait. Also used by InternalInstantiator. Note that some tests set
     * this to a small amount to speed up failures. Made public for unit test access.
     */
    @MutableForTesting
    public static int WAIT_MS = Integer.getInteger(
        GeodeGlossary.GEMFIRE_PREFIX + "InternalDataSerializer.WAIT_MS", 60 * 1000);

    /**
     * Returns the serializer associated with this marker. If the serializer has not been registered
     * yet, then this method will wait until the serializer is registered. If this method has to
     * wait for too long, then {@code null} is returned.
     */
    @Override
    DataSerializer getSerializer() {
      synchronized (this) {
        boolean firstTime = true;
        long endTime = 0;
        while (!this.hasBeenSet) {
          if (firstTime) {
            firstTime = false;
            endTime = System.currentTimeMillis() + WAIT_MS;
          }
          try {
            long remainingMs = endTime - System.currentTimeMillis();
            if (remainingMs > 0) {
              this.wait(remainingMs); // spurious wakeup ok
            } else {
              // timed out call setSerializer just to make sure that anyone else
              // also waiting on this marker times out also
              setSerializer(null);
              break;
            }
          } catch (InterruptedException ignored) {
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
   * A marker object for {@code DataSerializer}s that is in the process of being registered. It is
   * possible for getSerializer to return {@code null}
   *
   * @since GemFire 5.7
   */
  static class InitMarker extends Marker {
    /*
     * Returns the serializer associated with this marker. If the serializer has not been registered
     * yet, then this method will wait until the serializer is registered. If this method has to
     * wait for too long, then {@code null} is returned.
     */

    /**
     * Returns the serializer associated with this marker. Waits forever (unless interrupted) for it
     * to be initialized. Returns null if this Marker failed to initialize.
     */
    @Override
    DataSerializer getSerializer() {
      synchronized (this) {
        while (!this.hasBeenSet) {
          try {
            this.wait(); // spurious wakeup ok
          } catch (InterruptedException ignored) {
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
   * A distribution message that alerts other members of the distributed cache of a new {@code
   * DataSerializer} being registered.
   */
  public static class RegistrationMessage extends SerialDistributionMessage {
    /**
     * The versions in which this message was modified
     */
    @Immutable
    private static final KnownVersion[] dsfidVersions = new KnownVersion[] {};
    /**
     * The eventId of the {@code DataSerializer} that was registered
     */
    protected EventID eventId;
    /**
     * The id of the {@code DataSerializer} that was registered since 5.7 an int instead of a byte
     */
    private int id;
    /**
     * The name of the {@code DataSerializer} class
     */
    private String className;

    /**
     * Constructor for {@code DataSerializable}
     */
    public RegistrationMessage() {}

    /**
     * Creates a new {@code RegistrationMessage} that broadcasts that the given {@code
     * DataSerializer} was registered.
     */
    public RegistrationMessage(DataSerializer s) {
      this.className = s.getClass().getName();
      this.id = s.getId();
      this.eventId = (EventID) s.getEventId();
    }

    static String getFullMessage(Throwable t) {
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
    protected void process(ClusterDistributionManager dm) {
      if (CacheClientNotifier.getInstance() != null) {
        // This is a server so we need to send the dataserializer to clients
        // right away. For that we need to load the class as the constructor of
        // ClientDataSerializerMessage requires list of supported classes.
        Class<? extends DataSerializer> c;
        try {
          c = getCachedClass(this.className);
        } catch (ClassNotFoundException ex) {
          // fixes bug 44112
          logger.warn(
              "Could not load data serializer class {} so both clients of this server and this server will not have this data serializer. Load failed because: {}",
              this.className, getFullMessage(ex));
          return;
        }
        DataSerializer s;
        try {
          s = newInstance(c);
        } catch (IllegalArgumentException ex) {
          // fixes bug 44112
          logger.warn(
              "Could not create an instance of data serializer for class {} so both clients of this server and this server will not have this data serializer. Create failed because: {}",
              this.className, getFullMessage(ex));
          return;
        }
        s.setEventId(this.eventId);
        try {
          InternalDataSerializer._register(s, false);
        } catch (IllegalArgumentException | IllegalStateException ex) {
          logger.warn(
              "Could not register data serializer for class {} so both clients of this server and this server will not have this data serializer. Registration failed because: {}",
              this.className, getFullMessage(ex));
        }
      } else {
        try {
          InternalDataSerializer.register(this.className, false, this.eventId, null, this.id);
        } catch (IllegalArgumentException | IllegalStateException ex) {
          logger.warn(
              "Could not register data serializer for class {} so it will not be available in this JVM. Registration failed because: {}",
              this.className, getFullMessage(ex));
        }
      }
    }

    @Override
    public int getDSFID() {
      return IDS_REGISTRATION_MESSAGE;
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      DataSerializer.writeNonPrimitiveClassName(this.className, out);
      out.writeInt(this.id);
      DataSerializer.writeObject(this.eventId, out);
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      InternalDataSerializer.checkIn(in);
      this.className = DataSerializer.readNonPrimitiveClassName(in);
      this.id = in.readInt();
      this.eventId = DataSerializer.readObject(in);
    }

    @Override
    public String toString() {
      return String.format("Register DataSerializer %s of class %s",
          this.id, this.className);
    }

    @Override
    public KnownVersion[] getSerializationVersions() {
      return dsfidVersions;
    }
  }

  /**
   * An {@code ObjectInputStream} whose {@link #resolveClass} method loads classes from the current
   * context class loader.
   */
  private static class DSObjectInputStream extends ObjectInputStream {

    /**
     * Creates a new {@code DSObjectInputStream} that delegates its behavior to a given {@code
     * InputStream}.
     */
    DSObjectInputStream(InputStream stream) throws IOException {
      super(stream);
    }

    @Override
    protected Class resolveClass(ObjectStreamClass desc)
        throws IOException, ClassNotFoundException {

      String className = desc.getName();

      OldClientSupportService svc = getOldClientSupportService();
      if (svc != null) {
        className = svc.processIncomingClassName(className);
      }
      try {
        return getCachedClass(className);
      } catch (ClassNotFoundException ignored) {
        return super.resolveClass(desc);
      }
    }

    @Override
    protected Class resolveProxyClass(String[] interfaces) throws ClassNotFoundException {

      ClassLoader nonPublicLoader = null;
      boolean hasNonPublicInterface = false;

      // define proxy in class loader of non-public
      // interface(s), if any
      Class[] classObjs = new Class[interfaces.length];
      for (int i = 0; i < interfaces.length; i++) {
        Class cl = getCachedClass(interfaces[i]);
        if ((cl.getModifiers() & Modifier.PUBLIC) == 0) {
          if (hasNonPublicInterface) {
            if (nonPublicLoader != cl.getClassLoader()) {
              String s = "conflicting non-public interface class loaders";
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

  /**
   * Used to implement serialization code for the well known classes we support in DataSerializer.
   *
   * @since GemFire 5.7
   */
  protected abstract static class WellKnownDS extends DataSerializer {
    @Override
    public int getId() {
      // illegal for a customer to use but since our WellKnownDS is never registered
      // with this id it gives us one to use
      return 0;
    }

    @Override
    public Class[] getSupportedClasses() {
      // illegal for a customer to return null but we can do it since we never register
      // this serializer.
      return null;
    }

    @Override
    public Object fromData(DataInput in) throws IOException, ClassNotFoundException {
      throw new IllegalStateException("Should not be invoked");
    }
    // subclasses need to implement toData
  }

  /**
   * Just like a WellKnownDS but its type is compatible with PDX.
   */
  abstract static class WellKnownPdxDS extends WellKnownDS {
    // subclasses need to implement toData
  }
}
