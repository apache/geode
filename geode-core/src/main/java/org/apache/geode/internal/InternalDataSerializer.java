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

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
import java.net.URL;
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

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.CanonicalInstantiator;
import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.GemFireConfigException;
import org.apache.geode.GemFireIOException;
import org.apache.geode.GemFireRethrowable;
import org.apache.geode.Instantiator;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.SerializationException;
import org.apache.geode.SystemFailure;
import org.apache.geode.ToDataException;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.DistributedSystemService;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.LonerDistributionManager;
import org.apache.geode.distributed.internal.SerialDistributionMessage;
import org.apache.geode.i18n.StringId;
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
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.lang.ClassUtils;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.util.concurrent.CopyOnWriteHashMap;
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

/**
 * Contains static methods for data serializing instances of internal GemFire classes. It also
 * contains the implementation of the distribution messaging (and shared memory management) needed
 * to support data serialization.
 *
 * @since GemFire 3.5
 */
public abstract class InternalDataSerializer extends DataSerializer {
  private static final Logger logger = LogService.getLogger();

  /**
   * Maps Class names to their DataSerializer. This is used to find a DataSerializer during
   * serialization.
   */
  private static final Map<String, DataSerializer> classesToSerializers = new ConcurrentHashMap<>();


  /**
   * This list contains classes that Geode's classes subclass, such as antlr AST classes which are
   * used by our Object Query Language. It also contains certain classes that are DataSerializable
   * but end up being serialized as part of other serializable objects. VersionedObjectList, for
   * instance, is serialized as part of a partial putAll exception object.
   * <p>
   * Do not java-serialize objects that Geode does not have complete control over. This leaves us
   * open to security attacks such as Gadget Chains and compromises the ability to do a rolling
   * upgrade from one version of Geode to the next.
   * <p>
   * In general you shouldn't use java serialization and you should implement
   * DataSerializableFixedID for internal Geode objects. This gives you better control over
   * backward-compatibility.
   * <p>
   * Do not add to this list unless absolutely necessary. Instead put your classes either in the
   * sanctionedSerializables file for your module or in its excludedClasses file. Run
   * AnalyzeSerializables to generate the content for the file.
   * <p>
   */
  private static final String SANCTIONED_SERIALIZABLES_DEPENDENCIES_PATTERN =
      "java.**;javax.management.**" + ";javax.print.attribute.EnumSyntax" // used for some old enums
          + ";antlr.**" // query AST objects

          // old Admin API
          + ";org.apache.commons.modeler.AttributeInfo" + ";org.apache.commons.modeler.FeatureInfo"
          + ";org.apache.commons.modeler.ManagedBean"
          + ";org.apache.geode.distributed.internal.DistributionConfigSnapshot"
          + ";org.apache.geode.distributed.internal.RuntimeDistributionConfigImpl"
          + ";org.apache.geode.distributed.internal.DistributionConfigImpl"

          // WindowedExportFunction, RegionSnapshotService
          + ";org.apache.geode.distributed.internal.membership.InternalDistributedMember"
          // putAll
          + ";org.apache.geode.internal.cache.persistence.PersistentMemberID" // putAll
          + ";org.apache.geode.internal.cache.persistence.DiskStoreID" // putAll
          + ";org.apache.geode.internal.cache.tier.sockets.VersionedObjectList" // putAll

          // security services
          + ";org.apache.shiro.*;org.apache.shiro.authz.*;org.apache.shiro.authc.*"

          // export logs
          + ";org.apache.logging.log4j.Level" + ";org.apache.logging.log4j.spi.StandardLevel"

          // jar deployment
          + ";com.sun.proxy.$Proxy*" + ";com.healthmarketscience.rmiio.RemoteInputStream"
          + ";javax.rmi.ssl.SslRMIClientSocketFactory" + ";javax.net.ssl.SSLHandshakeException"
          + ";javax.net.ssl.SSLException;sun.security.validator.ValidatorException"
          + ";sun.security.provider.certpath.SunCertPathBuilderException"

          // geode-modules
          + ";org.apache.geode.modules.util.SessionCustomExpiry" + ";";


  private static InputStreamFilter defaultSerializationFilter = new EmptyInputStreamFilter();

  /**
   * A deserialization filter for ObjectInputStreams
   */
  private static InputStreamFilter serializationFilter = defaultSerializationFilter;

  private static final String serializationVersionTxt =
      System.getProperty(DistributionConfig.GEMFIRE_PREFIX + "serializationVersion");

  /**
   * support for old GemFire clients and WAN sites - needed to enable moving from GemFire to Geode
   */
  private static OldClientSupportService oldClientSupportService;

  /**
   * For backward compatibility we must swizzle the package of some classes that had to be moved
   * when GemFire was open- sourced. This preserves backward-compatibility.
   *
   * @param name the fully qualified class name
   * @return the name of the class in this implementation
   */
  public static String processIncomingClassName(String name) {
    // TCPServer classes are used before a cache exists and support for old clients has been
    // initialized
    String oldPackage = "com.gemstone.org.jgroups.stack.tcpserver";
    String newPackage = "org.apache.geode.distributed.internal.tcpserver";
    if (name.startsWith(oldPackage)) {
      return newPackage + name.substring(oldPackage.length());
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
   * @param name the fully qualified class name
   * @param out the consumer of the serialized object
   * @return the name of the class in this implementation
   */
  public static String processOutgoingClassName(String name, DataOutput out) {
    // TCPServer classes are used before a cache exists and support for old clients has been
    // initialized
    String oldPackage = "com.gemstone.org.jgroups.stack.tcpserver";
    String newPackage = "org.apache.geode.distributed.internal.tcpserver";
    if (name.startsWith(newPackage)) {
      return oldPackage + name.substring(newPackage.length());
    }
    OldClientSupportService svc = getOldClientSupportService();
    if (svc != null) {
      return svc.processOutgoingClassName(name, out);
    }
    return name;
  }

  /**
   * Initializes the optional serialization "white list" if the user has requested it in the
   * DistributionConfig
   *
   * @param distributionConfig the DistributedSystem configuration
   * @param services DistributedSystem services that might have classes to white-list
   */
  public static void initialize(DistributionConfig distributionConfig,
      Collection<DistributedSystemService> services) {
    logger.info("initializing InternalDataSerializer with {} services", services.size());
    if (distributionConfig.getValidateSerializableObjects()) {
      if (!ClassUtils.isClassAvailable("sun.misc.ObjectInputFilter")
          && !ClassUtils.isClassAvailable("java.io.ObjectInputFilter")) {
        throw new GemFireConfigException(
            "A serialization filter has been specified but this version of Java does not support serialization filters - ObjectInputFilter is not available");
      }
      serializationFilter =
          new ObjectInputStreamFilterWrapper(SANCTIONED_SERIALIZABLES_DEPENDENCIES_PATTERN
              + distributionConfig.getSerializableObjectFilter() + ";!*", services);
    } else {
      clearSerializationFilter();
    }
  }

  private static void clearSerializationFilter() {
    serializationFilter = defaultSerializationFilter;
  }


  /**
   * {@link DistributedSystemService}s that need to whitelist Serializable objects can use this to
   * read them from a file and then return them via
   * {@link DistributedSystemService#getSerializationWhitelist}
   */
  public static Collection<String> loadClassNames(URL sanctionedSerializables) throws IOException {
    Collection<String> result = new ArrayList(1000);
    InputStream inputStream = sanctionedSerializables.openStream();
    InputStreamReader reader = new InputStreamReader(inputStream);
    BufferedReader in = new BufferedReader(reader);
    try {
      String line;
      while ((line = in.readLine()) != null) {
        line = line.trim();
        if (line.startsWith("#") || line.startsWith("//")) {
          // comment line
        } else {
          line = line.replaceAll("/", ".");
          result.add(line.substring(0, line.indexOf(',')));
        }
      }
    } finally {
      inputStream.close();
    }
    // logger.info("loaded {} class names from {}", result.size(), sanctionedSerializables);
    return result;

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
   * Change this constant to be the last one in SERIALIZATION_VERSION
   */
  private static final SERIALIZATION_VERSION latestVersion = SERIALIZATION_VERSION.v662;

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

  private static final SERIALIZATION_VERSION serializationVersion = calculateSerializationVersion();

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

  static {
    initializeWellKnownSerializers();
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
          writePrimitiveClass(c, out);
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
    classesToSerializers.put(TimeUnit.NANOSECONDS.getClass().getName(), new WellKnownDS() {
      @Override
      public boolean toData(Object o, DataOutput out) throws IOException {
        out.writeByte(DSCODE.TIME_UNIT.toByte());
        out.writeByte(TIME_UNIT_NANOSECONDS);
        return true;
      }
    });
    classesToSerializers.put(TimeUnit.MICROSECONDS.getClass().getName(), new WellKnownDS() {
      @Override
      public boolean toData(Object o, DataOutput out) throws IOException {
        out.writeByte(DSCODE.TIME_UNIT.toByte());
        out.writeByte(TIME_UNIT_MICROSECONDS);
        return true;
      }
    });
    classesToSerializers.put(TimeUnit.MILLISECONDS.getClass().getName(), new WellKnownDS() {
      @Override
      public boolean toData(Object o, DataOutput out) throws IOException {
        out.writeByte(DSCODE.TIME_UNIT.toByte());
        out.writeByte(TIME_UNIT_MILLISECONDS);
        return true;
      }
    });
    classesToSerializers.put(TimeUnit.SECONDS.getClass().getName(), new WellKnownDS() {
      @Override
      public boolean toData(Object o, DataOutput out) throws IOException {
        out.writeByte(DSCODE.TIME_UNIT.toByte());
        out.writeByte(TIME_UNIT_SECONDS);
        return true;
      }
    });
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
   * Maps the id of a serializer to its {@code DataSerializer}.
   */
  private static final ConcurrentMap/* <Integer, DataSerializer|Marker> */ idsToSerializers =
      new ConcurrentHashMap();

  /**
   * Contains the classnames of the data serializers (and not the supported classes) not yet loaded
   * into the vm as keys and their corresponding holder instances as values.
   */
  private static final ConcurrentHashMap<String, SerializerAttributesHolder> dsClassesToHolders =
      new ConcurrentHashMap<>();

  /**
   * Contains the id of the data serializers not yet loaded into the vm as keys and their
   * corresponding holder instances as values.
   */
  private static final ConcurrentHashMap<Integer, SerializerAttributesHolder> idsToHolders =
      new ConcurrentHashMap<>();

  /**
   * Contains the classnames of supported classes as keys and their corresponding
   * SerializerAttributesHolder instances as values. This applies only to the data serializers which
   * have not been loaded into the vm.
   */
  private static final ConcurrentHashMap<String, SerializerAttributesHolder> supportedClassesToHolders =
      new ConcurrentHashMap<>();

  /**
   * {@code RegistrationListener}s that receive callbacks when {@code DataSerializer}s and {@code
   * Instantiator}s are registered. Note: copy-on-write access used for this set
   */
  private static volatile Set listeners = new HashSet();

  private static final Object listenersSync = new Object();

  /**
   * Convert the given unsigned byte to an int. The returned value will be in the range [0..255]
   * inclusive
   */
  private static int ubyteToInt(byte ub) {
    return ub & 0xFF;
  }

  public static void setOldClientSupportService(final OldClientSupportService svc) {
    oldClientSupportService = svc;
  }

  public static OldClientSupportService getOldClientSupportService() {
    return oldClientSupportService;
  }

  /**
   * Instantiates an instance of {@code DataSerializer}
   *
   * @throws IllegalArgumentException If the class can't be instantiated
   * @see DataSerializer#register(Class)
   */
  static DataSerializer newInstance(Class c) {
    if (!DataSerializer.class.isAssignableFrom(c)) {
      throw new IllegalArgumentException(
          LocalizedStrings.DataSerializer_0_DOES_NOT_EXTEND_DATASERIALIZER
              .toLocalizedString(c.getName()));
    }

    Constructor init;
    try {
      init = c.getDeclaredConstructor(new Class[0]);

    } catch (NoSuchMethodException ignored) {
      StringId s = LocalizedStrings.DataSerializer_CLASS_0_DOES_NOT_HAVE_A_ZEROARGUMENT_CONSTRUCTOR;
      Object[] args = new Object[] {c.getName()};
      if (c.getDeclaringClass() != null) {
        s =
            LocalizedStrings.DataSerializer_CLASS_0_DOES_NOT_HAVE_A_ZEROARGUMENT_CONSTRUCTOR_IT_IS_AN_INNER_CLASS_OF_1_SHOULD_IT_BE_A_STATIC_INNER_CLASS;
        args = new Object[] {c.getName(), c.getDeclaringClass()};
      }
      throw new IllegalArgumentException(s.toLocalizedString(args));
    }

    DataSerializer s;
    try {
      init.setAccessible(true);
      s = (DataSerializer) init.newInstance(new Object[0]);

    } catch (IllegalAccessException ignored) {
      throw new IllegalArgumentException(
          LocalizedStrings.DataSerializer_COULD_NOT_INSTANTIATE_AN_INSTANCE_OF_0
              .toLocalizedString(c.getName()));

    } catch (InstantiationException ex) {
      throw new IllegalArgumentException(
          LocalizedStrings.DataSerializer_COULD_NOT_INSTANTIATE_AN_INSTANCE_OF_0
              .toLocalizedString(c.getName()),
          ex);

    } catch (InvocationTargetException ex) {
      throw new IllegalArgumentException(
          LocalizedStrings.DataSerializer_WHILE_INSTANTIATING_AN_INSTANCE_OF_0
              .toLocalizedString(c.getName()),
          ex);
    }

    return s;
  }

  public static DataSerializer register(Class c, boolean distribute, EventID eventId,
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
  public static DataSerializer register(Class c, boolean distribute) {
    final DataSerializer s = newInstance(c);
    return _register(s, distribute);
  }

  public static DataSerializer _register(DataSerializer s, boolean distribute) {
    final int id = s.getId();
    DataSerializer dsForMarkers = s;
    if (id == 0) {
      throw new IllegalArgumentException(
          LocalizedStrings.InternalDataSerializer_CANNOT_CREATE_A_DATASERIALIZER_WITH_ID_0
              .toLocalizedString());
    }
    final Class[] classes = s.getSupportedClasses();
    if (classes == null || classes.length == 0) {
      final StringId msg =
          LocalizedStrings.InternalDataSerializer_THE_DATASERIALIZER_0_HAS_NO_SUPPORTED_CLASSES_ITS_GETSUPPORTEDCLASSES_METHOD_MUST_RETURN_AT_LEAST_ONE_CLASS;
      throw new IllegalArgumentException(msg.toLocalizedString(s.getClass().getName()));
    }

    for (Class aClass : classes) {
      if (aClass == null) {
        final StringId msg =
            LocalizedStrings.InternalDataSerializer_THE_DATASERIALIZER_GETSUPPORTEDCLASSES_METHOD_FOR_0_RETURNED_AN_ARRAY_THAT_CONTAINED_A_NULL_ELEMENT;
        throw new IllegalArgumentException(msg.toLocalizedString(s.getClass().getName()));
      } else if (aClass.isArray()) {
        final StringId msg =
            LocalizedStrings.InternalDataSerializer_THE_DATASERIALIZER_GETSUPPORTEDCLASSES_METHOD_FOR_0_RETURNED_AN_ARRAY_THAT_CONTAINED_AN_ARRAY_CLASS_WHICH_IS_NOT_ALLOWED_SINCE_ARRAYS_HAVE_BUILTIN_SUPPORT;
        throw new IllegalArgumentException(msg.toLocalizedString(s.getClass().getName()));
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
              LocalizedStrings.InternalDataSerializer_A_DATASERIALIZER_OF_CLASS_0_IS_ALREADY_REGISTERED_WITH_ID_1_SO_THE_DATASERIALIZER_OF_CLASS_2_COULD_NOT_BE_REGISTERED
                  .toLocalizedString(new Object[] {other.getClass().getName(), other.getId()}));
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
    InternalCache cache = GemFireCacheImpl.getInstance();
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
    // bridge servers send it all the clients irrelevant of
    // originator VM
    sendRegistrationMessageToClients(s);

    fireNewDataSerializer(s);

    return s;
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
            LocalizedStrings.InternalDataSerializer_A_DATASERIALIZER_OF_CLASS_0_IS_ALREADY_REGISTERED_WITH_ID_1_SO_THE_DATASERIALIZER_OF_CLASS_2_COULD_NOT_BE_REGISTERED
                .toLocalizedString(new Object[] {oldValue.getClass().getName(), oldValue.getId()}));
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

  private static void sendRegistrationMessageToServers(DataSerializer dataSerializer) {
    PoolManagerImpl.allPoolsRegisterDataSerializers(dataSerializer);
  }

  private static void sendRegistrationMessageToServers(SerializerAttributesHolder holder) {
    PoolManagerImpl.allPoolsRegisterDataSerializers(holder);
  }

  private static void sendRegistrationMessageToClients(DataSerializer dataSerializer) {
    InternalCache cache = GemFireCacheImpl.getInstance();
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
    InternalCache cache = GemFireCacheImpl.getInstance();
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
        } catch (ClassNotFoundException ignored) {
          logger.info(LogMarker.SERIALIZER_MARKER,
              LocalizedMessage.create(
                  LocalizedStrings.InternalDataSerializer_COULD_NOT_LOAD_DATASERIALIZER_CLASS_0,
                  dsClass));
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
        } catch (ClassNotFoundException ignored) {
          logger.info(LogMarker.SERIALIZER_MARKER,
              LocalizedMessage.create(
                  LocalizedStrings.InternalDataSerializer_COULD_NOT_LOAD_DATASERIALIZER_CLASS_0,
                  dsClass));
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
    for (Object v : idsToSerializers.values()) {
      if (v instanceof InitMarker) {
        v = ((Marker) v).getSerializer();
      }
      if (v instanceof DataSerializer) {
        coll.add(v);
      }
    }

    Iterator<Entry<String, SerializerAttributesHolder>> iterator =
        dsClassesToHolders.entrySet().iterator();
    while (iterator.hasNext()) {
      Entry<String, SerializerAttributesHolder> entry = iterator.next();
      String name = entry.getKey();
      SerializerAttributesHolder holder = entry.getValue();
      try {
        Class cl = getCachedClass(name);
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
        logger.info(LogMarker.SERIALIZER_MARKER, LocalizedMessage.create(
            LocalizedStrings.InternalDataSerializer_COULD_NOT_LOAD_DATASERIALIZER_CLASS_0, name));
      }
    }

    return (DataSerializer[]) coll.toArray(new DataSerializer[coll.size()]);
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

    return coll.toArray(new SerializerAttributesHolder[coll.size()]);
  }

  /**
   * Persist this class's map to out TODO: saveRegistrations is unused
   */
  public static void saveRegistrations(DataOutput out) throws IOException {
    for (Object v : idsToSerializers.values()) {
      if (v instanceof InitMarker) {
        v = ((Marker) v).getSerializer();
      }
      if (v instanceof DataSerializer) {
        DataSerializer ds = (DataSerializer) v;
        out.writeInt(ds.getId()); // since 5.7 an int instead of a byte
        DataSerializer.writeClass(ds.getClass(), out);
      }
    }
    if (!dsClassesToHolders.isEmpty()) {
      Iterator<Entry<String, SerializerAttributesHolder>> iterator =
          dsClassesToHolders.entrySet().iterator();
      Class dsClass = null;
      while (iterator.hasNext()) {
        try {
          dsClass = getCachedClass(iterator.next().getKey());
        } catch (ClassNotFoundException ignored) {
          logger.info(LogMarker.SERIALIZER_MARKER,
              LocalizedMessage.create(
                  LocalizedStrings.InternalDataSerializer_COULD_NOT_LOAD_DATASERIALIZER_CLASS_0,
                  dsClass));
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
   * Read the data from in and register it with this class. TODO: loadRegistrations is unused
   *
   * @throws IllegalArgumentException if a registration fails
   */
  public static void loadRegistrations(DataInput in) throws IOException {
    while (in.readInt() != 0) {
      Class dsClass = null;
      boolean skip = false;
      try {
        dsClass = DataSerializer.readClass(in);
      } catch (ClassNotFoundException ignored) {
        skip = true;
      }
      if (skip) {
        continue;
      }
      register(dsClass, /* dsId, */ true);
    }
  }

  /**
   * Adds a {@code RegistrationListener} that will receive callbacks when {@code DataSerializer}s
   * and {@code Instantiator}s are registered.
   */
  public static void addRegistrationListener(RegistrationListener l) {
    synchronized (listenersSync) {
      Set newSet = new HashSet(listeners);
      newSet.add(l);
      listeners = newSet;
    }
  }

  /**
   * Removes a {@code RegistrationListener} so that it no longer receives callbacks.
   */
  public static void removeRegistrationListener(RegistrationListener l) {
    synchronized (listenersSync) {
      Set newSet = new HashSet(listeners);
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
    for (Object listener1 : listeners) {
      RegistrationListener listener = (RegistrationListener) listener1;
      listener.newDataSerializer(ds);
    }
  }

  /**
   * Alerts all {@code RegistrationListener}s that a new {@code Instantiator} has been registered
   *
   * @see InternalDataSerializer.RegistrationListener#newInstantiator
   */
  static void fireNewInstantiator(Instantiator instantiator) {
    for (Object listener1 : listeners) {
      RegistrationListener listener = (RegistrationListener) listener1;
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
          LocalizedStrings.InternalDataSerializer_ATTEMPTED_TO_SERIALIZE_ILLEGAL_DSFID
              .toLocalizedString());
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
    if (dsfidToClassMap != null
        && logger.isTraceEnabled(LogMarker.SERIALIZER_WRITE_DSFID_VERBOSE)) {
      logger.trace(LogMarker.SERIALIZER_WRITE_DSFID_VERBOSE, "writeDSFID {} class={}", dsfid,
          o.getClass());
      if (dsfid != DataSerializableFixedID.NO_FIXED_ID
          && dsfid != DataSerializableFixedID.ILLEGAL) {
        // consistency check to make sure that the same DSFID is not used
        // for two different classes
        String newClassName = o.getClass().getName();
        String existingClassName = (String) dsfidToClassMap.putIfAbsent(dsfid, newClassName);
        if (existingClassName != null && !existingClassName.equals(newClassName)) {
          logger.trace(LogMarker.SERIALIZER_WRITE_DSFID_VERBOSE,
              "dsfid={} is used for class {} and class {}", dsfid, existingClassName, newClassName);
        }
      }
    }
    if (dsfid == DataSerializableFixedID.NO_FIXED_ID) {
      out.writeByte(DSCODE.DS_NO_FIXED_ID.toByte());
      DataSerializer.writeClass(o.getClass(), out);
    } else {
      writeDSFIDHeader(dsfid, out);
    }
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
            LocalizedStrings.DataSerializer_SERIALIZER_0_A_1_SAID_THAT_IT_COULD_SERIALIZE_AN_INSTANCE_OF_2_BUT_ITS_TODATA_METHOD_RETURNED_FALSE
                .toLocalizedString(serializer.getId(), serializer.getClass().getName(),
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
      throw new IOException(LocalizedStrings.DataSerializer_SERIALIZER_0_IS_NOT_REGISTERED
          .toLocalizedString(new Object[] {serializerId}));
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
      String s = "Null DataOutput";
      throw new NullPointerException(s);
    }
  }

  /**
   * Checks to make sure a {@code DataInput} is not {@code null}.
   *
   * @throws NullPointerException If {@code in} is {@code null}
   */
  public static void checkIn(DataInput in) {
    if (in == null) {
      String s = "Null DataInput";
      throw new NullPointerException(s);
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

    int size;
    if (set == null) {
      size = -1;
    } else {
      size = set.size();
    }
    writeArrayLength(size, out);
    if (logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE)) {
      logger.trace(LogMarker.SERIALIZER_VERBOSE, "Writing HashSet with {} elements: {}", size, set);
    }
    if (size > 0) {
      for (Object element : set) {
        writeObject(element, out);
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
   * Reads a {@code Set} from a {@code DataInput} into the given non-null collection. Returns true
   * if collection read is non-null else returns false. TODO: readCollection is unused
   *
   * @throws IOException A problem occurs while reading from {@code in}
   * @throws ClassNotFoundException The class of one of the {@code Set}'s elements cannot be found.
   * @see #writeSet
   */
  public static <E> boolean readCollection(DataInput in, Collection<E> c)
      throws IOException, ClassNotFoundException {

    checkIn(in);

    final int size = readArrayLength(in);
    if (size >= 0) {
      for (int index = 0; index < size; ++index) {
        E element = DataSerializer.readObject(in);
        c.add(element);
      }

      if (logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE)) {
        logger.trace(LogMarker.SERIALIZER_VERBOSE, "Read Collection with {} elements: {}", size, c);
      }
      return true;
    }
    return false;
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
      Set result = new HashSet(size);
      boolean longIDs = in.readBoolean();
      for (int i = 0; i < size; i++) {
        long l = longIDs ? in.readLong() : in.readInt();
        result.add(l);
      }
      return result;
    }
  }

  /**
   * write a set of Long objects TODO: writeListOfLongs is unused
   *
   * @param list the set of Long objects
   * @param hasLongIDs if false, write only ints, not longs
   * @param out the output stream
   */
  public static void writeListOfLongs(List list, boolean hasLongIDs, DataOutput out)
      throws IOException {
    if (list == null) {
      out.writeInt(-1);
    } else {
      out.writeInt(list.size());
      out.writeBoolean(hasLongIDs);
      for (Object aList : list) {
        Long l = (Long) aList;
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
  public static List<Long> readListOfLongs(DataInput in) throws IOException {
    int size = in.readInt();
    if (size < 0) {
      return null;
    } else {
      List result = new LinkedList();
      boolean longIDs = in.readBoolean();
      for (int i = 0; i < size; i++) {
        long l = longIDs ? in.readLong() : in.readInt();
        result.add(l);
      }
      return result;
    }
  }


  /**
   * Writes the type code for a primitive type Class to {@code DataOutput}.
   */
  public static void writePrimitiveClass(Class c, DataOutput out) throws IOException {
    if (c == Boolean.TYPE) {
      out.writeByte(DSCODE.BOOLEAN_TYPE.toByte());
    } else if (c == Character.TYPE) {
      out.writeByte(DSCODE.CHARACTER_TYPE.toByte());
    } else if (c == Byte.TYPE) {
      out.writeByte(DSCODE.BYTE_TYPE.toByte());
    } else if (c == Short.TYPE) {
      out.writeByte(DSCODE.SHORT_TYPE.toByte());
    } else if (c == Integer.TYPE) {
      out.writeByte(DSCODE.INTEGER_TYPE.toByte());
    } else if (c == Long.TYPE) {
      out.writeByte(DSCODE.LONG_TYPE.toByte());
    } else if (c == Float.TYPE) {
      out.writeByte(DSCODE.FLOAT_TYPE.toByte());
    } else if (c == Double.TYPE) {
      out.writeByte(DSCODE.DOUBLE_TYPE.toByte());
    } else if (c == Void.TYPE) {
      out.writeByte(DSCODE.VOID_TYPE.toByte());
    } else if (c == null) {
      out.writeByte(DSCODE.NULL.toByte());
    } else {
      throw new InternalGemFireError(
          LocalizedStrings.InternalDataSerializer_UNKNOWN_PRIMITIVE_TYPE_0
              .toLocalizedString(c.getName()));
    }
  }

  public static Class decodePrimitiveClass(byte typeCode) {
    if (typeCode == DSCODE.BOOLEAN_TYPE.toByte()) {
      return Boolean.TYPE;
    }
    if (typeCode == DSCODE.CHARACTER_TYPE.toByte()) {
      return Character.TYPE;
    }
    if (typeCode == DSCODE.BYTE_TYPE.toByte()) {
      return Byte.TYPE;
    }
    if (typeCode == DSCODE.SHORT_TYPE.toByte()) {
      return Short.TYPE;
    }
    if (typeCode == DSCODE.INTEGER_TYPE.toByte()) {
      return Integer.TYPE;
    }
    if (typeCode == DSCODE.LONG_TYPE.toByte()) {
      return Long.TYPE;
    }
    if (typeCode == DSCODE.FLOAT_TYPE.toByte()) {
      return Float.TYPE;
    }
    if (typeCode == DSCODE.DOUBLE_TYPE.toByte()) {
      return Double.TYPE;
    }
    if (typeCode == DSCODE.VOID_TYPE.toByte()) {
      return Void.TYPE;
    }
    if (typeCode == DSCODE.NULL.toByte()) {
      return null;
    }
    throw new InternalGemFireError(
        LocalizedStrings.InternalDataSerializer_UNEXPECTED_TYPECODE_0.toLocalizedString(typeCode));
  }

  private static final byte TIME_UNIT_NANOSECONDS = -1;
  private static final byte TIME_UNIT_MICROSECONDS = -2;
  private static final byte TIME_UNIT_MILLISECONDS = -3;
  private static final byte TIME_UNIT_SECONDS = -4;

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
        throw new IOException(
            LocalizedStrings.DataSerializer_UNKNOWN_TIMEUNIT_TYPE_0.toLocalizedString(type));
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
    BigInteger result = new BigInteger(DataSerializer.readByteArray(in));
    if (logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE)) {
      logger.trace(LogMarker.SERIALIZER_VERBOSE, "Read BigInteger: {}", result);
    }
    return result;
  }

  private static final ConcurrentMap dsfidToClassMap =
      logger.isTraceEnabled(LogMarker.SERIALIZER_WRITE_DSFID_VERBOSE) ? new ConcurrentHashMap()
          : null;

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
  public static void writeCharArray(char[] array, int length, DataOutput out) throws IOException {
    checkOut(out);

    if (array == null) {
      length = -1;
    }
    writeArrayLength(length, out);
    if (logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE)) {
      logger.trace(LogMarker.SERIALIZER_VERBOSE, "Writing char array of length {}", length);
    }
    if (length > 0) {
      for (int i = 0; i < length; i++) {
        out.writeChar(array[i]);
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

    } else if (o instanceof DataSerializableFixedID) {
      checkPdxCompatible(o, ensurePdxCompatibility);
      DataSerializableFixedID dsfid = (DataSerializableFixedID) o;
      writeDSFID(dsfid, out);
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
      DataSerializable ds = (DataSerializable) o;
      invokeToData(ds, out);

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
            LocalizedStrings.DataSerializer_0_IS_NOT_DATASERIALIZABLE_AND_JAVA_SERIALIZATION_IS_DISALLOWED
                .toLocalizedString(o.getClass().getName()));
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
    InternalCache internalCache = GemFireCacheImpl.getInstance();
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
          Version v = ((VersionedDataStream) stream).getVersion();
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
   * For backward compatibility this method should be used to invoke toData on a DSFID or
   * DataSerializable. It will invoke the correct toData method based on the class's version
   * information. This method does not write information about the class of the object. When
   * deserializing use the method invokeFromData to read the contents of the object.
   *
   * @param ds the object to write
   * @param out the output stream.
   */
  public static void invokeToData(Object ds, DataOutput out) throws IOException {
    boolean isDSFID = ds instanceof DataSerializableFixedID;
    try {
      boolean invoked = false;
      Version v = InternalDataSerializer.getVersionForDataStreamOrNull(out);

      if (v != null && v != Version.CURRENT) {
        // get versions where DataOutput was upgraded
        Version[] versions = null;
        if (ds instanceof SerializationVersions) {
          SerializationVersions sv = (SerializationVersions) ds;
          versions = sv.getSerializationVersions();
        }
        // check if the version of the peer or diskstore is different and
        // there has been a change in the message
        if (versions != null && versions.length > 0) {
          for (Version version : versions) {
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
        if (isDSFID) {
          ((DataSerializableFixedID) ds).toData(out);
        } else {
          ((DataSerializable) ds).toData(out);
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
        throw new ToDataException("toData failed on DataSerializable " + ds.getClass(), io);
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
          "toData failed on DataSerializable " + null == ds ? "null" : ds.getClass().toString(), t);
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
  public static void invokeFromData(Object ds, DataInput in)
      throws IOException, ClassNotFoundException {
    try {
      boolean invoked = false;
      Version v = InternalDataSerializer.getVersionForDataStreamOrNull(in);
      if (v != null && v != Version.CURRENT) {
        // get versions where DataOutput was upgraded
        Version[] versions = null;
        if (ds instanceof SerializationVersions) {
          SerializationVersions vds = (SerializationVersions) ds;
          versions = vds.getSerializationVersions();
        }
        // check if the version of the peer or diskstore is different and
        // there has been a change in the message
        if (versions != null && versions.length > 0) {
          for (Version version : versions) {
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
        if (ds instanceof DataSerializableFixedID) {
          ((DataSerializableFixedID) ds).fromData(in);
        } else {
          ((DataSerializable) ds).fromData(in);
        }
      }
    } catch (EOFException | ClassNotFoundException | CacheClosedException ex) {
      // client went away - ignore
      throw ex;
    } catch (Exception ex) {
      throw new SerializationException(
          LocalizedStrings.DataSerializer_COULD_NOT_CREATE_AN_INSTANCE_OF_0
              .toLocalizedString(ds.getClass().getName()),
          ex);
    }
  }


  private static Object readDataSerializable(final DataInput in)
      throws IOException, ClassNotFoundException {
    Class c = readClass(in);
    try {
      Constructor init = c.getConstructor(new Class[0]);
      init.setAccessible(true);
      Object o = init.newInstance(new Object[0]);
      Assert.assertTrue(o instanceof DataSerializable);
      invokeFromData(o, in);

      if (logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE)) {
        logger.trace(LogMarker.SERIALIZER_VERBOSE, "Read DataSerializable {}", o);
      }

      return o;

    } catch (EOFException ex) {
      // client went away - ignore
      throw ex;
    } catch (Exception ex) {
      throw new SerializationException(
          LocalizedStrings.DataSerializer_COULD_NOT_CREATE_AN_INSTANCE_OF_0
              .toLocalizedString(c.getName()),
          ex);
    }
  }

  private static Object readDataSerializableFixedID(final DataInput in)
      throws IOException, ClassNotFoundException {
    Class c = readClass(in);
    try {
      Constructor init = c.getConstructor(new Class[0]);
      init.setAccessible(true);
      Object o = init.newInstance(new Object[0]);

      invokeFromData(o, in);

      if (logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE)) {
        logger.trace(LogMarker.SERIALIZER_VERBOSE, "Read DataSerializableFixedID {}", o);
      }

      return o;

    } catch (Exception ex) {
      throw new SerializationException(
          LocalizedStrings.DataSerializer_COULD_NOT_CREATE_AN_INSTANCE_OF_0
              .toLocalizedString(c.getName()),
          ex);
    }
  }

  /**
   * Get the {@link Version} of the peer or disk store that created this {@link DataInput}.
   */
  public static Version getVersionForDataStream(DataInput in) {
    // check if this is a versioned data input
    if (in instanceof VersionedDataStream) {
      final Version v = ((VersionedDataStream) in).getVersion();
      return v != null ? v : Version.CURRENT;
    } else {
      // assume latest version
      return Version.CURRENT;
    }
  }

  /**
   * Get the {@link Version} of the peer or disk store that created this {@link DataInput}. Returns
   * null if the version is same as this member's.
   */
  public static Version getVersionForDataStreamOrNull(DataInput in) {
    // check if this is a versioned data input
    if (in instanceof VersionedDataStream) {
      return ((VersionedDataStream) in).getVersion();
    } else {
      // assume latest version
      return null;
    }
  }

  /**
   * Get the {@link Version} of the peer or disk store that created this {@link DataOutput}.
   */
  public static Version getVersionForDataStream(DataOutput out) {
    // check if this is a versioned data output
    if (out instanceof VersionedDataStream) {
      final Version v = ((VersionedDataStream) out).getVersion();
      return v != null ? v : Version.CURRENT;
    } else {
      // assume latest version
      return Version.CURRENT;
    }
  }

  /**
   * Get the {@link Version} of the peer or disk store that created this {@link DataOutput}. Returns
   * null if the version is same as this member's.
   */
  public static Version getVersionForDataStreamOrNull(DataOutput out) {
    // check if this is a versioned data output
    if (out instanceof VersionedDataStream) {
      return ((VersionedDataStream) out).getVersion();
    } else {
      // assume latest version
      return null;
    }
  }

  // array is null
  public static final byte NULL_ARRAY = -1;

  /**
   * array len encoded as unsigned short in next 2 bytes
   *
   * @since GemFire 5.7
   */
  private static final byte SHORT_ARRAY_LEN = -2;

  /**
   * array len encoded as int in next 4 bytes
   *
   * @since GemFire 5.7
   */
  public static final byte INT_ARRAY_LEN = -3;

  private static final int MAX_BYTE_ARRAY_LEN = (byte) -4 & 0xFF;

  public static void writeArrayLength(int len, DataOutput out) throws IOException {
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

  public static int readArrayLength(DataInput in) throws IOException {
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
   * Serializes a list of Integers. The argument may be null. Deserialize with
   * readListOfIntegers().
   *
   * TODO: writeListOfIntegers is unused
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
        out.writeInt(list.get(i));
      }
    }
  }

  public static Object readDSFID(final DataInput in) throws IOException, ClassNotFoundException {
    checkIn(in);
    byte header = in.readByte();
    if (logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE)) {
      logger.trace(LogMarker.SERIALIZER_VERBOSE, "readDSFID: header={}", header);
    }
    if (header == DSCODE.DS_FIXED_ID_BYTE.toByte()) {
      return DSFIDFactory.create(in.readByte(), in);
    } else if (header == DSCODE.DS_FIXED_ID_SHORT.toByte()) {
      return DSFIDFactory.create(in.readShort(), in);
    } else if (header == DSCODE.DS_NO_FIXED_ID.toByte()) {
      return readDataSerializableFixedID(in);
    } else if (header == DSCODE.DS_FIXED_ID_INT.toByte()) {
      return DSFIDFactory.create(in.readInt(), in);
    } else {
      throw new IllegalStateException("unexpected byte: " + header + " while reading dsfid");
    }
  }

  public static int readDSFIDHeader(final DataInput in) throws IOException {
    checkIn(in);
    byte header = in.readByte();
    if (header == DSCODE.DS_FIXED_ID_BYTE.toByte()) {
      return in.readByte();
    } else if (header == DSCODE.DS_FIXED_ID_SHORT.toByte()) {
      return in.readShort();
    } else if (header == DSCODE.DS_NO_FIXED_ID.toByte()) {
      // is that correct??
      return Integer.MAX_VALUE;
    } else if (header == DSCODE.DS_FIXED_ID_INT.toByte()) {
      return in.readInt();
    } else {
      throw new IllegalStateException("unexpected byte: " + header + " while reading dsfid");
    }
  }

  /**
   * Reads an instance of {@code String} from a {@code DataInput} given the header byte already
   * being read. The return value may be {@code null}.
   *
   * @throws IOException A problem occurs while reading from {@code in}
   * @since GemFire 5.7
   */
  public static String readString(DataInput in, byte header) throws IOException {
    if (header == DSCODE.STRING_BYTES.toByte()) {
      int len = in.readUnsignedShort();
      if (logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE)) {
        logger.trace(LogMarker.SERIALIZER_VERBOSE, "Reading STRING_BYTES of len={}", len);
      }
      byte[] buf = new byte[len];
      in.readFully(buf, 0, len);
      return new String(buf, 0); // intentionally using deprecated constructor
    } else if (header == DSCODE.STRING.toByte()) {
      if (logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE)) {
        logger.trace(LogMarker.SERIALIZER_VERBOSE, "Reading utf STRING");
      }
      return in.readUTF();
    } else if (header == DSCODE.NULL_STRING.toByte()) {
      if (logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE)) {
        logger.trace(LogMarker.SERIALIZER_VERBOSE, "Reading NULL_STRING");
      }
      return null;
    } else if (header == DSCODE.HUGE_STRING_BYTES.toByte()) {
      int len = in.readInt();
      if (logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE)) {
        logger.trace(LogMarker.SERIALIZER_VERBOSE, "Reading HUGE_STRING_BYTES of len={}", len);
      }
      byte[] buf = new byte[len];
      in.readFully(buf, 0, len);
      return new String(buf, 0); // intentionally using deprecated constructor
    } else if (header == DSCODE.HUGE_STRING.toByte()) {
      int len = in.readInt();
      if (logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE)) {
        logger.trace(LogMarker.SERIALIZER_VERBOSE, "Reading HUGE_STRING of len={}", len);
      }
      char[] buf = new char[len];
      for (int i = 0; i < len; i++) {
        buf[i] = in.readChar();
      }
      return new String(buf);
    } else {
      String s = "Unknown String header " + header;
      throw new IOException(s);
    }
  }

  private static DataSerializer dvddeserializer;

  // TODO: registerDVDDeserializer is unused
  public static void registerDVDDeserializer(DataSerializer dvddeslzr) {
    dvddeserializer = dvddeslzr;
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
    if (logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE)) {
      logger.trace(LogMarker.SERIALIZER_VERBOSE, "basicReadObject: header={}", header);
    }
    if (header == DSCODE.DS_FIXED_ID_BYTE.toByte()) {
      return DSFIDFactory.create(in.readByte(), in);
    }
    if (header == DSCODE.DS_FIXED_ID_SHORT.toByte()) {
      return DSFIDFactory.create(in.readShort(), in);
    }
    if (header == DSCODE.DS_FIXED_ID_INT.toByte()) {
      return DSFIDFactory.create(in.readInt(), in);
    }
    if (header == DSCODE.DS_NO_FIXED_ID.toByte()) {
      return readDataSerializableFixedID(in);
    }
    if (header == DSCODE.NULL.toByte()) {
      return null;
    }
    if (header == DSCODE.NULL_STRING.toByte() || header == DSCODE.STRING.toByte()
        || header == DSCODE.HUGE_STRING.toByte() || header == DSCODE.STRING_BYTES.toByte()
        || header == DSCODE.HUGE_STRING_BYTES.toByte()) {
      return readString(in, header);
    }
    if (header == DSCODE.CLASS.toByte()) {
      return readClass(in);
    }
    if (header == DSCODE.DATE.toByte()) {
      return readDate(in);
    }
    if (header == DSCODE.FILE.toByte()) {
      return readFile(in);
    }
    if (header == DSCODE.INET_ADDRESS.toByte()) {
      return readInetAddress(in);
    }
    if (header == DSCODE.BOOLEAN.toByte()) {
      return readBoolean(in);
    }
    if (header == DSCODE.CHARACTER.toByte()) {
      return readCharacter(in);
    }
    if (header == DSCODE.BYTE.toByte()) {
      return readByte(in);
    }
    if (header == DSCODE.SHORT.toByte()) {
      return readShort(in);
    }
    if (header == DSCODE.INTEGER.toByte()) {
      return readInteger(in);
    }
    if (header == DSCODE.LONG.toByte()) {
      return readLong(in);
    }
    if (header == DSCODE.FLOAT.toByte()) {
      return readFloat(in);
    }
    if (header == DSCODE.DOUBLE.toByte()) {
      return readDouble(in);
    }
    if (header == DSCODE.BYTE_ARRAY.toByte()) {
      return readByteArray(in);
    }
    if (header == DSCODE.ARRAY_OF_BYTE_ARRAYS.toByte()) {
      return readArrayOfByteArrays(in);
    }
    if (header == DSCODE.SHORT_ARRAY.toByte()) {
      return readShortArray(in);
    }
    if (header == DSCODE.STRING_ARRAY.toByte()) {
      return readStringArray(in);
    }
    if (header == DSCODE.INT_ARRAY.toByte()) {
      return readIntArray(in);
    }
    if (header == DSCODE.LONG_ARRAY.toByte()) {
      return readLongArray(in);
    }
    if (header == DSCODE.FLOAT_ARRAY.toByte()) {
      return readFloatArray(in);
    }
    if (header == DSCODE.DOUBLE_ARRAY.toByte()) {
      return readDoubleArray(in);
    }
    if (header == DSCODE.BOOLEAN_ARRAY.toByte()) {
      return readBooleanArray(in);
    }
    if (header == DSCODE.CHAR_ARRAY.toByte()) {
      return readCharArray(in);
    }
    if (header == DSCODE.OBJECT_ARRAY.toByte()) {
      return readObjectArray(in);
    }
    if (header == DSCODE.ARRAY_LIST.toByte()) {
      return readArrayList(in);
    }
    if (header == DSCODE.LINKED_LIST.toByte()) {
      return readLinkedList(in);
    }
    if (header == DSCODE.HASH_SET.toByte()) {
      return readHashSet(in);
    }
    if (header == DSCODE.LINKED_HASH_SET.toByte()) {
      return readLinkedHashSet(in);
    }
    if (header == DSCODE.HASH_MAP.toByte()) {
      return readHashMap(in);
    }
    if (header == DSCODE.IDENTITY_HASH_MAP.toByte()) {
      return readIdentityHashMap(in);
    }
    if (header == DSCODE.HASH_TABLE.toByte()) {
      return readHashtable(in);
    }
    if (header == DSCODE.CONCURRENT_HASH_MAP.toByte()) {
      return readConcurrentHashMap(in);
    }
    if (header == DSCODE.PROPERTIES.toByte()) {
      return readProperties(in);
    }
    if (header == DSCODE.TIME_UNIT.toByte()) {
      return readTimeUnit(in);
    }
    if (header == DSCODE.USER_CLASS.toByte()) {
      return readUserObject(in, in.readByte());
    }
    if (header == DSCODE.USER_CLASS_2.toByte()) {
      return readUserObject(in, in.readShort());
    }
    if (header == DSCODE.USER_CLASS_4.toByte()) {
      return readUserObject(in, in.readInt());
    }
    if (header == DSCODE.VECTOR.toByte()) {
      return readVector(in);
    }
    if (header == DSCODE.STACK.toByte()) {
      return readStack(in);
    }
    if (header == DSCODE.TREE_MAP.toByte()) {
      return readTreeMap(in);
    }
    if (header == DSCODE.TREE_SET.toByte()) {
      return readTreeSet(in);
    }
    if (header == DSCODE.BOOLEAN_TYPE.toByte()) {
      return Boolean.TYPE;
    }
    if (header == DSCODE.CHARACTER_TYPE.toByte()) {
      return Character.TYPE;
    }
    if (header == DSCODE.BYTE_TYPE.toByte()) {
      return Byte.TYPE;
    }
    if (header == DSCODE.SHORT_TYPE.toByte()) {
      return Short.TYPE;
    }
    if (header == DSCODE.INTEGER_TYPE.toByte()) {
      return Integer.TYPE;
    }
    if (header == DSCODE.LONG_TYPE.toByte()) {
      return Long.TYPE;
    }
    if (header == DSCODE.FLOAT_TYPE.toByte()) {
      return Float.TYPE;
    }
    if (header == DSCODE.DOUBLE_TYPE.toByte()) {
      return Double.TYPE;
    }
    if (header == DSCODE.VOID_TYPE.toByte()) {
      return Void.TYPE;
    }
    if (header == DSCODE.USER_DATA_SERIALIZABLE.toByte()) {
      return readUserDataSerializable(in, in.readByte());
    }
    if (header == DSCODE.USER_DATA_SERIALIZABLE_2.toByte()) {
      return readUserDataSerializable(in, in.readShort());
    }
    if (header == DSCODE.USER_DATA_SERIALIZABLE_4.toByte()) {
      return readUserDataSerializable(in, in.readInt());
    }
    if (header == DSCODE.DATA_SERIALIZABLE.toByte()) {
      return readDataSerializable(in);
    }
    if (header == DSCODE.SERIALIZABLE.toByte()) {
      final boolean isDebugEnabled_SERIALIZER = logger.isTraceEnabled(LogMarker.SERIALIZER_VERBOSE);
      Object serializableResult;
      if (in instanceof DSObjectInputStream) {
        serializableResult = ((DSObjectInputStream) in).readObject();
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
          Version v = ((VersionedDataStream) stream).getVersion();
          if (v != null && v != Version.CURRENT) {
            ois = new VersionedObjectInput(ois, v);
          }
        }

        serializableResult = ois.readObject();

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
    if (header == DSCODE.PDX.toByte()) {
      return readPdxSerializable(in);
    }
    if (header == DSCODE.PDX_ENUM.toByte()) {
      return readPdxEnum(in);
    }
    if (header == DSCODE.GEMFIRE_ENUM.toByte()) {
      return readGemFireEnum(in);
    }
    if (header == DSCODE.PDX_INLINE_ENUM.toByte()) {
      return readPdxInlineEnum(in);
    }
    if (header == DSCODE.BIG_INTEGER.toByte()) {
      return readBigInteger(in);
    }
    if (header == DSCODE.BIG_DECIMAL.toByte()) {
      return readBigDecimal(in);
    }
    if (header == DSCODE.UUID.toByte()) {
      return readUUID(in);
    }
    if (header == DSCODE.TIMESTAMP.toByte()) {
      return readTimestamp(in);
    }

    String s = "Unknown header byte: " + header;
    throw new IOException(s);
  }

  private static Object readUserDataSerializable(final DataInput in, int classId)
      throws IOException {
    Instantiator instantiator = InternalInstantiator.getInstantiator(classId);
    if (instantiator == null) {
      logger.error(LogMarker.SERIALIZER_MARKER,
          LocalizedMessage.create(
              LocalizedStrings.DataSerializer_NO_INSTANTIATOR_HAS_BEEN_REGISTERED_FOR_CLASS_WITH_ID_0,
              classId));
      throw new IOException(
          LocalizedStrings.DataSerializer_NO_INSTANTIATOR_HAS_BEEN_REGISTERED_FOR_CLASS_WITH_ID_0
              .toLocalizedString(classId));

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
            LocalizedStrings.DataSerializer_COULD_NOT_DESERIALIZE_AN_INSTANCE_OF_0
                .toLocalizedString(instantiator.getInstantiatedClass().getName()),
            ex);
      }
    }
  }

  private static final ThreadLocal<Boolean> pdxSerializationInProgress = new ThreadLocal<>();

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

  public static boolean writePdx(DataOutput out, InternalCache internalCache, Object pdx,
      PdxSerializer pdxSerializer) throws IOException {
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
        // Hack to make sure we don't pass internal objects to the user's
        // serializer
        if (isGemfireObject(pdx)) {
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
        PdxInputStream in = new PdxInputStream(dataBytes);
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
      } else if (type == DSCODE.PDX_INLINE_ENUM.toByte()) {
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
    public static int WAIT_MS = Integer.getInteger(
        DistributionConfig.GEMFIRE_PREFIX + "InternalDataSerializer.WAIT_MS", 60 * 1000);

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
     * The id of the {@code DataSerializer} that was registered since 5.7 an int instead of a byte
     */
    private int id;

    /**
     * The eventId of the {@code DataSerializer} that was registered
     */
    protected EventID eventId;

    /**
     * The name of the {@code DataSerializer} class
     */
    private String className;

    /**
     * The versions in which this message was modified
     */
    private static final Version[] dsfidVersions = new Version[] {};

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
        Class<?> c;
        try {
          c = getCachedClass(this.className); // fix for bug 41206
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
        } catch (IllegalArgumentException ex) {
          logger.warn(
              "Could not register data serializer for class {} so both clients of this server and this server will not have this data serializer. Registration failed because: {}",
              this.className, getFullMessage(ex));
        } catch (IllegalStateException ex) {
          logger.warn(
              "Could not register data serializer for class {} so both clients of this server and this server will not have this data serializer. Registration failed because: {}",
              this.className, getFullMessage(ex));
        }
      } else {
        try {
          InternalDataSerializer.register(this.className, false, this.eventId, null, this.id);
        } catch (IllegalArgumentException ex) {
          logger.warn(
              "Could not register data serializer for class {} so it will not be available in this JVM. Registration failed because: {}",
              this.className, getFullMessage(ex));
        } catch (IllegalStateException ex) {
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
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      DataSerializer.writeNonPrimitiveClassName(this.className, out);
      out.writeInt(this.id);
      DataSerializer.writeObject(this.eventId, out);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      InternalDataSerializer.checkIn(in);
      this.className = DataSerializer.readNonPrimitiveClassName(in);
      this.id = in.readInt();
      this.eventId = (EventID) DataSerializer.readObject(in);
    }

    @Override
    public String toString() {
      return LocalizedStrings.InternalDataSerializer_REGISTER_DATASERIALIZER_0_OF_CLASS_1
          .toLocalizedString(this.id, this.className);
    }

    @Override
    public Version[] getSerializationVersions() {
      return dsfidVersions;
    }
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
    protected Class resolveProxyClass(String[] interfaces)
        throws IOException, ClassNotFoundException {

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
      throw new IllegalStateException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
    }
    // subclasses need to implement toData
  }

  /**
   * Just like a WellKnownDS but its type is compatible with PDX.
   */
  protected abstract static class WellKnownPdxDS extends WellKnownDS {
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

  // Variable Length long encoded as int in next 4 bytes
  private static final byte INT_VL = 126;

  // Variable Length long encoded as long in next 8 bytes
  private static final byte LONG_VL = 127;

  private static final int MAX_BYTE_VL = 125;

  /**
   * Write a variable length long the old way (pre 7.0). Use this only in contexts where you might
   * need to communicate with pre 7.0 members or files.
   */
  public static void writeVLOld(long data, DataOutput out) throws IOException {
    if (data < 0) {
      Assert.fail("Data expected to be >=0 is " + data);
    }
    if (data <= MAX_BYTE_VL) {
      out.writeByte((byte) data);
    } else if (data <= 0x7FFF) {
      // set the sign bit to indicate a short
      out.write(((int) data >>> 8 | 0x80) & 0xFF);
      out.write((int) data >>> 0 & 0xFF);
    } else if (data <= Integer.MAX_VALUE) {
      out.writeByte(INT_VL);
      out.writeInt((int) data);
    } else {
      out.writeByte(LONG_VL);
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
    HeapDataOutputStream out = new HeapDataOutputStream(4 + 8, Version.CURRENT);
    long now = System.currentTimeMillis();
    try {
      writeUnsignedVL(now, out);
      writeUnsignedVL(InternalDataSerializer.encodeZigZag64(dsid), out);
    } catch (IOException ignored) {
      return 0;
    }
    return out.size();
  }

  public static final boolean LOAD_CLASS_EACH_TIME =
      Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "loadClassOnEveryDeserialization");

  private static final CopyOnWriteHashMap<String, WeakReference<Class<?>>> classCache =
      LOAD_CLASS_EACH_TIME ? null : new CopyOnWriteHashMap<>();

  private static final Object cacheAccessLock = new Object();

  public static Class<?> getCachedClass(String p_className) throws ClassNotFoundException {
    String className = processIncomingClassName(p_className);
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
            classCache.put(className, new WeakReference<>(result));
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
      // Not locking classCache during clear as doing so causes a deadlock in the DeployedJar
      classCache.clear();
    }
  }
}
