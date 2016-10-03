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
package org.apache.geode;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.logging.log4j.Logger;

import org.apache.geode.admin.RegionNotFoundException;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.DSCODE;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.ObjToByteArraySerializer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.OldClientSupportService;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.offheap.StoredObject;
import org.apache.geode.pdx.PdxInstance;

/**
 * Provides static helper methods for reading and writing
 * non-primitive data when working with a {@link DataSerializable}.
 * For instance, classes that implement <code>DataSerializable</code>
 * can use the <code>DataSerializer</code> in their
 * <code>toData</code> and <code>fromData</code> methods:
 *
 * <!--
 * The source code for the Employee class resides in
 *         tests/com/examples/ds/Employee.java
 * Please keep the below code snippet in sync with that file.
 * -->
 *
 * <PRE>
public class Employee implements DataSerializable {
  private int id;
  private String name;
  private Date birthday;
  private Company employer;

  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.id);
    out.writeUTF(this.name);
    DataSerializer.writeDate(this.birthday, out);
    DataSerializer.writeObject(this.employer, out);
  }

  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {

    this.id = in.readInt();
    this.name = in.readUTF();
    this.birthday = DataSerializer.readDate(in);
    this.employer = (Company) DataSerializer.readObject(in);
  }
}

 * </PRE>
 *
 * <P>
 *
 * Instances of <code>DataSerializer</code> are used to data serialize
 * objects (such as instances of standard Java classes or third-party
 * classes for which the source code is not available) that do not
 * implement the <code>DataSerializable</code> interface.
 *
 * <P>
 *
 * The following <code>DataSerializer</code> data serializes instances
 * of <code>Company</code>.  In order for the data serialization
 * framework to consult this custom serializer, it must be {@linkplain
 * #register(Class) registered} with the framework.
 *
 * <!--
 * The source code for the CompanySerializer class resides in
 *         tests/com/examples/ds/CompanySerializer.java
 * Please keep the below code snippet in sync with that file.
 * -->
 *
 * <PRE>
public class CompanySerializer extends DataSerializer {

  static {
    DataSerializer.register(CompanySerializer.class);
  }

  &#47;**
   * May be invoked reflectively if instances of Company are
   * distributed to other VMs.
   *&#47;
  public CompanySerializer() {

  }

  public Class[] getSupportedClasses() {
    return new Class[] { Company.class };
  }
  public int getId() {
    return 42;
  }

  public boolean toData(Object o, DataOutput out)
    throws IOException {
    if (o instanceof Company) {
      Company company = (Company) o;
      out.writeUTF(company.getName());

      // Let's assume that Address is java.io.Serializable
      Address address = company.getAddress();
      writeObject(address, out);
      return true;

    } else {
      return false;
    }
  }

  public Object fromData(DataInput in)
    throws IOException, ClassNotFoundException {

    String name = in.readUTF();
    Address address = (Address) readObject(in);
    return new Company(name, address);
  }
}
 * </PRE>
 *
 * Just like {@link Instantiator}s, a <code>DataSerializer</code> may
 * be sent to other members of the distributed system when it is
 * {@linkplain #register(Class) registered}.  The data serialization
 * framework does not require that a <code>DataSerializer</code> be
 * {@link Serializable}, but it does require that it provide a
 * {@linkplain #DataSerializer() zero-argument constructor}.
 *
 * @see #writeObject(Object, DataOutput)
 * @see #readObject
 *
 * @since GemFire 3.5 */
public abstract class DataSerializer {
  
  private static final Logger logger = LogService.getLogger();
  
  /** The eventId of this <code>DataSerializer</code> */
  private EventID eventId;

  /** The originator of this <code>DataSerializer</code> */
  private ClientProxyMembershipID context;

  protected static final boolean TRACE_SERIALIZABLE =
    Boolean.getBoolean("DataSerializer.TRACE_SERIALIZABLE");

  /* Used to prevent standard Java serialization when sending data to a non-Java client */
  protected static final ThreadLocal<Boolean> DISALLOW_JAVA_SERIALIZATION = new ThreadLocal<Boolean>();

  //////////////////////  Instance Fields  /////////////////////

  //////////////////////  Static Methods  //////////////////////

  /**
   * Writes an instance of <code>Class</code> to a
   * <code>DataOutput</code>.
   * This method will handle a
   * <code>null</code> value and not throw a
   * <code>NullPointerException</code>.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   *
   * @see #readClass
   */
  public static void writeClass(Class<?> c, DataOutput out)
    throws IOException {

    InternalDataSerializer.checkOut(out);

    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing Class {}", c);
    }

    if (c == null || c.isPrimitive()) {
      InternalDataSerializer.writePrimitiveClass(c, out);
    }
    else {
      // non-primitive classes have a second CLASS byte
      // if readObject/writeObject is called:
      // the first CLASS byte indicates it's a Class, the second
      // one indicates it's a non-primitive Class
      out.writeByte(DSCODE.CLASS);
      String cname = c.getName();
      cname = swizzleClassNameForWrite(cname, out);
      writeString(cname, out);
    }
  }

  /**
   * Writes class name to a <code>DataOutput</code>. This method will handle a
   * <code>null</code> value and not throw a <code>NullPointerException</code>.
   * 
   * @throws IOException
   *           A problem occurs while writing to <code>out</code>
   * 
   * @see #readNonPrimitiveClassName(DataInput)
   */
  public static void writeNonPrimitiveClassName(String className, DataOutput out)
    throws IOException {

    InternalDataSerializer.checkOut(out);

    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing Class name {}", className);
    }

    writeString(swizzleClassNameForWrite(className, out), out);
  }

  /**
   * Reads an instance of <code>Class</code> from a
   * <code>DataInput</code>.  The class will be loaded using the
   * {@linkplain Thread#getContextClassLoader current content class
   * loader}.
   * The return value may be <code>null</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   * @throws ClassNotFoundException
   *         The class cannot be loaded
   */
  public static Class<?> readClass(DataInput in)
    throws IOException, ClassNotFoundException {

    InternalDataSerializer.checkIn(in);

    byte typeCode = in.readByte();
    if (typeCode == DSCODE.CLASS) {
      String className = readString(in);
      className = swizzleClassNameForRead(className, in);
      Class<?> c = InternalDataSerializer.getCachedClass(className); // fix for bug 41206
      return c;
    }
    else {
      return InternalDataSerializer.decodePrimitiveClass(typeCode);
    }
  }
  
  /**
   * For backward compatibility we must swizzle the package of
   * some classes that had to be moved when GemFire was open-
   * sourced.  This preserves backward-compatibility.
   * 
   * @param name the fully qualified class name
   * @param in the source of the class name
   * @return the name of the class in this implementation
   */
  private static String swizzleClassNameForRead(String name, DataInput in) {
    // TCPServer classes are used before a cache exists and support for old clients has been initialized
    String oldPackage = "com.gemstone.org.jgroups.stack.tcpserver";
    String newPackage = "org.apache.geode.distributed.internal.tcpserver";
    if (name.startsWith(oldPackage)) {
      return newPackage + name.substring(oldPackage.length());
    }
    OldClientSupportService svc = InternalDataSerializer.getOldClientSupportService();
    if (svc != null) {
      return svc.processIncomingClassName(name, in);
    }
    return name;
  }
  
  /**
   * For backward compatibility we must swizzle the package of
   * some classes that had to be moved when GemFire was open-
   * sourced.  This preserves backward-compatibility.
   * 
   * @param name the fully qualified class name
   * @param out the consumer of the serialized object
   * @return the name of the class in this implementation
   */
  private static String swizzleClassNameForWrite(String name, DataOutput out) {
    // TCPServer classes are used before a cache exists and support for old clients has been initialized
    String oldPackage = "com.gemstone.org.jgroups.stack.tcpserver";
    String newPackage = "org.apache.geode.distributed.internal.tcpserver";
    if (name.startsWith(newPackage)) {
      return oldPackage + name.substring(newPackage.length());
    }
    OldClientSupportService svc = InternalDataSerializer.getOldClientSupportService();
    if (svc != null) {
      return svc.processOutgoingClassName(name, out);
    }
    return name;
  }
  
  /**
   * Reads name of an instance of <code>Class</code> from a
   * <code>DataInput</code>.
   * 
   * The return value may be <code>null</code>.
   * 
   * @throws IOException
   *           A problem occurs while reading from <code>in</code>
   * @see #writeNonPrimitiveClassName(String, DataOutput)
   */
  public static String readNonPrimitiveClassName(DataInput in)
      throws IOException {

    InternalDataSerializer.checkIn(in);

    return swizzleClassNameForRead(readString(in), in);
  }

  /**
   * Writes an instance of Region. A Region is serialized as just a reference
   * to a full path only. It will be recreated on the other end by calling
   * {@link CacheFactory#getAnyInstance} and then calling
   * <code>getRegion</code> on it.
   * This method will handle a
   * <code>null</code> value and not throw a
   * <code>NullPointerException</code>.
   */
  public static void writeRegion(Region<?,?> rgn, DataOutput out)
  throws IOException {
    writeString((rgn != null) ? rgn.getFullPath() : null, out);
  }

  /**
   * Reads an instance of Region. A Region is serialized as a reference to a
   * full path only. It is recreated on the other end by calling
   * {@link CacheFactory#getAnyInstance} and then calling
   * <code>getRegion</code> on it.
   * The return value may be <code>null</code>.
   *
   * @param in the input stream
   * @return the Region instance
   * @throws org.apache.geode.cache.CacheClosedException if a cache has not been created or the only
   * created one is closed.
   * @throws RegionNotFoundException if there is no region by this name
   * in the Cache
   */
  public static <K,V> Region<K,V> readRegion(DataInput in)
  throws IOException, ClassNotFoundException {
    String fullPath = readString(in);
    Region<K,V> rgn = null;
    if (fullPath != null) {
      // use getExisting to fix bug 43151
      rgn = ((Cache)GemFireCacheImpl.getExisting("Needed cache to find region.")).getRegion(fullPath);
      if (rgn == null) {
      throw new RegionNotFoundException(LocalizedStrings.DataSerializer_REGION_0_COULD_NOT_BE_FOUND_WHILE_READING_A_DATASERIALIZER_STREAM.toLocalizedString(fullPath));
      }
    }
    return rgn;
  }


  /**
   * Writes an instance of <code>Date</code> to a
   * <code>DataOutput</code>.  Note that even though <code>date</code>
   * may be an instance of a subclass of <code>Date</code>,
   * <code>readDate</code> will always return an instance of
   * <code>Date</code>, <B>not</B> an instance of the subclass.  To
   * preserve the class type of <code>date</code>,\
   * {@link #writeObject(Object, DataOutput)} should be used for data serialization.
   * This method will handle a
   * <code>null</code> value and not throw a
   * <code>NullPointerException</code>.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   *
   * @see #readDate
   */
  public static void writeDate(Date date, DataOutput out)
    throws IOException {

    InternalDataSerializer.checkOut(out);

    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing Date {}", date);
    }

    long v;
    if (date == null) {
      v = -1L;
    } else {
      v = date.getTime();
      if (v == -1L) {
        throw new IllegalArgumentException("Dates whose getTime returns -1 can not be DataSerialized.");
      }
    }
    out.writeLong(v);
  }

  /**
   * Reads an instance of <code>Date</code> from a
   * <code>DataInput</code>.
   * The return value may be <code>null</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   */
  public static Date readDate(DataInput in) throws IOException {
    InternalDataSerializer.checkIn(in);

    long time = in.readLong();
    Date date = null;
    if (time != -1L) {
      date = new Date(time);
    }

    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Read Date {}", date);
    }

    return date;
  }

  /**
   * Writes an instance of <code>File</code> to a
   * <code>DataOutput</code>.  Note that even though <code>file</code>
   * may be an instance of a subclass of <code>File</code>,
   * <code>readFile</code> will always return an instance of
   * <code>File</code>, <B>not</B> an instance of the subclass.  To
   * preserve the class type of <code>file</code>,
   * {@link #writeObject(Object, DataOutput)} should be used for data serialization.
   * This method will handle a
   * <code>null</code> value and not throw a
   * <code>NullPointerException</code>.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   *
   * @see #readFile
   * @see File#getCanonicalPath
   */
  public static void writeFile(File file, DataOutput out)
    throws IOException {

    InternalDataSerializer.checkOut(out);

    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing File {}", file);
    }

    writeString((file != null) ? file.getCanonicalPath() : null, out);
  }

  /**
   * Reads an instance of <code>File</code> from a
   * <code>DataInput</code>.
   * The return value may be <code>null</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   */
  public static File readFile(DataInput in) throws IOException {
    InternalDataSerializer.checkIn(in);

    String s = readString(in);
    File file = null;
    if (s != null) {
      file = new File(s);
    }
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Read File {}", file);
    }

    return file;
  }

  /**
   * Writes an instance of <code>InetAddress</code> to a
   * <code>DataOutput</code>.  The <code>InetAddress</code> is data
   * serialized by writing its {@link InetAddress#getAddress byte}
   * representation to the <code>DataOutput</code>.  {@link
   * #readInetAddress} converts the <code>byte</code> representation
   * to an instance of <code>InetAddress</code> using {@link
   * InetAddress#getAddress}.  As a result, if <code>address</code>
   * is an instance of a user-defined subclass of
   * <code>InetAddress</code> (that is, not an instance of one of the
   * subclasses from the <code>java.net</code> package), its class
   * will not be preserved.  In order to be able to read an instance
   * of the user-defined class, {@link #writeObject(Object, DataOutput)} should be used.
   * This method will handle a
   * <code>null</code> value and not throw a
   * <code>NullPointerException</code>.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   *
   * @see #readInetAddress
   */
  public static void writeInetAddress(InetAddress address,
                                      DataOutput out)
    throws IOException {

    InternalDataSerializer.checkOut(out);

    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing InetAddress {}", address);
    }

    writeByteArray((address != null) ? address.getAddress() : null, out);
  }

  /**
   * Reads an instance of <code>InetAddress</code> from a
   * <code>DataInput</code>.
   * The return value may be <code>null</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   *         or the address read from <code>in</code> is unknown
   *
   * @see InetAddress#getAddress
   */
  public static InetAddress readInetAddress(DataInput in)
    throws IOException {

    InternalDataSerializer.checkIn(in);

    byte[] address = readByteArray(in);
    if (address == null) {
      return null;
    }

    try {
      InetAddress addr = InetAddress.getByAddress(address);
      if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
        logger.trace(LogMarker.SERIALIZER, "Read InetAddress {}", addr);
      }
      return addr;
    } catch (UnknownHostException ex) {
      IOException ex2 = new IOException(LocalizedStrings.DataSerializer_WHILE_READING_AN_INETADDRESS.toLocalizedString());
      ex2.initCause(ex);
      throw ex2;
    }

  }

  /**
   * Writes an instance of <code>String</code> to a
   * <code>DataOutput</code>.
   * This method will handle a
   * <code>null</code> value and not throw a
   * <code>NullPointerException</code>.
   * <p>As of 5.7 strings longer than 0xFFFF can be serialized.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   *
   * @see #readString
   */
  public static void writeString(String value, DataOutput out)
    throws IOException {

    InternalDataSerializer.checkOut(out);

    final boolean isDebugEnabled = logger.isTraceEnabled(LogMarker.SERIALIZER);
    if (isDebugEnabled) {
      logger.trace(LogMarker.SERIALIZER, "Writing String \"{}\"", value);
    }

    if (value == null) {
      if (isDebugEnabled) {
        logger.trace(LogMarker.SERIALIZER, "Writing NULL_STRING");
      }
      out.writeByte(DSCODE.NULL_STRING);

    } else {
      // [bruce] writeUTF is expensive - it creates a char[] to fetch
      // the string's contents, iterates over the array to compute the
      // encoded length, creates a byte[] to hold the encoded bytes,
      // iterates over the char[] again to create the encode bytes,
      // then writes the bytes.  Since we usually deal with ISO-8859-1
      // strings, we can accelerate this by accessing chars directly
      // with charAt and fill a single-byte buffer.  If we run into
      // a multibyte char, we revert to using writeUTF()
      int len = value.length();
      int utfLen = len; // added for bug 40932
      for (int i=0; i<len; i++) {
        char c = value.charAt(i);
        if ((c <= 0x007F) && (c >= 0x0001)) {
          // nothing needed
        } else if (c > 0x07FF) {
          utfLen += 2;
        } else {
          utfLen += 1;
        }
        // Note we no longer have an early out when we detect the first
        // non-ascii char because we need to compute the utfLen for bug 40932.
        // This is not a performance problem because most strings are ascii
        // and they never did the early out.
      }
      boolean writeUTF = utfLen > len;
      if (writeUTF) {
        if (utfLen > 0xFFFF) {
          if (isDebugEnabled) {
            logger.trace(LogMarker.SERIALIZER, "Writing utf HUGE_STRING of len={}", len);
          }
          out.writeByte(DSCODE.HUGE_STRING);
          out.writeInt(len);
          out.writeChars(value);
        } else {
          if (isDebugEnabled) {
            logger.trace(LogMarker.SERIALIZER, "Writing utf STRING of len={}", len);
          }
          out.writeByte(DSCODE.STRING);
          out.writeUTF(value);
        }
      }
      else {
        if (len > 0xFFFF) {
          if (isDebugEnabled) {
            logger.trace(LogMarker.SERIALIZER, "Writing HUGE_STRING_BYTES of len={}", len);
          }
          out.writeByte(DSCODE.HUGE_STRING_BYTES);
          out.writeInt(len);
          out.writeBytes(value);
        } else {
          if (isDebugEnabled) {
            logger.trace(LogMarker.SERIALIZER, "Writing STRING_BYTES of len={}", len);
          }
          out.writeByte(DSCODE.STRING_BYTES);
          out.writeShort(len);
          out.writeBytes(value);
        }
      }
    }
  }

  /**
   * Reads an instance of <code>String</code> from a
   * <code>DataInput</code>.  The return value may be
   * <code>null</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   *
   * @see #writeString
   */
  public static String readString(DataInput in) throws IOException {
    return InternalDataSerializer.readString(in, in.readByte());
  }

  /**
   * Writes an instance of <code>Boolean</code> to a
   * <code>DataOutput</code>.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   * @throws NullPointerException if value is null.
   *
   * @see #readBoolean
   */
  public static void writeBoolean(Boolean value, DataOutput out)
    throws IOException {

    InternalDataSerializer.checkOut(out);

    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing Boolean {}", value);
    }

    out.writeBoolean(value.booleanValue());
  }

  /**
   * Reads an instance of <code>Boolean</code> from a
   * <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   */
  public static Boolean readBoolean(DataInput in) throws IOException {
    InternalDataSerializer.checkIn(in);

    Boolean value = Boolean.valueOf(in.readBoolean());
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Read Boolean {}", value);
    }
    return value;
  }

  /**
   * Writes an instance of <code>Character</code> to a
   * <code>DataOutput</code>.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   * @throws NullPointerException if value is null.
   *
   * @see #readCharacter
   */
  public static void writeCharacter(Character value, DataOutput out)
    throws IOException {

    InternalDataSerializer.checkOut(out);

    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing Character {}", value);
    }

    out.writeChar(value.charValue());
  }

  /**
   * Reads an instance of <code>Character</code> from a
   * <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   */
  public static Character readCharacter(DataInput in)
    throws IOException {

    InternalDataSerializer.checkIn(in);

    Character value = Character.valueOf(in.readChar());
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Read Character {}", value);
    }
    return value;
  }

  /**
   * Writes an instance of <code>Byte</code> to a
   * <code>DataOutput</code>.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   * @throws NullPointerException if value is null.
   *
   * @see #readByte
   */
  public static void writeByte(Byte value, DataOutput out)
    throws IOException {

    InternalDataSerializer.checkOut(out);

    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing Byte {}", value);
    }

    out.writeByte(value.byteValue());
  }

  /**
   * Reads an instance of <code>Byte</code> from a
   * <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   */
  public static Byte readByte(DataInput in) throws IOException {
    InternalDataSerializer.checkIn(in);

    Byte value = Byte.valueOf(in.readByte());
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Read Byte {}", value);
    }
    return value;
  }

  /**
   * Writes an instance of <code>Short</code> to a
   * <code>DataOutput</code>.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   * @throws NullPointerException if value is null.
   *
   * @see #readShort
   */
  public static void writeShort(Short value, DataOutput out)
    throws IOException {

    InternalDataSerializer.checkOut(out);

    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing Short {}", value);
    }

    out.writeShort(value.shortValue());
  }

  /**
   * Reads an instance of <code>Short</code> from a
   * <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   */
  public static Short readShort(DataInput in) throws IOException {
    InternalDataSerializer.checkIn(in);

    Short value = Short.valueOf(in.readShort());
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Read Short {}", value);
    }
    return value;
  }

  /**
   * Writes an instance of <code>Integer</code> to a
   * <code>DataOutput</code>.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   * @throws NullPointerException if value is null.
   *
   * @see #readInteger
   */
  public static void writeInteger(Integer value, DataOutput out)
    throws IOException {

    InternalDataSerializer.checkOut(out);

    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing Integer {}", value);
    }

    out.writeInt(value.intValue());
  }

  /**
   * Reads an instance of <code>Integer</code> from a
   * <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   */
  public static Integer readInteger(DataInput in) throws IOException {
    InternalDataSerializer.checkIn(in);

    Integer value = Integer.valueOf(in.readInt());
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Read Integer {}", value);
    }
    return value;
  }

  /**
   * Writes an instance of <code>Long</code> to a
   * <code>DataOutput</code>.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   * @throws NullPointerException if value is null.
   *
   * @see #readLong
   */
  public static void writeLong(Long value, DataOutput out)
    throws IOException {

    InternalDataSerializer.checkOut(out);

    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing Long {}", value);
    }

    out.writeLong(value.longValue());
  }

  /**
   * Reads an instance of <code>Long</code> from a
   * <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   */
  public static Long readLong(DataInput in) throws IOException {
    InternalDataSerializer.checkIn(in);

    Long value = Long.valueOf(in.readLong());
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Read Long {}", value);
    }
    return value;
  }

  /**
   * Writes an instance of <code>Float</code> to a
   * <code>DataOutput</code>.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   * @throws NullPointerException if value is null.
   *
   * @see #readFloat
   */
  public static void writeFloat(Float value, DataOutput out)
    throws IOException {

    InternalDataSerializer.checkOut(out);

    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing Float {}", value);
    }

    out.writeFloat(value.floatValue());
  }

  /**
   * Reads an instance of <code>Float</code> from a
   * <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   */
  public static Float readFloat(DataInput in) throws IOException {
    InternalDataSerializer.checkIn(in);

    Float value = Float.valueOf(in.readFloat());
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Read Float {}", value);
    }
    return value;
  }

  /**
   * Writes an instance of <code>Double</code> to a
   * <code>DataOutput</code>.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   * @throws NullPointerException if value is null.
   *
   * @see #readDouble
   */
  public static void writeDouble(Double value, DataOutput out)
    throws IOException {

    InternalDataSerializer.checkOut(out);

    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing Double {}", value);
    }

    out.writeDouble(value.doubleValue());
  }

  /**
   * Reads an instance of <code>Double</code> from a
   * <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   */
  public static Double readDouble(DataInput in) throws IOException {
    InternalDataSerializer.checkIn(in);

    Double value = Double.valueOf(in.readDouble());
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Read Double {}", value);
    }
    return value;
  }

  /**
   * Writes a primitive <code>boolean</code> to a
   * <code>DataOutput</code>.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   *
   * @see DataOutput#writeBoolean
   * @since GemFire 5.1
   */
  public static void writePrimitiveBoolean(boolean value, DataOutput out)
    throws IOException {

    InternalDataSerializer.checkOut(out);

    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing Boolean {}", value);
    }

    out.writeBoolean(value);
  }

  /**
   * Reads a primitive <code>boolean</code> from a
   * <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   * @see DataInput#readBoolean
   * @since GemFire 5.1
   */
  public static boolean readPrimitiveBoolean(DataInput in) throws IOException {
    InternalDataSerializer.checkIn(in);

    boolean value = in.readBoolean();
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Read Boolean {}", value);
    }
    return value;
  }

  /**
   * Writes a primitive <code>byte</code> to a
   * <code>DataOutput</code>.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   *
   * @see DataOutput#writeByte
   * @since GemFire 5.1
   */
  public static void writePrimitiveByte(byte value, DataOutput out)
    throws IOException {

    InternalDataSerializer.checkOut(out);

    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing Byte {}", value);
    }

    out.writeByte(value);
  }

  /**
   * Reads a primitive <code>byte</code> from a
   * <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   * @see DataInput#readByte
   * @since GemFire 5.1
   */
  public static byte readPrimitiveByte(DataInput in) throws IOException {
    InternalDataSerializer.checkIn(in);

    byte value = in.readByte();
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Read Byte {}", value);
    }
    return value;
  }

  /**
   * Writes a primitive  <code>char</code> to a
   * <code>DataOutput</code>.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   *
   * @see DataOutput#writeChar
   * @since GemFire 5.1
   */
  public static void writePrimitiveChar(char value, DataOutput out)
    throws IOException {

    InternalDataSerializer.checkOut(out);

    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing Char {}", value);
    }

    out.writeChar(value);
  }

  /**
   * Reads a primitive <code>char</code> from a
   * <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   * @see DataInput#readChar
   * @since GemFire 5.1
   */
  public static char readPrimitiveChar(DataInput in) throws IOException {
    InternalDataSerializer.checkIn(in);

    char value = in.readChar();
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Read Char {}", value);
    }
    return value;
  }

  /**
   * Writes a primitive <code>short</code> to a
   * <code>DataOutput</code>.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   *
   * @see DataOutput#writeShort
   * @since GemFire 5.1
   */
  public static void writePrimitiveShort(short value, DataOutput out)
    throws IOException {

    InternalDataSerializer.checkOut(out);

    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing Short {}", value);
    }

    out.writeShort(value);
  }

  /**
   * Reads a primitive <code>short</code> from a
   * <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   * @see DataInput#readShort
   * @since GemFire 5.1
   */
  public static short readPrimitiveShort(DataInput in) throws IOException {
    InternalDataSerializer.checkIn(in);

    short value = in.readShort();
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Read Short {}", value);
    }
    return value;
  }

  /**
   * Writes a primitive <code>int</code> as an unsigned byte to a
   * <code>DataOutput</code>.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   *
   * @see DataOutput#writeByte
   * @see DataInput#readUnsignedByte
   * @since GemFire 5.1
   */
  public static void writeUnsignedByte(int value, DataOutput out)
    throws IOException {

    InternalDataSerializer.checkOut(out);

    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing Unsigned Byte {}", value);
    }

    out.writeByte(value);
  }

  /**
   * Reads a primitive <code>int</code> as an unsigned byte from a
   * <code>DataInput</code> using {@link DataInput#readUnsignedByte}.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   * @since GemFire 5.1
   */
  public static int readUnsignedByte(DataInput in) throws IOException {
    InternalDataSerializer.checkIn(in);

    int value = in.readUnsignedByte();
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Read Unsigned Byte {}", value);
    }
    return value;
  }

  /**
   * Writes a primitive <code>int</code> as an unsigned short to a
   * <code>DataOutput</code>.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   *
   * @see DataOutput#writeShort
   * @see DataInput#readUnsignedShort
   * @since GemFire 5.1
   */
  public static void writeUnsignedShort(int value, DataOutput out)
    throws IOException {

    InternalDataSerializer.checkOut(out);

    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing Unsigned Short {}", value);
    }

    out.writeShort(value);
  }

  /**
   * Reads a primitive <code>int</code> as an unsigned short from a
   * <code>DataInput</code> using {@link DataInput#readUnsignedShort}.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   * @since GemFire 5.1
   */
  public static int readUnsignedShort(DataInput in) throws IOException {
    InternalDataSerializer.checkIn(in);

    int value = in.readUnsignedShort();
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Read Unsigned Short {}", value);
    }
    return value;
  }

  /**
   * Writes a primitive <code>int</code> to a
   * <code>DataOutput</code>.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   *
   * @see DataOutput#writeInt
   */
  public static void writePrimitiveInt(int value, DataOutput out)
    throws IOException {

    InternalDataSerializer.checkOut(out);

    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing Integer {}", value);
    }

    out.writeInt(value);
  }

  /**
   * Reads a primitive <code>int</code> from a
   * <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   * @see DataInput#readInt
   * @since GemFire 5.1
   */
  public static int readPrimitiveInt(DataInput in) throws IOException {
    InternalDataSerializer.checkIn(in);

    int value = in.readInt();
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Read Integer {}", value);
    }
    return value;
  }

  /**
   * Writes a primitive <code>long</code> to a
   * <code>DataOutput</code>.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   *
   * @see DataOutput#writeLong
   * @since GemFire 5.1
   */
  public static void writePrimitiveLong(long value, DataOutput out)
    throws IOException {

    InternalDataSerializer.checkOut(out);

    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing Long {}", value);
    }

    out.writeLong(value);
  }

  /**
   * Reads a primitive <code>long</code> from a
   * <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   * @see DataInput#readLong
   * @since GemFire 5.1
   */
  public static long readPrimitiveLong(DataInput in) throws IOException {
    InternalDataSerializer.checkIn(in);

    long value = in.readLong();
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Read Long {}", value);
    }
    return value;
  }

  /**
   * Writes a primitive <code>float</code> to a
   * <code>DataOutput</code>.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   *
   * @see DataOutput#writeFloat
   * @since GemFire 5.1
   */
  public static void writePrimitiveFloat(float value, DataOutput out)
    throws IOException {

    InternalDataSerializer.checkOut(out);

    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing Float {}", value);
    }

    out.writeFloat(value);
  }

  /**
   * Reads a primitive <code>float</code> from a
   * <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   * @see DataInput#readFloat
   * @since GemFire 5.1
   */
  public static float readPrimitiveFloat(DataInput in) throws IOException {
    InternalDataSerializer.checkIn(in);

    float value = in.readFloat();
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Read Float {}", value);
    }
    return value;
  }

  /**
   * Writes a primtive <code>double</code> to a
   * <code>DataOutput</code>.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   *
   * @see DataOutput#writeDouble
   * @since GemFire 5.1
   */
  public static void writePrimitiveDouble(double value, DataOutput out)
    throws IOException {

    InternalDataSerializer.checkOut(out);

    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing Double {}", value);
    }

    out.writeDouble(value);
  }

  /**
   * Reads a primitive <code>double</code> from a
   * <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   * @see DataInput#readDouble
   * @since GemFire 5.1
   */
  public static double readPrimitiveDouble(DataInput in) throws IOException {
    InternalDataSerializer.checkIn(in);

    double value = in.readDouble();
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Read Double {}", value);
    }
    return value;
  }

  /**
   * Writes an array of <code>byte</code>s to a
   * <code>DataOutput</code>.
   * This method will serialize a
   * <code>null</code> array and not throw a
   * <code>NullPointerException</code>.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   *
   * @see #readByteArray
   */
  public static void writeByteArray(byte[] array, DataOutput out)
    throws IOException {
    int len = 0;
    if (array != null) {
      len = array.length;
    }
    writeByteArray(array, len, out);
  }

  /**
   * Writes the first <code>len</code> elements
   * of an array of <code>byte</code>s to a
   * <code>DataOutput</code>.
   * This method will serialize a
   * <code>null</code> array and not throw a
   * <code>NullPointerException</code>.
   *
   * @param len the actual number of entries to write. If len is greater
   * than then length of the array then the entire array is written.
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   *
   * @see #readByteArray
   */
  public static void writeByteArray(byte[] array, int len, DataOutput out)
    throws IOException {

    InternalDataSerializer.checkOut(out);

    int length = len; // to avoid warnings about parameter assignment
    
    if (array == null) {
      length = -1;
    } else {
      if (length > array.length) {
        length = array.length;
      }
    }
    InternalDataSerializer.writeArrayLength(length, out);
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing byte array of length {}", length);
    }
    if (length > 0) {
      out.write(array, 0, length);
    }
  }
  /**
   * Serialize the given object <code>obj</code> into a byte array
   * using {@link #writeObject(Object, DataOutput)} and then writes the byte array
   * to the given data output <code>out</code> in the same format
   * {@link #writeByteArray(byte[], DataOutput)} does.
   * This method will serialize a
   * <code>null</code> obj and not throw a
   * <code>NullPointerException</code>.
   *
   * @param obj the object to serialize and write
   * @param out the data output to write the byte array to
   * @throws IllegalArgumentException
   *         if a problem occurs while serialize <code>obj</code>
   * @throws IOException
   *         if a problem occurs while writing to <code>out</code>
   *
   * @see #readByteArray
   * @since GemFire 5.0.2
   */
  public static void writeObjectAsByteArray(Object obj, DataOutput out)
    throws IOException {
    Object object = obj;
    if (obj instanceof CachedDeserializable) {
      if (obj instanceof StoredObject) {
        StoredObject so = (StoredObject)obj;
        if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
          logger.trace(LogMarker.SERIALIZER, "writeObjectAsByteArray StoredObject");
        }
        so.sendAsByteArray(out);
        return;
      } else {
        object = ((CachedDeserializable) obj).getSerializedValue();
      }
    }
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      if (object == null) {
        logger.trace(LogMarker.SERIALIZER, "writeObjectAsByteArray null");
      } else {
        logger.trace(LogMarker.SERIALIZER, "writeObjectAsByteArray obj.getClass={}", object.getClass());
      }
    }
    if (object instanceof byte[] || object == null) {
      writeByteArray((byte[])object, out);
    } else if (out instanceof ObjToByteArraySerializer) {
      ((ObjToByteArraySerializer)out).writeAsSerializedByteArray(object);
    }/*else if (obj instanceof Sendable) {
      ((Sendable)obj).sendTo(out); 
    } */ 
    else {
      HeapDataOutputStream hdos;
      if (object instanceof HeapDataOutputStream) {
        hdos = (HeapDataOutputStream)object;
      } else {
        Version v = InternalDataSerializer.getVersionForDataStreamOrNull(out);
        if (v == null) {
          v = Version.CURRENT;
        }
        hdos = new HeapDataOutputStream(v);
        try {
          DataSerializer.writeObject(object, hdos);
        } catch (IOException e) {
          RuntimeException e2 = new IllegalArgumentException(LocalizedStrings.DataSerializer_PROBELM_WHILE_SERIALIZING.toLocalizedString());
          e2.initCause(e);
          throw e2;
        }
      }
      InternalDataSerializer.writeArrayLength(hdos.size(), out);
      hdos.sendTo(out);
    }
  }

  /**
   * Reads an array of <code>byte</code>s from a
   * <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   *
   * @see #writeByteArray(byte[], DataOutput)
   */
  public static byte[] readByteArray(DataInput in)
    throws IOException {

      InternalDataSerializer.checkIn(in);

      int length = InternalDataSerializer.readArrayLength(in);
      if (length == -1) {
        return null;
      } else {
        byte[] array = new byte[length];
        in.readFully(array, 0, length);

        if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
          logger.trace(LogMarker.SERIALIZER, "Read byte array of length {}", length);
        }

        return array;
      }
    }

  /**
   * Writes an array of <code>String</code>s to a
   * <code>DataOutput</code>.
   * This method will serialize a
   * <code>null</code> array and not throw a
   * <code>NullPointerException</code>.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   *
   * @see #readStringArray
   * @see #writeString
   */
  public static void writeStringArray(String[] array, DataOutput out)
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
      logger.trace(LogMarker.SERIALIZER, "Writing String array of length {}", length);
    }
    if (length > 0) {
      for (int i = 0; i < length; i++) {
        writeString(array[i], out);
      }
    }
  }

  /**
   * Reads an array of <code>String</code>s from a
   * <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   *
   * @see #writeStringArray
   */
  public static String[] readStringArray(DataInput in)
    throws IOException {

      InternalDataSerializer.checkIn(in);

      int length = InternalDataSerializer.readArrayLength(in);
      if (length == -1) {
        return null;
      } else {
        String[] array = new String[length];
        for (int i = 0; i < length; i++) {
          array[i] = readString(in);
        }

        if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
          logger.trace(LogMarker.SERIALIZER, "Read String array of length {}", length);
        }

        return array;
      }
    }

  /**
   * Writes an array of <code>short</code>s to a
   * <code>DataOutput</code>.
   * This method will serialize a
   * <code>null</code> array and not throw a
   * <code>NullPointerException</code>.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   *
   * @see #readShortArray
   */
  public static void writeShortArray(short[] array, DataOutput out)
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
      logger.trace(LogMarker.SERIALIZER, "Writing short array of length {}", length);
    }
    if (length > 0) {
      for (int i = 0; i < length; i++) {
        out.writeShort(array[i]);
      }
    }
  }

  /**
   * Reads an array of <code>short</code>s from a
   * <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   *
   * @see #writeShortArray
   */
  public static short[] readShortArray(DataInput in)
    throws IOException {

      InternalDataSerializer.checkIn(in);

      int length = InternalDataSerializer.readArrayLength(in);
      if (length == -1) {
        return null;
      } else {
        short[] array = new short[length];
        for (int i = 0; i < length; i++) {
          array[i] = in.readShort();
        }

        if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
          logger.trace(LogMarker.SERIALIZER, "Read short array of length {}", length);
        }

        return array;
      }
    }

  /**
   * Writes an array of <code>char</code>s to a
   * <code>DataOutput</code>.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   *
   * @see #readCharArray
   * @since GemFire 5.7
   */
  public static void writeCharArray(char[] array, DataOutput out)
      throws IOException {

    InternalDataSerializer.writeCharArray(array, array != null ? array.length
        : -1, out);
  }

  /**
   * Reads an array of <code>char</code>s from a
   * <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   *
   * @see #writeCharArray
   * @since GemFire 5.7
   */
  public static char[] readCharArray(DataInput in)
    throws IOException {

    InternalDataSerializer.checkIn(in);

    int length = InternalDataSerializer.readArrayLength(in);
    if (length == -1) {
      return null;
    } else {
      char[] array = new char[length];
      for (int i = 0; i < length; i++) {
        array[i] = in.readChar();
      }

      if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
        logger.trace(LogMarker.SERIALIZER, "Read char array of length {}", length);
      }

      return array;
    }
  }
  /**
   * Writes an array of <code>boolean</code>s to a
   * <code>DataOutput</code>.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   *
   * @see #readBooleanArray
   * @since GemFire 5.7
   */
  public static void writeBooleanArray(boolean[] array, DataOutput out)
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
      logger.trace(LogMarker.SERIALIZER, "Writing boolean array of length {}", length);
    }
    if (length > 0) {
      for (int i = 0; i < length; i++) {
        out.writeBoolean(array[i]);
      }
    }
  }

  /**
   * Reads an array of <code>boolean</code>s from a
   * <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   *
   * @see #writeBooleanArray
   * @since GemFire 5.7
   */
  public static boolean[] readBooleanArray(DataInput in)
    throws IOException {

    InternalDataSerializer.checkIn(in);

    int length = InternalDataSerializer.readArrayLength(in);
    if (length == -1) {
      return null;
    } else {
      boolean[] array = new boolean[length];
      for (int i = 0; i < length; i++) {
        array[i] = in.readBoolean();
      }

      if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
        logger.trace(LogMarker.SERIALIZER, "Read boolean array of length {}", length);
      }

      return array;
    }
  }
  /**
   * Writes an <code>int</code> array to a <code>DataOutput</code>.
   * This method will serialize a
   * <code>null</code> array and not throw a
   * <code>NullPointerException</code>.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   *
   * @see #readIntArray
   */
  public static void writeIntArray(int[] array, DataOutput out)
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
      logger.trace(LogMarker.SERIALIZER, "Writing int array of length {}", length);
    }
    if (length > 0) {
      for (int i = 0; i < length; i++) {
        out.writeInt(array[i]);
      }
    }
  }

  /**
   * Reads an <code>int</code> array from a <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   *
   * @see #writeIntArray
   */
  public static int[] readIntArray(DataInput in)
    throws IOException {

      InternalDataSerializer.checkIn(in);

      int length = InternalDataSerializer.readArrayLength(in);
      if (length == -1) {
        return null;
      } else {
        int[] array = new int[length];
        for (int i = 0; i < length; i++) {
          array[i] = in.readInt();
        }

        if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
          logger.trace(LogMarker.SERIALIZER, "Read int array of length {}", length);
        }

        return array;
      }
    }

  /**
   * Writes an array of <code>long</code>s to a
   * <code>DataOutput</code>.
   * This method will serialize a
   * <code>null</code> array and not throw a
   * <code>NullPointerException</code>.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   *
   * @see #readLongArray
   */
  public static void writeLongArray(long[] array, DataOutput out)
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
      logger.trace(LogMarker.SERIALIZER, "Writing long array of length {}", length);
    }
    if (length > 0) {
      for (int i = 0; i < length; i++) {
        out.writeLong(array[i]);
      }
    }
  }

  /**
   * Reads an array of <code>long</code>s from a
   * <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   *
   * @see #writeLongArray
   */
  public static long[] readLongArray(DataInput in)
    throws IOException {

      InternalDataSerializer.checkIn(in);

      int length = InternalDataSerializer.readArrayLength(in);
      if (length == -1) {
        return null;
      } else {
        long[] array = new long[length];
        for (int i = 0; i < length; i++) {
          array[i] = in.readLong();
        }

        if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
          logger.trace(LogMarker.SERIALIZER, "Read long array of length {}", length);
        }

        return array;
      }
    }

  /**
   * Writes an array of <code>float</code>s to a
   * <code>DataOutput</code>.
   * This method will serialize a
   * <code>null</code> array and not throw a
   * <code>NullPointerException</code>.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   *
   * @see #readFloatArray
   */
  public static void writeFloatArray(float[] array, DataOutput out)
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
      logger.trace(LogMarker.SERIALIZER, "Writing float array of length {}", length);
    }
    if (length > 0) {
      for (int i = 0; i < length; i++) {
        out.writeFloat(array[i]);
      }
    }
  }

  /**
   * Reads an array of <code>float</code>s from a
   * <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   *
   * @see #writeFloatArray
   */
  public static float[] readFloatArray(DataInput in)
    throws IOException {

      InternalDataSerializer.checkIn(in);

      int length = InternalDataSerializer.readArrayLength(in);
      if (length == -1) {
        return null;
      } else {
        float[] array = new float[length];
        for (int i = 0; i < length; i++) {
          array[i] = in.readFloat();
        }

        if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
          logger.trace(LogMarker.SERIALIZER, "Read float array of length {}", length);
        }

        return array;
      }
    }

  /**
   * Writes an array of <code>double</code>s to a
   * <code>DataOutput</code>.
   * This method will serialize a
   * <code>null</code> array and not throw a
   * <code>NullPointerException</code>.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   *
   * @see #readDoubleArray
   */
  public static void writeDoubleArray(double[] array, DataOutput out)
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
      logger.trace(LogMarker.SERIALIZER, "Writing double array of length {}", length);
    }
    if (length > 0) {
      for (int i = 0; i < length; i++) {
        out.writeDouble(array[i]);
      }
    }
  }

  /**
   * Reads an array of <code>double</code>s from a
   * <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   *
   * @see #writeDoubleArray
   */
  public static double[] readDoubleArray(DataInput in)
    throws IOException {

      InternalDataSerializer.checkIn(in);

      int length = InternalDataSerializer.readArrayLength(in);
      if (length == -1) {
        return null;
      } else {
        double[] array = new double[length];
        for (int i = 0; i < length; i++) {
          array[i] = in.readDouble();
        }

        if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
          logger.trace(LogMarker.SERIALIZER, "Read double array of length {}", length);
        }

        return array;
      }
    }

  /**
   * Writes an array of <code>Object</code>s to a
   * <code>DataOutput</code>.
   * This method will serialize a
   * <code>null</code> array and not throw a
   * <code>NullPointerException</code>.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   *
   * @see #readObjectArray
   * @see #writeObject(Object, DataOutput)
   */
  public static void writeObjectArray(Object[] array, DataOutput out)
    throws IOException {
    InternalDataSerializer.writeObjectArray(array, out, false);
  }
  
  /**
   * Reads an array of <code>Object</code>s from a
   * <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   *
   * @see #writeObjectArray
   * @see #readObject
   */
  public static Object[] readObjectArray(DataInput in)
    throws IOException, ClassNotFoundException {

      InternalDataSerializer.checkIn(in);

      int length = InternalDataSerializer.readArrayLength(in);
      if (length == -1) {
        return null;
      } else {
        Class<?> c = null;
        byte typeCode = in.readByte();
        String typeString = null;
        if (typeCode == DSCODE.CLASS) {
          typeString = readString(in);
        }
        
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        boolean lookForPdxInstance = false;
        ClassNotFoundException cnfEx = null;
        if (typeCode == DSCODE.CLASS
            && cache != null && cache.getPdxReadSerializedByAnyGemFireServices()) {
          try {
            c = InternalDataSerializer.getCachedClass(typeString);
            lookForPdxInstance = true;
          } catch (ClassNotFoundException ignore) {
            c = Object.class;
            cnfEx = ignore;
          }
        } else {
          if (typeCode == DSCODE.CLASS) {
            c = InternalDataSerializer.getCachedClass(typeString);
          } else {
            c = InternalDataSerializer.decodePrimitiveClass(typeCode);
          }
        }
        Object o = null;
        if (length > 0) {
          o = readObject(in);
          if (lookForPdxInstance && o instanceof PdxInstance) {
            lookForPdxInstance = false;
            c = Object.class;
          }
        }
        Object[] array = (Object[]) Array.newInstance(c, length);
        if (length > 0) {
          array[0] = o;
        }
        for (int i = 1; i < length; i++) {
          o = readObject(in);
          if (lookForPdxInstance && o instanceof PdxInstance) {
            // create an Object[] and copy all the entries we already did into it
            lookForPdxInstance = false;
            c = Object.class;
            Object[] newArray = (Object[])Array.newInstance(c, length);
            System.arraycopy(array, 0, newArray, 0, i);
            array = newArray;
          }
          array[i] = o;
        }
        if (lookForPdxInstance && cnfEx != null && length > 0) {
          // We have a non-empty array and didn't find any
          // PdxInstances in it. So we should have been able
          // to load the element type.
          // Note that empty arrays in this case will deserialize
          // as an Object[] since we can't tell if the element
          // type is a pdx one.
          throw cnfEx;
        }

        if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
          logger.trace(LogMarker.SERIALIZER, "Read Object array of length {}", length);
        }

        return array;
      }
    }
  
  /**
   * Writes an array of <tt>byte[]</tt> to a <tt>DataOutput</tt>.
   *
   * @throws IOException
   *         A problem occurs while writing to <tt>out</tt>.
   *
   */
  public static void writeArrayOfByteArrays(byte[][] array, DataOutput out)
  throws IOException {    
    
    InternalDataSerializer.checkOut(out);
    int length;
    if (array == null) {
      length = -1;
    }
    else {
      length = array.length;
    }
    InternalDataSerializer.writeArrayLength(length, out);
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing byte[][] of length {}", length);
    }
    if (length >= 0) {
      for (int i = 0; i < length; i++) {
        writeByteArray(array[i], out);
      }
    }
  }
  
  /**
   * Reads an array of <code>byte[]</code>s from a
   * <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   */
  public static byte[][] readArrayOfByteArrays(DataInput in)
  throws IOException, ClassNotFoundException {
    
    InternalDataSerializer.checkIn(in);
    
    int length = InternalDataSerializer.readArrayLength(in);
    if (length == -1) {
      return null;
    } else {
      byte[][] array = new byte[length][];
      for (int i = 0; i < length; i++) {
        array[i] = readByteArray(in);
      }
      
      if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
        logger.trace(LogMarker.SERIALIZER, "Read byte[][] of length {}", length);
      }
      
      return array;
    }
  }
  
  
  /**
   * Writes an <code>ArrayList</code> to a <code>DataOutput</code>.
   * Note that even though <code>list</code> may be an instance of a
   * subclass of <code>ArrayList</code>, <code>readArrayList</code>
   * will always return an instance of <code>ArrayList</code>,
   * <B>not</B> an instance of the subclass.  To preserve the class
   * type of <code>list</code>, {@link #writeObject(Object, DataOutput)} should be used
   * for data serialization.
   * This method will serialize a
   * <code>null</code> list and not throw a
   * <code>NullPointerException</code>.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   *
   * @see #readArrayList
   */
  public static void writeArrayList(ArrayList<?> list, DataOutput out)
    throws IOException {

    InternalDataSerializer.checkOut(out);

    int size;
    if (list == null) {
      size = -1;
    } else {
      size = list.size();
    }
    InternalDataSerializer.writeArrayLength(size, out);
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing ArrayList with {} elements: {}", size, list);
    }
    if (size > 0) {
      for (int i=0; i < size; i++) {
        writeObject(list.get(i), out);
      }
    }
  }
  
  

  /**
   * Reads an <code>ArrayList</code> from a <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   * @throws ClassNotFoundException
   *         The class of one of the <Code>ArrayList</code>'s
   *         elements cannot be found.
   *
   * @see #writeArrayList
   */
  public static <E> ArrayList<E> readArrayList(DataInput in)
    throws IOException, ClassNotFoundException {

    InternalDataSerializer.checkIn(in);

    int size = InternalDataSerializer.readArrayLength(in);
    if (size == -1) {
      return null;
    } else {
      ArrayList<E> list = new ArrayList<E>(size);
      for (int i = 0; i < size; i++) {
        E element = DataSerializer.<E>readObject(in);
        list.add(element);
      }

      if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
        logger.trace(LogMarker.SERIALIZER, "Read ArrayList with {} elements: {}", size, list);
      }

      return list;
    }
  }

  /**
   * Writes an <code>Vector</code> to a <code>DataOutput</code>.
   * Note that even though <code>list</code> may be an instance of a
   * subclass of <code>Vector</code>, <code>readVector</code>
   * will always return an instance of <code>Vector</code>,
   * <B>not</B> an instance of the subclass.  To preserve the class
   * type of <code>list</code>, {@link #writeObject(Object, DataOutput)} should be used
   * for data serialization.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   *
   * @see #readVector
   * @since GemFire 5.7
   */
  public static void writeVector(Vector<?> list, DataOutput out)
    throws IOException {

    InternalDataSerializer.checkOut(out);

    int size;
    if (list == null) {
      size = -1;
    } else {
      size = list.size();
    }
    InternalDataSerializer.writeArrayLength(size, out);
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing Vector with {} elements: {}", size, list);
    }
    if (size > 0) {
      for (int i=0; i < size; i++) {
        writeObject(list.get(i), out);
      }
    }
  }

  /**
   * Reads an <code>Vector</code> from a <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   * @throws ClassNotFoundException
   *         The class of one of the <Code>Vector</code>'s
   *         elements cannot be found.
   *
   * @see #writeVector
   * @since GemFire 5.7
   */
  public static <E> Vector<E> readVector(DataInput in)
    throws IOException, ClassNotFoundException {

    InternalDataSerializer.checkIn(in);

    int size = InternalDataSerializer.readArrayLength(in);
    if (size == -1) {
      return null;
    } else {
      Vector<E> list = new Vector<E>(size);
      for (int i = 0; i < size; i++) {
        E element = DataSerializer.<E>readObject(in);
        list.add(element);
      }

      if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
        logger.trace(LogMarker.SERIALIZER, "Read Vector with {} elements: {}", size, list);
      }

      return list;
    }
  }

  /**
   * Writes an <code>Stack</code> to a <code>DataOutput</code>.
   * Note that even though <code>list</code> may be an instance of a
   * subclass of <code>Stack</code>, <code>readStack</code>
   * will always return an instance of <code>Stack</code>,
   * <B>not</B> an instance of the subclass.  To preserve the class
   * type of <code>list</code>, {@link #writeObject(Object, DataOutput)} should be used
   * for data serialization.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   *
   * @see #readStack
   * @since GemFire 5.7
   */
  public static void writeStack(Stack<?> list, DataOutput out)
    throws IOException {

    InternalDataSerializer.checkOut(out);

    int size;
    if (list == null) {
      size = -1;
    } else {
      size = list.size();
    }
    InternalDataSerializer.writeArrayLength(size, out);
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing Stack with {} elements: {}", size, list);
    }
    if (size > 0) {
      for (int i=0; i < size; i++) {
        writeObject(list.get(i), out);
      }
    }
  }

  /**
   * Reads an <code>Stack</code> from a <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   * @throws ClassNotFoundException
   *         The class of one of the <Code>Stack</code>'s
   *         elements cannot be found.
   *
   * @see #writeStack
   * @since GemFire 5.7
   */
  public static <E> Stack<E> readStack(DataInput in)
    throws IOException, ClassNotFoundException {

    InternalDataSerializer.checkIn(in);

    int size = InternalDataSerializer.readArrayLength(in);
    if (size == -1) {
      return null;
    } else {
      Stack<E> list = new Stack<E>();
      for (int i = 0; i < size; i++) {
        E element = DataSerializer.<E>readObject(in);
        list.add(element);
      }

      if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
        logger.trace(LogMarker.SERIALIZER, "Read Stack with {} elements: {}", size, list);
      }

      return list;
    }
  }

  /**
   * Writes an <code>LinkedList</code> to a <code>DataOutput</code>.
   * Note that even though <code>list</code> may be an instance of a
   * subclass of <code>LinkedList</code>, <code>readLinkedList</code>
   * will always return an instance of <code>LinkedList</code>,
   * <B>not</B> an instance of the subclass.  To preserve the class
   * type of <code>list</code>, {@link #writeObject(Object, DataOutput)} should be used
   * for data serialization.
   * This method will serialize a
   * <code>null</code> list and not throw a
   * <code>NullPointerException</code>.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   *
   * @see #readLinkedList
   */
  public static void writeLinkedList(LinkedList<?> list, DataOutput out)
    throws IOException {

    InternalDataSerializer.checkOut(out);

    int size;
    if (list == null) {
      size = -1;
    } else {
      size = list.size();
    }
    InternalDataSerializer.writeArrayLength(size, out);
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing LinkedList with {} elements: {}", size, list);
    }
    if (size > 0) {
      for (Object e: list) {
        writeObject(e, out);
      }
    }
  }

  /**
   * Reads an <code>LinkedList</code> from a <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   * @throws ClassNotFoundException
   *         The class of one of the <Code>LinkedList</code>'s
   *         elements cannot be found.
   *
   * @see #writeLinkedList
   */
  public static <E> LinkedList<E> readLinkedList(DataInput in)
    throws IOException, ClassNotFoundException {

    InternalDataSerializer.checkIn(in);

    int size = InternalDataSerializer.readArrayLength(in);
    if (size == -1) {
      return null;
    } else {
      LinkedList<E> list = new LinkedList<E>();
      for (int i = 0; i < size; i++) {
        E element = DataSerializer.<E>readObject(in);
        list.add(element);
      }

      if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
        logger.trace(LogMarker.SERIALIZER, "Read LinkedList with {} elements: {}", size, list);
      }

      return list;
    }
  }

  /**
   * Writes a <code>HashSet</code> to a <code>DataOutput</code>.  Note
   * that even though <code>set</code> may be an instance of a
   * subclass of <code>HashSet</code>, <code>readHashSet</code> will
   * always return an instance of <code>HashSet</code>, <B>not</B> an
   * instance of the subclass.  To preserve the class type of
   * <code>set</code>, {@link #writeObject(Object, DataOutput)} should be used for data
   * serialization.
   * This method will serialize a
   * <code>null</code> set and not throw a
   * <code>NullPointerException</code>.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   *
   * @see #readHashSet
   */
  public static void writeHashSet(HashSet<?> set, DataOutput out)
    throws IOException {
    InternalDataSerializer.writeSet(set, out);
  }

  /**
   * Reads a <code>HashSet</code> from a <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   * @throws ClassNotFoundException
   *         The class of one of the <Code>HashSet</code>'s
   *         elements cannot be found.
   *
   * @see #writeHashSet
   */
  public static <E> HashSet<E> readHashSet(DataInput in)
    throws IOException, ClassNotFoundException {

    InternalDataSerializer.checkIn(in);

    int size = InternalDataSerializer.readArrayLength(in);
    if (size == -1) {
      return null;
    } else {
      HashSet<E> set = new HashSet<E>(size);
      for (int i = 0; i < size; i++) {
        E element = DataSerializer.<E>readObject(in);
        set.add(element);
      }

      if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
        logger.trace(LogMarker.SERIALIZER, "Read HashSet with {} elements: {}", size, set);
      }

      return set;
    }
  }

  /**
   * Writes a <code>LinkedHashSet</code> to a <code>DataOutput</code>.  Note
   * that even though <code>set</code> may be an instance of a
   * subclass of <code>LinkedHashSet</code>, <code>readLinkedHashSet</code> will
   * always return an instance of <code>LinkedHashSet</code>, <B>not</B> an
   * instance of the subclass.  To preserve the class type of
   * <code>set</code>, {@link #writeObject(Object, DataOutput)} should be used for data
   * serialization.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   *
   * @see #readLinkedHashSet
   * @since GemFire 5.7
   */
  public static void writeLinkedHashSet(LinkedHashSet<?> set, DataOutput out)
    throws IOException {
    InternalDataSerializer.writeSet(set, out);
  }

  /**
   * Reads a <code>LinkedHashSet</code> from a <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   * @throws ClassNotFoundException
   *         The class of one of the <Code>LinkedHashSet</code>'s
   *         elements cannot be found.
   *
   * @see #writeLinkedHashSet
   * @since GemFire 5.7
   */
  public static <E> LinkedHashSet<E> readLinkedHashSet(DataInput in)
    throws IOException, ClassNotFoundException {

    InternalDataSerializer.checkIn(in);

    int size = InternalDataSerializer.readArrayLength(in);
    if (size == -1) {
      return null;
    } else {
      LinkedHashSet<E> set = new LinkedHashSet<E>(size);
      for (int i = 0; i < size; i++) {
        E element = DataSerializer.<E>readObject(in);
        set.add(element);
      }

      if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
        logger.trace(LogMarker.SERIALIZER, "Read LinkedHashSet with {} elements: {}", size, set);
      }

      return set;
    }
  }

  /**
   * Writes a <code>HashMap</code> to a <code>DataOutput</code>.  Note
   * that even though <code>map</code> may be an instance of a
   * subclass of <code>HashMap</code>, <code>readHashMap</code> will
   * always return an instance of <code>HashMap</code>, <B>not</B> an
   * instance of the subclass.  To preserve the class type of
   * <code>map</code>, {@link #writeObject(Object, DataOutput)} should be used for data
   * serialization.
   * This method will serialize a
   * <code>null</code> map and not throw a
   * <code>NullPointerException</code>.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   *
   * @see #readHashMap
   */
  public static void writeHashMap(Map<?,?> map, DataOutput out)
    throws IOException {

    InternalDataSerializer.checkOut(out);

    int size;
    if (map == null) {
      size = -1;
    } else {
      size = map.size();
    }
    InternalDataSerializer.writeArrayLength(size, out);
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing HashMap with {} elements: {}", size, map);
    }
    if (size > 0) {
      for (Map.Entry<?,?> entry: map.entrySet()) {
        writeObject(entry.getKey(), out);
        writeObject(entry.getValue(), out);
      }
    }
  }

  /**
   * Reads a <code>HashMap</code> from a <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   * @throws ClassNotFoundException
   *         The class of one of the <Code>HashMap</code>'s
   *         elements cannot be found.
   *
   * @see #writeHashMap
   */
  public static <K,V> HashMap<K,V> readHashMap(DataInput in)
    throws IOException, ClassNotFoundException {

    InternalDataSerializer.checkIn(in);

    int size = InternalDataSerializer.readArrayLength(in);
    if (size == -1) {
      return null;
    } else {
      HashMap<K,V> map = new HashMap<K,V>(size);
      for (int i = 0; i < size; i++) {
        K key = DataSerializer.<K>readObject(in);
        V value = DataSerializer.<V>readObject(in);
        map.put(key, value);
      }

      if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
        logger.trace(LogMarker.SERIALIZER, "Read HashMap with {} elements: {}", size, map);
      }

      return map;
    }
  }

  /**
   * Writes a <code>IdentityHashMap</code> to a <code>DataOutput</code>.  Note
   * that even though <code>map</code> may be an instance of a
   * subclass of <code>IdentityHashMap</code>, <code>readIdentityHashMap</code> will
   * always return an instance of <code>IdentityHashMap</code>, <B>not</B> an
   * instance of the subclass.  To preserve the class type of
   * <code>map</code>, {@link #writeObject(Object, DataOutput)} should be used for data
   * serialization.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   *
   * @see #readIdentityHashMap
   */
  public static void writeIdentityHashMap(IdentityHashMap<?,?> map, DataOutput out)
    throws IOException {

    InternalDataSerializer.checkOut(out);

    int size;
    if (map == null) {
      size = -1;
    } else {
      size = map.size();
    }
    InternalDataSerializer.writeArrayLength(size, out);
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing IdentityHashMap with {} elements: {}", size, map);
    }
    if (size > 0) {
      for (Map.Entry<?,?> entry: map.entrySet()){
        writeObject(entry.getKey(), out);
        writeObject(entry.getValue(), out);
      }
    }
  }

  /**
   * Reads a <code>IdentityHashMap</code> from a <code>DataInput</code>.
   * Note that key identity is not preserved unless the keys belong to a class
   * whose serialization preserves identity.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   * @throws ClassNotFoundException
   *         The class of one of the <Code>IdentityHashMap</code>'s
   *         elements cannot be found.
   *
   * @see #writeIdentityHashMap
   */
  public static <K,V> IdentityHashMap<K,V> readIdentityHashMap(DataInput in)
    throws IOException, ClassNotFoundException {

    InternalDataSerializer.checkIn(in);

    int size = InternalDataSerializer.readArrayLength(in);
    if (size == -1) {
      return null;
    } else {
      IdentityHashMap<K,V> map = new IdentityHashMap<K,V>(size);
      for (int i = 0; i < size; i++) {
        K key = DataSerializer.<K>readObject(in);
        V value = DataSerializer.<V>readObject(in);
        map.put(key, value);
      }

      if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
        logger.trace(LogMarker.SERIALIZER, "Read IdentityHashMap with {} elements: {}", size, map);
      }

      return map;
    }
  }

  /**
   * Writes a <code>ConcurrentHashMap</code> to a <code>DataOutput</code>.  Note
   * that even though <code>map</code> may be an instance of a
   * subclass of <code>ConcurrentHashMap</code>, <code>readConcurrentHashMap</code> will
   * always return an instance of <code>ConcurrentHashMap</code>, <B>not</B> an
   * instance of the subclass.  To preserve the class type of
   * <code>map</code>, {@link #writeObject(Object, DataOutput)} should be used for data
   * serialization.
   * <P>At this time if {@link #writeObject(Object, DataOutput)} is called with an instance
   * of ConcurrentHashMap then it will be serialized with normal java.io Serialization. So
   * if you want the keys and values of a ConcurrentHashMap to take advantage of GemFire serialization
   * it must be serialized with this method.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   *
   * @see #readConcurrentHashMap
   * @since GemFire 6.6
   */
  public static void writeConcurrentHashMap(ConcurrentHashMap<?,?> map, DataOutput out)
    throws IOException {

    InternalDataSerializer.checkOut(out);

    int size;
    Collection<Map.Entry<?,?>> entrySnapshot = null;
    if (map == null) {
      size = -1;
    } else {
      // take a snapshot to fix bug 44562
      entrySnapshot = new ArrayList<Map.Entry<?,?>>(map.entrySet());
      size = entrySnapshot.size();
    }
    InternalDataSerializer.writeArrayLength(size, out);
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing ConcurrentHashMap with {} elements: {}", size, entrySnapshot);
    }
    if (size > 0) {
      for (Map.Entry<?,?> entry: entrySnapshot) {
        writeObject(entry.getKey(), out);
        writeObject(entry.getValue(), out);
      }
    }
  }

  /**
   * Reads a <code>ConcurrentHashMap</code> from a <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   * @throws ClassNotFoundException
   *         The class of one of the <Code>ConcurrentHashMap</code>'s
   *         elements cannot be found.
   *
   * @see #writeConcurrentHashMap
   * @since GemFire 6.6
   */
  public static <K,V> ConcurrentHashMap<K,V> readConcurrentHashMap(DataInput in)
    throws IOException, ClassNotFoundException {

    InternalDataSerializer.checkIn(in);

    int size = InternalDataSerializer.readArrayLength(in);
    if (size == -1) {
      return null;
    } else {
      ConcurrentHashMap<K,V> map = new ConcurrentHashMap<K,V>(size);
      for (int i = 0; i < size; i++) {
        K key = DataSerializer.<K>readObject(in);
        V value = DataSerializer.<V>readObject(in);
        map.put(key, value);
      }

      if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
        logger.trace(LogMarker.SERIALIZER, "Read ConcurrentHashMap with {} elements: {}", size, map);
      }

      return map;
    }
  }

  /**
   * Writes a <code>Hashtable</code> to a <code>DataOutput</code>.  Note
   * that even though <code>map</code> may be an instance of a
   * subclass of <code>Hashtable</code>, <code>readHashtable</code> will
   * always return an instance of <code>Hashtable</code>, <B>not</B> an
   * instance of the subclass.  To preserve the class type of
   * <code>map</code>, {@link #writeObject(Object, DataOutput)} should be used for data
   * serialization.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   *
   * @see #readHashtable
   * @since GemFire 5.7
   */
  public static void writeHashtable(Hashtable<?,?> map, DataOutput out)
    throws IOException {

    InternalDataSerializer.checkOut(out);

    int size;
    if (map == null) {
      size = -1;
    } else {
      size = map.size();
    }
    InternalDataSerializer.writeArrayLength(size, out);
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing Hashtable with {} elements: {}", size, map);
    }
    if (size > 0) {
      for (Map.Entry<?,?> entry: map.entrySet()) {
        writeObject(entry.getKey(), out);
        writeObject(entry.getValue(), out);
      }
    }
  }

  /**
   * Reads a <code>Hashtable</code> from a <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   * @throws ClassNotFoundException
   *         The class of one of the <Code>Hashtable</code>'s
   *         elements cannot be found.
   *
   * @see #writeHashtable
   * @since GemFire 5.7
   */
  public static <K,V> Hashtable<K,V> readHashtable(DataInput in)
    throws IOException, ClassNotFoundException {

    InternalDataSerializer.checkIn(in);

    int size = InternalDataSerializer.readArrayLength(in);
    if (size == -1) {
      return null;
    } else {
      Hashtable<K,V> map = new Hashtable<K,V>(size);
      for (int i = 0; i < size; i++) {
        K key = DataSerializer.<K>readObject(in);
        V value = DataSerializer.<V>readObject(in);
        map.put(key, value);
      }

      if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
        logger.trace(LogMarker.SERIALIZER, "Read Hashtable with {} elements: {}", size, map);
      }

      return map;
    }
  }

  /**
   * Writes a <code>TreeMap</code> to a <code>DataOutput</code>.  Note
   * that even though <code>map</code> may be an instance of a
   * subclass of <code>TreeMap</code>, <code>readTreeMap</code> will
   * always return an instance of <code>TreeMap</code>, <B>not</B> an
   * instance of the subclass.  To preserve the class type of
   * <code>map</code>, {@link #writeObject(Object, DataOutput)} should be used for data
   * serialization.
   * <p>If the map has a comparator then it must also be serializable.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   *
   * @see #readTreeMap
   * @since GemFire 5.7
   */
  public static void writeTreeMap(TreeMap<?,?> map, DataOutput out)
    throws IOException {

    InternalDataSerializer.checkOut(out);

    int size;
    if (map == null) {
      size = -1;
    } else {
      size = map.size();
    }
    InternalDataSerializer.writeArrayLength(size, out);
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing TreeMap with {} elements: {}", size, map);
    }
    if (size >= 0) {
      writeObject(map.comparator(), out);
      for (Map.Entry<?,?> entry: map.entrySet()) {
        writeObject(entry.getKey(), out);
        writeObject(entry.getValue(), out);
      }
    }
  }

  /**
   * Reads a <code>TreeMap</code> from a <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   * @throws ClassNotFoundException
   *         The class of one of the <Code>TreeMap</code>'s
   *         elements cannot be found.
   *
   * @see #writeTreeMap
   * @since GemFire 5.7
   */
  public static <K,V> TreeMap<K,V> readTreeMap(DataInput in)
    throws IOException, ClassNotFoundException {

    InternalDataSerializer.checkIn(in);

    int size = InternalDataSerializer.readArrayLength(in);
    if (size == -1) {
      return null;
    } else {
      Comparator<? super K> c = InternalDataSerializer.<Comparator<? super K>>readNonPdxInstanceObject(in);
      TreeMap<K,V> map = new TreeMap<K,V>(c);
      for (int i = 0; i < size; i++) {
        K key = DataSerializer.<K>readObject(in);
        V value = DataSerializer.<V>readObject(in);
        map.put(key, value);
      }

      if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
        logger.trace(LogMarker.SERIALIZER, "Read TreeMap with {} elements: {}", size, map);
      }

      return map;
    }
  }

  /**
   * Writes a <code>TreeSet</code> to a <code>DataOutput</code>.  Note
   * that even though <code>set</code> may be an instance of a
   * subclass of <code>TreeSet</code>, <code>readTreeSet</code> will
   * always return an instance of <code>TreeSet</code>, <B>not</B> an
   * instance of the subclass.  To preserve the class type of
   * <code>set</code>, {@link #writeObject(Object, DataOutput)} should be used for data
   * serialization.
   * <p>If the set has a comparator then it must also be serializable.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   *
   * @see #readTreeSet
   */
  public static void writeTreeSet(TreeSet<?> set, DataOutput out)
    throws IOException {
    InternalDataSerializer.checkOut(out);

    int size;
    
    if (set == null) {
      size = -1;
    } else {
      size = set.size();
    }
    InternalDataSerializer.writeArrayLength(size, out);
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing TreeSet with {} elements: {}", size, set);
    }
    if (size >= 0) {
      writeObject(set.comparator(), out);
      for (Object e: set) {
        writeObject(e, out);
      }
    }
  }

  /**
   * Reads a <code>TreeSet</code> from a <code>DataInput</code>.
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   * @throws ClassNotFoundException
   *         The class of one of the <Code>TreeSet</code>'s
   *         elements cannot be found.
   *
   * @see #writeTreeSet
   */
  public static <E> TreeSet<E> readTreeSet(DataInput in)
    throws IOException, ClassNotFoundException {

    InternalDataSerializer.checkIn(in);

    int size = InternalDataSerializer.readArrayLength(in);
    if (size == -1) {
      return null;
    } else {
      Comparator<? super E> c = InternalDataSerializer.<Comparator<? super E>>readNonPdxInstanceObject(in);
      TreeSet<E> set = new TreeSet<E>(c);
      for (int i = 0; i < size; i++) {
        E element = DataSerializer.<E>readObject(in);
        set.add(element);
      }

      if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
        logger.trace(LogMarker.SERIALIZER, "Read TreeSet with {} elements: {}", size, set);
      }

      return set;
    }
  }

  /**
   * Writes a <code>Properties</code> to a <code>DataOutput</code>.
   * <P> NOTE: The <code>defaults</code> of the specified <code>props</code>
   * are not serialized.
   * <p> Note that even though <code>props</code> may be an instance of a
   * subclass of <code>Properties</code>, <code>readProperties</code> will
   * always return an instance of <code>Properties</code>, <B>not</B> an
   * instance of the subclass.  To preserve the class type of
   * <code>props</code>, {@link #writeObject(Object, DataOutput)} should be used for data
   * serialization.
   *
   * @throws IOException
   *         A problem occurs while writing to <code>out</code>
   *
   * @see #readProperties
   */
  public static void writeProperties(Properties props, DataOutput out)
      throws IOException {
    InternalDataSerializer.checkOut(out);

    Set<Map.Entry<Object,Object>> s;
    int size;
    if (props == null) {
      s = null;
      size = -1;
    } else {
      s = props.entrySet();
      size = s.size();
    }
    InternalDataSerializer.writeArrayLength(size, out);
    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing Properties with {} elements: {}", size, props);
    }
    if (size > 0) {
      for (Map.Entry<Object,Object> entry: s) {
        // although we should have just String instances in a Properties
        // object our security code stores byte[] instances in them
        // so the following code must use writeObject instead of writeString.
        Object key = entry.getKey();
        Object val = entry.getValue();
        writeObject(key, out);
        writeObject(val, out);
      }
    }
  }

  /**
   * Reads a <code>Properties</code> from a <code>DataInput</code>.
   * <P> NOTE: the <code>defaults</code> are always empty in the returned <code>Properties</code>.
   *
   *
   * @throws IOException
   *         A problem occurs while reading from <code>in</code>
   *
   * @see #writeProperties
   */
  public static Properties readProperties(DataInput in) throws IOException,
      ClassNotFoundException {

    InternalDataSerializer.checkIn(in);

    int size = InternalDataSerializer.readArrayLength(in);
    if (size == -1) {
      return null;
    }
    else {
      Properties props = new Properties();
      for (int index = 0; index < size; index++) {
        Object key = readObject(in);
        Object value = readObject(in);
        props.put(key, value);
      }
      if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
        logger.trace(LogMarker.SERIALIZER, "Read Properties with {} elements: {}", size, props);
      }
      return props;
    }
  }

  // since 5.7 moved all time unit code to InternalDataSerializer
  // since the TimeUnit class is part of the non-public backport.

  /**
   * Writes an arbitrary object to a <code>DataOutput</code>.  If
   * <code>o</code> is not an instance of a specially-handled
   * standard Java class (see the list in {@link #getSupportedClasses}),
   * the {@link DataSerializer#toData toData} method of each
   * registered <code>DataSerializer</code> is invoked until the object
   * is serialized.  If no registered serializer can serialize the
   * object and <code>o</code> does not implement
   * <code>DataSerializable</code>, then it is serialized to
   * <code>out</code> using standard Java {@linkplain
   * java.io.Serializable serialization}.
   * This method will serialize a
   * <code>null</code> o and not throw a
   * <code>NullPointerException</code>.
   *
   * @param allowJavaSerialization
   *        If false, then a NotSerializableException is thrown
   *        in the case where standard Java serialization would
   *        otherwise be used for object <code>o</code> or for any nested
   *        subobject of <code>o</code>. This is used to prevent
   *        Java serialization from being used when sending data
   *        to a non-Java client
   * @throws IOException
   *         A problem occurs while writing <code>o</code> to
   *         <code>out</code>
   *
   * @see #readObject(DataInput)
   * @see Instantiator
   * @see ObjectOutputStream#writeObject
   */
  public static final void writeObject(Object o, DataOutput out, boolean allowJavaSerialization)
    throws IOException {
    
      if (allowJavaSerialization) {
        writeObject(o, out);
        return;
      }

      DISALLOW_JAVA_SERIALIZATION.set(Boolean.TRUE);
      try {
        writeObject(o, out);
      } finally {
        DISALLOW_JAVA_SERIALIZATION.set(Boolean.FALSE); // with JDK 1.5 this can be changed to remove()
      }
  }


  /**
   * Writes an arbitrary object to a <code>DataOutput</code>.  If
   * <code>o</code> is not an instance of a specially-handled
   * standard Java class (such as <code>Date</code>,
   * <code>Integer</code>, or <code>ArrayList</code>), the {@link
   * DataSerializer#toData toData} method of each
   * registered <code>DataSerializer</code> is invoked until the object
   * is serialized.  If no registered serializer can serialize the
   * object and <code>o</code> does not implement
   * <code>DataSerializable</code>, then it is serialized to
   * <code>out</code> using standard Java {@linkplain
   * java.io.Serializable serialization}.
   * This method will serialize a
   * <code>null</code> o and not throw a
   * <code>NullPointerException</code>.
   *
   * @throws IOException
   *         A problem occurs while writing <code>o</code> to
   *         <code>out</code>
   *
   * @see #readObject(DataInput)
   * @see DataSerializer
   * @see ObjectOutputStream#writeObject
   */
  public static final void writeObject(Object o, DataOutput out)
    throws IOException {
    InternalDataSerializer.basicWriteObject(o, out, false);
  }

  /**
   * Reads an arbitrary object from a <code>DataInput</code>.
   * Instances of classes that are not handled specially (such as
   * <code>String</code>, <code>Class</code>, and
   * <code>DataSerializable</code>) are read using standard Java
   * {@linkplain java.io.Serializable serialization}.
   *
   * <P>
   *
   * Note that if an object is deserialized using standard Java
   * serialization, its class will be loaded using the current
   * thread's {@link Thread#getContextClassLoader context class
   * loader} before the one normally used by Java serialization is
   * consulted.
   *
   * @throws IOException
   *         A problem occured while reading from <code>in</code>
   *        (may wrap another exception)
   * @throws ClassNotFoundException
   *         The class of an object read from <code>in</code> could
   *         not be found
   *
   * @see #writeObject(Object, DataOutput)
   * @see ObjectInputStream#readObject
   */
  @SuppressWarnings("unchecked")
  public static final <T> T readObject(final DataInput in)
    throws IOException, ClassNotFoundException {
    return (T) InternalDataSerializer.basicReadObject(in);
  }

  /**
   * Registers a <code>DataSerializer</code>  class with the data
   * serialization framework.  This method uses reflection to create
   * an instance of the <code>DataSerializer</code> class by invoking
   * its zero-argument constructor.
   *
   * <P>
   *
   * The <code>DataSerializer</code> instance will be consulted by the
   * {@link #writeObject(Object, DataOutput)} and {@link #readObject} methods.
   * Note that no two serializers can support the same class.
   * <P>
   *
   * This method invokes the <Code>DataSerializer</code> instance's
   * {@link #getSupportedClasses} method and keeps track of which
   * classes can have their instances serialized by by this data serializer.
   *
   * @param c
   *        the <code>DataSerializer</code> class to create and
   *        register with the data serialization framework.
   * @return the registered serializer instance 
   *
   * @throws IllegalArgumentException
   *         If <code>c</code> does not subclass
   *         <code>DataSerializer</code>, if <code>c</code> does not
   *         have a zero-argument constructor,
   *         if <code>id</code> is 0,
   *         if getSupportedClasses returns null or an empty array,
   *         if getSupportedClasses returns and array with null elements
   * @throws IllegalStateException
   *         if another serializer
   *         instance with id <code>id</code> has already been registered,
   *         if another serializer instance that supports one of this instances
   *         classes has already been registered,
   *         if an attempt is made to support any of the classes reserved by DataSerializer
   *         (see {@link #getSupportedClasses} for a list).
   * @see #getSupportedClasses
   */
  public static final DataSerializer register(Class<?> c) {
    return InternalDataSerializer.register(c, true);
  }
  /**
   * @deprecated as of 5.7 use {@link #register(Class)} instead
   */
  @Deprecated
  public static final DataSerializer register(Class<?> c, byte b) {
    return register(c);
  }

  ///////////////////////  Constructors  ///////////////////////

  /**
   * Creates a new <code>DataSerializer</code>.  All class that
   * implement <code>DataSerializer</code> must provide a
   * zero-argument constructor.
   *
   * @see #register(Class)
   */
  public DataSerializer() {

  }

  /////////////////////  Instance Methods  /////////////////////

  /**
   * Returns the <code>Class</code>es whose instances are data
   * serialized by this <code>DataSerializer</code>.  This method is
   * invoked when this serializer is {@linkplain #register(Class)
   * registered}. This method is not allowed to return <code>null</code>
   * nor an empty array.
   * Only instances whose class name is the same as one of the class names
   * in the result will be serialized by this <code>DataSerializer</code>.
   * Two <code>DataSerializer</code>s are not allowed to support the same class.
   * The following classes can not be supported by user defined data serializers
   * since they are all supported by the predefined data serializer:
   * <ul>
   * <li>{@link java.lang.Class}
   * <li>{@link java.lang.String}
   * <li>{@link java.lang.Boolean}
   * <li>{@link java.lang.Byte}
   * <li>{@link java.lang.Character}
   * <li>{@link java.lang.Short}
   * <li>{@link java.lang.Integer}
   * <li>{@link java.lang.Long}
   * <li>{@link java.lang.Float}
   * <li>{@link java.lang.Double}
   * <li>{@link java.io.File}
   * <li>{@link java.net.InetAddress}
   * <li>{@link java.net.Inet4Address}
   * <li>{@link java.net.Inet6Address}
   * <li>{@link java.util.ArrayList}
   * <li>{@link java.util.Date}
   * <li>{@link java.util.HashMap}
   * <li>{@link java.util.IdentityHashMap}
   * <li>{@link java.util.HashSet}
   * <li>{@link java.util.Hashtable}
   * <li>{@link java.util.LinkedHashSet}
   * <li>{@link java.util.LinkedList}
   * <li>{@link java.util.Properties}
   * <li>{@link java.util.TreeMap}
   * <li>{@link java.util.TreeSet}
   * <li>{@link java.util.Vector}
   * <li><code>any array class</code>
   * </ul>
   */
  public abstract Class<?>[] getSupportedClasses();

  /**
   * Data serializes an object to a <code>DataOutput</code>.  It is
   * very important that when performing the "switch" on
   * <code>o</code>'s class, your code test for a subclass before it
   * tests for a superclass.  Otherwise, the incorrect class id could
   * be written to the serialization stream.
   *
   * @param o
   *        The object to data serialize.  It will never be
   *        <code>null</code>.
   *
   * @return <code>false</code> if this <code>DataSerializer</code> does
   *         not know how to data serialize <code>o</code>.
   */
  public abstract boolean toData(Object o, DataOutput out)
    throws IOException;

  /**
   * Reads an object from a <code>DataInput</code>.
   * This implementation must support deserializing everything serialized by
   * the matching {@link #toData}.
   *
   * @throws IOException
   *         If this serializer cannot read an object from
   *         <code>in</code>.
   *
   * @see #toData
   */
  public abstract Object fromData(DataInput in)
    throws IOException, ClassNotFoundException;

  /**
   * Returns the id of this <code>DataSerializer</code>.
   * <p> Returns an int instead of a byte since 5.7.
   */
  public abstract int getId();

  /**
   * Two <code>DataSerializer</code>s are consider to be equal if they
   * have the same id and the same class
   */
  @Override
  public boolean equals(Object o) {
    if (o instanceof DataSerializer) {
      DataSerializer oDS = (DataSerializer)o;
      return oDS.getId() == getId() && getClass().equals(oDS.getClass());
    } else {
      return false;
    }
  }
  @Override
  public int hashCode() {
    return getId();
  }
  
  /**
   * For internal use only.
   * Sets the unique <code>eventId</code> of this
   * <code>DataSerializer</code>. 
   * @since GemFire 6.5
   */
  public final void setEventId(Object/*EventID*/ eventId) {
    this.eventId = (EventID)eventId;
  }
  
  /**
   * For internal use only.
   * Returns the unique <code>eventId</code> of this
   * <code>DataSerializer</code>.
   * @since GemFire 6.5
   */
  public final Object/*EventID*/ getEventId() {
    return this.eventId;
  }
  
  /**
   * For internal use only.
   * Sets the context of this
   * <code>DataSerializer</code>. 
   * @since GemFire 6.5
   */
  public final void setContext(Object/*ClientProxyMembershipID*/ context) {
    this.context = (ClientProxyMembershipID)context;
  }
  
  /**
   * For internal use only.
   * Returns the context of this
   * <code>DataSerializer</code>.
   * @since GemFire 6.5
   */
  public final Object/*ClientProxyMembershipID*/ getContext() {
    return this.context;
  }
  
  /**
   * maps a class to its enum constants.
   */
  private final static ConcurrentMap knownEnums = new ConcurrentHashMap();

  /**
   * gets the enum constants for the given class.
   * {@link Class#getEnumConstants()} uses reflection, so we keep around the
   * class to enumConstants mapping in the {@link #knownEnums} map
   * 
   * @param <E>
   * @param clazz
   * @return enum constants for the given class
   */
  private static <E extends Enum> E[] getEnumConstantsForClass(Class<E> clazz) {
    E[] returnVal = (E[])knownEnums.get(clazz);
    if (returnVal == null) {
      returnVal = clazz.getEnumConstants();
      knownEnums.put(clazz, returnVal);
    }
    return returnVal;
  }

  /**
   * Writes the <code>Enum constant</code> to <code>DataOutput</code>. Unlike
   * standard java serialization which serializes both the enum name String and
   * the ordinal, GemFire only serializes the ordinal byte, so for backward
   * compatibility new enum constants should only be added to the end of the
   * enum type.<br />
   * Example: <code>DataSerializer.writeEnum(DAY_OF_WEEK.SUN, out);</code>
   * 
   * @see #readEnum(Class, DataInput)
   * @since GemFire 6.5
   * @throws IOException
   */
  public static void writeEnum(Enum e, DataOutput out) throws IOException {

    InternalDataSerializer.checkOut(out);

    if (e == null) {
      throw new NullPointerException(
          LocalizedStrings.DataSerializer_ENUM_TO_SERIALIZE_IS_NULL
              .toLocalizedString());
    }

    if (logger.isTraceEnabled(LogMarker.SERIALIZER)) {
      logger.trace(LogMarker.SERIALIZER, "Writing enum {}", e);
    }
    InternalDataSerializer.writeArrayLength(e.ordinal(), out);
  }

  /**
   * Reads a <code>Enum constant</code> from <code>DataInput</code>. Unlike
   * standard java serialization which serializes both the enum name String and
   * the ordinal, GemFire only serializes the ordinal byte, so be careful about
   * using the correct enum class. Also, for backward compatibility new enum
   * constants should only be added to the end of the enum type.<br />
   * Example:
   * <code>DAY_OF_WEEK d = DataSerializer.readEnum(DAY_OF_WEEK.class, in);</code>
   * 
   * @since GemFire 6.5
   * @see #writeEnum(Enum, DataOutput)
   * @throws IOException
   *           A problem occurs while writing to <code>out</code>
   * @throws ArrayIndexOutOfBoundsException
   *           if the wrong enum class/enum class with a different version and
   *           less enum constants is used
   */
  public static <E extends Enum<E>> E readEnum(Class<E> clazz, DataInput in)
      throws IOException {

    InternalDataSerializer.checkIn(in);

    if (clazz == null) {
      throw new NullPointerException(
          LocalizedStrings.DataSerializer_ENUM_CLASS_TO_DESERIALIZE_IS_NULL
              .toLocalizedString());
    } else if (!clazz.isEnum()) {
      throw new IllegalArgumentException(
          LocalizedStrings.DataSerializer_CLASS_0_NOT_ENUM
              .toLocalizedString(clazz.getName()));
    }

    int ordinal = InternalDataSerializer.readArrayLength(in);

    return getEnumConstantsForClass(clazz)[ordinal];
  }
}

