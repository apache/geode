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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Random;
import java.util.Stack;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.CanonicalInstantiator;
import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.Instantiator;
import org.apache.geode.SystemFailure;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.VersionedDataInputStream;
import org.apache.geode.internal.serialization.VersionedDataOutputStream;
import org.apache.geode.internal.statistics.StatArchiveWriter;
import org.apache.geode.internal.tcp.ByteBufferInputStream;
import org.apache.geode.test.junit.categories.SerializationTest;

/**
 * Tests the functionality of the {@link DataSerializable} class.
 *
 * @since GemFire 3.0
 */
@Category({SerializationTest.class})
public class DataSerializableJUnitTest implements Serializable {

  /** A <code>ByteArrayOutputStream</code> that data is serialized to */
  private transient ByteArrayOutputStream baos;

  @Rule
  public transient TestName testName = new TestName();

  /**
   * Creates a new <code>ByteArrayOutputStream</code> for this test to work with.
   */
  @Before
  public void setUp() {
    baos = new ByteArrayOutputStream();
  }

  public void tearDown() {
    baos = null;
  }

  /**
   * Returns a <code>DataOutput</code> to write to
   */
  private DataOutputStream getDataOutput() {
    return new DataOutputStream(baos);
  }

  /**
   * Returns a <code>DataInput</code> to read from
   */
  private DataInput getDataInput() {
    // changed this to use ByteBufferInputStream to give us better
    // test coverage of this class.
    ByteBuffer bb = ByteBuffer.wrap(baos.toByteArray());
    return new ByteBufferInputStream(bb);
  }

  private DataInputStream getDataInputStream() {
    ByteBuffer bb = ByteBuffer.wrap(baos.toByteArray());
    ByteBufferInputStream bbis = new ByteBufferInputStream(bb);
    return new DataInputStream(bbis);
  }

  /**
   * Returns a random number generator
   */
  private Random getRandom() {
    long seed = Long.getLong("SEED", System.currentTimeMillis());
    System.out.println("SEED for " + testName.getMethodName() + ": " + seed);
    return new Random(seed);
  }

  /**
   * Tests data serializing a {@link Class}
   */
  @Test
  public void testClass() throws Exception {
    Class<? extends DataSerializableJUnitTest> c = getClass();

    DataOutputStream out = getDataOutput();
    DataSerializer.writeClass(c, out);
    out.flush();

    DataInput in = getDataInput();
    Class<?> c2 = DataSerializer.readClass(in);
    assertEquals(c, c2);
  }

  /**
   * Tests data serializing a {@link Class} using {@link DataSerializer#writeObject}.
   */
  @Test
  public void testClassObject() throws Exception {
    Class<? extends DataSerializableJUnitTest> c = getClass();

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(c, out);
    out.flush();

    DataInput in = getDataInput();
    Class<?> c2 = DataSerializer.readObject(in);
    assertEquals(c, c2);
  }

  @Test
  public void testBigInteger() throws Exception {
    BigInteger o = new BigInteger("12345678901234567890");

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(o, out, false);
    out.flush();

    DataInput in = getDataInput();
    BigInteger o2 = DataSerializer.readObject(in);
    assertEquals(o, o2);
  }

  @Test
  public void testBigDecimal() throws Exception {
    BigDecimal o = new BigDecimal("1234567890.1234567890");

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(o, out, false);
    out.flush();

    DataInput in = getDataInput();
    BigDecimal o2 = DataSerializer.readObject(in);
    assertEquals(o, o2);
  }

  @Test
  public void testUUID() throws Exception {
    UUID o = UUID.randomUUID();

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(o, out, false);
    out.flush();

    DataInput in = getDataInput();
    UUID o2 = DataSerializer.readObject(in);
    assertEquals(o, o2);
  }

  @Test
  public void testTimestamp() throws Exception {
    Timestamp o = new Timestamp(new Date().getTime() + 79);

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(o, out, false);
    out.flush();

    DataInput in = getDataInput();
    Timestamp o2 = DataSerializer.readObject(in);
    assertEquals(o, o2);
  }

  /**
   * Tests data serializing a {@link Date}
   */
  @Test
  public void testDate() throws Exception {
    Date date = new Date();

    DataOutputStream out = getDataOutput();
    DataSerializer.writeDate(date, out);
    out.flush();

    DataInput in = getDataInput();
    Date date2 = DataSerializer.readDate(in);
    assertEquals(date, date2);
  }

  /**
   * Tests data serializing a {@link Date} using {@link DataSerializer#writeObject}.
   */
  @Test
  public void testDateObject() throws Exception {
    Date date = new Date();

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(date, out);
    out.flush();

    DataInput in = getDataInput();
    Date date2 = DataSerializer.readObject(in);
    assertEquals(date, date2);
  }

  /**
   * Tests data serializing a {@link File}
   */
  @Test
  public void testFile() throws Exception {
    File file = new File(System.getProperty("user.dir"));

    DataOutputStream out = getDataOutput();
    DataSerializer.writeFile(file, out);
    out.flush();

    DataInput in = getDataInput();
    File file2 = DataSerializer.readFile(in);
    assertEquals(file, file2);
  }

  /**
   * Tests data serializing a {@link File} using {@link DataSerializer#writeObject}.
   */
  @Test
  public void testFileObject() throws Exception {
    File file = new File(System.getProperty("user.dir"));

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(file, out);
    out.flush();

    DataInput in = getDataInput();
    File file2 = DataSerializer.readObject(in);
    assertEquals(file, file2);
  }

  /**
   * Tests data serializing a {@link InetAddress}
   */
  @Test
  public void testInetAddress() throws Exception {
    InetAddress address = InetAddress.getLocalHost();

    DataOutputStream out = getDataOutput();
    DataSerializer.writeInetAddress(address, out);
    out.flush();

    DataInput in = getDataInput();
    InetAddress address2 = DataSerializer.readInetAddress(in);
    assertEquals(address, address2);
  }

  /**
   * Tests data serializing a {@link InetAddress} using {@link DataSerializer#writeObject}.
   */
  @Test
  public void testInetAddressObject() throws Exception {
    InetAddress address = InetAddress.getLocalHost();

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(address, out);
    out.flush();

    DataInput in = getDataInput();
    InetAddress address2 = DataSerializer.readObject(in);
    assertEquals(address, address2);
  }

  /**
   * Tests data serializing <code>null</code> using {@link DataSerializer#writeObject}.
   */
  @Test
  public void testNullObject() throws Exception {
    Object value = null;

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(value, out);
    out.flush();

    DataInput in = getDataInput();
    Object value2 = DataSerializer.readObject(in);
    assertEquals(value, value2);
  }

  /**
   * Tests data serializing a non-<code>null</code> {@link String}
   */
  @Test
  public void testString() throws Exception {
    String value = "Hello";

    DataOutputStream out = getDataOutput();
    DataSerializer.writeString(value, out);
    DataSerializer.writeObject(value, out);
    out.flush();

    DataInput in = getDataInput();
    String value2 = DataSerializer.readString(in);
    assertEquals(value, value2);
    value2 = DataSerializer.readObject(in);
    assertEquals(value, value2);
  }

  @Test
  public void testUtfString() throws Exception {
    String value = "Hello" + Character.MIN_VALUE + Character.MAX_VALUE;

    DataOutputStream out = getDataOutput();
    DataSerializer.writeString(value, out);
    DataSerializer.writeObject(value, out);
    out.flush();

    DataInput in = getDataInput();
    String value2 = DataSerializer.readString(in);
    assertEquals(value, value2);
    value2 = DataSerializer.readObject(in);
    assertEquals(value, value2);
  }

  @Test
  public void testBigString() throws Exception {
    StringBuilder sb = new StringBuilder(100000);
    for (int i = 0; i < 100000; i++) {
      sb.append("a");
    }
    String value = sb.toString();

    DataOutputStream out = getDataOutput();
    DataSerializer.writeString(value, out);
    DataSerializer.writeObject(value, out);
    out.flush();

    DataInput in = getDataInput();
    String value2 = DataSerializer.readString(in);
    assertEquals(value, value2);
    value2 = DataSerializer.readObject(in);
    assertEquals(value, value2);
  }

  /**
   * Tests data serializing a non-<code>null</code> {@link String} longer than 64k.
   */
  @Test
  public void testBigUtfString() throws Exception {
    StringBuilder sb = new StringBuilder(100000);
    for (int i = 0; i < 100000; i++) {
      if ((i % 2) == 0) {
        sb.append(Character.MAX_VALUE);
      } else {
        sb.append(Character.MIN_VALUE);
      }
    }
    String value = sb.toString();

    DataOutputStream out = getDataOutput();
    DataSerializer.writeString(value, out);
    DataSerializer.writeObject(value, out);
    out.flush();

    DataInput in = getDataInput();
    String value2 = DataSerializer.readString(in);
    assertEquals(value, value2);
    value2 = DataSerializer.readObject(in);
    assertEquals(value, value2);
  }

  /**
   * Tests data serializing a non-ascii {@link String}
   */
  @Test
  public void testNonAsciiString() throws Exception {
    basicTestString("Hello1" + '\u0000');
    setUp();
    basicTestString("Hello2" + '\u0080');
    setUp();
    basicTestString("Hello3" + '\uFFFF');
  }

  private void basicTestString(String value) throws IOException {
    DataOutputStream out = getDataOutput();
    DataSerializer.writeString(value, out);
    out.flush();

    DataInput in = getDataInput();
    String value2 = DataSerializer.readString(in);
    assertEquals(value, value2);

  }

  /**
   * Tests data serializing a <code>null</code> {@link String}
   */
  @Ignore("for unknown reason")
  @Test
  public void testNullString() throws Exception {
    basicTestString(null);
  }

  /**
   * Tests data serializing a {@link Boolean}
   */
  @Test
  public void testBoolean() throws Exception {
    Boolean value = getRandom().nextInt() % 2 == 0;

    DataOutputStream out = getDataOutput();
    DataSerializer.writeBoolean(value, out);
    out.flush();

    DataInput in = getDataInput();
    Boolean value2 = DataSerializer.readBoolean(in);
    assertEquals(value, value2);
  }

  /**
   * Tests data serializing a {@link Boolean} using {@link DataSerializer#writeObject}.
   */
  @Test
  public void testBooleanObject() throws Exception {

    Boolean value = getRandom().nextInt() % 2 == 0;

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(value, out);
    out.flush();

    DataInput in = getDataInput();
    Boolean value2 = DataSerializer.readObject(in);
    assertEquals(value, value2);
  }

  @Test
  public void testWriteObjectAsByteArray() throws Exception {
    // make sure recursive calls to WriteObjectAsByteArray work to test bug 38194
    Object v = new WOABA();
    DataOutputStream out = getDataOutput();
    DataSerializer.writeObjectAsByteArray(v, out);
    out.flush();

    DataInput in = getDataInput();
    byte[] b2 = DataSerializer.readByteArray(in);
    // todo should we deserislize the byte[] and make sure it is equal to v?
    ByteArrayInputStream bais = new ByteArrayInputStream(b2);
    DataInputStream dis = new DataInputStream(bais);
    Object v2 = DataSerializer.readObject(dis);
    if (!(v2 instanceof WOABA)) {
      fail("expected instance of WOABA but found " + v2.getClass());
    }
  }

  private static class WOABA implements DataSerializable {
    private byte[] deserialized;
    private WOABA2 f = new WOABA2();

    public WOABA() {}

    public void validate() throws Exception {
      ByteArrayInputStream bais = new ByteArrayInputStream(deserialized);
      DataInputStream dis = new DataInputStream(bais);
      Object v = DataSerializer.readObject(dis);
      if (!(v instanceof WOABA2)) {
        fail("expected instance of WOABA2 but found " + v.getClass());
      }
      f = (WOABA2) v;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeObjectAsByteArray(f, out);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      deserialized = DataSerializer.readByteArray(in);
    }
  }

  private static class WOABA2 implements DataSerializable {

    public WOABA2() {}

    @Override
    public void toData(DataOutput out) throws IOException {
      String f = "foobar";
      DataSerializer.writeObjectAsByteArray(f, out);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      DataSerializer.readByteArray(in);
    }
  }

  /**
   * Tests data serializing a {@link Character}
   */
  @Test
  public void testCharacter() throws Exception {
    Character value = (char) ('A' + getRandom().nextInt('Z' - 'A'));

    DataOutputStream out = getDataOutput();
    DataSerializer.writeCharacter(value, out);
    out.flush();

    DataInput in = getDataInput();
    Character value2 = DataSerializer.readCharacter(in);
    assertEquals(value, value2);
  }

  /**
   * Tests data serializing a {@link Character} using {@link DataSerializer#writeObject}.
   */
  @Test
  public void testCharacterObject() throws Exception {
    Character value = (char) ('A' + getRandom().nextInt('Z' - 'A'));

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(value, out);
    out.flush();

    DataInput in = getDataInput();
    Character value2 = DataSerializer.readObject(in);
    assertEquals(value, value2);
  }

  /**
   * Tests data serializing a {@link Byte}
   */
  @Test
  public void testByte() throws Exception {
    Byte value = (byte) getRandom().nextInt();

    DataOutputStream out = getDataOutput();
    DataSerializer.writeByte(value, out);
    out.flush();

    DataInput in = getDataInput();
    Byte value2 = DataSerializer.readByte(in);
    assertEquals(value, value2);
  }

  /**
   * Tests data serializing a {@link Byte} using {@link DataSerializer#writeObject}.
   */
  @Test
  public void testByteObject() throws Exception {
    Byte value = (byte) getRandom().nextInt();

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(value, out);
    out.flush();

    DataInput in = getDataInput();
    Byte value2 = DataSerializer.readObject(in);
    assertEquals(value, value2);
  }

  /**
   * Tests data serializing a {@link Short}
   */
  @Test
  public void testShort() throws IOException {
    Short value = (short) getRandom().nextInt();

    DataOutputStream out = getDataOutput();
    DataSerializer.writeShort(value, out);
    out.flush();

    DataInput in = getDataInput();
    Short value2 = DataSerializer.readShort(in);
    assertEquals(value, value2);
  }

  /**
   * Tests data serializing a {@link Short} using {@link DataSerializer#writeObject}.
   */
  @Test
  public void testShortObject() throws IOException, ClassNotFoundException {

    Short value = (short) getRandom().nextInt();

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(value, out);
    out.flush();

    DataInput in = getDataInput();
    Short value2 = DataSerializer.readObject(in);
    assertEquals(value, value2);
  }

  /**
   * Tests data serializing a {@link Integer}
   */
  @Test
  public void testInteger() throws Exception {
    Integer value = getRandom().nextInt();

    DataOutputStream out = getDataOutput();
    DataSerializer.writeInteger(value, out);
    out.flush();

    DataInput in = getDataInput();
    Integer value2 = DataSerializer.readInteger(in);
    assertEquals(value, value2);
  }

  /**
   * Tests data serializing a {@link Integer} using {@link DataSerializer#writeObject}.
   */
  @Test
  public void testIntegerObject() throws Exception {
    Integer value = getRandom().nextInt();

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(value, out);
    out.flush();

    DataInput in = getDataInput();
    Integer value2 = DataSerializer.readObject(in);
    assertEquals(value, value2);
  }

  /**
   * Tests data serializing a {@link Long}
   */
  @Test
  public void testLong() throws Exception {
    Long value = getRandom().nextLong();

    DataOutputStream out = getDataOutput();
    DataSerializer.writeLong(value, out);
    out.flush();

    DataInput in = getDataInput();
    Long value2 = DataSerializer.readLong(in);
    assertEquals(value, value2);
  }

  /**
   * Tests data serializing a {@link Long} using {@link DataSerializer#writeObject}.
   */
  @Test
  public void testLongObject() throws Exception {
    Long value = getRandom().nextLong();

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(value, out);
    out.flush();

    DataInput in = getDataInput();
    Long value2 = DataSerializer.readObject(in);
    assertEquals(value, value2);
  }

  /**
   * Tests data serializing a {@link Float}
   */
  @Test
  public void testFloat() throws Exception {
    Float value = getRandom().nextFloat();

    DataOutputStream out = getDataOutput();
    DataSerializer.writeFloat(value, out);
    out.flush();

    DataInput in = getDataInput();
    Float value2 = DataSerializer.readFloat(in);
    assertEquals(value, value2);
  }

  /**
   * Tests data serializing a {@link Float} using {@link DataSerializer#writeObject}.
   */
  @Test
  public void testFloatObject() throws Exception {
    Float value = getRandom().nextFloat();

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(value, out);
    out.flush();

    DataInput in = getDataInput();
    Float value2 = DataSerializer.readObject(in);
    assertEquals(value, value2);
  }

  /**
   * Tests data serializing a {@link Double}
   */
  @Test
  public void testDouble() throws Exception {
    Double value = getRandom().nextDouble();

    DataOutputStream out = getDataOutput();
    DataSerializer.writeDouble(value, out);
    out.flush();

    DataInput in = getDataInput();
    Double value2 = DataSerializer.readDouble(in);
    assertEquals(value, value2);
  }

  /**
   * Tests data serializing a {@link Double} using {@link DataSerializer#writeObject}.
   */
  @Test
  public void testDoubleObject() throws Exception {
    Double value = getRandom().nextDouble();

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(value, out);
    out.flush();

    DataInput in = getDataInput();
    Double value2 = DataSerializer.readObject(in);
    assertEquals(value, value2);
  }

  /**
   * Tests data serializing a <code>byte</code> array
   */
  @Test
  public void testByteArray() throws Exception {
    byte[] array = new byte[] {(byte) 4, (byte) 5, (byte) 6};

    DataOutputStream out = getDataOutput();
    DataSerializer.writeByteArray(array, out);
    DataSerializer.writeObject(array, out);
    out.flush();

    DataInput in = getDataInput();
    for (int idx = 0; idx < 2; idx++) {
      byte[] array2 =
          (idx == 0) ? DataSerializer.readByteArray(in) : (byte[]) DataSerializer.readObject(in);
      assertEquals(array.length, array2.length);
      for (int i = 0; i < array.length; i++) {
        assertEquals(array[i], array2[i]);
      }
    }
  }

  /**
   * Tests data serializing a <code>byte</code> array using {@link DataSerializer#writeObject}.
   */
  @Test
  public void testByteArrayObject() throws Exception {
    byte[] array = new byte[] {(byte) 4, (byte) 5, (byte) 6};

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(array, out);
    out.flush();

    DataInput in = getDataInput();
    byte[] array2 = DataSerializer.readObject(in);

    assertEquals(array.length, array2.length);
    for (int i = 0; i < array.length; i++) {
      assertEquals(array[i], array2[i]);
    }
  }

  /**
   * Tests data serializing a <code>short</code> array
   */
  @Test
  public void testShortArray() throws Exception {
    short[] array = new short[] {(short) 4, (short) 5, (short) 6};

    DataOutputStream out = getDataOutput();
    DataSerializer.writeShortArray(array, out);
    out.flush();

    DataInput in = getDataInput();
    short[] array2 = DataSerializer.readShortArray(in);

    assertEquals(array.length, array2.length);
    for (int i = 0; i < array.length; i++) {
      assertEquals(array[i], array2[i]);
    }
  }

  /**
   * Tests data serializing a <code>short</code> array using {@link DataSerializer#writeObject}.
   */
  @Test
  public void testShortArrayObject() throws Exception {
    short[] array = new short[] {(short) 4, (short) 5, (short) 6};

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(array, out);
    out.flush();

    DataInput in = getDataInput();
    short[] array2 = DataSerializer.readObject(in);

    assertEquals(array.length, array2.length);
    for (int i = 0; i < array.length; i++) {
      assertEquals(array[i], array2[i]);
    }
  }

  /**
   * Tests data serializing a <code>String</code> array
   */
  @Test
  public void testStringArray() throws Exception {
    Random random = getRandom();

    String[] array = new String[] {String.valueOf(random.nextLong()),
        String.valueOf(random.nextLong()), String.valueOf(random.nextLong())};

    DataOutputStream out = getDataOutput();
    DataSerializer.writeStringArray(array, out);
    out.flush();

    DataInput in = getDataInput();
    String[] array2 = DataSerializer.readStringArray(in);

    assertEquals(array.length, array2.length);
    for (int i = 0; i < array.length; i++) {
      assertEquals(array[i], array2[i]);
    }
  }

  /**
   * Tests data serializing a <code>String</code> array that contains a <code>null</code>
   * <code>String</code>.
   */
  @Test
  public void testStringArrayWithNull() throws Exception {
    Random random = getRandom();

    String[] array =
        new String[] {String.valueOf(random.nextLong()), null, String.valueOf(random.nextLong())};

    DataOutputStream out = getDataOutput();
    DataSerializer.writeStringArray(array, out);
    out.flush();

    DataInput in = getDataInput();
    String[] array2 = DataSerializer.readStringArray(in);

    assertEquals(array.length, array2.length);
    for (int i = 0; i < array.length; i++) {
      assertEquals(array[i], array2[i]);
    }
  }

  /**
   * Tests data serializing a <code>String</code> array using {@link DataSerializer#writeObject}.
   */
  @Test
  public void testStringArrayObject() throws Exception {
    Random random = getRandom();

    String[] array = new String[] {String.valueOf(random.nextLong()),
        String.valueOf(random.nextLong()), String.valueOf(random.nextLong())};

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(array, out);
    out.flush();

    DataInput in = getDataInput();
    String[] array2 = DataSerializer.readObject(in);

    assertEquals(array.length, array2.length);
    for (int i = 0; i < array.length; i++) {
      assertEquals(array[i], array2[i]);
    }
  }

  /**
   * Tests data serializing a <code>int</code> array
   */
  @Test
  public void testIntArray() throws Exception {
    int[] array = new int[] {4, 5, 6};

    DataOutputStream out = getDataOutput();
    DataSerializer.writeIntArray(array, out);
    out.flush();

    DataInput in = getDataInput();
    int[] array2 = DataSerializer.readIntArray(in);

    assertEquals(array.length, array2.length);
    for (int i = 0; i < array.length; i++) {
      assertEquals(array[i], array2[i]);
    }
  }

  /**
   * Tests data serializing a <code>int</code> array using {@link DataSerializer#writeObject}.
   */
  @Test
  public void testIntArrayObject() throws Exception {
    int[] array = new int[] {4, 5, 6};

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(array, out);
    out.flush();

    DataInput in = getDataInput();
    int[] array2 = DataSerializer.readObject(in);

    assertEquals(array.length, array2.length);
    for (int i = 0; i < array.length; i++) {
      assertEquals(array[i], array2[i]);
    }
  }

  /**
   * Tests data serializing a <code>long</code> array
   */
  @Test
  public void testLongArray() throws Exception {
    long[] array = new long[] {4, 5, 6};

    DataOutputStream out = getDataOutput();
    DataSerializer.writeLongArray(array, out);
    out.flush();

    DataInput in = getDataInput();
    long[] array2 = DataSerializer.readLongArray(in);

    assertEquals(array.length, array2.length);
    for (int i = 0; i < array.length; i++) {
      assertEquals(array[i], array2[i]);
    }
  }

  /**
   * Tests data serializing a <code>long</code> array using {@link DataSerializer#writeObject}.
   */
  @Test
  public void testLongArrayObject() throws Exception {
    long[] array = new long[] {4, 5, 6};

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(array, out);
    out.flush();

    DataInput in = getDataInput();
    long[] array2 = DataSerializer.readObject(in);

    assertEquals(array.length, array2.length);
    for (int i = 0; i < array.length; i++) {
      assertEquals(array[i], array2[i]);
    }
  }

  /**
   * Tests data serializing a <code>float</code> array
   */
  @Test
  public void testFloatArray() throws Exception {
    float[] array = new float[] {(float) 4.0, (float) 5.0, (float) 6.0};

    DataOutputStream out = getDataOutput();
    DataSerializer.writeFloatArray(array, out);
    out.flush();

    DataInput in = getDataInput();
    float[] array2 = DataSerializer.readFloatArray(in);

    assertEquals(array.length, array2.length);
    for (int i = 0; i < array.length; i++) {
      assertEquals(array[i], array2[i], 0.0f);
    }
  }

  /**
   * Tests data serializing a <code>float</code> array using {@link DataSerializer#writeObject}.
   */
  @Test
  public void testFloatArrayObject() throws Exception {
    float[] array = new float[] {(float) 4.0, (float) 5.0, (float) 6.0};

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(array, out);
    out.flush();

    DataInput in = getDataInput();
    float[] array2 = DataSerializer.readObject(in);

    assertEquals(array.length, array2.length);
    for (int i = 0; i < array.length; i++) {
      assertEquals(array[i], array2[i], 0.0f);
    }
  }

  /**
   * Tests data serializing a <code>double</code> array
   */
  @Test
  public void testDoubleArray() throws Exception {
    double[] array = new double[] {4.0, 5.0, 6.0};

    DataOutputStream out = getDataOutput();
    DataSerializer.writeDoubleArray(array, out);
    out.flush();

    DataInput in = getDataInput();
    double[] array2 = DataSerializer.readDoubleArray(in);

    assertEquals(array.length, array2.length);
    for (int i = 0; i < array.length; i++) {
      assertEquals(array[i], array2[i], 0.0f);
    }
  }

  /**
   * Tests data serializing a <code>double</code> array using {@link DataSerializer#writeObject}.
   */
  @Test
  public void testDoubleArrayObject() throws Exception {
    double[] array = new double[] {4.0, 5.0, 6.0};

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(array, out);
    out.flush();

    DataInput in = getDataInput();
    double[] array2 = DataSerializer.readObject(in);

    assertEquals(array.length, array2.length);
    for (int i = 0; i < array.length; i++) {
      assertEquals(array[i], array2[i], 0.0f);
    }
  }

  /**
   * Tests data serializing a <code>Object</code> array
   */
  @Test
  public void testObjectArray() throws Exception {
    Random random = getRandom();
    SerializableImpl[] array = new SerializableImpl[] {new SerializableImpl(random),
        new SerializableImpl(random), new SerializableImpl(random)};

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObjectArray(array, out);
    out.flush();

    DataInput in = getDataInput();
    SerializableImpl[] array2 = (SerializableImpl[]) DataSerializer.readObjectArray(in);

    assertEquals(array.length, array2.length);
    for (int i = 0; i < array.length; i++) {
      assertEquals(array[i], array2[i]);
    }
  }

  /**
   * Tests data serializing a <code>Object</code> array using {@link DataSerializer#writeObject}.
   */
  @Test
  public void testObjectArrayObject() throws Exception {
    Random random = getRandom();
    SerializableImpl[] array = new SerializableImpl[] {new SerializableImpl(random),
        new SerializableImpl(random), new SerializableImpl(random)};

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(array, out);
    out.flush();

    DataInput in = getDataInput();
    SerializableImpl[] array2 = DataSerializer.readObject(in);

    assertEquals(array.length, array2.length);
    for (int i = 0; i < array.length; i++) {
      assertEquals(array[i], array2[i]);
    }
  }

  /**
   * Tests serializing an object that is {@link DataSerializableJUnitTest.SerializableImpl not
   * specially cased}.
   */
  @Test
  public void testUnspecialObject() throws Exception {
    Object o = new SerializableImpl(getRandom());

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(o, out);
    out.flush();

    DataInput in = getDataInput();
    Object o2 = DataSerializer.readObject(in);
    assertEquals(o, o2);
  }

  /**
   * Tests serializing an object that implements {@link DataSerializable}
   */
  @Test
  public void testDataSerializable() throws Exception {
    DataSerializable ds = new DataSerializableImpl(getRandom());

    DataOutputStream out = getDataOutput();
    ds.toData(out);
    out.flush();

    DataSerializable ds2 = new DataSerializableImpl();
    ds2.fromData(getDataInput());

    assertEquals(ds, ds2);
  }

  /**
   * Tests serializing an object that implements {@link DataSerializable}
   */
  @Test
  public void testVersionedDataSerializable() throws Exception {
    VersionedDataSerializableImpl ds = new VersionedDataSerializableImpl(getRandom());

    VersionedDataOutputStream v =
        new VersionedDataOutputStream(baos, KnownVersion.GEODE_1_11_0);
    DataSerializer.writeObject(ds, v);
    v.flush();

    ByteBuffer bb = ByteBuffer.wrap(baos.toByteArray());
    ByteBufferInputStream bbis = new ByteBufferInputStream(bb);
    VersionedDataInputStream vin = new VersionedDataInputStream(bbis, KnownVersion.GEODE_1_11_0);
    VersionedDataSerializableImpl ds2 =
        DataSerializer.readObject(vin);

    assertEquals(ds, ds2);
    assertTrue(ds.preMethodInvoked());
    assertTrue(ds2.preMethodInvoked());
  }

  /**
   * Tests writing a {@link org.apache.geode.DataSerializable.Replaceable} object
   */
  @Test
  public void testReplaceable() throws Exception {
    Object o = new ReplaceableImpl();

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(o, out);
    out.flush();

    Object o2 = DataSerializer.readObject(getDataInput());

    assertEquals(42, o2);
  }

  /**
   * Tests data serializing an {@link ArrayList}
   */
  @Test
  public void testArrayList() throws Exception {
    tryArrayList(-1);
    tryArrayList(50);
    tryArrayList(0x100);
    tryArrayList(0x10000);
  }

  private void tryArrayList(int size) throws IOException, ClassNotFoundException {
    setUp();
    final Random random = getRandom();
    final ArrayList<Long> list = size == -1 ? null : new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      list.add(random.nextLong());
    }

    DataOutputStream out = getDataOutput();
    DataSerializer.writeArrayList(list, out);
    out.flush();

    DataInput in = getDataInput();
    ArrayList<Long> list2 = DataSerializer.readArrayList(in);
    assertEquals(list, list2);
    tearDown();
  }

  /**
   * Tests data serializing an {@link ArrayList} using {@link DataSerializer#writeObject}.
   */
  @Test
  public void testArrayListObject() throws Exception {
    Random random = getRandom();
    ArrayList<Long> list = new ArrayList<>();
    int size = random.nextInt(50);
    for (int i = 0; i < size; i++) {
      list.add(random.nextLong());
    }

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(list, out);
    out.flush();

    DataInput in = getDataInput();
    ArrayList<Long> list2 = DataSerializer.readObject(in);
    assertEquals(list, list2);
  }

  /**
   * Tests data serializing an {@link HashSet}
   */
  @Test
  public void testHashSet() throws Exception {
    Random random = getRandom();
    HashSet<Long> set = new HashSet<>();
    int size = random.nextInt(50);
    for (int i = 0; i < size; i++) {
      set.add(random.nextLong());
    }

    DataOutputStream out = getDataOutput();
    DataSerializer.writeHashSet(set, out);
    out.flush();

    DataInput in = getDataInput();
    HashSet<Long> set2 = DataSerializer.readHashSet(in);
    assertEquals(set, set2);
  }

  /**
   * Tests data serializing an {@link HashSet} using {@link DataSerializer#writeObject}.
   */
  @Test
  public void testHashSetObject() throws Exception {
    Random random = getRandom();
    HashSet<Long> set = new HashSet<>();
    int size = random.nextInt(50);
    for (int i = 0; i < size; i++) {
      set.add(random.nextLong());
    }

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(set, out);
    out.flush();

    DataInput in = getDataInput();
    HashSet<Long> set2 = DataSerializer.readObject(in);
    assertEquals(set, set2);
  }

  /**
   * Tests data serializing an {@link TreeSet}
   */
  @Test
  public void testTreeSet() throws Exception {
    Random random = getRandom();
    TreeSet<Long> set = new TreeSet<>();
    int size = random.nextInt(50);
    for (int i = 0; i < size; i++) {
      set.add(random.nextLong());
    }

    DataOutputStream out = getDataOutput();
    DataSerializer.writeTreeSet(null, out);
    DataSerializer.writeTreeSet(new TreeSet<>(), out);
    DataSerializer.writeTreeSet(set, out);
    out.flush();

    DataInput in = getDataInput();
    assertNull(DataSerializer.readTreeSet(in));
    assertEquals(new TreeSet<>(), DataSerializer.readTreeSet(in));
    TreeSet<Long> set2 = DataSerializer.readTreeSet(in);
    assertEquals(set, set2);
  }

  /**
   * Tests data serializing an {@link TreeSet}
   */
  @Test
  public void testTreeSetWithComparator() throws Exception {
    Random random = getRandom();
    int size = random.nextInt(50);
    TreeSet<Long> set = new TreeSet<>(new MyComparator(size));
    for (int i = 0; i < size; i++) {
      set.add(random.nextLong());
    }

    DataOutputStream out = getDataOutput();
    DataSerializer.writeTreeSet(new TreeSet<>(new MyComparator(0)), out);
    DataSerializer.writeTreeSet(set, out);
    out.flush();

    DataInput in = getDataInput();
    TreeSet<Long> emptySet = DataSerializer.readTreeSet(in);
    assertEquals(new TreeSet<>(new MyComparator(0)), emptySet);
    assertEquals(new MyComparator(0), emptySet.comparator());
    TreeSet<Long> set2 = DataSerializer.readTreeSet(in);
    assertEquals(set, set2);
    assertEquals(set.comparator(), set2.comparator());
  }

  /**
   * Tests data serializing an {@link TreeSet} using {@link DataSerializer#writeObject}.
   */
  @Test
  public void testTreeSetObject() throws Exception {
    Random random = getRandom();
    TreeSet<Long> set = new TreeSet<>();
    int size = random.nextInt(50);
    for (int i = 0; i < size; i++) {
      set.add(random.nextLong());
    }

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(set, out);
    out.flush();

    DataInput in = getDataInput();
    TreeSet<Long> set2 = DataSerializer.readObject(in);
    assertEquals(set, set2);
  }

  /**
   * Tests data serializing an {@link HashMap}
   */
  @Test
  public void testHashMap() throws Exception {
    Random random = getRandom();
    HashMap<Long, String> map = new HashMap<>();
    int size = random.nextInt(50);
    for (int i = 0; i < size; i++) {
      Long key = random.nextLong();
      String value = String.valueOf(random.nextLong());
      map.put(key, value);
    }

    DataOutputStream out = getDataOutput();
    DataSerializer.writeHashMap(map, out);
    out.flush();

    DataInput in = getDataInput();
    HashMap<Long, String> map2 = DataSerializer.readHashMap(in);
    assertEquals(map, map2);
  }

  /**
   * Tests data serializing an {@link HashMap} using {@link DataSerializer#writeObject}.
   */
  @Test
  public void testHashMapObject() throws Exception {
    Random random = getRandom();
    HashMap<Long, String> map = new HashMap<>();
    int size = random.nextInt(50);
    for (int i = 0; i < size; i++) {
      Long key = random.nextLong();
      String value = String.valueOf(random.nextLong());
      map.put(key, value);
    }

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(map, out);
    out.flush();

    DataInput in = getDataInput();
    HashMap<Long, String> map2 = DataSerializer.readObject(in);
    assertEquals(map, map2);
  }

  /**
   * Tests data serializing an {@link TreeMap}
   */
  @Test
  public void testTreeMap() throws Exception {
    Random random = getRandom();
    TreeMap<Long, String> map = new TreeMap<>();
    int size = random.nextInt(50);
    for (int i = 0; i < size; i++) {
      Long key = random.nextLong();
      String value = String.valueOf(random.nextLong());
      map.put(key, value);
    }

    DataOutputStream out = getDataOutput();
    DataSerializer.writeTreeMap(null, out);
    DataSerializer.writeTreeMap(new TreeMap<Long, String>(), out);
    DataSerializer.writeTreeMap(map, out);
    out.flush();

    DataInput in = getDataInput();
    assertNull(DataSerializer.readTreeMap(in));
    assertEquals(new TreeMap<Long, String>(), DataSerializer.readTreeMap(in));
    TreeMap<Long, String> map2 = DataSerializer.readTreeMap(in);
    assertEquals(map, map2);
  }

  /**
   * Tests data serializing an {@link TreeMap}
   */
  @Test
  public void testTreeMapWithComparator() throws Exception {
    Random random = getRandom();
    int size = random.nextInt(50);
    TreeMap<Long, String> map = new TreeMap<>(new MyComparator(size));
    for (int i = 0; i < size; i++) {
      Long key = random.nextLong();
      String value = String.valueOf(random.nextLong());
      map.put(key, value);
    }

    DataOutputStream out = getDataOutput();
    DataSerializer.writeTreeMap(new TreeMap<Long, String>(new MyComparator(0)), out);
    DataSerializer.writeTreeMap(map, out);
    out.flush();

    DataInput in = getDataInput();
    TreeMap<Long, String> emptyMap = DataSerializer.readTreeMap(in);
    assertEquals(new TreeMap<Long, String>(new MyComparator(0)), emptyMap);
    assertEquals(new MyComparator(0), emptyMap.comparator());
    TreeMap<Long, String> map2 = DataSerializer.readTreeMap(in);
    assertEquals(map, map2);
    assertEquals(map.comparator(), map2.comparator());
  }

  private static class MyComparator implements Comparator<Long>, java.io.Serializable {
    private final int id;

    public MyComparator(int id) {
      this.id = id;
    }

    @Override
    public int compare(Long o1, Long o2) {
      return 0; // noop
    }

    public boolean equals(Object obj) {
      if (obj instanceof MyComparator) {
        MyComparator other = (MyComparator) obj;
        return id == other.id;
      }
      return false;
    }
  }

  /**
   * Tests data serializing an {@link TreeMap} using {@link DataSerializer#writeObject}.
   */
  @Test
  public void testTreeMapObject() throws Exception {
    Random random = getRandom();
    TreeMap<Long, String> map = new TreeMap<>();
    int size = random.nextInt(50);
    for (int i = 0; i < size; i++) {
      Long key = random.nextLong();
      String value = String.valueOf(random.nextLong());
      map.put(key, value);
    }

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(map, out);
    out.flush();

    DataInput in = getDataInput();
    TreeMap<Long, String> map2 = DataSerializer.readObject(in);
    assertEquals(map, map2);
  }

  /**
   * Tests data serializing an {@link LinkedHashSet}
   */
  @Test
  public void testLinkedHashSet() throws Exception {
    Random random = getRandom();
    LinkedHashSet<Long> set = new LinkedHashSet<>();
    int size = random.nextInt(50);
    for (int i = 0; i < size; i++) {
      set.add(random.nextLong());
    }

    DataOutputStream out = getDataOutput();
    DataSerializer.writeLinkedHashSet(set, out);
    out.flush();

    DataInput in = getDataInput();
    LinkedHashSet<Long> set2 = DataSerializer.readLinkedHashSet(in);
    assertEquals(set, set2);
  }

  /**
   * Tests data serializing an {@link LinkedHashSet} using {@link DataSerializer#writeObject}.
   */
  @Test
  public void testLinkedHashSetObject() throws Exception {
    Random random = getRandom();
    LinkedHashSet<Long> set = new LinkedHashSet<>();
    int size = random.nextInt(50);
    for (int i = 0; i < size; i++) {
      set.add(random.nextLong());
    }

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(set, out);
    out.flush();

    DataInput in = getDataInput();
    LinkedHashSet<Long> set2 = DataSerializer.readObject(in);
    assertEquals(set, set2);
  }

  /**
   * Tests data serializing an {@link Hashtable}
   */
  @Test
  public void testHashtable() throws Exception {
    Random random = getRandom();
    Hashtable<Long, String> map = new Hashtable<>();
    int size = random.nextInt(50);
    for (int i = 0; i < size; i++) {
      Long key = random.nextLong();
      String value = String.valueOf(random.nextLong());
      map.put(key, value);
    }

    DataOutputStream out = getDataOutput();
    DataSerializer.writeHashtable(map, out);
    out.flush();

    DataInput in = getDataInput();
    Hashtable<Long, String> map2 = DataSerializer.readHashtable(in);
    assertEquals(map, map2);
  }

  /**
   * Tests data serializing an {@link Hashtable} using {@link DataSerializer#writeObject}.
   */
  @Test
  public void testHashtableObject() throws Exception {
    Random random = getRandom();
    Hashtable<Long, String> map = new Hashtable<>();
    int size = random.nextInt(50);
    for (int i = 0; i < size; i++) {
      Long key = random.nextLong();
      String value = String.valueOf(random.nextLong());
      map.put(key, value);
    }

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(map, out);
    out.flush();

    DataInput in = getDataInput();
    Hashtable<Long, String> map2 = DataSerializer.readObject(in);
    assertEquals(map, map2);
  }

  /**
   * Tests data serializing an {@link java.util.LinkedHashMap}
   */
  @Test
  public void testLinkedHashMap() throws Exception {
    Random random = getRandom();
    LinkedHashMap<Long, String> map = new LinkedHashMap<>();
    int size = random.nextInt(50);
    for (int i = 0; i < size; i++) {
      Long key = random.nextLong();
      String value = String.valueOf(random.nextLong());
      map.put(key, value);
    }

    DataOutputStream out = getDataOutput();
    DataSerializer.writeLinkedHashMap(map, out);
    out.flush();

    DataInput in = getDataInput();
    LinkedHashMap<Long, String> map2 = DataSerializer.readLinkedHashMap(in);
    assertEquals(map, map2);
  }

  /**
   * Tests data serializing an {@link LinkedHashMap} using {@link DataSerializer#writeObject}.
   */
  @Test
  public void testLinkedHashMapObject() throws Exception {
    Random random = getRandom();
    LinkedHashMap<Long, String> map = new LinkedHashMap<>();
    int size = random.nextInt(50);
    for (int i = 0; i < size; i++) {
      Long key = random.nextLong();
      String value = String.valueOf(random.nextLong());
      map.put(key, value);
    }

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(map, out);
    out.flush();

    DataInput in = getDataInput();
    LinkedHashMap<Long, String> map2 = DataSerializer.readObject(in);
    assertEquals(map, map2);
  }


  /**
   * Tests data serializing an {@link IdentityHashMap}
   */
  @Test
  public void testIdentityHashMap() throws Exception {
    Random random = getRandom();
    IdentityHashMap<Long, String> map = new IdentityHashMap<>();
    int size = random.nextInt(50);
    for (int i = 0; i < size; i++) {
      Long key = random.nextLong();
      String value = String.valueOf(random.nextLong());
      map.put(key, value);
    }

    DataOutputStream out = getDataOutput();
    DataSerializer.writeIdentityHashMap(map, out);
    out.flush();

    DataInput in = getDataInput();
    IdentityHashMap<Long, String> map2 = DataSerializer.readIdentityHashMap(in);
    assertEquals(new HashMap<>(map), new HashMap<>(map2));
  }

  /**
   * Tests data serializing an {@link IdentityHashMap} using {@link DataSerializer#writeObject}.
   */
  @Test
  public void testIdentityHashMapObject() throws Exception {
    Random random = getRandom();
    IdentityHashMap<Long, String> map = new IdentityHashMap<>();
    int size = random.nextInt(50);
    for (int i = 0; i < size; i++) {
      Long key = random.nextLong();
      String value = String.valueOf(random.nextLong());
      map.put(key, value);
    }

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(map, out);
    out.flush();

    DataInput in = getDataInput();
    IdentityHashMap<Long, String> map2 = DataSerializer.readObject(in);
    assertEquals(new HashMap<>(map), new HashMap<>(map2));
  }

  /**
   * Tests data serializing an {@link Vector}
   */
  @Test
  public void testVector() throws Exception {
    Random random = getRandom();
    Vector<Long> list = new Vector<>();
    int size = random.nextInt(50);
    for (int i = 0; i < size; i++) {
      list.add(random.nextLong());
    }

    DataOutputStream out = getDataOutput();
    DataSerializer.writeVector(list, out);
    out.flush();

    DataInput in = getDataInput();
    Vector<Long> list2 = DataSerializer.readVector(in);
    assertEquals(list, list2);
  }

  /**
   * Tests data serializing an {@link Vector} using {@link DataSerializer#writeObject}.
   */
  @Test
  public void testVectorObject() throws Exception {
    Random random = getRandom();
    Vector<Long> list = new Vector<>();
    int size = random.nextInt(50);
    for (int i = 0; i < size; i++) {
      list.add(random.nextLong());
    }

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(list, out);
    out.flush();

    DataInput in = getDataInput();
    Vector<Long> list2 = DataSerializer.readObject(in);
    assertEquals(list, list2);
  }

  /**
   * Tests data serializing an {@link Stack}
   */
  @Test
  public void testStack() throws Exception {
    Random random = getRandom();
    Stack<Long> list = new Stack<>();
    int size = random.nextInt(50);
    for (int i = 0; i < size; i++) {
      list.add(random.nextLong());
    }

    DataOutputStream out = getDataOutput();
    DataSerializer.writeStack(list, out);
    out.flush();

    DataInput in = getDataInput();
    Stack<Long> list2 = DataSerializer.readStack(in);
    assertEquals(list, list2);
  }

  /**
   * Tests data serializing an {@link Stack} using {@link DataSerializer#writeObject}.
   */
  @Test
  public void testStackObject() throws Exception {
    Random random = getRandom();
    Stack<Long> list = new Stack<>();
    int size = random.nextInt(50);
    for (int i = 0; i < size; i++) {
      list.add(random.nextLong());
    }

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(list, out);
    out.flush();

    DataInput in = getDataInput();
    Stack<Long> list2 = DataSerializer.readObject(in);
    assertEquals(list, list2);
  }

  /**
   * Tests data serializing {@link TimeUnit}s using {@link DataSerializer#writeObject}.
   */
  @Test
  public void testTimeUnitObject() throws Exception {
    DataOutputStream out = getDataOutput();
    for (TimeUnit v : TimeUnit.values()) {
      DataSerializer.writeObject(v, out, false /* no java serialization allowed */);
    }
    out.flush();

    DataInput in = getDataInput();
    for (TimeUnit v : TimeUnit.values()) {
      assertEquals(v, DataSerializer.readObject(in));
    }
  }

  @Test
  public void testProperties() throws Exception {
    DataOutputStream out = getDataOutput();
    DataSerializer.writeProperties(new Properties(), out);
    DataSerializer.writeProperties(null, out);
    Properties p1 = new Properties();
    p1.setProperty("aKey1", "aValue1");
    p1.setProperty("aKey2", "aValue2");
    DataSerializer.writeProperties(p1, out);
    Properties p2 = new Properties();
    p2.put("aKey1", 1);
    p2.put("aKey2", 2);
    DataSerializer.writeProperties(p2, out);
    out.flush();

    DataInput in = getDataInput();
    assertEquals(new Properties(), DataSerializer.readProperties(in));
    assertNull(DataSerializer.readProperties(in));
    assertEquals(p1, DataSerializer.readProperties(in));
    assertEquals(p2, DataSerializer.readProperties(in));
  }

  /**
   * Tests that registering a <code>Serializer</code> with id 0 throws an exception.
   */
  @Test
  public void testSerializerZero() {
    try {
      DataSerializer.register(DS0.class);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }
  }

  private static class DS0 extends DataSerializerImpl {
    @Override
    public int getId() {
      return 0;
    }

    @Override
    public Class<?>[] getSupportedClasses() {
      return new Class[] {getClass()};
    }
  }

  /**
   * Tests that registering two <code>Serializer</code>s with the same id throws an exception.
   */
  @Test
  public void testRegisterTwoSerializers() {
    byte id = (byte) 42;
    DataSerializer.register(DS42.class);

    DataSerializer serializer2 = new DS42() {};
    try {
      DataSerializer.register(serializer2.getClass());
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...

    } finally {
      InternalDataSerializer.unregister(id);
    }
  }

  // I removed this test since it tested a feature that was
  // removed for performance reasons.

  // I removed this test since it tested a feature that was
  // removed for performance reasons.

  /**
   * Tests that an <code>IOException</code> is thrown when the serializer for an object cannot be
   * found.
   */
  @Test
  public void testNoDeSerializer() throws Exception {
    Random random = new Random();

    byte id = (byte) 100;
    Class<?> c = NonDataSerializable.NonDSSerializer.class;
    DataSerializer.register(c);

    Object o = new NonDataSerializable(random);
    DataSerializer.writeObject(o, getDataOutput());
    InternalDataSerializer.unregister(id);

    int savVal = InternalDataSerializer.GetMarker.WAIT_MS;
    InternalDataSerializer.GetMarker.WAIT_MS = 10;
    try {
      DataSerializer.readObject(getDataInput());
      fail("Should have thrown an IOException");

    } catch (IOException ex) {
      // pass...
    } finally {
      InternalDataSerializer.GetMarker.WAIT_MS = savVal;
    }

  }

  /**
   * Tests that a late-registering <code>DataSerializable</code> indeed causes a waiting
   * readObject() method to be notified.
   */
  @Test
  public void testLateDeSerializer() throws Exception {
    Random random = new Random();

    final byte id = (byte) 100;
    final Class<?> c = NonDataSerializable.NonDSSerializer.class;
    DataSerializer.register(c);

    Object o = new NonDataSerializable(random);
    DataSerializer.writeObject(o, getDataOutput());
    InternalDataSerializer.unregister(id);

    ThreadGroup group = new ThreadGroup("Group") {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        if (e instanceof VirtualMachineError) {
          SystemFailure.setFailure((VirtualMachineError) e); // don't throw
        }
        fail("Uncaught exception in thread " + t + e);
      }
    };
    Thread thread = new Thread(group, "Registrar") {
      @Override
      public void run() {
        try {
          Thread.sleep(300);
          DataSerializer.register(c);

        } catch (Exception ex) {
          fail("Interrupted while registering" + ex);
        }
      }
    };
    thread.start();

    try {
      DataSerializer.readObject(getDataInput());
    } finally {
      try {
        long ms = 30 * 1000;
        thread.join(ms);
        if (thread.isAlive()) {
          thread.interrupt();
          fail("Thread did not terminate after " + ms + " ms: " + thread);
        }
      } finally {
        InternalDataSerializer.unregister(id);
      }
    }
  }

  /**
   * Tests that a late-registering <code>Instantiator</code> indeed causes a waiting readObject()
   * method to be notified.
   */
  @Test
  public void testLateInstantiator() throws Exception {
    Random random = new Random();

    final byte id = (byte) 100;
    final Class<DataSerializableImpl> c = DataSerializableImpl.class;
    final Instantiator inst = new Instantiator(c, id) {
      @Override
      public DataSerializable newInstance() {
        return new DataSerializableImpl();
      }
    };
    Instantiator.register(inst);

    Object o = new DataSerializableImpl(random);
    DataSerializer.writeObject(o, getDataOutput());
    InternalInstantiator.unregister(c, id);

    ThreadGroup group = new ThreadGroup("Group") {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        if (e instanceof VirtualMachineError) {
          SystemFailure.setFailure((VirtualMachineError) e); // don't throw
        }
        fail("Uncaught exception in thread " + t + e);
      }
    };
    Thread thread = new Thread(group, "Registrar") {
      @Override
      public void run() {
        try {
          Thread.sleep(300);
          Instantiator.register(inst);

        } catch (Exception ex) {
          fail("Interrupted while registering" + ex);
        }
      }
    };
    thread.start();

    try {
      DataSerializer.readObject(getDataInput());
    } finally {
      try {
        long ms = 30 * 1000;
        thread.join(ms);
        if (thread.isAlive()) {
          thread.interrupt();
          fail("Thread did not terminate after " + ms + " ms: " + thread);
        }
      } finally {
        InternalInstantiator.unregister(c, id);
      }
    }
  }

  /**
   * Tests that a custom serializer is consulted
   */
  @Test
  public void testCustomSerializer() throws Exception {
    Random random = new Random();

    Class<?> c = NonDataSerializable.NonDSSerializer.class;
    byte id = (byte) 100;
    DataSerializer.register(c);

    Object o = new NonDataSerializable(random);
    try {
      DataSerializer.writeObject(o, getDataOutput());
      Object o2 = DataSerializer.readObject(getDataInput());
      assertEquals(o, o2);

    } finally {
      InternalDataSerializer.unregister(id);
    }
  }

  /**
   * Tests that the appropriate exceptions are thrown by {@link Instantiator#register} when given
   * bad input.
   */
  @Test
  public void testInstantiatorExceptions() {

    try {
      new Instantiator(null, (byte) 42) {
        @Override
        public DataSerializable newInstance() {
          return null;
        }
      };
      fail("Should have thrown a NullPointerException");

    } catch (NullPointerException ex) {
      // pass...
    }

    try {
      Instantiator.register(null);
      fail("Should have thrown a NullPointerException");

    } catch (NullPointerException ex) {
      // pass...
    }

    Instantiator.register(new Instantiator(DataSerializableImpl.class, (byte) 42) {
      @Override
      public DataSerializable newInstance() {
        return null;
      }
    });
    try {

      try {
        Instantiator.register(new Instantiator(DataSerializableImpl.class, (byte) 41) {
          @Override
          public DataSerializable newInstance() {
            return null;
          }
        });
        fail("Should have thrown an IllegalStateException");

      } catch (IllegalStateException ex) {
        // pass...
      }

      try {
        Instantiator.register(new Instantiator(DSIntWrapper.class, (byte) 42) {
          @Override
          public DataSerializable newInstance() {
            return null;
          }
        });
        fail("Should have thrown an IllegalStateException");

      } catch (IllegalStateException ex) {
        // pass...
      }

    } finally {
      InternalInstantiator.unregister(DataSerializableImpl.class, (byte) 42);
    }
  }

  /**
   * Tests that an <code>Instantiator</code> is invoked at the appropriate times.
   */
  @Test
  public void testInstantiator() throws Exception {
    final boolean[] wasInvoked = new boolean[] {false};
    Instantiator.register(new Instantiator(DataSerializableImpl.class, (byte) 45) {
      @Override
      public DataSerializable newInstance() {
        wasInvoked[0] = true;
        return new DataSerializableImpl();
      }
    });
    try {
      byte id = (byte) 57;
      Class_testInstantiator.supClass = DataSerializableImpl.class;
      DataSerializer.register(Class_testInstantiator.class);
      try {
        Object o = new DataSerializableImpl(new Random());
        DataSerializer.writeObject(o, getDataOutput());
        Object o2 = DataSerializer.readObject(getDataInput());
        assertTrue(wasInvoked[0]);
        assertEquals(o, o2);
      } finally {
        InternalDataSerializer.unregister(id);
      }
    } finally {
      InternalInstantiator.unregister(DataSerializableImpl.class, (byte) 45);
    }
  }

  @Test
  public void testInstantiator2() throws Exception {
    final boolean[] wasInvoked = new boolean[] {false};
    Instantiator.register(new Instantiator(DataSerializableImpl.class, 20000) {
      @Override
      public DataSerializable newInstance() {
        wasInvoked[0] = true;
        return new DataSerializableImpl();
      }
    });
    try {
      byte id = (byte) 57;
      Class_testInstantiator.supClass = DataSerializableImpl.class;
      DataSerializer.register(Class_testInstantiator.class);
      try {
        Object o = new DataSerializableImpl(new Random());
        DataSerializer.writeObject(o, getDataOutput());
        Object o2 = DataSerializer.readObject(getDataInput());
        assertTrue(wasInvoked[0]);
        assertEquals(o, o2);
      } finally {
        InternalDataSerializer.unregister(id);
      }
    } finally {
      InternalInstantiator.unregister(DataSerializableImpl.class, 20000);
    }
  }

  @Test
  public void testInstantiator4() throws Exception {
    final boolean[] wasInvoked = new boolean[] {false};
    Instantiator.register(new Instantiator(DataSerializableImpl.class, 123456789) {
      @Override
      public DataSerializable newInstance() {
        wasInvoked[0] = true;
        return new DataSerializableImpl();
      }
    });
    try {
      byte id = (byte) 57;
      Class_testInstantiator.supClass = DataSerializableImpl.class;
      DataSerializer.register(Class_testInstantiator.class);
      try {
        Object o = new DataSerializableImpl(new Random());
        DataSerializer.writeObject(o, getDataOutput());
        Object o2 = DataSerializer.readObject(getDataInput());
        assertTrue(wasInvoked[0]);
        assertEquals(o, o2);
      } finally {
        InternalDataSerializer.unregister(id);
      }
    } finally {
      InternalInstantiator.unregister(DataSerializableImpl.class, 123456789);
    }
  }

  private static class Class_testInstantiator extends DataSerializerImpl {
    public static Class<?> supClass;

    @Override
    public int getId() {
      return 57;
    }

    @Override
    public Class<?>[] getSupportedClasses() {
      return new Class[] {supClass};
    }

    @Override
    public boolean toData(Object o, DataOutput out) throws IOException {
      if (o instanceof DataSerializableImpl) {
        fail("toData() should not be invoked with a " + o.getClass().getName());
      }
      return false;
    }
  }

  /**
   * Tests that an <code>CanonicalInstantiator</code> is invoked at the appropriate times.
   */
  @Test
  public void testCanonicalInstantiator() throws Exception {
    final boolean[] wasInvoked = new boolean[] {false};
    Instantiator
        .register(new CanonicalInstantiator(CanonicalDataSerializableImpl.class, (byte) 45) {
          @Override
          public DataSerializable newInstance(DataInput di) throws IOException {
            wasInvoked[0] = true;
            return CanonicalDataSerializableImpl.create(di.readByte());
          }
        });
    try {
      byte id = (byte) 57;

      Class_testInstantiator.supClass = CanonicalDataSerializableImpl.class;
      DataSerializer.register(Class_testInstantiator.class);
      try {
        Object o = CanonicalDataSerializableImpl.create();
        DataSerializer.writeObject(o, getDataOutput());
        Object o2 = DataSerializer.readObject(getDataInput());
        assertTrue(wasInvoked[0]);
        assertSame(o, o2);
      } finally {
        InternalDataSerializer.unregister(id);
      }
    } finally {
      InternalInstantiator.unregister(CanonicalDataSerializableImpl.class, (byte) 45);
    }
  }

  /**
   * Tests that only one serializer is invoked when a serializer specifies its supported classes.
   * Alos tests UDDS1.
   */
  @Test
  public void testSupportedClasses() throws Exception {
    assertThat(Class_testSupportedClasses2.wasInvoked).isFalse();
    assertThat(Class_testSupportedClasses2.toDataInvoked).isFalse();
    assertThat(Class_testSupportedClasses2.fromDataInvoked).isFalse();

    DataSerializer ds1 = DataSerializer.register(Class_testSupportedClasses1.class);
    int id = ds1.getId();

    DataSerializer ds2 = DataSerializer.register(Class_testSupportedClasses2.class);
    int id2 = ds2.getId();

    try {
      Object o = new NonDataSerializable(new Random());
      DataSerializer.writeObject(o, getDataOutput());
      assertThat(Class_testSupportedClasses2.wasInvoked).isTrue();
      assertThat(Class_testSupportedClasses2.toDataInvoked).isTrue();
      assertThat(Class_testSupportedClasses2.fromDataInvoked).isFalse();

      Object o2 = DataSerializer.readObject(getDataInput());
      assertThat(Class_testSupportedClasses2.fromDataInvoked).isTrue();
      assertThat(o).isEqualTo(o2);
    } finally {
      InternalDataSerializer.unregister(id);
      InternalDataSerializer.unregister(id2);
    }

    Class_testSupportedClasses2.wasInvoked = false;
    Class_testSupportedClasses2.toDataInvoked = false;
    Class_testSupportedClasses2.fromDataInvoked = false;
  }

  /**
   * Make sure a user defined ds with an id of 2 bytes works.
   */
  @Test
  public void testUDDS2() throws Exception {
    assertThat(Class_testSupportedClasses3.wasInvoked).isFalse();
    assertThat(Class_testSupportedClasses3.toDataInvoked).isFalse();
    assertThat(Class_testSupportedClasses3.fromDataInvoked).isFalse();

    DataSerializer ds2 = DataSerializer.register(Class_testSupportedClasses3.class);
    int id2 = ds2.getId();

    try {
      Object o = new NonDataSerializable(new Random());
      DataSerializer.writeObject(o, getDataOutput());
      assertThat(Class_testSupportedClasses3.wasInvoked).isTrue();
      assertThat(Class_testSupportedClasses3.toDataInvoked).isTrue();
      assertThat(Class_testSupportedClasses3.fromDataInvoked).isFalse();

      Object o2 = DataSerializer.readObject(getDataInput());
      assertThat(Class_testSupportedClasses3.fromDataInvoked).isTrue();
      assertThat(o).isEqualTo(o2);
    } finally {
      InternalDataSerializer.unregister(id2);
    }

    Class_testSupportedClasses3.wasInvoked = false;
    Class_testSupportedClasses3.toDataInvoked = false;
    Class_testSupportedClasses3.fromDataInvoked = false;
  }

  /**
   * Make sure a user defined ds with an id of42 bytes works.
   */
  @Test
  public void testUDDS4() throws Exception {
    assertThat(Class_testSupportedClasses4.wasInvoked).isFalse();
    assertThat(Class_testSupportedClasses4.toDataInvoked).isFalse();
    assertThat(Class_testSupportedClasses4.fromDataInvoked).isFalse();

    DataSerializer ds2 = DataSerializer.register(Class_testSupportedClasses4.class);
    int id2 = ds2.getId();

    try {
      Object o = new NonDataSerializable(new Random());
      DataSerializer.writeObject(o, getDataOutput());
      assertThat(Class_testSupportedClasses4.wasInvoked).isTrue();
      assertThat(Class_testSupportedClasses4.toDataInvoked).isTrue();
      assertThat(Class_testSupportedClasses4.fromDataInvoked).isFalse();

      Object o2 = DataSerializer.readObject(getDataInput());
      assertTrue(Class_testSupportedClasses4.fromDataInvoked);
      assertThat(o).isEqualTo(o2);
    } finally {
      InternalDataSerializer.unregister(id2);
    }

    Class_testSupportedClasses4.wasInvoked = false;
    Class_testSupportedClasses4.toDataInvoked = false;
    Class_testSupportedClasses4.fromDataInvoked = false;
  }

  private static class Class_testSupportedClasses1 extends DataSerializerImpl {
    @Override
    public int getId() {
      return 29;
    }

    @Override
    public Class<?>[] getSupportedClasses() {
      return new Class[] {getClass()};
    }

    @Override
    public boolean toData(Object o, DataOutput out) throws IOException {

      if (o instanceof NonDataSerializable) {
        fail("toData() should not be invoked with a " + "NonDataSerializable");
      }
      return false;
    }
  }

  private static class Class_testSupportedClasses2 extends NonDataSerializable.NonDSSerializer {
    public static boolean wasInvoked = false;
    public static boolean toDataInvoked = false;
    public static boolean fromDataInvoked = false;

    @Override
    public int getId() {
      return 30;
    }

    @Override
    public Class<?>[] getSupportedClasses() {
      wasInvoked = true;
      return super.getSupportedClasses();
    }

    @Override
    public boolean toData(Object o, DataOutput out) throws IOException {
      toDataInvoked = true;
      return super.toData(o, out);
    }

    @Override
    public Object fromData(DataInput in) throws IOException, ClassNotFoundException {
      fromDataInvoked = true;
      return super.fromData(in);
    }
  }

  private static class Class_testSupportedClasses3 extends NonDataSerializable.NonDSSerializer {
    public static boolean wasInvoked = false;
    public static boolean toDataInvoked = false;
    public static boolean fromDataInvoked = false;

    @Override
    public int getId() {
      return 32767;
    }

    @Override
    public Class<?>[] getSupportedClasses() {
      wasInvoked = true;
      return super.getSupportedClasses();
    }

    @Override
    public boolean toData(Object o, DataOutput out) throws IOException {
      toDataInvoked = true;
      return super.toData(o, out);
    }

    @Override
    public Object fromData(DataInput in) throws IOException, ClassNotFoundException {
      fromDataInvoked = true;
      return super.fromData(in);
    }
  }

  private static class Class_testSupportedClasses4 extends NonDataSerializable.NonDSSerializer {

    public static boolean wasInvoked = false;
    public static boolean toDataInvoked = false;
    public static boolean fromDataInvoked = false;

    @Override
    public int getId() {
      return 1000000;
    }

    @Override
    public Class<?>[] getSupportedClasses() {
      wasInvoked = true;
      return super.getSupportedClasses();
    }

    @Override
    public boolean toData(Object o, DataOutput out) throws IOException {
      toDataInvoked = true;
      return super.toData(o, out);
    }

    @Override
    public Object fromData(DataInput in) throws IOException, ClassNotFoundException {
      fromDataInvoked = true;
      return super.fromData(in);
    }
  }

  /**
   * Tests that data serializing a cyclical object graph results in an infinite loop
   * ({@link StackOverflowError}).
   *
   * This test is disabled due a bug in JaCoCo 0.6.2 agent while handling stackoverflow exceptions
   * during tests.
   */
  @Ignore("disabled due a bug in JaCoCo 0.6.2 agent while handling stackoverflow exceptions")
  @Test
  public void testCyclicalObjectGraph() throws Exception {
    Link link1 = new Link(1);
    Link link2 = new Link(2);
    link1.next = link2;
    Link link3 = new Link(3);
    link2.next = link3;
    Link link4 = new Link(4);
    link3.next = link4;
    Link link5 = new Link(5);
    link4.next = link5;
    link5.next = link1;

    ObjectOutputStream oos = new ObjectOutputStream(getDataOutput());
    oos.writeObject(link1);
    oos.flush();
    oos.close();

    SystemFailureTestHook.setExpectedFailureClass(StackOverflowError.class);
    try {
      DataSerializer.writeObject(link1, getDataOutput());
      fail("Should have thrown a StackOverflowError");

    } catch (java.lang.StackOverflowError ex) {
      // pass...
    } finally {
      SystemFailureTestHook.setExpectedFailureClass(null);
    }

  }

  /**
   * Tests that data serializing the same object through two different reference paths does not
   * preserve referential integrity.
   */
  @Test
  public void testReferentialIntegrity() throws Exception {
    Link top = new Link(1);
    Link left = new Link(2);
    Link right = new Link(3);
    Link bottom = new Link(4);

    top.next = left;
    top.next2 = right;
    top.next.next = bottom;
    top.next2.next = bottom;

    ObjectOutputStream oos = new ObjectOutputStream(getDataOutput());
    oos.writeObject(top);
    oos.flush();
    oos.close();

    ObjectInputStream ois = new ObjectInputStream(getDataInputStream());
    Link top2 = (Link) ois.readObject();
    ois.close();

    assertSame(top2.next.next, top2.next2.next);

    setUp();

    DataSerializer.writeObject(top, getDataOutput());
    top2 = DataSerializer.readObject(getDataInput());

    assertNotSame(top2.next.next, top2.next2.next);
  }

  /**
   * Tests that <code>RegistrationListener</code>s are invoked at the proper times.
   */
  @Test
  public void testRegistrationListeners() {
    final DataSerializer[] array = new DataSerializer[2];

    TestRegistrationListener l1 = new TestRegistrationListener() {
      @Override
      public void newDataSerializer2(DataSerializer ds) {
        array[0] = ds;
      }
    };
    TestRegistrationListener l2 = new TestRegistrationListener() {
      @Override
      public void newDataSerializer2(DataSerializer ds) {
        array[1] = ds;
      }
    };

    InternalDataSerializer.addRegistrationListener(l1);
    InternalDataSerializer.addRegistrationListener(l2);

    byte id = (byte) 42;
    try {
      DataSerializer ds = DataSerializer.register(DS42.class);
      assertTrue(l1.wasInvoked());
      assertSame(ds, array[0]);
      assertTrue(l2.wasInvoked());
      assertSame(ds, array[1]);

    } finally {
      InternalDataSerializer.unregister(id);
      InternalDataSerializer.removeRegistrationListener(l1);
      InternalDataSerializer.removeRegistrationListener(l2);
    }


    Class<DataSerializableImpl> c = DataSerializableImpl.class;
    id = (byte) 100;
    final Instantiator inst0 = new Instantiator(c, id) {
      @Override
      public DataSerializable newInstance() {
        return new DataSerializableImpl();
      }
    };

    TestRegistrationListener l3 = new TestRegistrationListener() {
      @Override
      public void newInstantiator2(Instantiator inst) {
        assertEquals(inst0, inst);
      }
    };
    TestRegistrationListener l4 = new TestRegistrationListener() {
      @Override
      public void newInstantiator2(Instantiator inst) {
        assertEquals(inst0, inst);
      }
    };

    InternalDataSerializer.addRegistrationListener(l3);
    InternalDataSerializer.addRegistrationListener(l4);

    try {
      Instantiator.register(inst0);
      assertTrue(l3.wasInvoked());
      assertTrue(l4.wasInvoked());

    } finally {
      InternalInstantiator.unregister(c, id);
      InternalDataSerializer.removeRegistrationListener(l3);
      InternalDataSerializer.removeRegistrationListener(l4);
    }
  }

  private static class DS42 extends DataSerializerImpl {

    @Override
    public int getId() {
      return 42;
    }

    @Override
    public Class<?>[] getSupportedClasses() {
      return new Class[] {DS42.class};
    }
  }

  @Test
  public void testIllegalSupportedClasses() {
    tryToSupport(String.class);
    tryToSupport(java.net.InetAddress.class);
    tryToSupport(java.net.Inet4Address.class);
    tryToSupport(java.net.Inet6Address.class);
    tryToSupport(Class.class);
    tryToSupport(Boolean.class);
    tryToSupport(Character.class);
    tryToSupport(Byte.class);
    tryToSupport(Short.class);
    tryToSupport(Integer.class);
    tryToSupport(Long.class);
    tryToSupport(Float.class);
    tryToSupport(Double.class);
    tryToSupport(Date.class);
    tryToSupport(File.class);
    tryToSupport(ArrayList.class);
    tryToSupport(LinkedList.class);
    tryToSupport(HashSet.class);
    tryToSupport(HashMap.class);
    tryToSupport(Properties.class);
    tryToSupport(Hashtable.class);
    tryToSupport(Vector.class);
    tryToSupport(IdentityHashMap.class);
    tryToSupport(LinkedHashSet.class);
    tryToSupport(Stack.class);
    tryToSupport(TreeMap.class);
    tryToSupport(TreeSet.class);
    // arrays
    tryToSupport(boolean[].class);
    tryToSupport(byte[].class);
    tryToSupport(char[].class);
    tryToSupport(double[].class);
    tryToSupport(float[].class);
    tryToSupport(int[].class);
    tryToSupport(long[].class);
    tryToSupport(short[].class);
    tryToSupport(String[].class);
    tryToSupport(Object[].class);
  }

  private void tryToSupport(final Class<?> c) {
    illegalClass = c;
    try {
      DataSerializer.register(IllegalDS.class);
      fail("expected IllegalStateException");
    } catch (IllegalArgumentException expected) {
      if (c.isArray()) {
        assertTrue(expected.getMessage(),
            expected.getMessage().contains("an array class which is not allowed"));
      } else {
        assertTrue(expected.getMessage(),
            expected.getMessage().contains("has built-in support for class " + c.getName()));
      }
    }
  }

  protected static Class<?> illegalClass = null;

  private static class IllegalDS extends DataSerializerImpl {
    public IllegalDS() {}

    @Override
    public int getId() {
      return 337788;
    }

    @Override
    public Class<?>[] getSupportedClasses() {
      return new Class[] {illegalClass};
    }
  }

  /**
   * Data serializes and then data de-serializes the given object and asserts that the class of
   * object the pre- and post- data serialized objects is the same.
   */
  private void checkClass(Object o) throws IOException, ClassNotFoundException {

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(o, out);
    out.flush();
    DataInput in = getDataInput();
    assertSame(o.getClass(), DataSerializer.readObject(in).getClass());
    baos = new ByteArrayOutputStream();
  }

  /**
   * Tests that subclasses of well-known data serializer classes are not read back as instances of
   * their superclass. See bug 32391.
   *
   * @since GemFire 4.0
   */
  @Test
  public void testSubclasses() throws Exception {
    checkClass(new Date());
    checkClass(new Date() {});

    checkClass(new File(""));
    checkClass(new File("") {});

    checkClass(new ArrayList<>());
    checkClass(new ArrayList<Object>() {});

    checkClass(new LinkedList<>());
    checkClass(new LinkedList<Object>() {});

    checkClass(new HashSet<>());
    checkClass(new HashSet<Object>() {});

    checkClass(new HashMap<>());
    checkClass(new HashMap<Object, Object>() {});

    checkClass(new Properties());
    checkClass(new Properties() {});

    checkClass(new Hashtable<>());
    checkClass(new Hashtable<Object, Object>() {});

    checkClass(new Vector<>());
    checkClass(new Vector<Object>() {});

    checkClass(new IdentityHashMap<>());
    checkClass(new IdentityHashMap<Object, Object>() {});

    checkClass(new LinkedHashSet<>());
    checkClass(new LinkedHashSet<Object>() {});

    checkClass(new Stack<>());
    checkClass(new Stack<Object>() {});

    checkClass(new TreeMap<>());
    checkClass(new TreeMap<Object, Object>() {});

    checkClass(new TreeSet<>());
    checkClass(new TreeSet<Object>() {});
  }

  /**
   * Test for {@link StatArchiveWriter#writeCompactValue} and
   * {@link StatArchiveWriter#readCompactValue}. Also added test for
   * ByteBufferInputStream#readUnsigned* methods (bug #41197).
   */
  @Test
  public void testStatArchiveCompactValueSerialization() throws Exception {
    // test all combos of valueToTest and + and -offsets
    long[] valuesToTest = new long[] {0, Byte.MAX_VALUE, Byte.MIN_VALUE, Short.MAX_VALUE,
        Short.MIN_VALUE, Integer.MAX_VALUE, Integer.MIN_VALUE, Long.MAX_VALUE, Long.MIN_VALUE};
    int[] offsets = new int[] {0, 1, 4, 9, 14, 15, 16, -1, -4, -9, -14, -15, -16};

    // write all combos of longs to the outputstream
    HeapDataOutputStream hdos = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataOutput out = hdos;
    for (long valueToTest : valuesToTest) {
      for (int offset : offsets) {
        long val = valueToTest + offset;
        StatArchiveWriter.writeCompactValue(val, out);
      }
    }
    // now read all the combos
    byte[] bytes = hdos.toByteArray();
    DataInput in = new DataInputStream(new ByteArrayInputStream(bytes));
    for (long valueToTest : valuesToTest) {
      for (int offset : offsets) {
        long expectedVal = valueToTest + offset;
        long val = StatArchiveWriter.readCompactValue(in);
        assertEquals(expectedVal, val);
      }
    }

    // also test using ByteBufferInputStream (bug #41197)
    in = new ByteBufferInputStream(ByteBuffer.wrap(bytes));
    for (long valueToTest : valuesToTest) {
      for (int offset : offsets) {
        long expectedVal = valueToTest + offset;
        long val = StatArchiveWriter.readCompactValue(in);
        assertEquals(expectedVal, val);
      }
    }

    // now check ByteBufferInputStream#readUnsignedShort explicitly
    // readUnsignedByte is already tested in StatArchiveWriter.readCompactValue
    // above likely in a more thorough manner than a simple explicit test would
    short[] shortValuesToTest =
        new short[] {0, Byte.MAX_VALUE, Byte.MIN_VALUE, Short.MAX_VALUE, Short.MIN_VALUE};

    ByteBufferOutputStream bos = new ByteBufferOutputStream();
    out = new DataOutputStream(bos);
    for (short valueToTest : shortValuesToTest) {
      for (int offset : offsets) {
        short val = (short) (valueToTest + offset);
        out.writeShort(val);
      }
    }
    // now read all the combos
    in = new ByteBufferInputStream(bos.getContentBuffer());
    for (short valueToTest : shortValuesToTest) {
      for (int offset : offsets) {
        int expectedVal = (valueToTest + offset) & 0xffff;
        int val = in.readUnsignedShort();
        assertEquals(expectedVal, val);
      }
    }
  }

  /**
   * A <code>DataSerializer</code> that provides default implementations of its methods.
   */
  private abstract static class DataSerializerImpl extends DataSerializer {

    public DataSerializerImpl() {}

    @Override
    public boolean toData(Object o, DataOutput out) throws IOException {
      fail("toData() should not be invoked");
      return false;
    }

    @Override
    public Object fromData(DataInput in) throws IOException, ClassNotFoundException {
      fail("fromData() should not be invoked");
      return null;
    }
  }

  /**
   * A class that implements {@link Serializable} and has fields of each type.
   */
  private static class SerializableImpl implements Serializable {

    protected byte byteField;
    protected short shortField;
    protected int intField;
    protected long longField;
    protected float floatField;
    protected double doubleField;
    protected char charField;
    protected boolean booleanField;
    protected String stringField;
    protected Object objectField;

    protected byte byteFieldPrim;
    protected short shortFieldPrim;
    protected int intFieldPrim;
    protected long longFieldPrim;
    protected float floatFieldPrim;
    protected double doubleFieldPrim;
    protected char charFieldPrim;
    protected boolean booleanFieldPrim;

    protected int unsignedByteField;
    protected int unsignedShortField;

    /**
     * Creates a new <code>SerializableImpl</code> whose contents is randomly generated.
     */
    SerializableImpl(Random random) {
      byteField = (byte) random.nextInt();
      shortField = (short) random.nextInt();
      intField = random.nextInt();
      longField = random.nextLong();
      floatField = random.nextFloat();
      doubleField = random.nextDouble();
      charField = (char) ('A' + random.nextInt('Z' - 'A'));
      booleanField = random.nextInt() % 2 == 0;

      byteFieldPrim = (byte) random.nextInt();
      shortFieldPrim = (short) random.nextInt();
      intFieldPrim = random.nextInt();
      longFieldPrim = random.nextLong();
      floatFieldPrim = random.nextFloat();
      doubleFieldPrim = random.nextDouble();
      charFieldPrim = (char) ('A' + random.nextInt('Z' - 'A'));
      booleanFieldPrim = random.nextInt() % 2 == 0;

      unsignedByteField = random.nextInt(256);
      unsignedShortField = random.nextInt(65536);

      int length = random.nextInt(100);
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < length; i++) {
        char c = (char) ('A' + random.nextInt('Z' - 'A'));
        sb.append(c);
      }
      stringField = sb.toString();

      objectField = new SerializableImpl();
    }

    /**
     * Creates a new <code>SerializableImpl</code> whose contents is empty.
     */
    SerializableImpl() {

    }

    /**
     * Two <code>SerializableImpl</code>s are equal if their contents are equal.
     */
    @Override
    public boolean equals(Object o) {
      if (!(o instanceof SerializableImpl)) {
        return false;
      }

      SerializableImpl other = (SerializableImpl) o;
      return byteField == other.byteField && shortField == other.shortField
          && intField == other.intField && longField == other.longField
          && floatField == other.floatField && doubleField == other.doubleField
          && charField == other.charField && booleanField == other.booleanField
          && byteFieldPrim == other.byteFieldPrim
          && shortFieldPrim == other.shortFieldPrim && intFieldPrim == other.intFieldPrim
          && longFieldPrim == other.longFieldPrim
          && floatFieldPrim == other.floatFieldPrim
          && doubleFieldPrim == other.doubleFieldPrim
          && charFieldPrim == other.charFieldPrim
          && booleanFieldPrim == other.booleanFieldPrim
          && unsignedByteField == other.unsignedByteField
          && unsignedShortField == other.unsignedShortField
          && (stringField == null || stringField.equals(other.stringField))
          && (objectField == null || objectField.equals(other.objectField));
    }

    @Override
    public String toString() {

      return getClass().getName()
          + " byte: "
          + byteField
          + ", short: "
          + shortField
          + ", int: "
          + intField
          + ", long: "
          + longField
          + ", float: "
          + floatField
          + ", double: "
          + doubleField
          + " bytePrim: "
          + byteFieldPrim
          + ", shortPrim: "
          + shortFieldPrim
          + ", intPrim: "
          + intFieldPrim
          + ", longPrim: "
          + longFieldPrim
          + ", floatPrim: "
          + floatFieldPrim
          + ", doublePrim: "
          + doubleFieldPrim
          + ", unsignedByte: "
          + unsignedByteField
          + ", unsignedShort: "
          + unsignedShortField
          + ", string: \""
          + stringField
          + "\", object: "
          + objectField;
    }
  }

  /**
   * A class that implements {@link DataSerializable}
   *
   * Also used by DataTypeJUnitTest
   */
  public static class DataSerializableImpl extends SerializableImpl implements DataSerializable {

    /**
     * Creates a new <code>DataSerializableImpl</code> whose contents is randomly generated.
     */
    public DataSerializableImpl(Random random) {
      super(random);
    }

    /**
     * Creates a new <code>DataSerializableImpl</code> whose contents is empty.
     */
    public DataSerializableImpl() {
      super();
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeByte(byteField, out);
      DataSerializer.writeShort(shortField, out);
      DataSerializer.writeInteger(intField, out);
      DataSerializer.writeLong(longField, out);
      DataSerializer.writeFloat(floatField, out);
      DataSerializer.writeDouble(doubleField, out);
      DataSerializer.writeCharacter(charField, out);
      DataSerializer.writeBoolean(booleanField, out);

      DataSerializer.writePrimitiveByte(byteFieldPrim, out);
      DataSerializer.writePrimitiveShort(shortFieldPrim, out);
      DataSerializer.writePrimitiveInt(intFieldPrim, out);
      DataSerializer.writePrimitiveLong(longFieldPrim, out);
      DataSerializer.writePrimitiveFloat(floatFieldPrim, out);
      DataSerializer.writePrimitiveDouble(doubleFieldPrim, out);
      DataSerializer.writePrimitiveChar(charFieldPrim, out);
      DataSerializer.writePrimitiveBoolean(booleanFieldPrim, out);

      DataSerializer.writeUnsignedByte(unsignedByteField, out);
      DataSerializer.writeUnsignedShort(unsignedShortField, out);

      DataSerializer.writeString(stringField, out);
      DataSerializer.writeObject(objectField, out);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {

      byteField = DataSerializer.readByte(in);
      shortField = DataSerializer.readShort(in);
      intField = DataSerializer.readInteger(in);
      longField = DataSerializer.readLong(in);
      floatField = DataSerializer.readFloat(in);
      doubleField = DataSerializer.readDouble(in);
      charField = DataSerializer.readCharacter(in);
      booleanField = DataSerializer.readBoolean(in);

      byteFieldPrim = DataSerializer.readPrimitiveByte(in);
      shortFieldPrim = DataSerializer.readPrimitiveShort(in);
      intFieldPrim = DataSerializer.readPrimitiveInt(in);
      longFieldPrim = DataSerializer.readPrimitiveLong(in);
      floatFieldPrim = DataSerializer.readPrimitiveFloat(in);
      doubleFieldPrim = DataSerializer.readPrimitiveDouble(in);
      charFieldPrim = DataSerializer.readPrimitiveChar(in);
      booleanFieldPrim = DataSerializer.readPrimitiveBoolean(in);

      unsignedByteField = DataSerializer.readUnsignedByte(in);
      unsignedShortField = DataSerializer.readUnsignedShort(in);

      stringField = DataSerializer.readString(in);
      objectField = DataSerializer.readObject(in);
    }

  }

  private static class VersionedDataSerializableImpl extends DataSerializableImpl
      implements VersionedDataSerializable {

    @Override
    public KnownVersion[] getSerializationVersions() {
      return new KnownVersion[] {KnownVersion.GEODE_1_12_0};
    }

    transient boolean preMethodInvoked;

    @SuppressWarnings("unused") // for deserialization
    public VersionedDataSerializableImpl() {}

    public VersionedDataSerializableImpl(Random random) {
      super(random);
    }

    @SuppressWarnings("unused") // for serialization
    public void toDataPre_GEODE_1_12_0_0(DataOutput out) throws IOException {
      preMethodInvoked = true;
      toData(out);
    }

    @SuppressWarnings("unused") // for deserialization
    public void fromDataPre_GEODE_1_12_0_0(DataInput in)
        throws IOException, ClassNotFoundException {
      preMethodInvoked = true;
      fromData(in);
    }

    public boolean preMethodInvoked() {
      return preMethodInvoked;
    }
  }

  private static class CanonicalDataSerializableImpl extends SerializableImpl
      implements DataSerializable {

    private static final byte SINGLETON_BYTE = 23;
    private static final CanonicalDataSerializableImpl singleton =
        new CanonicalDataSerializableImpl(new Random());

    public static CanonicalDataSerializableImpl create() {
      return singleton;
    }

    public static CanonicalDataSerializableImpl create(byte b) {
      assertEquals(SINGLETON_BYTE, b);
      return singleton;
    }

    /**
     * Creates a new <code>CanonicalDataSerializableImpl</code> whose contents is randomly
     * generated.
     */
    private CanonicalDataSerializableImpl(Random random) {
      super(random);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      out.writeByte(SINGLETON_BYTE);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {}

  }

  /**
   * A class that replaces itself with an <code>Integer</code> when written.
   */
  private static class ReplaceableImpl implements DataSerializable.Replaceable {

    @Override
    public Object replace() throws IOException {
      return 42;
    }
  }

  /**
   * A non-<code>DataSerializable</code> class whose instances are data serialized using a
   * <code>Serializer</code>.
   *
   * @since GemFire 3.5
   */
  private static class NonDataSerializable {

    protected int intValue;
    protected double doubleValue;
    protected String stringValue;
    protected DataSerializable dsValue;
    protected Serializable serValue;
    protected Object objectValue;

    public NonDataSerializable(Random random) {
      intValue = random.nextInt();
      doubleValue = random.nextDouble();
      stringValue = "STRING" + random.nextInt();
      dsValue = new DataSerializableImpl(random);
      objectValue = random.nextInt();
      serValue = new SerializableImpl(random);
    }

    protected NonDataSerializable() {}

    public boolean equals(Object o) {
      if (!(o instanceof NonDataSerializable)) {
        return false;
      }

      NonDataSerializable other = (NonDataSerializable) o;
      return other.intValue == intValue && other.doubleValue == doubleValue
          && (other.stringValue != null && stringValue != null)
          && other.stringValue.equals(stringValue)
          && (other.dsValue != null && dsValue != null) && other.dsValue.equals(dsValue)
          && (other.serValue != null && serValue != null)
          && other.serValue.equals(serValue)
          && (other.objectValue != null && objectValue != null)
          && other.objectValue.equals(objectValue);
    }

    /**
     * A <code>Serializer</code> that data serializes instances of <code>NonDataSerializable</code>.
     */
    public static class NonDSSerializer extends DataSerializer {

      private static final byte CLASS_ID = (byte) 100;

      public NonDSSerializer() {}

      @Override
      public int getId() {
        return CLASS_ID;
      }

      @Override
      public Class<?>[] getSupportedClasses() {
        return new Class[] {NonDataSerializable.class};
      }

      @Override
      public boolean toData(Object o, DataOutput out) throws IOException {

        if (o instanceof NonDataSerializable) {
          NonDataSerializable nds = (NonDataSerializable) o;

          out.writeByte(CLASS_ID);
          out.writeInt(nds.intValue);
          out.writeDouble(nds.doubleValue);
          out.writeUTF(nds.stringValue);
          writeObject(nds.dsValue, out);
          writeObject(nds.serValue, out);
          writeObject(nds.objectValue, out);

          return true;

        } else {
          return false;
        }
      }

      @Override
      public Object fromData(DataInput in) throws IOException, ClassNotFoundException {

        byte classId = in.readByte();
        assertEquals(CLASS_ID, classId);
        NonDataSerializable nds = new NonDataSerializable();
        nds.intValue = in.readInt();
        nds.doubleValue = in.readDouble();
        nds.stringValue = in.readUTF();
        nds.dsValue = readObject(in);
        nds.serValue = readObject(in);
        nds.objectValue = readObject(in);

        return nds;
      }
    }
  }

  /**
   * A much more simple class to be data serialized
   */
  private static class IntWrapper {

    public int intValue;

    public IntWrapper(Random random) {
      intValue = random.nextInt();
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof IntWrapper) {
        IntWrapper other = (IntWrapper) o;
        return other.intValue == intValue;

      } else {
        return false;
      }
    }
  }

  /**
   * A <code>DataSerializable</code> int wrapper
   */
  private static class DSIntWrapper extends IntWrapper implements DataSerializable {

    public DSIntWrapper(Random random) {
      super(random);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      out.writeInt(intValue);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {

      intValue = in.readInt();
    }
  }

  private static class SerializableIntWrapper implements Serializable {

    private final int data;

    public SerializableIntWrapper(int intValue) {
      data = intValue;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + data;
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      SerializableIntWrapper other = (SerializableIntWrapper) obj;
      return data == other.data;
    }
  }

  /**
   * A node in an object chain
   */
  private static class Link implements DataSerializable, Serializable {

    private int id;
    Link next;
    Link next2;

    public Link(int id) {
      this.id = id;
    }

    @SuppressWarnings("unused") // for deserialization
    public Link() {}

    @Override
    public void toData(DataOutput out) throws IOException {
      out.writeInt(id);
      DataSerializer.writeObject(next, out);
      DataSerializer.writeObject(next2, out);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      id = in.readInt();
      next = DataSerializer.readObject(in);
      next2 = DataSerializer.readObject(in);
    }
  }

  /**
   * A <code>RegistrationListener</code> used for testing
   */
  private static class TestRegistrationListener
      implements InternalDataSerializer.RegistrationListener {

    /** Was this listener invoked? */
    private boolean invoked = false;

    /** An error thrown in a callback */
    private Throwable callbackError = null;

    /**
     * Returns wether or not one of this listener methods was invoked. Before returning, the
     * <code>invoked</code> flag is cleared.
     */
    public boolean wasInvoked() {
      checkForError();
      boolean value = invoked;
      invoked = false;
      return value;
    }

    private void checkForError() {
      if (callbackError != null) {
        throw new AssertionError("Exception occurred in callback", callbackError);
      }
    }

    @Override
    public void newDataSerializer(DataSerializer ds) {
      invoked = true;
      try {
        newDataSerializer2(ds);

      } catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        throw e;
      } catch (Throwable t) {
        callbackError = t;
      }
    }

    public void newDataSerializer2(DataSerializer ds) {
      String s = "Unexpected callback invocation";
      throw new UnsupportedOperationException(s);
    }

    @Override
    public void newInstantiator(Instantiator instantiator) {
      invoked = true;
      try {
        newInstantiator2(instantiator);

      } catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        throw e;
      } catch (Throwable t) {
        callbackError = t;
      }
    }

    public void newInstantiator2(Instantiator instantiator) {
      String s = "Unexpected callback invocation";
      throw new UnsupportedOperationException(s);
    }

  }

  // tests added to cover bug 41206

  /**
   * Tests data serializing a <code>byte[][]</code> using {@link DataSerializer#writeObject}.
   */
  @Test
  public void testByteArrayArrayObject() throws Exception {
    byte[] ar0 = new byte[] {(byte) 1, (byte) 2, (byte) 3};
    byte[] ar1 = new byte[] {(byte) 4, (byte) 5, (byte) 6};
    byte[] ar2 = new byte[] {(byte) 7, (byte) 8, (byte) 9};
    byte[][] array = new byte[3][];
    array[0] = ar0;
    array[1] = ar1;
    array[2] = ar2;

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(array, out);
    out.flush();

    DataInput in = getDataInput();
    byte[][] array2 = DataSerializer.readObject(in);

    assertEquals(array.length, array2.length);
    for (int i = 0; i < array.length; i++) {
      byte[] compArray = array2[i];
      for (int j = 0; j < compArray.length; j++) {
        assertEquals(array[i][j], array2[i][j]);
      }
    }
  }

  /**
   * Tests data serializing a <code>byte[][]</code> using {@link DataSerializer#writeObjectArray}.
   */
  @Test
  public void testByteArrayArray() throws Exception {
    byte[] ar0 = new byte[] {(byte) 1, (byte) 2, (byte) 3};
    byte[] ar1 = new byte[] {(byte) 4, (byte) 5, (byte) 6};
    byte[] ar2 = new byte[] {(byte) 7, (byte) 8, (byte) 9};
    byte[][] array = new byte[3][];
    array[0] = ar0;
    array[1] = ar1;
    array[2] = ar2;

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObjectArray(array, out);
    out.flush();

    DataInput in = getDataInput();
    byte[][] array2 = (byte[][]) DataSerializer.readObjectArray(in);

    assertEquals(array.length, array2.length);
    for (int i = 0; i < array.length; i++) {
      byte[] compArray = array2[i];
      for (int j = 0; j < compArray.length; j++) {
        assertEquals(array[i][j], array2[i][j]);
      }
    }
  }

  // see bug 41721
  @Test
  public void testArrayMinShortLength() throws Exception {
    DataOutputStream out = getDataOutput();
    DataSerializer.writeByteArray(new byte[0x8000], out);
    out.flush();

    DataInput in = getDataInput();
    byte[] array = DataSerializer.readByteArray(in);
    assertEquals(0x8000, array.length);
  }

  @Test
  public void testArrayMaxShortLength() throws Exception {
    DataOutputStream out = getDataOutput();
    DataSerializer.writeByteArray(new byte[0xFFFF], out);
    out.flush();

    DataInput in = getDataInput();
    byte[] array = DataSerializer.readByteArray(in);
    assertEquals(0xFFFF, array.length);
  }

  /**
   * Tests data serializing a non-<code>null</code> String whose length is > 0xFFFF, but who's utf-8
   * encoded length is < 0xFFFF See bug 40932.
   */
  @Test
  public void testStringEncodingLengthCrossesBoundry() throws Exception {
    StringBuilder sb = new StringBuilder(0xFFFF);
    for (int i = 0; i < 0xFFFF; i++) {
      if (i == 0) {
        sb.append(Character.MAX_VALUE);
      } else {
        sb.append("a");
      }
    }
    String value = sb.toString();

    DataOutputStream out = getDataOutput();
    DataSerializer.writeString(value, out);
    DataSerializer.writeObject(value, out);
    out.flush();

    DataInput in = getDataInput();
    String value2 = DataSerializer.readString(in);
    assertEquals(value, value2);
    value2 = DataSerializer.readObject(in);
    assertEquals(value, value2);
  }

  private enum DAY_OF_WEEK implements PdxSerializerObject {
    MON, TUE, WED, THU, FRI, SAT, SUN
  }

  private enum MONTH implements PdxSerializerObject {
    JAN, FEB, MAR
  }

  /**
   * Tests Dataserializing an Enum
   */
  @Test
  public void testEnum() throws Exception {
    DAY_OF_WEEK e = DAY_OF_WEEK.SUN;
    MONTH m = MONTH.FEB;
    DataOutputStream out = getDataOutput();
    DataSerializer.writeEnum(e, out);
    DataSerializer.writeEnum(m, out);
    try {
      DataSerializer.writeEnum(null, out);
      fail("Expected exception not thrown");
    } catch (NullPointerException ignored) {
    }
    out.flush();

    DataInput in = getDataInput();
    @SuppressWarnings("rawtypes") // for testing
    Class c = null;
    try {
      // noinspection unchecked
      DataSerializer.readEnum(c, in);
      fail("Expected exception not thrown");
    } catch (NullPointerException ignored) {
    }
    c = Foo.class;
    try {
      // noinspection unchecked
      DataSerializer.readEnum(c, in);
      fail("Expected exception not thrown");
    } catch (IllegalArgumentException ignored) {
    }
    DAY_OF_WEEK e2 = DataSerializer.readEnum(DAY_OF_WEEK.class, in);
    MONTH m2 = DataSerializer.readEnum(MONTH.class, in);
    assertEquals(e, e2);
    assertEquals(m, m2);
  }

  @Test
  public void testObjectEnum() throws Exception {
    DAY_OF_WEEK e = DAY_OF_WEEK.SUN;
    MONTH m = MONTH.FEB;
    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(e, out);
    DataSerializer.writeObject(m, out);
    out.flush();

    DataInput in = getDataInput();
    DAY_OF_WEEK e2 = DataSerializer.readObject(in);
    MONTH m2 = DataSerializer.readObject(in);
    assertEquals(e, e2);
    assertEquals(m, m2);
    // Make sure there's nothing left in the stream
    assertEquals(0, in.skipBytes(1));
  }

  /**
   * Usually the DataInput instance passed to DataSerializer.readObject is an instance of
   * InputStream. Make sure that an object that uses standard java serialization can be written and
   * read from a DataInput that is not an instance of InputStream. See bug 47249.
   */
  @Test
  public void testOddDataInput() throws Exception {
    SerializableIntWrapper o = new SerializableIntWrapper(-1);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataSerializer.writeObject(o, new DataOutputStream(baos));
    OddDataInput odi = new OddDataInput(ByteBuffer.wrap(baos.toByteArray()));
    Object o2 = DataSerializer.readObject(odi);
    assertEquals(o, o2);
  }

  private static class OddDataInput implements DataInput {

    private final ByteBufferInputStream bbis;

    public OddDataInput(ByteBuffer bb) {
      bbis = new ByteBufferInputStream(bb);
    }

    @Override
    public void readFully(byte[] b) {
      bbis.readFully(b);
    }

    @Override
    public void readFully(byte[] b, int off, int len) {
      bbis.readFully(b, off, len);
    }

    @Override
    public int skipBytes(int n) {
      return bbis.skipBytes(n);
    }

    @Override
    public boolean readBoolean() {
      return bbis.readBoolean();
    }

    @Override
    public byte readByte() {
      return bbis.readByte();
    }

    @Override
    public int readUnsignedByte() {
      return bbis.readUnsignedByte();
    }

    @Override
    public short readShort() {
      return bbis.readShort();
    }

    @Override
    public int readUnsignedShort() {
      return bbis.readUnsignedShort();
    }

    @Override
    public char readChar() {
      return bbis.readChar();
    }

    @Override
    public int readInt() throws IOException {
      return bbis.readInt();
    }

    @Override
    public long readLong() {
      return bbis.readLong();
    }

    @Override
    public float readFloat() {
      return bbis.readFloat();
    }

    @Override
    public double readDouble() {
      return bbis.readDouble();
    }

    @Override
    public String readLine() {
      return bbis.readLine();
    }

    @Override
    public String readUTF() throws IOException {
      return bbis.readUTF();
    }
  }

  private static class Foo {
  }
}
