package org.apache.geode.serialization;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.nio.charset.Charset;

@RunWith(JUnitQuickcheck.class)
public class SerializationTypeTest {
  @Property
  public void testStringSerialization(String testString) throws Exception {
    byte[] expectedBytes = testString.getBytes(Charset.forName("UTF-8"));
    byte[] serializedBytes = SerializationType.STRING.serialize(testString);
    assertArrayEquals(expectedBytes, serializedBytes);
    assertEquals(testString, SerializationType.STRING.deserialize(expectedBytes));
  }


  @Test
  public void testLongSerialization() throws Exception {
    long testValue = 1311768465047740160L; // otherwise known as 0x123456780abadf00 bytes;
    byte[] expectedBytes =
        new byte[] {0x12, 0x34, 0x56, 0x78, 0x0a, (byte) 0xba, (byte) 0xdf, 0x00};
    byte[] serializedBytes = SerializationType.LONG.serialize(testValue);
    assertArrayEquals(expectedBytes, serializedBytes);
    assertEquals(testValue, SerializationType.LONG.deserialize(expectedBytes));

    testValue = -1311234265047740160L; // otherwise known as 0xedcd8f6216825100 bytes;
    expectedBytes = new byte[] {(byte) 0xed, (byte) 0xcd, (byte) 0x8f, 0x62, 0x16, (byte) 0x82,
        (byte) 0x51, 0x00};
    serializedBytes = SerializationType.LONG.serialize(testValue);
    assertArrayEquals(expectedBytes, serializedBytes);
    assertEquals(testValue, SerializationType.LONG.deserialize(expectedBytes));
  }

  @Test
  public void testIntSerialization() throws Exception {
    int testValue = 313216366; // otherwise known as 0x12ab4d6e bytes;
    byte[] expectedBytes = new byte[] {0x12, (byte) 0xab, 0x4d, 0x6e};
    byte[] serializedBytes = SerializationType.INT.serialize(testValue);
    assertArrayEquals(expectedBytes, serializedBytes);
    assertEquals(testValue, SerializationType.INT.deserialize(expectedBytes));

    testValue = -313216366; // otherwise known as 0xed54b292 bytes;
    expectedBytes = new byte[] {(byte) 0xed, 0x54, (byte) 0xb2, (byte) 0x92};
    serializedBytes = SerializationType.INT.serialize(testValue);
    assertArrayEquals(expectedBytes, serializedBytes);
    assertEquals(testValue, SerializationType.INT.deserialize(expectedBytes));
  }

  @Test
  public void testShortSerialization() throws Exception {
    short testValue = 31321; // otherwise known as 0x7a59 bytes;
    byte[] expectedBytes = new byte[] {0x7a, 0x59};
    byte[] serializedBytes = SerializationType.SHORT.serialize(testValue);
    assertArrayEquals(expectedBytes, serializedBytes);
    assertEquals(testValue, SerializationType.SHORT.deserialize(expectedBytes));

    testValue = -22357; // otherwise known as 0xa8ab bytes;
    expectedBytes = new byte[] {(byte) 0xa8, (byte) 0xab};
    serializedBytes = SerializationType.SHORT.serialize(testValue);
    assertArrayEquals(expectedBytes, serializedBytes);
    assertEquals(testValue, SerializationType.SHORT.deserialize(expectedBytes));
  }

  @Test
  public void testByteSerialization() throws Exception {
    byte testValue = 116;
    byte[] expectedBytes = new byte[] {116};
    byte[] serializedBytes = SerializationType.BYTE.serialize(testValue);
    assertArrayEquals(expectedBytes, serializedBytes);
    assertEquals(testValue, SerializationType.BYTE.deserialize(expectedBytes));

    testValue = -87;
    expectedBytes = new byte[] {-87};
    serializedBytes = SerializationType.BYTE.serialize(testValue);
    assertArrayEquals(expectedBytes, serializedBytes);
    assertEquals(testValue, SerializationType.BYTE.deserialize(expectedBytes));

  }

  @Property
  public void testBinarySerialization(byte[] bytes) throws Exception {
    assertArrayEquals(bytes, SerializationType.BYTE_BLOB.serialize(bytes));
    assertArrayEquals(bytes, (byte[]) SerializationType.BYTE_BLOB.deserialize(bytes));
  }

  @Ignore("Not currently testable without a Cache, because JSON gets serialized to a PDXInstance, "
      + "which requires a Cache.")
  @Test
  public void testJSONSerialization() {
    String[] testStrings =
        {"\"testString\"", "{}", "{\"foo\":\"bar\",\"list :\":[1,2,\"boo\"],hash:{1:2,3:4}}"};

    for (String string : testStrings) {
      byte[] serialized = SerializationType.JSON.serialize(string);
      String deserialized = (String) SerializationType.JSON.deserializer.deserialize(serialized);
      assertEquals(string, deserialized);
      fail("This test is not yet complete");
    }
  }
}
