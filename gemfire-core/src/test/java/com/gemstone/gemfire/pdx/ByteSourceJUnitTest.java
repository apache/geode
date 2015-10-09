package com.gemstone.gemfire.pdx;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.tcp.ByteBufferInputStream.ByteSource;
import com.gemstone.gemfire.internal.tcp.ByteBufferInputStream.ByteSourceFactory;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class ByteSourceJUnitTest {
  
  protected ByteSource createByteSource(byte[] bytes) {
    return ByteSourceFactory.wrap(bytes);
  }

  @Test
  public void testHashCode() {
    ByteSource bs = createByteSource(new byte[]{});
    bs.hashCode();
    bs = createByteSource(new byte[]{1, 2, 3, 4, 5});
    int hc1 = bs.hashCode();
    bs = createByteSource(new byte[]{1, 2, 3, 4, 5});
    int hc2 = bs.hashCode();
    assertEquals(hc1, hc2);
  }

  @Test
  public void testEqualsObject() {
    assertEquals(createByteSource(new byte[]{}), createByteSource(new byte[]{}));
    assertEquals(createByteSource(new byte[]{1,2,3,4,5,6,7,8,9,0}), createByteSource(new byte[]{1,2,3,4,5,6,7,8,9,0}));
    assertNotEquals(createByteSource(new byte[]{1,2,3,4,5,6,7,8,9,0}), createByteSource(new byte[]{1,2,3,4,5,6,7,8,9,0,1}));
    assertNotEquals(createByteSource(new byte[]{1,2,3,4,5,6,7,8,9,0}), createByteSource(new byte[]{1,2,3,4,5,6,7,8,9,9}));
  }

  @Test
  public void testDuplicate() {
    ByteSource bs = createByteSource(new byte[]{1,2,3,4,5,6,7,8,9,0});
    ByteSource dup = bs.duplicate();
    assertEquals(bs, dup);
    dup.position(1);
    assertNotEquals(bs, dup);
    dup.position(0);
    assertEquals(bs, dup);
    dup.limit(2);
    assertNotEquals(bs, dup);
    dup.limit(bs.limit());
    assertEquals(bs, dup);
    if (bs.hasArray()) {
      byte[] bsBytes = bs.array();
      byte[] dupBytes = dup.array();
      assertEquals(bsBytes, dupBytes);
      assertNotEquals(new byte[]{1,2,3,4,5,6,7,8,9,0}, dupBytes);
    }
    bs.position(1);
    bs.limit(2);
    ByteSource dup2 = bs.duplicate();
    assertEquals(bs, dup2);
    assertEquals(1, dup2.position());
    assertEquals(2, dup2.limit());
  }

  @Test
  public void testPosition() {
    assertEquals(0, createByteSource(new byte[]{}).position());
    ByteSource bs = createByteSource(new byte[]{1,2,3,4,5,6,7,8,9,0});
    assertEquals(0, bs.position());
    try {
      bs.position(-1);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
    try {
      bs.position(11);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
    assertEquals(0, bs.position());
    bs.position(10);
    assertEquals(10, bs.position());
    bs.limit(3);
    assertEquals(3, bs.position());
    try {
      bs.position(4);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
  }

  @Test
  public void testLimit() {
    assertEquals(0, createByteSource(new byte[]{}).limit());
    ByteSource bs = createByteSource(new byte[]{1,2,3,4,5,6,7,8,9,0});
    assertEquals(10, bs.limit());
    try {
      bs.limit(11);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
    try {
      bs.limit(-1);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
    assertEquals(10, bs.limit());
    bs.limit(3);
    assertEquals(3, bs.limit());
  }

  @Test
  public void testCapacity() {
    ByteSource bs = createByteSource(new byte[]{1,2,3,4,5,6,7,8,9,0});
    assertEquals(10, bs.capacity());
    bs = createByteSource(new byte[]{});
    assertEquals(0, bs.capacity());
  }

  @Test
  public void testRemaining() {
    assertEquals(0, createByteSource(new byte[]{}).remaining());
    ByteSource bs = createByteSource(new byte[]{1,2,3,4,5,6,7,8,9,0});
    assertEquals(10, bs.remaining());
    bs.position(1);
    assertEquals(9, bs.remaining());
    bs.limit(9);
    assertEquals(8, bs.remaining());
    bs.position(8);
    assertEquals(1, bs.remaining());
    bs.position(9);
    assertEquals(0, bs.remaining());
  }

  @Test
  public void testGet() {
    ByteSource bs = createByteSource(new byte[]{1,2,3,4,5,6,7,8,9,0});
    byte b = bs.get();
    assertEquals(1, b);
    assertEquals(1, bs.position());
    b = bs.get();
    assertEquals(2, b);
    assertEquals(2, bs.position());
    bs.position(bs.limit()-1);
    b = bs.get();
    assertEquals(0, b);
    assertEquals(10, bs.position());
    try {
      bs.get();
      fail("expected BufferUnderflowException");
    } catch (BufferUnderflowException expected) {
    }
  }

  @Test
  public void testGetInt() {
    ByteSource bs = createByteSource(new byte[]{1,2,3,4,5,6,7,8,9,0});
    bs.position(4);
    byte b = bs.get(0);
    assertEquals(1, b);
    assertEquals(4, bs.position());
    b = bs.get(1);
    assertEquals(2, b);
    assertEquals(4, bs.position());
    b = bs.get(9);
    assertEquals(0, b);
    try {
      bs.get(10);
      fail("expected IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException expected) {
    }
  }

  @Test
  public void testGetByteArray() {
    ByteSource bs = createByteSource(new byte[]{1,2,3,4,5,6,7,8,9,0});
    byte[] bytes = new byte[2];
    bs.get(bytes);
    assertArrayEquals(new byte[]{1,2}, bytes);
    assertEquals(2, bs.position());
    
    bytes = new byte[0];
    bs.get(bytes);
    assertEquals(2, bs.position());
    
    bytes = new byte[3];
    bs.get(bytes);
    assertArrayEquals(new byte[]{3,4,5}, bytes);
    assertEquals(5, bs.position());

    try {
      bytes = new byte[6];
      bs.get(bytes);
      fail("expected BufferUnderflowException");
    } catch (BufferUnderflowException expected) {
    }

    bytes = new byte[5];
    bs.get(bytes);
    assertArrayEquals(new byte[]{6,7,8,9,0}, bytes);
    assertEquals(10, bs.position());

    bytes = new byte[0];
    bs.get(bytes);
    assertEquals(10, bs.position());

    try {
      bytes = new byte[1];
      bs.get(bytes);
      fail("expected BufferUnderflowException");
    } catch (BufferUnderflowException expected) {
    }
  }

  @Test
  public void testGetByteArrayIntInt() {
    ByteSource bs = createByteSource(new byte[]{1,2,3,4,5,6,7,8,9,0});
    byte[] bytes = new byte[10];
    bs.get(bytes, 0, 2);
    assertEquals(2, bs.position());
    
    bs.get(bytes, 2, 0);
    assertEquals(2, bs.position());
    
    bs.get(bytes, 2, 3);
    assertEquals(5, bs.position());

    try {
      bs.get(bytes, 4, 6);
      fail("expected BufferUnderflowException");
    } catch (BufferUnderflowException expected) {
    }

    bs.get(bytes, 5, 5);
    assertEquals(10, bs.position());

    bs.get(bytes, 10, 0);
    assertEquals(10, bs.position());
    
    assertArrayEquals(new byte[]{1,2,3,4,5,6,7,8,9,0}, bytes);

    try {
      bs.get(bytes, 9, 1);
      fail("expected BufferUnderflowException");
    } catch (BufferUnderflowException expected) {
    }
    try {
      bs.get(bytes, 10, 1);
      fail("expected IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException expected) {
    }
    try {
      bs.get(bytes, 3, -1);
      fail("expected IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException expected) {
    }
    try {
      bs.get(bytes, -1, 2);
      fail("expected IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException expected) {
    }
  }

  @Test
  public void testGetChar() {
    ByteBuffer bb = ByteBuffer.allocate(10);
    CharBuffer cb = bb.asCharBuffer();
    cb.put("abcde");
    byte[] bytes = bb.array();
    ByteSource bs = createByteSource(bytes);
    char c = bs.getChar();
    assertEquals('a', c);
    assertEquals(2, bs.position());
    c = bs.getChar();
    assertEquals('b', c);
    assertEquals(4, bs.position());
    bs.position(8);
    c = bs.getChar();
    assertEquals('e', c);
    assertEquals(10, bs.position());
    try {
      bs.getChar();
      fail("expected BufferUnderflowException");
    } catch (BufferUnderflowException expected) {
    }
  }

  @Test
  public void testGetCharInt() {
    ByteBuffer bb = ByteBuffer.allocate(10);
    CharBuffer cb = bb.asCharBuffer();
    cb.put("abcde");
    byte[] bytes = bb.array();
    ByteSource bs = createByteSource(bytes);
    bs.position(3);
    char c = bs.getChar(0);
    assertEquals('a', c);
    assertEquals(3, bs.position());
    c = bs.getChar(2);
    assertEquals('b', c);
    assertEquals(3, bs.position());
    c = bs.getChar(8);
    assertEquals('e', c);
    assertEquals(3, bs.position());
    try {
      bs.getChar(10);
      fail("expected IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException expected) {
    }
  }

  @Test
  public void testGetShort() {
    ByteBuffer bb = ByteBuffer.allocate(10);
    ShortBuffer sb = bb.asShortBuffer();
    sb.put((short)0x1110);
    sb.put((short)0x2220);
    sb.put((short)0x3330);
    sb.put((short)0x4440);
    sb.put((short)0x5550);
    byte[] bytes = bb.array();
    ByteSource bs = createByteSource(bytes);
    short s = bs.getShort();
    assertEquals(0x1110, s);
    assertEquals(2, bs.position());
    s = bs.getShort();
    assertEquals(0x2220, s);
    assertEquals(4, bs.position());
    bs.position(8);
    s = bs.getShort();
    assertEquals(0x5550, s);
    assertEquals(10, bs.position());
    try {
      bs.getShort();
      fail("expected BufferUnderflowException");
    } catch (BufferUnderflowException expected) {
    }
  }

  @Test
  public void testGetShortInt() {
    ByteBuffer bb = ByteBuffer.allocate(10);
    ShortBuffer sb = bb.asShortBuffer();
    sb.put((short)0x1110);
    sb.put((short)0x2220);
    sb.put((short)0x3330);
    sb.put((short)0x4440);
    sb.put((short)0x5550);
    byte[] bytes = bb.array();
    ByteSource bs = createByteSource(bytes);
    bs.position(3);
    short s = bs.getShort(0);
    assertEquals(0x1110, s);
    assertEquals(3, bs.position());
    s = bs.getShort(2);
    assertEquals(0x2220, s);
    assertEquals(3, bs.position());
    s = bs.getShort(8);
    assertEquals(0x5550, s);
    assertEquals(3, bs.position());
    try {
      bs.getShort(9);
      fail("expected IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException expected) {
    }
  }

  @Test
  public void testGetInt1() {
    ByteBuffer bb = ByteBuffer.allocate(20);
    IntBuffer ib = bb.asIntBuffer();
    ib.put(0x11102233);
    ib.put(0x22203344);
    ib.put(0x33304455);
    ib.put(0x44405566);
    ib.put(0x55506677);
    byte[] bytes = bb.array();
    ByteSource bs = createByteSource(bytes);
    int i = bs.getInt();
    assertEquals(0x11102233, i);
    assertEquals(4, bs.position());
    i = bs.getInt();
    assertEquals(0x22203344, i);
    assertEquals(8, bs.position());
    bs.position(4*4);
    i = bs.getInt();
    assertEquals(0x55506677, i);
    assertEquals(20, bs.position());
    try {
      bs.getInt();
      fail("expected BufferUnderflowException");
    } catch (BufferUnderflowException expected) {
    }
  }

  @Test
  public void testGetIntInt() {
    ByteBuffer bb = ByteBuffer.allocate(20);
    IntBuffer ib = bb.asIntBuffer();
    ib.put(0x11102233);
    ib.put(0x22203344);
    ib.put(0x33304455);
    ib.put(0x44405566);
    ib.put(0x55506677);
    byte[] bytes = bb.array();
    ByteSource bs = createByteSource(bytes);
    bs.position(3);
    int i = bs.getInt(0);
    assertEquals(0x11102233, i);
    assertEquals(3, bs.position());
    i = bs.getInt(4);
    assertEquals(0x22203344, i);
    assertEquals(3, bs.position());
    i = bs.getInt(4*4);
    assertEquals(0x55506677, i);
    assertEquals(3, bs.position());
    try {
      bs.getInt((4*4)+1);
      fail("expected IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException expected) {
    }
  }

  @Test
  public void testGetLong() {
    ByteBuffer bb = ByteBuffer.allocate(40);
    LongBuffer lb = bb.asLongBuffer();
    lb.put(0x1110223344556677L);
    lb.put(0x2220334455667788L);
    lb.put(0x3330445566778899L);
    lb.put(0x4440556677889900L);
    lb.put(0x55506677889900AAL);
    byte[] bytes = bb.array();
    ByteSource bs = createByteSource(bytes);
    long l = bs.getLong();
    assertEquals(0x1110223344556677L, l);
    assertEquals(8, bs.position());
    l = bs.getLong();
    assertEquals(0x2220334455667788L, l);
    assertEquals(16, bs.position());
    bs.position(4*8);
    l = bs.getLong();
    assertEquals(0x55506677889900AAL, l);
    assertEquals(40, bs.position());
    try {
      bs.getLong();
      fail("expected BufferUnderflowException");
    } catch (BufferUnderflowException expected) {
    }
  }

  @Test
  public void testGetLongInt() {
    ByteBuffer bb = ByteBuffer.allocate(40);
    LongBuffer lb = bb.asLongBuffer();
    lb.put(0x1110223344556677L);
    lb.put(0x2220334455667788L);
    lb.put(0x3330445566778899L);
    lb.put(0x4440556677889900L);
    lb.put(0x55506677889900AAL);
    byte[] bytes = bb.array();
    ByteSource bs = createByteSource(bytes);
    bs.position(3);
    long l = bs.getLong(0);
    assertEquals(0x1110223344556677L, l);
    assertEquals(3, bs.position());
    l = bs.getLong(8);
    assertEquals(0x2220334455667788L, l);
    assertEquals(3, bs.position());
    l = bs.getLong(4*8);
    assertEquals(0x55506677889900AAL, l);
    assertEquals(3, bs.position());
    try {
      bs.getLong((4*8)+1);
      fail("expected IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException expected) {
    }
  }

  @Test
  public void testGetFloat() {
    ByteBuffer bb = ByteBuffer.allocate(20);
    FloatBuffer fb = bb.asFloatBuffer();
    fb.put(1.1f);
    fb.put(2.2f);
    fb.put(3.3f);
    fb.put(4.4f);
    fb.put(5.5f);
    byte[] bytes = bb.array();
    ByteSource bs = createByteSource(bytes);
    float f = bs.getFloat();
    assertEquals(1.1f, f, 0.0001);
    assertEquals(4, bs.position());
    f = bs.getFloat();
    assertEquals(2.2f, f, 0.0001);
    assertEquals(8, bs.position());
    bs.position(4*4);
    f = bs.getFloat();
    assertEquals(5.5f, f, 0.0001);
    assertEquals(20, bs.position());
    try {
      bs.getFloat();
      fail("expected BufferUnderflowException");
    } catch (BufferUnderflowException expected) {
    }
  }

  @Test
  public void testGetFloatInt() {
    ByteBuffer bb = ByteBuffer.allocate(20);
    FloatBuffer fb = bb.asFloatBuffer();
    fb.put(1.1f);
    fb.put(2.2f);
    fb.put(3.3f);
    fb.put(4.4f);
    fb.put(5.5f);
    byte[] bytes = bb.array();
    ByteSource bs = createByteSource(bytes);
    bs.position(3);
    float f = bs.getFloat(0);
    assertEquals(1.1f, f, 0.0001);
    assertEquals(3, bs.position());
    f = bs.getFloat(4);
    assertEquals(2.2f, f, 0.0001);
    assertEquals(3, bs.position());
    f = bs.getFloat(4*4);
    assertEquals(5.5f, f, 0.0001);
    assertEquals(3, bs.position());
    try {
      bs.getFloat((4*4)+1);
      fail("expected IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException expected) {
    }
  }

  @Test
  public void testGetDouble() {
    ByteBuffer bb = ByteBuffer.allocate(40);
    DoubleBuffer db = bb.asDoubleBuffer();
    db.put(1.1d);
    db.put(2.2d);
    db.put(3.3d);
    db.put(4.4d);
    db.put(5.5d);
    byte[] bytes = bb.array();
    ByteSource bs = createByteSource(bytes);
    double d = bs.getDouble();
    assertEquals(1.1d, d, 0.0001);
    assertEquals(8, bs.position());
    d = bs.getDouble();
    assertEquals(2.2d, d, 0.0001);
    assertEquals(16, bs.position());
    bs.position(4*8);
    d = bs.getDouble();
    assertEquals(5.5d, d, 0.0001);
    assertEquals(40, bs.position());
    try {
      bs.getDouble();
      fail("expected BufferUnderflowException");
    } catch (BufferUnderflowException expected) {
    }
  }

  @Test
  public void testGetDoubleInt() {
    ByteBuffer bb = ByteBuffer.allocate(40);
    DoubleBuffer db = bb.asDoubleBuffer();
    db.put(1.1d);
    db.put(2.2d);
    db.put(3.3d);
    db.put(4.4d);
    db.put(5.5d);
    byte[] bytes = bb.array();
    ByteSource bs = createByteSource(bytes);
    bs.position(3);
    double d = bs.getDouble(0);
    assertEquals(1.1d, d, 0.0001);
    assertEquals(3, bs.position());
    d = bs.getDouble(8);
    assertEquals(2.2d, d, 0.0001);
    assertEquals(3, bs.position());
    d = bs.getDouble(4*8);
    assertEquals(5.5d, d, 0.0001);
    assertEquals(3, bs.position());
    try {
      bs.getDouble((4*8)+1);
      fail("expected IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException expected) {
    }
  }

  protected boolean isTestOffHeap() {
    return false;
  }
  
  @Test
  public void testHasArray() {
    ByteSource bs = createByteSource(new byte[]{1,2,3,4,5,6,7,8,9,0});
    assertEquals(!isTestOffHeap(), bs.hasArray());
  }

  @Test
  public void testArray() {
    byte[] bytes = new byte[]{1,2,3,4,5,6,7,8,9,0};
    ByteSource bs = createByteSource(bytes);
    if (bs.hasArray()) {
      assertEquals(bytes, bs.array());
      ByteBuffer bb = ByteBuffer.wrap(bytes);
      bb.position(1);
      bs = ByteSourceFactory.create(bb.slice());
      assertEquals(bytes, bs.array());
    } else {
      try {
        bs.array();
        fail("expected UnsupportedOperationException");
      } catch (UnsupportedOperationException expected) {
      }
    }
  }

  @Test
  public void testArrayOffset() {
    byte[] bytes = new byte[]{1,2,3,4,5,6,7,8,9,0};
    ByteSource bs = createByteSource(bytes);
    if (bs.hasArray()) {
      assertEquals(0, bs.arrayOffset());
      ByteBuffer bb = ByteBuffer.wrap(bytes);
      bb.position(1);
      bs = ByteSourceFactory.create(bb.slice());
      assertEquals(1, bs.arrayOffset());
    } else {
      try {
        bs.array();
        fail("expected UnsupportedOperationException");
      } catch (UnsupportedOperationException expected) {
      }
    }
  }

  @Test
  public void testSliceInt() {
    ByteSource bs = createByteSource(new byte[]{1,2,3,4,5,6,7,8,9,0});
    bs.position(1);
    try {
      bs.slice(10);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
    try {
      bs.slice(-1);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
    assertEquals(0, bs.slice(0).remaining());
    ByteSource slice = bs.slice(2);
    assertEquals(createByteSource(new byte[]{2,3}), slice);
    assertEquals(0, slice.position());
    assertEquals(2, slice.limit());
    slice.position(1);
    slice.limit(1);
    assertEquals(1, bs.position());
    assertEquals(10, bs.limit());
  }

  @Test
  public void testSliceIntInt() {
    ByteSource bs = createByteSource(new byte[]{1,2,3,4,5,6,7,8,9,0});
    try {
      bs.slice(-1,9);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
    try {
      bs.slice(0, 11);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
    assertEquals(bs, bs.slice(0, 10));
    ByteSource slice = bs.slice(1, 9);
    assertEquals(createByteSource(new byte[]{2,3,4,5,6,7,8,9}), slice);
  }

  @Test
  public void testSendToByteBuffer() {
    ByteSource bs = createByteSource(new byte[]{1,2,3,4,5,6,7,8,9,0});
    ByteBuffer bb = ByteBuffer.allocate(10);
    bs.sendTo(bb);
    assertEquals(0, bs.remaining());
    assertEquals(10, bb.position());
    bb.position(0);
    assertEquals(ByteBuffer.wrap(new byte[]{1,2,3,4,5,6,7,8,9,0}), bb);

    bs.position(1);
    bs.limit(9);
    bb = ByteBuffer.allocate(8);
    
    bs.sendTo(bb);
    assertEquals(0, bs.remaining());
    assertEquals(8, bb.position());
    bb.position(0);
    assertEquals(ByteBuffer.wrap(new byte[]{2,3,4,5,6,7,8,9}), bb);
  }

  @Test
  public void testSendToDataOutput() throws IOException {
    HeapDataOutputStream hdos = new HeapDataOutputStream((Version)null);
    ByteSource bs = createByteSource(new byte[]{1,2,3,4,5,6,7,8,9,0});
    bs.sendTo(hdos);
    assertEquals(0, bs.remaining());
    ByteBuffer bb = hdos.toByteBuffer();
    assertEquals(10, bb.limit());
    assertEquals(ByteBuffer.wrap(new byte[]{1,2,3,4,5,6,7,8,9,0}), bb);

    bs.position(1);
    bs.limit(9);
    hdos = new HeapDataOutputStream((Version)null);
    bs.sendTo(hdos);
    assertEquals(0, bs.remaining());
    bb = hdos.toByteBuffer();
    assertEquals(8, bb.limit());
    assertEquals(ByteBuffer.wrap(new byte[]{2,3,4,5,6,7,8,9}), bb);
  }

}
