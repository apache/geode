/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence.soplog;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.DSCODE;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedReader.SerializedComparator;
import com.gemstone.gemfire.internal.util.Bytes;

/**
 * Provides type-optimized comparisons for serialized objects.  All data is
 * assumed to have been serialized via a call to 
 * {@link DataSerializer#writeObject(Object, java.io.DataOutput) }.  The following
 * data types have optimized comparisons:
 * <ul>
 *  <li>boolean
 *  <li>byte
 *  <li>short
 *  <li>char
 *  <li>int
 *  <li>long
 *  <li>float
 *  <li>double
 *  <li>String (not {@link DSCODE#HUGE_STRING} or {@link DSCODE#HUGE_STRING_BYTES})
 * </ul>
 * Types that are not listed above fallback to deserialization and comparison
 * via the {@link Comparable} API.
 * <p>
 * Any numeric type may be compared against another numeric type (e.g. double
 * to int).
 * <p>
 * <strong>Any changes to the serialized format may cause version incompatibilities.
 * In addition, the comparison operations will need to be updated.</strong>
 * <p>
 * 
 * @author bakera
 */
public class LexicographicalComparator implements SerializedComparator {

  //////////////////////////////////////////////////////////////////////////////
  //
  // constants for any-to-any numeric comparisons 
  //
  //////////////////////////////////////////////////////////////////////////////

  private static final int BYTE_TO_BYTE     = DSCODE.BYTE   << 8 | DSCODE.BYTE;
  private static final int BYTE_TO_SHORT    = DSCODE.BYTE   << 8 | DSCODE.SHORT;
  private static final int BYTE_TO_INT      = DSCODE.BYTE   << 8 | DSCODE.INTEGER;
  private static final int BYTE_TO_LONG     = DSCODE.BYTE   << 8 | DSCODE.LONG;
  private static final int BYTE_TO_FLOAT    = DSCODE.BYTE   << 8 | DSCODE.FLOAT;
  private static final int BYTE_TO_DOUBLE   = DSCODE.BYTE   << 8 | DSCODE.DOUBLE;

  private static final int SHORT_TO_BYTE    = DSCODE.SHORT  << 8 | DSCODE.BYTE;
  private static final int SHORT_TO_SHORT   = DSCODE.SHORT  << 8 | DSCODE.SHORT;
  private static final int SHORT_TO_INT     = DSCODE.SHORT  << 8 | DSCODE.INTEGER;
  private static final int SHORT_TO_LONG    = DSCODE.SHORT  << 8 | DSCODE.LONG;
  private static final int SHORT_TO_FLOAT   = DSCODE.SHORT  << 8 | DSCODE.FLOAT;
  private static final int SHORT_TO_DOUBLE  = DSCODE.SHORT  << 8 | DSCODE.DOUBLE;

  private static final int LONG_TO_BYTE     = DSCODE.LONG   << 8 | DSCODE.BYTE;
  private static final int LONG_TO_SHORT    = DSCODE.LONG   << 8 | DSCODE.SHORT;
  private static final int LONG_TO_INT      = DSCODE.LONG   << 8 | DSCODE.INTEGER;
  private static final int LONG_TO_LONG     = DSCODE.LONG   << 8 | DSCODE.LONG;
  private static final int LONG_TO_FLOAT    = DSCODE.LONG   << 8 | DSCODE.FLOAT;
  private static final int LONG_TO_DOUBLE   = DSCODE.LONG   << 8 | DSCODE.DOUBLE;
  
  private static final int INT_TO_BYTE      = DSCODE.INTEGER<< 8 | DSCODE.BYTE;
  private static final int INT_TO_SHORT     = DSCODE.INTEGER<< 8 | DSCODE.SHORT;
  private static final int INT_TO_INT       = DSCODE.INTEGER<< 8 | DSCODE.INTEGER;
  private static final int INT_TO_LONG      = DSCODE.INTEGER<< 8 | DSCODE.LONG;
  private static final int INT_TO_FLOAT     = DSCODE.INTEGER<< 8 | DSCODE.FLOAT;
  private static final int INT_TO_DOUBLE    = DSCODE.INTEGER<< 8 | DSCODE.DOUBLE;

  private static final int FLOAT_TO_BYTE    = DSCODE.FLOAT  << 8 | DSCODE.BYTE;
  private static final int FLOAT_TO_SHORT   = DSCODE.FLOAT  << 8 | DSCODE.SHORT;
  private static final int FLOAT_TO_INT     = DSCODE.FLOAT  << 8 | DSCODE.INTEGER;
  private static final int FLOAT_TO_LONG    = DSCODE.FLOAT  << 8 | DSCODE.LONG;
  private static final int FLOAT_TO_FLOAT   = DSCODE.FLOAT  << 8 | DSCODE.FLOAT;
  private static final int FLOAT_TO_DOUBLE  = DSCODE.FLOAT  << 8 | DSCODE.DOUBLE;

  private static final int DOUBLE_TO_BYTE   = DSCODE.DOUBLE << 8 | DSCODE.BYTE;
  private static final int DOUBLE_TO_SHORT  = DSCODE.DOUBLE << 8 | DSCODE.SHORT;
  private static final int DOUBLE_TO_INT    = DSCODE.DOUBLE << 8 | DSCODE.INTEGER;
  private static final int DOUBLE_TO_LONG   = DSCODE.DOUBLE << 8 | DSCODE.LONG;
  private static final int DOUBLE_TO_FLOAT  = DSCODE.DOUBLE << 8 | DSCODE.FLOAT;
  private static final int DOUBLE_TO_DOUBLE = DSCODE.DOUBLE << 8 | DSCODE.DOUBLE;
  
  //////////////////////////////////////////////////////////////////////////////
  //
  // constants for any-to-any string comparisons 
  //
  //////////////////////////////////////////////////////////////////////////////

  private static final int STRING_TO_STRING             = DSCODE.STRING       << 8 | DSCODE.STRING;
  private static final int STRING_TO_STRING_BYTES       = DSCODE.STRING       << 8 | DSCODE.STRING_BYTES;
  private static final int STRING_BYTES_TO_STRING       = DSCODE.STRING_BYTES << 8 | DSCODE.STRING;
  private static final int STRING_BYTES_TO_STRING_BYTES = DSCODE.STRING_BYTES << 8 | DSCODE.STRING_BYTES;
  
  @Override
  public int compare(byte[] o1, byte[] o2) {
    return compare(o1, 0, o1.length, o2, 0, o2.length);
  }
  
  @Override
  public int compare(byte[] b1, int o1, int l1, byte[] b2, int o2, int l2) {
    byte type1 = b1[o1];
    byte type2 = b2[o2];

    // optimized comparisons
    if (isString(type1) && isString(type2)) {
      return compareAsString(type1, b1, o1, type2, b2, o2);
      
    } else if (isNumeric(type1) && isNumeric(type2)) {
      return compareAsNumeric(type1, b1, o1, type2, b2, o2);
      
    } else if (type1 == DSCODE.BOOLEAN && type2 == DSCODE.BOOLEAN) {
      return compareAsBoolean(getBoolean(b1, o1), getBoolean(b2, o2));

    } else if (type1 == DSCODE.CHARACTER && type2 == DSCODE.CHARACTER) {
      return compareAsChar(getChar(b1, o1), getChar(b2, o2));

    } else if (type1 == DSCODE.NULL || type2 == DSCODE.NULL) {
      // null check, assumes NULLs sort last
      return type1 == type2 ? 0 : type1 == DSCODE.NULL ? 1 : -1;
    }
    
    // fallback, will deserialize to Comparable 
    return compareAsObject(b1, o1, l1, b2, o2, l2);
  }

  private static boolean isNumeric(int type) {
    return type == DSCODE.BYTE 
        || type == DSCODE.SHORT 
        || type == DSCODE.INTEGER 
        || type == DSCODE.LONG 
        || type == DSCODE.FLOAT 
        || type == DSCODE.DOUBLE;
  }
  
  private static boolean isString(int type) {
    return type == DSCODE.STRING 
        || type == DSCODE.STRING_BYTES;
  }
  
  //////////////////////////////////////////////////////////////////////////////
  //
  // type comparisons
  //
  //////////////////////////////////////////////////////////////////////////////
  
  private static int compareAsString(byte type1, byte[] b1, int o1, byte type2, byte[] b2, int o2) {
    // TODO these comparisons do not provide true alphabetical collation 
    // support (for example upper case sort before lower case).  Need to use a
    // collation key instead of unicode ordinal number comparison
    switch (type1 << 8 | type2) {
    case STRING_TO_STRING:
      return compareAsStringOfUtf(b1, o1, b2, o2);
    
    case STRING_TO_STRING_BYTES:
      return -compareAsStringOfByteToUtf(b2, o2, b1, o1);
    
    case STRING_BYTES_TO_STRING:
      return compareAsStringOfByteToUtf(b1, o1, b2, o2);

    case STRING_BYTES_TO_STRING_BYTES:
      return compareAsStringOfByte(b1, o1, b2, o2);

    default:
      throw new ClassCastException(String.format("Incomparable types: %d %d", type1, type2));
    }
  }
  
  private static int compareAsNumeric(byte type1, byte[] b1, int o1, byte type2, byte[] b2, int o2) {
    switch (type1 << 8 | type2) {
    case BYTE_TO_BYTE:     return compareAsShort (getByte  (b1, o1), getByte  (b2, o2));
    case BYTE_TO_SHORT:    return compareAsShort (getByte  (b1, o1), getShort (b2, o2));
    case BYTE_TO_INT:      return compareAsInt   (getByte  (b1, o1), getInt   (b2, o2));
    case BYTE_TO_LONG:     return compareAsLong  (getByte  (b1, o1), getLong  (b2, o2));
    case BYTE_TO_FLOAT:    return compareAsFloat (getByte  (b1, o1), getFloat (b2, o2));
    case BYTE_TO_DOUBLE:   return compareAsDouble(getByte  (b1, o1), getDouble(b2, o2));

    case SHORT_TO_BYTE:    return compareAsShort (getShort (b1, o1), getByte  (b2, o2));
    case SHORT_TO_SHORT:   return compareAsShort (getShort (b1, o1), getShort (b2, o2));
    case SHORT_TO_INT:     return compareAsInt   (getShort (b1, o1), getInt   (b2, o2));
    case SHORT_TO_LONG:    return compareAsLong  (getShort (b1, o1), getLong  (b2, o2));
    case SHORT_TO_FLOAT:   return compareAsFloat (getShort (b1, o1), getFloat (b2, o2));
    case SHORT_TO_DOUBLE:  return compareAsDouble(getShort (b1, o1), getDouble(b2, o2));

    case INT_TO_BYTE:      return compareAsInt   (getInt   (b1, o1), getByte  (b2, o2));
    case INT_TO_SHORT:     return compareAsInt   (getInt   (b1, o1), getShort (b2, o2));
    case INT_TO_INT:       return compareAsInt   (getInt   (b1, o1), getInt   (b2, o2));
    case INT_TO_LONG:      return compareAsLong  (getInt   (b1, o1), getLong  (b2, o2));
    case INT_TO_FLOAT:     return compareAsFloat (getInt   (b1, o1), getFloat (b2, o2));
    case INT_TO_DOUBLE:    return compareAsDouble(getInt   (b1, o1), getDouble(b2, o2));

    case LONG_TO_BYTE:     return compareAsLong  (getLong  (b1, o1), getByte  (b2, o2));
    case LONG_TO_SHORT:    return compareAsLong  (getLong  (b1, o1), getShort (b2, o2));
    case LONG_TO_INT:      return compareAsLong  (getLong  (b1, o1), getInt   (b2, o2));
    case LONG_TO_LONG:     return compareAsLong  (getLong  (b1, o1), getLong  (b2, o2));
    case LONG_TO_FLOAT:    return compareAsDouble(getLong  (b1, o1), getFloat (b2, o2));
    case LONG_TO_DOUBLE:   return compareAsDouble(getLong  (b1, o1), getDouble(b2, o2));

    case FLOAT_TO_BYTE:    return compareAsFloat (getFloat (b1, o1), getByte  (b2, o2));
    case FLOAT_TO_SHORT:   return compareAsFloat (getFloat (b1, o1), getShort (b2, o2));
    case FLOAT_TO_INT:     return compareAsFloat (getFloat (b1, o1), getInt   (b2, o2));
    case FLOAT_TO_LONG:    return compareAsFloat (getFloat (b1, o1), getLong  (b2, o2));
    case FLOAT_TO_FLOAT:   return compareAsFloat (getFloat (b1, o1), getFloat (b2, o2));
    case FLOAT_TO_DOUBLE:  return compareAsDouble(getFloat (b1, o1), getDouble(b2, o2));

    case DOUBLE_TO_BYTE:   return compareAsDouble(getDouble(b1, o1), getByte  (b2, o2));
    case DOUBLE_TO_SHORT:  return compareAsDouble(getDouble(b1, o1), getShort (b2, o2));
    case DOUBLE_TO_INT:    return compareAsDouble(getDouble(b1, o1), getInt   (b2, o2));
    case DOUBLE_TO_LONG:   return compareAsDouble(getDouble(b1, o1), getLong  (b2, o2));
    case DOUBLE_TO_FLOAT:  return compareAsDouble(getDouble(b1, o1), getFloat (b2, o2));
    case DOUBLE_TO_DOUBLE: return compareAsDouble(getDouble(b1, o1), getDouble(b2, o2));
    
    default:
      throw new ClassCastException(String.format("Incomparable types: %d %d", type1, type2));
    }
  }
  
  private static int compareAsBoolean(boolean b1, boolean b2) {
    return (b1 == b2) ? 0 : (b1 ? 1 : -1);
  }
  
  private static int compareAsShort(short s1, short s2) {
    return s1 - s2;
  }
  
  private static int compareAsChar(char c1, char c2) {
    // TODO non-collating sort
    return c1 - c2;
  }
  
  private static int compareAsInt(long l1, long l2) {
    return (int) (l1 - l2);
  }
  
  private static int compareAsLong(long l1, long l2) {
    return (l1 < l2) ? -1 : ((l1 == l2) ? 0 : 1);
  }
  
  private static int compareAsFloat(float f1, float f2) {
    return Float.compare(f1, f2);
  }

  private static int compareAsDouble(double d1, double d2) {
    return Double.compare(d1, d2);
  }

  private static int compareAsStringOfByte(byte[] b1, int o1, byte[] b2, int o2) {
    int offset = 3;
    int l1 = Bytes.toUnsignedShort(b1[o1 + 1], b1[o1 + 2]);
    int l2 = Bytes.toUnsignedShort(b2[o1 + 1], b2[o1 + 2]);
    
    assert b1.length >= o1 + offset + l1;
    assert b2.length >= o2 + offset + l2;
    
    int end = o1 + offset + Math.min(l1, l2);
    for (int i = o1 + offset, j = o2 + offset; i < end; i++, j++) {
      int diff = b1[i] - b2[j];
      if (diff != 0) {
        return diff;
      }
    }
    return l1 - l2;
  }

  private static int compareAsStringOfUtf(byte[] b1, int o1, byte[] b2, int o2) {
    int offset = 3;
    int l1 = Bytes.toUnsignedShort(b1[o1 + 1], b1[o1 + 2]);
    int l2 = Bytes.toUnsignedShort(b2[o1 + 1], b2[o1 + 2]);

    assert b1.length >= o1 + offset + l1;
    assert b2.length >= o2 + offset + l2;
    
    int i = 0;
    int j = 0;
    while (i < l1 && j < l2) {
      final int idx = o1 + offset + i;
      final int ilen = getUtfLength(b1[idx]);
      final char c1 = getUtfChar(b1, idx, ilen);
      i += ilen;
      
      final int jdx = o2 + offset + j;
      final int jlen = getUtfLength(b2[jdx]);
      char c2 = getUtfChar(b2, jdx, jlen);
      j += jlen;
      
      int diff = compareAsChar(c1, c2);
      if (diff != 0) {
        return diff;
      }
    }
    return (l1 - i) - (l2 - j);
  }

  private static int compareAsStringOfByteToUtf(byte[] b1, int o1, byte[] b2, int o2) {
    int offset = 3;
    int l1 = Bytes.toUnsignedShort(b1[o1 + 1], b1[o1 + 2]);
    int l2 = Bytes.toUnsignedShort(b2[o1 + 1], b2[o1 + 2]);

    assert b1.length >= o1 + offset + l1;
    assert b2.length >= o2 + offset + l2;
    
    int i = 0;
    int j = 0;
    while (i < l1 && j < l2) {
      final int idx = o1 + offset + i;
      final char c1 = (char) b1[idx];
      i++;
      
      final int jdx = o2 + offset + j;
      final int jlen = getUtfLength(b2[jdx]);
      char c2 = getUtfChar(b2, jdx, jlen);
      j += jlen;
      
      int diff = compareAsChar(c1, c2);
      if (diff != 0) {
        return diff;
      }
    }
    return (l1 - i) - (l2 - j);
  }
  
  private static int compareAsObject(byte[] b1, int o1, int l1, byte[] b2, int o2, int l2) {
    DataInput in1 = new DataInputStream(new ByteArrayInputStream(b1, o1, l1));
    DataInput in2 = new DataInputStream(new ByteArrayInputStream(b2, o2, l2));
  
    try {
      Comparable<Object> obj1 = DataSerializer.readObject(in1);
      Comparable<Object> obj2 = DataSerializer.readObject(in2);
  
      return obj1.compareTo(obj2);
      
    } catch (Exception e) {
      throw (RuntimeException) new ClassCastException().initCause(e);
    }
  }
  
  //////////////////////////////////////////////////////////////////////////////
  //
  //
  // Get a char from modified UTF8, as defined by DataInput.readUTF().
  //
  //////////////////////////////////////////////////////////////////////////////

  private static int getUtfLength(byte b) {
    int c = b & 0xff;
    
    // 0xxxxxxx
    if (c < 0x80) {
      return 1;
    
    // 110xxxxx 10xxxxxx
    } else if (c < 0xe0) {
      return 2;
    } 

    // 1110xxxx 10xxxxxx 10xxxxxx
    return 3;
  }
  
  private static char getUtfChar(byte[] b, int off, int len) {
    assert b.length >= off + len;
    switch (len) {
    case 1: 
      return (char) b[off];
    case 2: 
      return getUtf2(b, off);
    case 3: 
    default:
      return getUtf3(b, off);
    }
  }

  private static char getUtf2(byte[] b, int off) {
    assert b.length >= off + 2;
    assert (b[off] & 0xff) >= 0xc0;
    assert (b[off] & 0xff) < 0xe0;
    assert (b[off + 1] & 0xff) >= 0x80;
    
    return (char) (((b[off] & 0x1f) << 6) | (b[off + 1] & 0x3f));
  }
  
  private static char getUtf3(byte[] b, int off) {
    assert b.length >= off + 3;
    assert (b[off]     & 0xff) >= 0xe0;
    assert (b[off + 1] & 0xff) >= 0x80;
    assert (b[off + 2] & 0xff) >= 0x80;
    
    return (char) (((b[off] & 0x0f) << 12) | ((b[off + 1] & 0x3f) << 6) | (b[off + 2] & 0x3f));
  }


  //////////////////////////////////////////////////////////////////////////////
  //
  // Get a serialized primitive from byte[];  b[0] is the DSCODE.
  //
  //////////////////////////////////////////////////////////////////////////////
  
  private static boolean getBoolean(byte[] b, int off) {
    assert b.length >= off + 2;
    return b[off + 1] != 0;  
  }
  
  private static byte getByte(byte[] b, int off) {
    assert b.length >= off + 2;
    return b[off + 1];
  }

  private static short getShort(byte[] b, int off) {
    assert b.length >= off + 3;
    return Bytes.toShort(b[off + 1], b[off + 2]);
  }
  
  private static char getChar(byte[] b, int off) {
    assert b.length >= off + 3;
    return Bytes.toChar(b[off + 1], b[off + 2]);
  }

  private static int getInt(byte[] b, int off) {
    assert b.length >= off + 5;
    return Bytes.toInt(b[off + 1], b[off + 2], b[off + 3], b[off + 4]);
  }

  private static long getLong(byte[] b, int off) {
    assert b.length >= off + 9;
    return Bytes.toLong(b[off + 1], b[off + 2], b[off + 3], b[off + 4], 
                        b[off + 5], b[off + 6], b[off + 7], b[off + 8]);
  }

  private static float getFloat(byte[] b, int off) {
    assert b.length >= off + 5;
    return Float.intBitsToFloat(getInt(b, off));
  }

  private static double getDouble(byte[] b, int off) {
    assert b.length >= off + 9;
    return Double.longBitsToDouble(getLong(b, off));
  }
}
