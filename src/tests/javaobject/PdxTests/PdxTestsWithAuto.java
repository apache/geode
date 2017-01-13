/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package PdxTests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.*;

import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;

public class PdxTestsWithAuto implements PdxSerializable{

  char m_char;
  boolean m_bool;
  byte m_byte;
  byte m_sbyte;
  short m_int16;
  short m_uint16;
  int m_int32;
  int m_uint32;
  long m_long;
  long m_ulong;
  float m_float;
  double m_double;
  
  String m_string;

  boolean[] m_boolArray;
  byte[] m_byteArray;
  byte[] m_sbyteArray;

  char[] m_charArray;

  Date m_dateTime;

  short[] m_int16Array;
  short[] m_uint16Array;

  int[] m_int32Array;
  int[] m_uint32Array;

  long[] m_longArray;
  long[] m_ulongArray;

  float[] m_floatArray;
  double[] m_doubleArray;

  byte[][] m_byteByteArray ;

  String[] m_stringArray ;

  //Address[] m_address;
/*  List<object> m_arraylist = new List<object>();
  IDictionary<object, object> m_map = new Dictionary<object, object>();    
  Hashtable m_hashtable = new Hashtable();
  ArrayList m_vector = new ArrayList();*/

  ArrayList<Object> m_arraylist = new ArrayList<Object>();
  Map<Object, Object> m_map = new HashMap<Object, Object>();
  Hashtable m_hashtable = new Hashtable();
  Vector m_vector = new Vector();
  
  /*CacheableHashSet m_chs = CacheableHashSet.Create();
  CacheableLinkedHashSet m_clhs = CacheableLinkedHashSet.Create();*/
  
  HashSet m_chs = new HashSet();
  LinkedHashSet m_clhs = new LinkedHashSet();
  
   byte[] m_byte252 = new byte[252];
   byte[] m_byte253 = new byte[253];
   byte[] m_byte65535 = new byte[65535];
   byte[] m_byte65536 = new byte[65536];
   pdxEnumTest m_pdxEnum = pdxEnumTest.pdx2;
   Object[] m_address;
  public PdxTestsWithAuto()
  {    
    init();
  }

  public void init()
  {
    m_char = 'C';
    m_bool = true;
    m_byte = (byte)0x74;
    m_sbyte = 0x67;
    m_int16 = 0xab;
    m_uint16 = (short)0x2dd5;
    m_int32 = 0x2345abdc;
    m_uint32 = 0x2a65c434;
    m_long = (long)324897980;
    m_ulong = (long)238749898;
    m_float = 23324.324f;
    m_double = 3243298498d;
    
    m_string = "gfestring";

    m_boolArray = new boolean[] { true, false, true };
    m_byteArray = new byte[] { 0x34, 0x64 };
    m_sbyteArray = new byte[] { 0x34, 0x64 };

    m_charArray = new char[] { 'c', 'v' };

    long ticksMillis = 1310447869154L;//from epoch
    m_dateTime = new Date(ticksMillis);
    //m_dateTime = new Date( System.currentTimeMillis());
    
    m_int16Array = new short[] { 0x2332, 0x4545 };
    m_uint16Array = new short[] { 0x3243, 0x3232 };

    m_int32Array = new int[] { 23, 676868, 34343, 2323 };
    m_uint32Array = new int[] { 435, 234324, 324324, 23432432 };

    m_longArray = new long[] { 324324L, 23434545L };
    m_ulongArray = new long[] { 3245435, 3425435 };

    m_floatArray = new float[] { 232.565f, 2343254.67f };
    m_doubleArray = new double[] { 23423432d, 4324235435d };

    m_byteByteArray = new byte[][]{new byte[] {0x23},
                                           new byte[]{0x34, 0x55}   
                                            };

    m_stringArray = new String[] { "one", "two" };

    m_arraylist = new ArrayList<Object>();
    m_arraylist.add(1);
    m_arraylist.add(2);


    m_map = new HashMap<Object, Object>();
    m_map.put(1, 1);
    m_map.put(2, 2);

    m_hashtable = new Hashtable();
    m_hashtable.put(1, "1111111111111111");
    m_hashtable.put(2, "2222222222221111111111111111");

    m_vector = new Vector();
    m_vector.add(1);
    m_vector.add(2);
    m_vector.add(3);
    
    m_chs.add(1);
    m_clhs.add(1);
    m_clhs.add(2);
    
    m_pdxEnum = pdxEnumTest.pdx2;
    
    //m_address = new Address[10];
    //for (int i = 0; i < 10; i++)
    //{
      //m_address[i] = new Address(i + 1, "street" + String.valueOf(i), "city" + String.valueOf(i));
    //}
    
     m_address = new Object[10];
    for (int i = 0; i < 10; i++)
    {
      m_address[i] = new Address(i + 1, "street" + String.valueOf(i), "city" + String.valueOf(i));
    }  
  }
  
  public void fromData(PdxReader reader)  {
  
	System.out.println("Avinash PdxType 1");
    // TODO Auto-generated method stub
    byte[][] baa = reader.readArrayOfByteArrays("m_byteByteArray");
    m_byteByteArray = compareByteByteArray(baa, m_byteByteArray);
    
    m_char = GenericValCompare(reader.readChar("m_char"), m_char);

    boolean bl = reader.readBoolean("m_bool");
    m_bool = GenericValCompare(bl, m_bool);
    m_boolArray = compareBoolArray(reader.readBooleanArray("m_boolArray"), m_boolArray);

    m_byte = GenericValCompare(reader.readByte("m_byte"), m_byte);
    m_byteArray = compareByteArray(reader.readByteArray("m_byteArray"), m_byteArray);
    m_charArray = compareCharArray(reader.readCharArray("m_charArray"), m_charArray);
    //List<Object> tmpl = new ArrayList<Object>();
    Collection tmpl = (Collection)reader.readObject("m_arraylist");
    m_arraylist = (ArrayList)compareCompareCollection(tmpl, m_arraylist);
    
    HashMap<Object, Object> tmpM = (HashMap<Object, Object>)reader.readObject("m_map");
    if(tmpM.size() != m_map.size())
      throw new IllegalStateException("Not got expected value for type: " + m_map.toString());

    Hashtable tmpH = (Hashtable)reader.readObject("m_hashtable");

    if(tmpH.size()!= m_hashtable.size())
      throw new IllegalStateException("Not got expected value for type: " + m_hashtable.toString());

    Vector vector = (Vector)reader.readObject("m_vector");

    if(vector.size()!= m_vector.size())
      throw new IllegalStateException("Not got expected value for type: " + m_vector.toString());
    
    HashSet rmpChs = (HashSet)reader.readObject("m_chs");

    if (rmpChs.size() != m_chs.size())
      throw new IllegalStateException("Not got expected value for type: " + m_chs.toString());

    LinkedHashSet rmpClhs = (LinkedHashSet)reader.readObject("m_clhs");

    if (rmpClhs.size()!= m_clhs.size())
      throw new IllegalStateException("Not got expected value for type: " + m_clhs.toString());

    
    m_string = GenericValCompare(reader.readString("m_string"), m_string);
    m_dateTime = compareData(reader.readDate("m_dateTime"), m_dateTime);

    m_double = GenericValCompare(reader.readDouble("m_double"), m_double);

    m_doubleArray = compareDoubleArray(reader.readDoubleArray("m_doubleArray"), m_doubleArray);
    m_float = GenericValCompare(reader.readFloat("m_float"), m_float);
    m_floatArray = compareFloatArray(reader.readFloatArray("m_floatArray"), m_floatArray);
    m_int16 = GenericValCompare(reader.readShort("m_int16"), m_int16);
    m_int32 = GenericValCompare(reader.readInt("m_int32"), m_int32);
    m_long = GenericValCompare(reader.readLong("m_long"), m_long);
    m_int32Array = compareIntArray(reader.readIntArray("m_int32Array"), m_int32Array);
    m_longArray = compareLongArray(reader.readLongArray("m_longArray"), m_longArray);
    m_int16Array = compareSHortArray(reader.readShortArray("m_int16Array"), m_int16Array);
    m_sbyte = GenericValCompare(reader.readByte("m_sbyte"), m_sbyte);
    m_sbyteArray = compareByteArray(reader.readByteArray("m_sbyteArray"), m_sbyteArray);
    m_stringArray = compareStringArray(reader.readStringArray("m_stringArray"), m_stringArray);
    m_uint16 = GenericValCompare(reader.readShort("m_uint16"), m_uint16);
    m_uint32 = GenericValCompare(reader.readInt("m_uint32"), m_uint32);
    m_ulong = GenericValCompare(reader.readLong("m_ulong"), m_ulong);
    m_uint32Array = compareIntArray(reader.readIntArray("m_uint32Array"), m_uint32Array);
    m_ulongArray = compareLongArray(reader.readLongArray("m_ulongArray"), m_ulongArray);
    m_uint16Array = compareSHortArray(reader.readShortArray("m_uint16Array"), m_uint16Array);      

    byte[] ret = reader.readByteArray("m_byte252");
      if (ret.length != 252)
        throw new IllegalStateException("Array len 252 not found");

      ret = reader.readByteArray("m_byte253");
      if (ret.length != 253)
        throw new IllegalStateException("Array len 253 not found");

      ret = reader.readByteArray("m_byte65535");
      if (ret.length != 65535)
        throw new IllegalStateException("Array len 65535 not found");

      ret = reader.readByteArray("m_byte65536");
      if (ret.length != 65536)
        throw new IllegalStateException("Array len 65536 not found");
     
    pdxEnumTest retEnum = (pdxEnumTest)reader.readObject("m_pdxEnum");
    if(retEnum != m_pdxEnum)
     throw new IllegalStateException("pdx enum is not equal");
     
     //Address[] otherA = (Address[])reader.readObject("m_address");
     //{
        //for (int i = 0; i < m_address.length; i++)
        //{
          //if (!m_address[i].equals(otherA[i]))
//            throw new IllegalStateException("Address array not matched " + i);           
  //      }
    // }
     
      Object[] retoa = reader.readObjectArray("m_address");
      for (int i = 0; i < m_address.length; i++)
      { 
        if(!m_address[i].equals(retoa[i]))
          throw new IllegalStateException("Object array not mateched " + i);
      }
  }

  public void toData(PdxWriter writer) {
  System.out.println("Avinash PdxType 2");
    // TODO Auto-generated method stub
    writer.writeArrayOfByteArrays("m_byteByteArray", m_byteByteArray);
    writer.writeChar("m_char", m_char);
    writer.writeBoolean("m_bool", m_bool);
    writer.writeBooleanArray("m_boolArray", m_boolArray);
    writer.writeByte("m_byte", m_byte);
    writer.writeByteArray("m_byteArray", m_byteArray);
    writer.writeCharArray("m_charArray", m_charArray);
    
    writer.writeObject("m_arraylist", m_arraylist);
    writer.writeObject("m_map", m_map);
    writer.writeObject("m_hashtable", m_hashtable);
    writer.writeObject("m_vector", m_vector);
    
    writer.writeObject("m_chs", m_chs);
    writer.writeObject("m_clhs", m_clhs);
    
    writer.writeString("m_string", m_string);
    writer.writeDate("m_dateTime", m_dateTime);
    writer.writeDouble("m_double", m_double);
    writer.writeDoubleArray("m_doubleArray", m_doubleArray);
    writer.writeFloat("m_float", m_float);
    writer.writeFloatArray("m_floatArray", m_floatArray);
    writer.writeShort("m_int16", m_int16);
    writer.writeInt("m_int32", m_int32);
    writer.writeLong("m_long", m_long);
    writer.writeIntArray("m_int32Array", m_int32Array);
    writer.writeLongArray("m_longArray", m_longArray);
    writer.writeShortArray("m_int16Array", m_int16Array);
    writer.writeByte("m_sbyte", m_sbyte);
    writer.writeByteArray("m_sbyteArray", m_sbyteArray);
    writer.writeStringArray("m_stringArray", m_stringArray);
    writer.writeShort("m_uint16", m_uint16);
    writer.writeInt("m_uint32", m_uint32);
    writer.writeLong("m_ulong", m_ulong);
    writer.writeIntArray("m_uint32Array", m_uint32Array);
    writer.writeLongArray("m_ulongArray", m_ulongArray);
    writer.writeShortArray("m_uint16Array", m_uint16Array);      
    
    writer.writeByteArray("m_byte252", m_byte252);
    writer.writeByteArray("m_byte253", m_byte253);
    writer.writeByteArray("m_byte65535", m_byte65535);
    writer.writeByteArray("m_byte65536", m_byte65536);
    writer.writeObject("m_pdxEnum", m_pdxEnum);
    //writer.writeObject("m_address", m_address);
    writer.writeObjectArray("m_address", m_address);
  }

  byte[][] compareByteByteArray(byte[][] baa, byte[][] baa2)
  {
    if (baa.length == baa2.length)
    { 
      int i = 0;
      while (i < baa.length)
      {
             i++;
      }
      if (i == baa2.length)
        return baa2;
    }

    throw new IllegalStateException("Not got expected value for type: " + baa2.toString());
  }
  
  
  
  <T> T GenericValCompare(T b, T b2)
  {
    if (b.equals( b2))
      return b;
    throw new IllegalStateException("Not got expected value for type: " + b2.toString());
  }
  
  boolean[] compareBoolArray(boolean[] a, boolean[] a2)
  {
    if (a.length == a2.length)
    {
      int i = 0;
      while (i < a.length)
      {
        if (a[i] != a2[i])
          break;
        else
          i++;
      }
      if (i == a2.length)
        return a2;
    }

    throw new IllegalStateException("Not got expected value for type: " + a2.toString());
  }
  
  byte[] compareByteArray(byte[] a, byte[] a2) 
  {
    if (a.length == a2.length)
    {
      int i = 0;
      while (i < a.length)
      {
        if (a[i] != a2[i])
          break;
        else
          i++;
      }
      if (i == a2.length)
        return a2;
    }

    throw new IllegalStateException("Not got expected value for type: " + a2.toString());
  }
  
  char[] compareCharArray(char[] a, char[] a2) 
  {
    if (a.length == a2.length)
    {
      int i = 0;
      while (i < a.length)
      {
        if (a[i] != a2[i])
          break;
        else
          i++;
      }
      if (i == a2.length)
        return a2;
    }

    throw new IllegalStateException("Not got expected value for type: " + a2.toString());
  }
  Collection compareCompareCollection(Collection c, Collection c2) 
  {
    Object[] a = c.toArray();
    Object[] a2 = c.toArray();
    if (a.length== a2.length)
    {
      int i = 0;
      while (i < a.length)
      {       
        if (!a[i].equals(a2[i]))
          break;
        else
          i++;
      }
      if (i == a2.length)
        return c2;
    }

    throw new IllegalStateException("Not got expected value for type: " + a2.toString());
  }
  Date compareData(Date b, Date b2) 
  {
    //TODO:
    System.out.println(b + " := " + b2 );
    if(b.equals(b2))
    //TODO:
    return b;
    throw new IllegalStateException("Not got expected value for type: " + b2.toString());
    
  }
  double[] compareDoubleArray(double[] a, double[] a2) 
  {
    if (a.length == a2.length)
    {
      int i = 0;
      while (i < a.length)
      {
        if (a[i] != a2[i])
          break;
        else
          i++;
      }
      if (i == a2.length)
        return a2;
    }

    throw new IllegalStateException("Not got expected value for type: " + a2.toString());
  }
  float[] compareFloatArray(float[] a, float[] a2) 
  {
    if (a.length == a2.length)
    {
      int i = 0;
      while (i < a.length)
      {
        if (a[i] != a2[i])
          break;
        else
          i++;
      }
      if (i == a2.length)
        return a2;
    }

    throw new IllegalStateException("Not got expected value for type: " + a2.toString());
  }
  int[] compareIntArray(int[] a, int[] a2) 
  {
    if (a.length == a2.length)
    {
      int i = 0;
      while (i < a.length)
      {
        if (a[i] != a2[i])
          break;
        else
          i++;
      }
      if (i == a2.length)
        return a2;
    }

    throw new IllegalStateException("Not got expected value for type: " + a2.toString());
  }
  long[] compareLongArray(long[] a, long[] a2) 
  {
    if (a.length == a2.length)
    {
      int i = 0;
      while (i < a.length)
      {
        if (a[i] != a2[i])
          break;
        else
          i++;
      }
      if (i == a2.length)
        return a2;
    }

    throw new IllegalStateException("Not got expected value for type: " + a2.toString());
  }
  short[] compareSHortArray(short[] a, short[] a2) 
  {
    if (a.length == a2.length)
    {
      int i = 0;
      while (i < a.length)
      {
        if (a[i] != a2[i])
          break;
        else
          i++;
      }
      if (i == a2.length)
        return a2;
    }

    throw new IllegalStateException("Not got expected value for type: " + a2.toString());
  }
  String[] compareStringArray(String[] a, String[] a2) 
  {
    if (a.length == a2.length)
    {
      int i = 0;
      while (i < a.length)
      {
        if (!a[i].equals(a2[i]))
          break;
        else
          i++;
      }
      if (i == a2.length)
        return a2;
    }

    throw new IllegalStateException("Not got expected value for type: " + a2.toString());
  }
}
