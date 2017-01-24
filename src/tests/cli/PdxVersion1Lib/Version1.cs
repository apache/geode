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

using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using GemStone.GemFire.Cache.Generic;
using GemStone.GemFire.Cache.Generic.Internal;

namespace PdxVersionTests
{

    public enum pdxEnumTest { pdx1, pdx2, pdx3 };
  public class PdxTypes1 : IPdxSerializable
  {
    static int m_diffInSameFields = 0;
    static bool m_useWeakHashMap;
    IPdxUnreadFields m_pdxUreadFields;

    int m_i1 = 34324;
    int m_i2 = 2144;
    int m_i3 = 4645734;
    int m_i4 = 73567;

    public PdxTypes1()
    {

    }
    public static void Reset(bool useWeakHashMap)
    {
      m_diffInSameFields = 0;
      m_useWeakHashMap = useWeakHashMap;
    }
    public static IPdxSerializable CreateDeserializable()
    {
      return new PdxTypes1();
    }

    public override int GetHashCode()
    {
      return base.GetHashCode();
    }

    public override string ToString()
    {
        return "PdxTypes1 version2 " + m_i1 + " :" + m_i2 + " :" + m_i3 + " :" + m_i4 + ":m_diffInSameFields:" + m_diffInSameFields;
    }
    public override bool Equals(object obj)
    {
      if (obj == null)
        return false;
      PdxTypes1 pap = obj as PdxTypes1;

      if (pap == null)
        return false;

      if (pap == this)
        return true;

      if (m_i1 + m_diffInSameFields <= pap.m_i1
         && m_i2 + m_diffInSameFields <= pap.m_i2
          && m_i3 + m_diffInSameFields <= pap.m_i3
           && m_i4 + m_diffInSameFields <= pap.m_i4)
        return true;

      return false;
    }

    #region IPdxSerializable Members

    public void FromData(IPdxReader reader)
    {
      if (!m_useWeakHashMap)
        m_pdxUreadFields = reader.ReadUnreadFields();
      m_i1 = reader.ReadInt("i1");
      m_i2 = reader.ReadInt("i2");
      m_i3 = reader.ReadInt("i3");
      m_i4 = reader.ReadInt("i4");

      bool hasField = reader.HasField("i7");

      if (hasField)
        throw new System.Exception("i7 is not an valid field");

      hasField = reader.HasField("i4");

      if (!hasField)
        throw new System.Exception("i4 is an valid field");
    }

    public void ToData(IPdxWriter writer)
    {
      if (!m_useWeakHashMap)
        writer.WriteUnreadFields(m_pdxUreadFields);
      writer.WriteInt("i1", m_i1 + 1);
      writer.MarkIdentityField("i1");
      writer.WriteInt("i2", m_i2 + 1);
      writer.WriteInt("i3", m_i3 + 1);
      writer.WriteInt("i4", m_i4 + 1);

      m_diffInSameFields++;
    }

    #endregion
  }

  public class PdxTypes2 : IPdxSerializable
  {
    static int m_diffInSameFields = 0;
    static bool m_useWeakHashMap;
    IPdxUnreadFields m_unreadFields;
    int m_i1 = 34324;
    int m_i2 = 2144;
    int m_i3 = 4645734;
    int m_i4 = 73567;


    public PdxTypes2()
    {

    }
    public static void Reset(bool useWeakhashMap)
    {
      m_diffInSameFields = 0;
      m_useWeakHashMap = useWeakhashMap;
    }
    public static IPdxSerializable CreateDeserializable()
    {
      return new PdxTypes2();
    }

    public override int GetHashCode()
    {
      return base.GetHashCode();
    }

    public override string ToString()
    {
      return m_i1 + " :" + m_i2 + " :" + m_i3 + " :" + m_i4 ;
    }
    public override bool Equals(object obj)
    {
      if (obj == null)
        return false;
      PdxTypes2 pap = obj as PdxTypes2;

      if (pap == null)
        return false;

      if (pap == this)
        return true;

      if (m_i1 + m_diffInSameFields <= pap.m_i1
         && m_i2 + m_diffInSameFields <= pap.m_i2
          && m_i3 + m_diffInSameFields <= pap.m_i3
           && m_i4 + m_diffInSameFields <= pap.m_i4)
        return true;

      return false;
    }

    #region IPdxSerializable Members

    public void FromData(IPdxReader reader)
    {
      if(!m_useWeakHashMap)
        m_unreadFields = reader.ReadUnreadFields();
      m_i1 = reader.ReadInt("i1");
      m_i2 = reader.ReadInt("i2");
      m_i3 = reader.ReadInt("i3");
      m_i4 = reader.ReadInt("i4");
    }

    public void ToData(IPdxWriter writer)
    {
      if (!m_useWeakHashMap)
        writer.WriteUnreadFields(m_unreadFields);
      writer.WriteInt("i1", m_i1 + 1);
      writer.WriteInt("i2", m_i2 + 1);
      writer.WriteInt("i3", m_i3 + 1);
      writer.WriteInt("i4", m_i4 + 1);
    }

    #endregion
  }

  public class PdxTypes3 : IPdxSerializable
  {
    static int m_diffInSameFields = 0;
    static int m_diffInExtraFields = 0;
    static bool m_useWeakHashMap;
    IPdxUnreadFields m_pdxUreadFields;

    int m_i1 = 1;
    int m_i2 = 21;
    string m_str1 = "common";
    int m_i3 = 31;
    int m_i4 = 41;
    int m_i5 = 0;
    string m_str2 = "0";
    

    public PdxTypes3()
    {

    }
    public static void Reset(bool useWeakHashMap)
    {
      m_diffInSameFields = 0;
      m_diffInExtraFields = 0;
      m_useWeakHashMap = useWeakHashMap;
    }
    public static IPdxSerializable CreateDeserializable()
    {
      return new PdxTypes3();
    }
    public override string ToString()
    {
      return m_i1 + " :" + m_i2 + " :" + m_i3 + " :" + m_i4 + " :" + m_i5 + " : " + (m_str1 == null ? "null" : m_str1) + " :" + (m_str2 == null ? "null" : m_str2);
    }
    public override int GetHashCode()
    {
      return base.GetHashCode();
    }
    public override bool Equals(object obj)
    {
      if (obj == null)
        return false;
      PdxTypes3 pap = obj as PdxTypes3;

      if (pap == null)
        return false;

      if (pap == this)
        return true;

      if (m_i1 + m_diffInSameFields <= pap.m_i1
         && m_i2 + m_diffInSameFields <= pap.m_i2
          && m_i3 + m_diffInSameFields <= pap.m_i3
           && m_i4 + m_diffInSameFields <= pap.m_i4
        && m_i5 + m_diffInExtraFields == pap.m_i5)
      {
        if (m_str1 == pap.m_str1)
        {
          if (string.Compare(m_str2, pap.m_str2) <= 0)
            return true;
        }
      }
       
      return false;
    }

    #region IPdxSerializable Members

    public void FromData(IPdxReader reader)
    {
      if (!m_useWeakHashMap)
        m_pdxUreadFields = reader.ReadUnreadFields();
      m_i1 = reader.ReadInt("i1");
      m_i2 = reader.ReadInt("i2");
      m_str1 = reader.ReadString("m_str1");
      m_i3 = reader.ReadInt("i3");
      m_i4 = reader.ReadInt("i4");
      m_i5 = reader.ReadInt("i5");
      string tmp = reader.ReadString("m_str2");

      if (tmp == null)
      {
        m_str2 = m_diffInExtraFields.ToString();
      }
      else
      {
        m_str2 = m_diffInExtraFields + m_str2;
      }
    }

    public void ToData(IPdxWriter writer)
    {
      if (!m_useWeakHashMap)
        writer.WriteUnreadFields(m_pdxUreadFields);
      writer.WriteInt("i1", m_i1 + 1);
      writer.WriteInt("i2", m_i2 + 1);
      writer.WriteString("m_str1", m_str1);
      writer.WriteInt("i3", m_i3 + 1);
      writer.WriteInt("i4", m_i4 + 1);
      writer.WriteInt("i5", m_i5 + 1);
      writer.WriteString("m_str2", m_str2);

      m_diffInSameFields++;
      m_diffInExtraFields++;
    }

    #endregion
  }

  #region Version two first

  public class PdxTypesR1 : IPdxSerializable
  {
    static int m_diffInSameFields = 0;
    static bool m_useWeakHashMap;
    IPdxUnreadFields m_pdxUreadFields;

    int m_i1 = 34324;
    int m_i2 = 2144;
    int m_i3 = 4645734;
    int m_i4 = 73567;

    public PdxTypesR1()
    {
    }

    public static void Reset(bool useWeakHashMap)
    {
      m_diffInSameFields = 0;
      m_useWeakHashMap = useWeakHashMap;
    }

    public static IPdxSerializable CreateDeserializable()
    {
      return new PdxTypesR1();
    }

    public override int GetHashCode()
    {
      return base.GetHashCode();
    }
    public override bool Equals(object obj)
    {
      if (obj == null)
        return false;
      PdxTypesR1 pap = obj as PdxTypesR1;

      if (pap == null)
        return false;

      if (pap == this)
        return true;

      if (m_i1 + m_diffInSameFields <= pap.m_i1
         && m_i2 + m_diffInSameFields <= pap.m_i2
          && m_i3 + m_diffInSameFields <= pap.m_i3
           && m_i4 + m_diffInSameFields <= pap.m_i4)
      {
          return true;
      }

      return false;
    }

    public override string ToString()
    {
      return m_i1 + " :" + m_i2 + " :" + m_i3 + " :" + m_i4 ;
    }

    #region IPdxSerializable Members

    public void FromData(IPdxReader reader)
    {
      if (!m_useWeakHashMap)
        m_pdxUreadFields = reader.ReadUnreadFields();
      m_i1 = reader.ReadInt("i1");
      m_i2 = reader.ReadInt("i2");
      m_i3 = reader.ReadInt("i3");
      m_i4 = reader.ReadInt("i4");
    }

    public void ToData(IPdxWriter writer)
    {
      if (!m_useWeakHashMap)
        writer.WriteUnreadFields(m_pdxUreadFields);
      writer.WriteInt("i1", m_i1 + 1);
      writer.WriteInt("i2", m_i2 + 1);
      writer.WriteInt("i3", m_i3 + 1);
      writer.WriteInt("i4", m_i4 + 1);

      m_diffInSameFields++;
    }

    #endregion
  }

  public class PdxTypesR2 : IPdxSerializable
  {
    static int m_diffInSameFields = 0;
    static bool m_useWeakHashMap;
    IPdxUnreadFields m_pdxUreadFields;

    int m_i1 = 34324;
    int m_i2 = 2144;
    int m_i3 = 4645734;
    int m_i4 = 73567;

    public PdxTypesR2()
    {

    }
    public static void Reset(bool useWeakHashMap)
    {
      m_diffInSameFields = 0;
      m_useWeakHashMap = useWeakHashMap;
    }
    public static IPdxSerializable CreateDeserializable()
    {
      return new PdxTypesR2();
    }

    public override int GetHashCode()
    {
      return base.GetHashCode();
    }

    public override string ToString()
    {
      return m_i1 + " :" + m_i2 + " :" + m_i3 + " :" + m_i4;
    }
    public override bool Equals(object obj)
    {
      if (obj == null)
        return false;
      PdxTypesR2 pap = obj as PdxTypesR2;

      if (pap == null)
        return false;

      if (pap == this)
        return true;

      if (m_i1 + m_diffInSameFields <= pap.m_i1
         && m_i2 + m_diffInSameFields <= pap.m_i2
          && m_i3 + m_diffInSameFields <= pap.m_i3
           && m_i4 + m_diffInSameFields <= pap.m_i4)
        return true;

      return false;
    }

    #region IPdxSerializable Members

    public void FromData(IPdxReader reader)
    {
      if (!m_useWeakHashMap)
        m_pdxUreadFields = reader.ReadUnreadFields();
      m_i1 = reader.ReadInt("i1");
      m_i2 = reader.ReadInt("i2");
      m_i3 = reader.ReadInt("i3");
      m_i4 = reader.ReadInt("i4");
    }

    public void ToData(IPdxWriter writer)
    {
      if (!m_useWeakHashMap)
        writer.WriteUnreadFields(m_pdxUreadFields);
      writer.WriteInt("i1", m_i1 + 1);
      writer.WriteInt("i2", m_i2 + 1);
      writer.WriteInt("i3", m_i3 + 1);
      writer.WriteInt("i4", m_i4 + 1);
    }

    #endregion
  }
  #endregion

  #region PdxIgnoreUnreadField test

  public class PdxTypesIgnoreUnreadFields : IPdxSerializable
  {
    static int m_diffInSameFields = 0;
    static bool m_useWeakHashMap;
    IPdxUnreadFields m_pdxUreadFields;

    int m_i1 = 34324;
    int m_i2 = 2144;
    int m_i3 = 4645734;
    int m_i4 = 73567;

    public PdxTypesIgnoreUnreadFields()
    {

    }
    public static void Reset(bool useWeakHashMap)
    {
      m_diffInSameFields = 0;
      m_useWeakHashMap = useWeakHashMap;
    }
    public static IPdxSerializable CreateDeserializable()
    {
      return new PdxTypesIgnoreUnreadFields();
    }

    public override int GetHashCode()
    {
      return base.GetHashCode();
    }

    public override string ToString()
    {
      return m_i1 + " :" + m_i2 + " :" + m_i3 + " :" + m_i4;
    }
    public override bool Equals(object obj)
    {
      if (obj == null)
        return false;
      PdxTypesIgnoreUnreadFields pap = obj as PdxTypesIgnoreUnreadFields;

      if (pap == null)
        return false;

      if (pap == this)
        return true;

      if (m_i1 + m_diffInSameFields <= pap.m_i1
         && m_i2 + m_diffInSameFields <= pap.m_i2
          && m_i3 + m_diffInSameFields <= pap.m_i3
           && m_i4 + m_diffInSameFields <= pap.m_i4)
        return true;

      return false;
    }

    #region IPdxSerializable Members

    public void FromData(IPdxReader reader)
    {
      if (!m_useWeakHashMap)
        m_pdxUreadFields = reader.ReadUnreadFields();
      m_i1 = reader.ReadInt("i1");
      m_i2 = reader.ReadInt("i2");
      m_i3 = reader.ReadInt("i3");
      m_i4 = reader.ReadInt("i4");

      bool hasField = reader.HasField("i7");

      if (hasField)
        throw new System.Exception("i7 is not an valid field");

      hasField = reader.HasField("i4");

      if (!hasField)
        throw new System.Exception("i4 is an valid field");
    }

    public void ToData(IPdxWriter writer)
    {
      if (!m_useWeakHashMap)
        writer.WriteUnreadFields(m_pdxUreadFields);
      writer.WriteInt("i1", m_i1 + 1);
      writer.MarkIdentityField("i1");
      writer.WriteInt("i2", m_i2 + 1);
      writer.WriteInt("i3", m_i3 + 1);
      writer.WriteInt("i4", m_i4 + 1);

      m_diffInSameFields++;
    }

    #endregion
  }
  #endregion
 /* [Serializable]
  public class PdxVersioned : IPdxSerializable
  {
    char m_char;
      
    bool m_bool;
    sbyte m_byte;
    sbyte m_sbyte;
    short m_int16;
    short m_uint16;
    Int32 m_int32;
    Int32 m_uint32;
    long m_long;
    Int64 m_ulong;
    float m_float;
    double m_double;

    string m_string;

    bool[] m_boolArray;
    sbyte[] m_byteArray;
    sbyte[] m_sbyteArray;

    char[] m_charArray;

    DateTime m_dateTime;

    Int16[] m_int16Array;
    Int16[] m_uint16Array;

    Int32[] m_int32Array;
    Int32[] m_uint32Array;

    long[] m_longArray;
    Int64[] m_ulongArray;

    float[] m_floatArray;
    double[] m_doubleArray;

    byte[][] m_byteByteArray;

    string[] m_stringArray;

    IDictionary<object, object> m_map;
    List<object> m_list;

    sbyte[] m_byte252;
    sbyte[] m_byte253;
    sbyte[] m_byte65535;
    sbyte[] m_byte65536;
      
    static Random _random = new Random();
    public static char GetLetter()
    {
        // This method returns a random lowercase letter.
        // ... Between 'a' and 'z' inclusize.
        int num = _random.Next(0, 26); // Zero to 25
        char let = (char)('a' + num);
        return let;
    }
    public void Init(int size)
    {

        m_char = GetLetter();
      //m_char = 'C';
        
      m_bool = true;
      m_byte = 0x74;
      m_sbyte = 0x67;
      m_int16 = 0xab;
      m_uint16 = 0x2dd5;
      m_int32 = 0x2345abdc;
      m_uint32 = 0x2a65c434;
      m_long = 324897980;
      m_ulong = 238749898;
      m_float = 23324.324f;
      m_double = 3243298498d;
      
      m_string = "gfestring";

      m_boolArray = new bool[] { true, false, true };
      m_byteArray = new sbyte[] { 0x34, 0x64 };
      m_sbyteArray = new sbyte[] { 0x34, 0x64 };

      m_charArray = new char[] { 'c', 'v' };

      long ticks = 837498372498L;
      m_dateTime = new DateTime(ticks);
      Console.WriteLine(m_dateTime.Ticks);
      
      m_int16Array = new short[size];
      m_uint16Array = new short[size];

      m_int32Array = new int[size];
      m_uint32Array = new int[size];

      m_longArray = new long[size];
      m_ulongArray = new Int64[size];

      m_floatArray = new float[size];
      m_doubleArray = new double[size];

      m_byteByteArray = new byte[][]{new byte[] {0x23},
                                             new byte[]{0x34, 0x55}   
                                              };

      m_stringArray = new string[size];

      m_map = new Dictionary<object, object>();
      m_list = new List<object>();
      for(int i=0;i<size;i++){
        m_int16Array[i] = 0x2332;
        m_uint16Array[i] = 0x3243;
        m_int32Array[i] = 34343+i;
        m_uint32Array[i] = 324324 + i;
        m_longArray[i] = 324324L + i;
        m_ulongArray[i] = 3245435 + i;
        m_floatArray[i] = 232.565f + i;
        m_doubleArray[i] = 23423432d + i;
        m_stringArray[i] = String.Format("one{0}", i);
        m_map.Add(i+1, 1+1);
        m_list.Add(i+1);
    }
    m_byte252 = new sbyte[252];
    m_byte253 = new sbyte[253];
    m_byte65535 = new sbyte[65535];
    m_byte65536 = new sbyte[65536];
     
    }
    
    public PdxVersioned(int size)
    {
      Init(size);
      Console.WriteLine("PdxVersioned 1");
    }
    
    public PdxVersioned()
    {
    }
    public static IPdxSerializable CreateDeserializable()
    {
      return new PdxVersioned();
    }
      public override string ToString()
      {
          return "PdxVersioned 1 :" + m_char.ToString();
      }
    public void FromData(IPdxReader reader)
    {

      //m_byteByteArray = reader.ReadArrayOfByteArrays("m_byteByteArray");
      m_char = reader.ReadChar("m_char");
      
      m_bool = reader.ReadBoolean("m_bool");
      m_boolArray = reader.ReadBooleanArray("m_boolArray");
      m_byte = reader.ReadByte("m_byte");
      m_byteArray = reader.ReadByteArray("m_byteArray");
      m_charArray = reader.ReadCharArray("m_charArray");
      //List<object> tmpl = new List<object>();
      //tmpl = (List<object>)reader.ReadObject("m_list");
      //m_list = compareCompareCollection(tmpl, m_list);
      m_string = reader.ReadString("m_string");

      m_dateTime = reader.ReadDate("m_dateTime");

      m_double = reader.ReadDouble("m_double");

      m_doubleArray = reader.ReadDoubleArray("m_doubleArray");
      m_float = reader.ReadFloat("m_float");
      m_floatArray = reader.ReadFloatArray("m_floatArray");
      m_int16 = reader.ReadShort("m_int16");
      m_int32 = reader.ReadInt("m_int32");
      m_long = reader.ReadLong("m_long");
      m_int32Array = reader.ReadIntArray("m_int32Array");
      m_longArray = reader.ReadLongArray("m_longArray");
      m_int16Array = reader.ReadShortArray("m_int16Array");
      m_sbyte = reader.ReadByte("m_sbyte");
      m_sbyteArray = reader.ReadByteArray("m_sbyteArray");
      m_stringArray = reader.ReadStringArray("m_stringArray");
      m_uint16 = reader.ReadShort("m_uint16");
      m_uint32 = reader.ReadInt("m_uint32");
      m_ulong = reader.ReadLong("m_ulong");
      m_uint32Array = reader.ReadIntArray("m_uint32Array");
      m_ulongArray = reader.ReadLongArray("m_ulongArray");
      m_uint16Array = reader.ReadShortArray("m_uint16Array");

      sbyte[] ret = reader.ReadByteArray("m_byte252");
      if (ret.Length != 252)
        throw new Exception("Array len 252 not found");

      ret = reader.ReadByteArray("m_byte253");
      if (ret.Length != 253)
        throw new Exception("Array len 253 not found");

      ret = reader.ReadByteArray("m_byte65535");
      if (ret.Length != 65535)
        throw new Exception("Array len 65535 not found");

      ret = reader.ReadByteArray("m_byte65536");
      if (ret.Length != 65536)
        throw new Exception("Array len 65536 not found");
      
    }

    public void ToData(IPdxWriter writer)
    {
      //writer.WriteArrayOfByteArrays("m_byteByteArray", m_byteByteArray);
      writer.WriteChar("m_char", m_char);
      writer.WriteBoolean("m_bool", m_bool);
      writer.WriteBooleanArray("m_boolArray", m_boolArray);
      writer.WriteByte("m_byte", m_byte);
      writer.WriteByteArray("m_byteArray", m_byteArray);
      writer.WriteCharArray("m_charArray", m_charArray);
      //writer.WriteCollection("m_list", m_list);
      //writer.WriteObject("m_list", m_list);
      writer.WriteString("m_string", m_string);
      writer.WriteDate("m_dateTime", m_dateTime);
      writer.WriteDouble("m_double", m_double);
      writer.WriteDoubleArray("m_doubleArray", m_doubleArray);
      writer.WriteFloat("m_float", m_float);
      writer.WriteFloatArray("m_floatArray", m_floatArray);
      writer.WriteShort("m_int16", m_int16);
      writer.WriteInt("m_int32", m_int32);
      writer.WriteLong("m_long", m_long);
      writer.WriteIntArray("m_int32Array", m_int32Array);
      writer.WriteLongArray("m_longArray", m_longArray);
      writer.WriteShortArray("m_int16Array", m_int16Array);
      writer.WriteByte("m_sbyte", m_sbyte);
      writer.WriteByteArray("m_sbyteArray", m_sbyteArray);
      writer.WriteStringArray("m_stringArray", m_stringArray);
      writer.WriteShort("m_uint16", m_uint16);
      writer.WriteInt("m_uint32", m_uint32);
      writer.WriteLong("m_ulong", m_ulong);
      writer.WriteIntArray("m_uint32Array", m_uint32Array);
      writer.WriteLongArray("m_ulongArray", m_ulongArray);
      writer.WriteShortArray("m_uint16Array", m_uint16Array);
      writer.WriteByteArray("m_byte252", m_byte252);
      writer.WriteByteArray("m_byte253", m_byte253);
      writer.WriteByteArray("m_byte65535", m_byte65535);
      writer.WriteByteArray("m_byte65536", m_byte65536);
      
    }
    public override int GetHashCode()
    {
        return base.GetHashCode();
    }

    public override bool Equals(object obj)
    {
        Console.WriteLine("PdxVersioned 1 Equals called");
        if (obj == null)
            return false;
        PdxVersioned pap = obj as PdxVersioned;

        if (pap == null)
            return false;

        if (pap == this)
            return true;
        if (m_char.Equals(pap.m_char))
            return true;
        
        return false;
    }
  }*/

  public class TestKey
  {
    private string _id;

    public TestKey() { }
    public TestKey(string id) { _id = id; }

    public void FromData(IPdxReader reader)
    {
      _id = reader.ReadString("id");
    }

    public void ToData(IPdxWriter writer)
    {
      writer.WriteString("id", _id);
      writer.MarkIdentityField("id");
    }
    public override bool Equals(object obj)
    {
      if (obj == null)
        return false;
      TestKey tk = obj as TestKey;
      if (tk == null)
        return false;

      bool ret = _id.Equals(tk._id);
      Console.WriteLine("equals " + ret);
      return ret;
    }
    public override int GetHashCode()
    {
      int ret = _id.GetHashCode();
      Console.WriteLine("gethashcode" + ret);
      return ret;
    }
  }

  public class TestDiffTypePdxS : IPdxSerializer
  {
    private string _id;
    private string _name;

    public TestDiffTypePdxS(bool init)
    {
      if (init)
      {
        _id = "id:100";
        _name = "HK";
      }
    }

    public static IPdxSerializer Create()
    {
      return new TestDiffTypePdxS(false);
    }

    public override string ToString()
    {
      return _id + " : " + _name;
    }
    #region IPdxSerializable Members

    public void FromData(IPdxReader reader)
    {
      _id = reader.ReadString("id");
      _name = reader.ReadString("name");
    }

    public void ToData(IPdxWriter writer)
    {
      writer.WriteString("id", _id);
      writer.MarkIdentityField("id");
      writer.WriteString("name", _name);
    }

    #endregion

    public override bool Equals(object obj)
    {
      if (obj == null)
        return false;

      TestDiffTypePdxS other = obj as TestDiffTypePdxS;
      if (other == null)
        return false;

      if (other._id == _id && other._name == _name)
        return true;
      return false;
    }

    //public override int GetHashCode()
    //{
    //  return _id.GetHashCode();
    //}
    #region IPdxSerializer Members

    public object FromData(string classname, IPdxReader reader)
    {
      if (classname.IndexOf("TestDiffTypePdxS") >= 0)
      {
        TestDiffTypePdxS ret = (TestDiffTypePdxS)TestDiffTypePdxS.Create();
        ret.FromData(reader);
        return ret;
      }
      else if (classname.IndexOf("TestKey") >= 0)
      {
        TestKey tk = new TestKey();
        tk.FromData(reader);
        return tk;
      }
      throw new IllegalStateException("In TestDiffTypePdxS.FromData serializer class not found " + classname);

    }

    public bool ToData(object o, IPdxWriter writer)
    {
      string classname = o.GetType().FullName;
      if (classname.IndexOf("TestDiffTypePdxS") >= 0)
      {
        ((TestDiffTypePdxS)o).ToData(writer);
        return true;
      }
      else if (classname.IndexOf("TestKey") >= 0)
      {
        TestKey tk = (TestKey)o;
        tk.ToData(writer);
        return true;
      }
      throw new IllegalStateException("In TestDiffTypePdxS.ToData serializer class not found " + classname);
    }

    #endregion
  }

  public class TestEquals : IPdxSerializable
  {
    int i1 = 1;
    int i2 = 0;
    string s1 = "s1";
    string[] sArr = new string[]{"sa1" , "sa2"};
    int[] intArr = new int[] { 1, 2, 3 };


    public static IPdxSerializable CreateDeserializable()
    {
      return new TestEquals();
    }
    #region IPdxSerializable Members

    public void FromData(IPdxReader reader)
    {
      i1 = reader.ReadInt("i1");
      i2 = reader.ReadInt("i2");
      s1 = reader.ReadString("s1");
      sArr = reader.ReadStringArray("sArr");
      intArr = reader.ReadIntArray("intArr");
      intArr = (int[])reader.ReadObject("intArrObject");
      sArr = (string[])reader.ReadObject("sArrObject");
    }

    public void ToData(IPdxWriter writer)
    {
      writer.WriteInt("i1", i1);
      writer.WriteInt("i2", i2);
      writer.WriteString("s1", s1);
      writer.WriteStringArray("sArr", sArr);
      writer.WriteIntArray("intArr", intArr);
      writer.WriteObject("intArrObject", intArr);
      writer.WriteObject("sArrObject", sArr);
    }

    #endregion
  }
    [Serializable]
    public class PdxVersioned : IPdxSerializable
    {
        char m_char;
        sbyte m_byte;
        sbyte m_sbyte;
        short m_int16;
        short m_uint16;
        Int32 m_int32;
        //Int32 m_uint32;
        //long m_long;
        Int64 m_ulong;
        float m_float;
        double m_double;

        string m_string;

        bool[] m_boolArray;
        byte[] m_byteArray;
        byte[] m_sbyteArray;

        char[] m_charArray;

        DateTime m_dateTime;

        Int16[] m_int16Array;
        Int16[] m_uint16Array;

        Int32[] m_int32Array;
        Int32[] m_uint32Array;

        long[] m_longArray;
        Int64[] m_ulongArray;

        float[] m_floatArray;
        double[] m_doubleArray;

        byte[][] m_byteByteArray;

        string[] m_stringArray;

        List<object> m_arraylist = new List<object>();
        IDictionary<object, object> m_map = new Dictionary<object, object>();
        
        byte[] m_byte252 = new byte[252];
        byte[] m_byte253 = new byte[253];
        byte[] m_byte65535 = new byte[65535];
        byte[] m_byte65536 = new byte[65536];
        pdxEnumTest m_pdxEnum = pdxEnumTest.pdx2;
        static Random _random = new Random();
        public static char GetLetter()
        {
            // This method returns a random lowercase letter.
            // ... Between 'a' and 'z' inclusize.
            int num = _random.Next(0, 26); // Zero to 25
            char let = (char)('A' + num);
            return let;
        }
        
        public PdxVersioned()
        {
            Init("Version 1");
        }
        public PdxVersioned(string setString)
        {
            Init(setString);
        }
        public void Init(string setString)
        {
            m_char = 'C';
            m_byte = 0x74;
            m_sbyte = 0x67;
            m_int16 = 0xab;
            m_uint16 = 0x2dd5;
            m_int32 = 0x2345abdc;
            //m_uint32 = 0x2a65c434;
            //m_long = 324897980;
            m_ulong = 238749898;
            m_float = 23324.324f;
            m_double = 3243298498d;

            m_string = "PdxVersioned " + setString;

            m_boolArray = new bool[] { true, false, true };
            m_byteArray = new byte[] { 0x34, 0x64 };
            m_sbyteArray = new byte[] { 0x34, 0x64 };

            m_charArray = new char[] { 'c', 'v' };

            DateTime n = new DateTime((62135596800000/*epoch*/ + 1310447869154) * 10000, DateTimeKind.Utc);
            m_dateTime = n.ToLocalTime();

            Console.WriteLine(m_dateTime.Ticks);

            m_int16Array = new short[] { 0x2332, 0x4545 };
            m_uint16Array = new short[] { 0x3243, 0x3232 };

            m_int32Array = new int[] { 23, 676868, 34343, 2323 };
            m_uint32Array = new int[] { 435, 234324, 324324, 23432432 };

            m_longArray = new long[] { 324324L, 23434545L };
            m_ulongArray = new Int64[] { 3245435, 3425435 };

            m_floatArray = new float[] { 232.565f, 2343254.67f };
            m_doubleArray = new double[] { 23423432d, 4324235435d };

            m_byteByteArray = new byte[][]{new byte[] {0x23},
                                             new byte[]{0x34, 0x55}   
                                              };

            m_stringArray = new string[] { "one", "two" };

            m_arraylist = new List<object>();
            m_arraylist.Add(1);
            m_arraylist.Add(2);


            m_map = new Dictionary<object, object>();
            m_map.Add(1, 1);
            m_map.Add(2, 2);



            m_pdxEnum = pdxEnumTest.pdx2;
        }

        public static IPdxSerializable CreateDeserializable()
        {
            return new PdxVersioned();
        }



        #region IPdxSerializable Members
        public static byte[][] compareByteByteArray(byte[][] baa, byte[][] baa2)
        {
            if (baa.Length == baa2.Length)
            {
                int i = 0;
                while (i < baa.Length)
                {
                    compareByteArray(baa[i], baa2[i]);
                    i++;
                }
                if (i == baa2.Length)
                    return baa2;
            }

            throw new IllegalStateException("Not got expected value for type: " + baa2.GetType().ToString());
        }
        bool compareBool(bool b, bool b2)
        {
            if (b == b2)
                return b;
            throw new IllegalStateException("Not got expected value for type: " + b2.GetType().ToString());
        }
        bool[] compareBoolArray(bool[] a, bool[] a2)
        {
            if (a.Length == a2.Length)
            {
                int i = 0;
                while (i < a.Length)
                {
                    if (a[i] != a2[i])
                        break;
                    else
                        i++;
                }
                if (i == a2.Length)
                    return a2;
            }

            throw new IllegalStateException("Not got expected value for type: " + a2.GetType().ToString());
        }
        byte compareByte(byte b, byte b2)
        {
            if (b == b2)
                return b;
            throw new IllegalStateException("Not got expected value for type: " + b2.GetType().ToString());
        }
        public static byte[] compareByteArray(byte[] a, byte[] a2)
        {
            Console.WriteLine("Compare byte array " + a.Length + " ; " + a2.Length);
            if (a.Length == a2.Length)
            {
                int i = 0;
                while (i < a.Length)
                {
                    Console.WriteLine("Compare byte array " + a[i] + " : " + a2[i]);
                    if (a[i] != a2[i])
                        break;
                    else
                        i++;
                }
                if (i == a2.Length)
                    return a2;
            }

            throw new IllegalStateException("Not got expected value for type: " + a2.GetType().ToString());
        }
        char[] compareCharArray(char[] a, char[] a2)
        {
            if (a.Length == a2.Length)
            {
                int i = 0;
                while (i < a.Length)
                {
                    if (a[i] != a2[i])
                        break;
                    else
                        i++;
                }
                if (i == a2.Length)
                    return a2;
            }

            throw new IllegalStateException("Not got expected value for type: " + a2.GetType().ToString());
        }
        public static List<object> compareCompareCollection(List<object> a, List<object> a2)
        {
            if (a.Count == a2.Count)
            {
                int i = 0;
                while (i < a.Count)
                {
                    if (!a[i].Equals(a2[i]))
                        break;
                    else
                        i++;
                }
                if (i == a2.Count)
                    return a2;
            }

            throw new IllegalStateException("Not got expected value for type: " + a2.GetType().ToString());
        }
        public static DateTime compareData(DateTime b, DateTime b2)
        {
            Console.WriteLine("date " + b.Ticks + " : " + b2.Ticks);
            //TODO: 
            // return b;
            if ((b.Ticks / 10000L) == (b2.Ticks / 10000L))
                return b;
            throw new IllegalStateException("Not got expected value for type: " + b2.GetType().ToString());
        }
        Double compareDouble(Double b, Double b2)
        {
            if (b == b2)
                return b;
            throw new IllegalStateException("Not got expected value for type: " + b2.GetType().ToString());
        }
        double[] compareDoubleArray(double[] a, double[] a2)
        {
            if (a.Length == a2.Length)
            {
                int i = 0;
                while (i < a.Length)
                {
                    if (a[i] != a2[i])
                        break;
                    else
                        i++;
                }
                if (i == a2.Length)
                    return a2;
            }

            throw new IllegalStateException("Not got expected value for type: " + a2.GetType().ToString());
        }
        float compareFloat(float b, float b2)
        {
            if (b == b2)
                return b;
            throw new IllegalStateException("Not got expected value for type: " + b2.GetType().ToString());
        }
        float[] compareFloatArray(float[] a, float[] a2)
        {
            if (a.Length == a2.Length)
            {
                int i = 0;
                while (i < a.Length)
                {
                    if (a[i] != a2[i])
                        break;
                    else
                        i++;
                }
                if (i == a2.Length)
                    return a2;
            }

            throw new IllegalStateException("Not got expected value for type: " + a2.GetType().ToString());
        }
        Int16 compareInt16(Int16 b, Int16 b2)
        {
            if (b == b2)
                return b;
            throw new IllegalStateException("Not got expected value for type: " + b2.GetType().ToString());
        }
        Int32 compareInt32(Int32 b, Int32 b2)
        {
            if (b == b2)
                return b;
            throw new IllegalStateException("Not got expected value for type: " + b2.GetType().ToString());
        }
        Int64 compareInt64(Int64 b, Int64 b2)
        {
            if (b == b2)
                return b;
            throw new IllegalStateException("Not got expected value for type: " + b2.GetType().ToString());
        }
        Int32[] compareIntArray(Int32[] a, Int32[] a2)
        {
            if (a.Length == a2.Length)
            {
                int i = 0;
                while (i < a.Length)
                {
                    if (a[i] != a2[i])
                        break;
                    else
                        i++;
                }
                if (i == a2.Length)
                    return a2;
            }

            throw new IllegalStateException("Not got expected value for type: " + a2.GetType().ToString());
        }
        long[] compareLongArray(long[] a, long[] a2)
        {
            if (a.Length == a2.Length)
            {
                int i = 0;
                while (i < a.Length)
                {
                    if (a[i] != a2[i])
                        break;
                    else
                        i++;
                }
                if (i == a2.Length)
                    return a2;
            }

            throw new IllegalStateException("Not got expected value for type: " + a2.GetType().ToString());
        }
        Int16[] compareSHortArray(Int16[] a, Int16[] a2)
        {
            if (a.Length == a2.Length)
            {
                int i = 0;
                while (i < a.Length)
                {
                    if (a[i] != a2[i])
                        break;
                    else
                        i++;
                }
                if (i == a2.Length)
                    return a2;
            }

            throw new IllegalStateException("Not got expected value for type: " + a2.GetType().ToString());
        }
        sbyte compareSByte(sbyte b, sbyte b2)
        {
            if (b == b2)
                return b;
            throw new IllegalStateException("Not got expected value for type: " + b2.GetType().ToString());
        }
        sbyte[] compareSByteArray(sbyte[] a, sbyte[] a2)
        {
            if (a.Length == a2.Length)
            {
                int i = 0;
                while (i < a.Length)
                {
                    if (a[i] != a2[i])
                        break;
                    else
                        i++;
                }
                if (i == a2.Length)
                    return a2;
            }

            throw new IllegalStateException("Not got expected value for type: " + a2.GetType().ToString());
        }
        string[] compareStringArray(string[] a, string[] a2)
        {
            if (a.Length == a2.Length)
            {
                int i = 0;
                while (i < a.Length)
                {
                    if (a[i] != a2[i])
                        break;
                    else
                        i++;
                }
                if (i == a2.Length)
                    return a2;
            }

            throw new IllegalStateException("Not got expected value for type: " + a2.GetType().ToString());
        }
        UInt16 compareUInt16(UInt16 b, UInt16 b2)
        {
            if (b == b2)
                return b;
            throw new IllegalStateException("Not got expected value for type: " + b2.GetType().ToString());
        }
        UInt32 compareUInt32(UInt32 b, UInt32 b2)
        {
            if (b == b2)
                return b;
            throw new IllegalStateException("Not got expected value for type: " + b2.GetType().ToString());
        }
        UInt64 compareUint64(UInt64 b, UInt64 b2)
        {
            if (b == b2)
                return b;
            throw new IllegalStateException("Not got expected value for type: " + b2.GetType().ToString());
        }
        UInt32[] compareUnsignedIntArray(UInt32[] a, UInt32[] a2)
        {
            if (a.Length == a2.Length)
            {
                int i = 0;
                while (i < a.Length)
                {
                    if (a[i] != a2[i])
                        break;
                    else
                        i++;
                }
                if (i == a2.Length)
                    return a2;
            }

            throw new IllegalStateException("Not got expected value for type: " + a2.GetType().ToString());
        }
        UInt64[] compareUnsignedLongArray(UInt64[] a, UInt64[] a2)
        {
            if (a.Length == a2.Length)
            {
                int i = 0;
                while (i < a.Length)
                {
                    if (a[i] != a2[i])
                        break;
                    else
                        i++;
                }
                if (i == a2.Length)
                    return a2;
            }

            throw new IllegalStateException("Not got expected value for type: " + a2.GetType().ToString());
        }
        UInt16[] compareUnsignedShortArray(UInt16[] a, UInt16[] a2)
        {
            if (a.Length == a2.Length)
            {
                int i = 0;
                while (i < a.Length)
                {
                    if (a[i] != a2[i])
                        break;
                    else
                        i++;
                }
                if (i == a2.Length)
                    return a2;
            }

            throw new IllegalStateException("Not got expected value for type: " + a2.GetType().ToString());
        }

        public static T[] GenericCompare<T>(T[] a, T[] a2)
        {
            if (a.Length == a2.Length)
            {
                int i = 0;
                while (i < a.Length)
                {
                    if (!a[i].Equals(a2[i]))
                        break;
                    else
                        i++;
                }
                if (i == a2.Length)
                    return a2;
            }

            throw new IllegalStateException("Not got expected value for type: " + a2.GetType().ToString());
        }


        public static T GenericValCompare<T>(T b, T b2)
        {
            if (b.Equals(b2))
                return b;
            throw new IllegalStateException("Not got expected value for type: " + b2.GetType().ToString() + " : values " + b.ToString() + ": " + b2.ToString());
        }
        public override bool Equals(object obj)
        {
            if (obj == null)
                return false;

            PdxVersioned other = obj as PdxVersioned;
            if (other == null)
                return false;

            if (other == this)
                return true;

            compareByteByteArray(other.m_byteByteArray, m_byteByteArray);
            GenericValCompare(m_char, other.m_char);

            GenericCompare(other.m_boolArray, m_boolArray);

            GenericValCompare(other.m_byte, m_byte);
            GenericCompare(other.m_byteArray, m_byteArray);
            GenericCompare(other.m_charArray, m_charArray);

            compareCompareCollection(other.m_arraylist, m_arraylist);

            if (other.m_map.Count != m_map.Count)
                throw new IllegalStateException("Not got expected value for type: " + m_map.GetType().ToString());

            
            GenericValCompare(other.m_string, m_string);

            compareData(other.m_dateTime, m_dateTime);

            GenericValCompare(other.m_double, m_double);

            GenericCompare(other.m_doubleArray, m_doubleArray);
            GenericValCompare(other.m_float, m_float);
            GenericCompare(other.m_floatArray, m_floatArray);
            GenericValCompare(other.m_int16, m_int16);
            GenericValCompare(other.m_int32, m_int32);
            //GenericValCompare(other.m_long, m_long);
            GenericCompare(other.m_int32Array, m_int32Array);
            GenericCompare(other.m_longArray, m_longArray);
            GenericCompare(other.m_int16Array, m_int16Array);
            GenericValCompare(other.m_sbyte, m_sbyte);
            GenericCompare(other.m_sbyteArray, m_sbyteArray);
            GenericCompare(other.m_stringArray, m_stringArray);
            GenericValCompare(other.m_uint16, m_uint16);
            //GenericValCompare(other.m_uint32, m_uint32);
            GenericValCompare(other.m_ulong, m_ulong);
            GenericCompare(other.m_uint32Array, m_uint32Array);
            GenericCompare(other.m_ulongArray, m_ulongArray);
            GenericCompare(other.m_uint16Array, m_uint16Array);

            if (m_byte252.Length != 252 && other.m_byte252.Length != 252)
                throw new Exception("Array len 252 not found");

            if (m_byte253.Length != 253 && other.m_byte253.Length != 253)
                throw new Exception("Array len 253 not found");

            if (m_byte65535.Length != 65535 && other.m_byte65535.Length != 65535)
                throw new Exception("Array len 65535 not found");

            if (m_byte65536.Length != 65536 && other.m_byte65536.Length != 65536)
                throw new Exception("Array len 65536 not found");
            if (m_pdxEnum != other.m_pdxEnum)
                throw new Exception("pdx enum is not equal");
            return true;
        }
        public void FromData(IPdxReader reader)
        {
            //byte[][] baa = reader.ReadArrayOfByteArrays("m_byteByteArray");
            //m_byteByteArray = compareByteByteArray(baa, m_byteByteArray);

            //bool bl = reader.ReadBoolean("m_bool");
            //m_bool = compareBool(bl, m_bool);
            //m_boolArray =  compareBoolArray(reader.ReadBooleanArray("m_boolArray"), m_boolArray);

            //m_byte = compareByte(reader.ReadByte("m_byte"), m_byte);
            //m_byteArray = compareByteArray(reader.ReadByteArray("m_byteArray"), m_byteArray);
            //m_charArray = compareCharArray(reader.ReadCharArray("m_charArray"), m_charArray);
            //List<object> tmpl = new List<object>();
            //reader.ReadCollection("m_list", tmpl);
            //m_list = compareCompareCollection(tmpl, m_list);

            //m_dateTime = compareData(reader.ReadDate("m_dateTime"), m_dateTime);

            //m_double = compareDouble(reader.ReadDouble("m_double"), m_double);

            //m_doubleArray = compareDoubleArray(reader.ReadDoubleArray("m_doubleArray"), m_doubleArray);
            //m_float = compareFloat(reader.ReadFloat("m_float"), m_float);
            //m_floatArray = compareFloatArray(reader.ReadFloatArray("m_floatArray"), m_floatArray);
            //m_int16 = compareInt16(reader.ReadInt16("m_int16"), m_int16);
            //m_int32 = compareInt32(reader.ReadInt32("m_int32"), m_int32);
            //m_long = compareInt64(reader.ReadInt64("m_long"), m_long);
            //m_int32Array = compareIntArray(reader.ReadIntArray("m_int32Array"), m_int32Array);
            //m_longArray = compareLongArray(reader.ReadLongArray("m_longArray"), m_longArray);
            //m_int16Array = compareSHortArray(reader.ReadShortArray("m_int16Array"), m_int16Array);
            //m_sbyte = compareSByte(reader.ReadSByte("m_sbyte"), m_sbyte);
            //m_sbyteArray = compareSByteArray(reader.ReadSByteArray("m_sbyteArray"), m_sbyteArray);
            //m_stringArray = compareStringArray(reader.ReadStringArray("m_stringArray"), m_stringArray);
            //m_uint16 = compareUInt16(reader.ReadUInt16("m_uint16"), m_uint16);
            //m_uint32 = compareUInt32(reader.ReadUInt32("m_uint32") , m_uint32);
            //m_ulong = compareUint64(reader.ReadUInt64("m_ulong"), m_ulong);
            //m_uint32Array = compareUnsignedIntArray(reader.ReadUnsignedIntArray("m_uint32Array"), m_uint32Array);
            //m_ulongArray = compareUnsignedLongArray(reader.ReadUnsignedLongArray("m_ulongArray"), m_ulongArray);
            //m_uint16Array = compareUnsignedShortArray(reader.ReadUnsignedShortArray("m_uint16Array"), m_uint16Array);      
            /*
            byte[][] baa = reader.ReadArrayOfByteArrays("m_byteByteArray");
            m_byteByteArray = compareByteByteArray(baa, m_byteByteArray);
            m_char = GenericValCompare(reader.ReadChar("m_char"), m_char);

            m_boolArray = GenericCompare(reader.ReadBooleanArray("m_boolArray"), m_boolArray);

            m_byte = GenericValCompare(reader.ReadByte("m_byte"), m_byte);
            m_byteArray = GenericCompare(reader.ReadByteArray("m_byteArray"), m_byteArray);
            m_charArray = GenericCompare(reader.ReadCharArray("m_charArray"), m_charArray);

            List<object> tmpl = new List<object>();
            tmpl = (List<object>)reader.ReadObject("m_arraylist");
            m_arraylist = compareCompareCollection(tmpl, m_arraylist);

            IDictionary<object, object> tmpM = (IDictionary<object, object>)reader.ReadObject("m_map");
            if (tmpM.Count != m_map.Count)
                throw new IllegalStateException("Not got expected value for type: " + m_map.GetType().ToString());

            
            m_string = GenericValCompare(reader.ReadString("m_string"), m_string);

            m_dateTime = compareData(reader.ReadDate("m_dateTime"), m_dateTime);

            m_double = GenericValCompare(reader.ReadDouble("m_double"), m_double);

            m_doubleArray = GenericCompare(reader.ReadDoubleArray("m_doubleArray"), m_doubleArray);
            m_float = GenericValCompare(reader.ReadFloat("m_float"), m_float);
            m_floatArray = GenericCompare(reader.ReadFloatArray("m_floatArray"), m_floatArray);
            m_int16 = GenericValCompare(reader.ReadShort("m_int16"), m_int16);
            m_int32 = GenericValCompare(reader.ReadInt("m_int32"), m_int32);
            //m_long = GenericValCompare(reader.ReadLong("m_long"), m_long);
            m_int32Array = GenericCompare(reader.ReadIntArray("m_int32Array"), m_int32Array);
            m_longArray = GenericCompare(reader.ReadLongArray("m_longArray"), m_longArray);
            m_int16Array = GenericCompare(reader.ReadShortArray("m_int16Array"), m_int16Array);
            m_sbyte = GenericValCompare(reader.ReadByte("m_sbyte"), m_sbyte);
            m_sbyteArray = GenericCompare(reader.ReadByteArray("m_sbyteArray"), m_sbyteArray);
            m_stringArray = GenericCompare(reader.ReadStringArray("m_stringArray"), m_stringArray);
            m_uint16 = GenericValCompare(reader.ReadShort("m_uint16"), m_uint16);
            //m_uint32 = GenericValCompare(reader.ReadInt("m_uint32"), m_uint32);
            m_ulong = GenericValCompare(reader.ReadLong("m_ulong"), m_ulong);
            m_uint32Array = GenericCompare(reader.ReadIntArray("m_uint32Array"), m_uint32Array);
            m_ulongArray = GenericCompare(reader.ReadLongArray("m_ulongArray"), m_ulongArray);
            m_uint16Array = GenericCompare(reader.ReadShortArray("m_uint16Array"), m_uint16Array);

            byte[] ret = reader.ReadByteArray("m_byte252");
            if (ret.Length != 252)
                throw new Exception("Array len 252 not found");

            ret = reader.ReadByteArray("m_byte253");
            if (ret.Length != 253)
                throw new Exception("Array len 253 not found");

            ret = reader.ReadByteArray("m_byte65535");
            if (ret.Length != 65535)
                throw new Exception("Array len 65535 not found");

            ret = reader.ReadByteArray("m_byte65536");
            if (ret.Length != 65536)
                throw new Exception("Array len 65536 not found");

            //pdxEnumTest retenum = (pdxEnumTest)reader.ReadObject("m_pdxEnum");
            //if (retenum != m_pdxEnum)
             //   throw new Exception("Enum is not equal");
            //byte[] m_byte252 = new byte[252];
            //byte[] m_byte253 = new byte[253];
            //byte[] m_byte65535 = new byte[65535];
            //byte[] m_byte65536 = new byte[65536];
            */
            m_byteByteArray = reader.ReadArrayOfByteArrays("m_byteByteArray");
            m_char = reader.ReadChar("m_char");

            m_boolArray = reader.ReadBooleanArray("m_boolArray");

            m_byte = reader.ReadByte("m_byte");
            m_byteArray = reader.ReadByteArray("m_byteArray");
            m_charArray = reader.ReadCharArray("m_charArray");

            m_arraylist = (List<object>)reader.ReadObject("m_arraylist");

            m_map = (IDictionary<object, object>)reader.ReadObject("m_map");
            

            m_string = reader.ReadString("m_string");

            m_dateTime = reader.ReadDate("m_dateTime");

            m_double = reader.ReadDouble("m_double");

            m_doubleArray = reader.ReadDoubleArray("m_doubleArray");
            m_float = reader.ReadFloat("m_float");
            m_floatArray = reader.ReadFloatArray("m_floatArray");
            m_int16 = reader.ReadShort("m_int16");
            m_int32 = reader.ReadInt("m_int32");
            //m_long = GenericValCompare(reader.ReadLong("m_long"), m_long);
            m_int32Array = reader.ReadIntArray("m_int32Array");
            m_longArray = reader.ReadLongArray("m_longArray");
            m_int16Array = reader.ReadShortArray("m_int16Array");
            m_sbyte = reader.ReadByte("m_sbyte");
            m_sbyteArray = reader.ReadByteArray("m_sbyteArray");
            m_stringArray = reader.ReadStringArray("m_stringArray");
            m_uint16 = GenericValCompare(reader.ReadShort("m_uint16"), m_uint16);
            //m_uint32 = GenericValCompare(reader.ReadInt("m_uint32"), m_uint32);
            m_ulong = reader.ReadLong("m_ulong");
            m_uint32Array = reader.ReadIntArray("m_uint32Array");
            m_ulongArray = reader.ReadLongArray("m_ulongArray");
            m_uint16Array = reader.ReadShortArray("m_uint16Array");

            byte[] ret = reader.ReadByteArray("m_byte252");
            if (ret.Length != 252)
                throw new Exception("Array len 252 not found");

            ret = reader.ReadByteArray("m_byte253");
            if (ret.Length != 253)
                throw new Exception("Array len 253 not found");

            ret = reader.ReadByteArray("m_byte65535");
            if (ret.Length != 65535)
                throw new Exception("Array len 65535 not found");

            ret = reader.ReadByteArray("m_byte65536");
            if (ret.Length != 65536)
                throw new Exception("Array len 65536 not found");
            m_pdxEnum = (pdxEnumTest)reader.ReadObject("m_pdxEnum");
            //if (retenum != m_pdxEnum)
            //   throw new Exception("Enum is not equal");
        }
        public string PString
        {
            get { return m_string; }
        }
        public void ToData(IPdxWriter writer)
        {
            writer.WriteArrayOfByteArrays("m_byteByteArray", m_byteByteArray);
            writer.WriteChar("m_char", m_char);
            writer.WriteBooleanArray("m_boolArray", m_boolArray);
            writer.WriteByte("m_byte", m_byte);
            writer.WriteByteArray("m_byteArray", m_byteArray);
            writer.WriteCharArray("m_charArray", m_charArray);
            //writer.WriteCollection("m_list", m_list);

            writer.WriteObject("m_arraylist", m_arraylist);
            writer.WriteObject("m_map", m_map);
            
            
            writer.WriteString("m_string", m_string);
            writer.WriteDate("m_dateTime", m_dateTime);
            writer.WriteDouble("m_double", m_double);
            writer.WriteDoubleArray("m_doubleArray", m_doubleArray);
            writer.WriteFloat("m_float", m_float);
            writer.WriteFloatArray("m_floatArray", m_floatArray);
            writer.WriteShort("m_int16", m_int16);
            writer.WriteInt("m_int32", m_int32);
            //writer.WriteLong("m_long", m_long);
            writer.WriteIntArray("m_int32Array", m_int32Array);
            writer.WriteLongArray("m_longArray", m_longArray);
            writer.WriteShortArray("m_int16Array", m_int16Array);
            writer.WriteByte("m_sbyte", m_sbyte);
            writer.WriteByteArray("m_sbyteArray", m_sbyteArray);
            writer.WriteStringArray("m_stringArray", m_stringArray);
            writer.WriteShort("m_uint16", m_uint16);
            //writer.WriteInt("m_uint32", m_uint32);
            writer.WriteLong("m_ulong", m_ulong);
            writer.WriteIntArray("m_uint32Array", m_uint32Array);
            writer.WriteLongArray("m_ulongArray", m_ulongArray);
            writer.WriteShortArray("m_uint16Array", m_uint16Array);
            writer.WriteByteArray("m_byte252", m_byte252);
            writer.WriteByteArray("m_byte253", m_byte253);
            writer.WriteByteArray("m_byte65535", m_byte65535);
            writer.WriteByteArray("m_byte65536", m_byte65536);
            writer.WriteObject("m_pdxEnum", m_pdxEnum);
            //byte[] m_byte252 = new byte[252];
            //byte[] m_byte253 = new byte[253];
            //byte[] m_byte65535 = new byte[65535];
            //byte[] m_byte65536 = new byte[65536];
        }

        public char Char
        {
            get { return m_char; }
        }
        
        public sbyte Byte
        {
            get { return m_byte; }
        }
        public sbyte Sbyte
        {
            get { return m_sbyte; }
        }
        public short Int16
        {
            get { return m_int16; }
        }

        public short Uint16
        {
            get { return m_uint16; }
        }
        public Int32 Int32
        {
            get { return m_int32; }
        }
        /*
        public Int32 Uint32
        {
            get { return m_uint32; }
        }
        public long Long
        {
            get { return m_long; }
        }
         */
        public Int64 Ulong
        {
            get { return m_ulong; }
        }
        public float Float
        {
            get { return m_float; }
        }
        public double Double
        {
            get { return m_double; }
        }


        public bool[] BoolArray
        {
            get { return m_boolArray; }
        }
        public byte[] ByteArray
        {
            get { return m_byteArray; }
        }
        public byte[] SbyteArray
        {
            get { return m_sbyteArray; }
        }

        public char[] CharArray
        {
            get { return m_charArray; }
        }

        public DateTime DateTime
        {
            get { return m_dateTime; }
        }

        public Int16[] Int16Array
        {
            get { return m_int16Array; }
        }
        public Int16[] Uint16Array
        {
            get { return m_uint16Array; }
        }

        public Int32[] Int32Array
        {
            get { return m_int32Array; }
        }
        public Int32[] Uint32Array
        {
            get { return m_uint32Array; }
        }

        public long[] LongArray
        {
            get { return m_longArray; }
        }
        public Int64[] UlongArray
        {
            get { return m_ulongArray; }
        }

        public float[] FloatArray
        {
            get { return m_floatArray; }
        }
        public double[] DoubleArray
        {
            get { return m_doubleArray; }
        }

        public byte[][] ByteByteArray
        {
            get { return m_byteByteArray; }
        }

        public string[] StringArray
        {
            get { return m_stringArray; }
        }

        public List<object> Arraylist
        {
            get { return m_arraylist; }
        }
        public IDictionary<object, object> Map
        {
            get { return m_map; }
        }
       

        
        public byte[] Byte252
        {
            get { return m_byte252; }
        }
        public byte[] Byte253
        {
            get { return m_byte253; }
        }
        public byte[] Byte65535
        {
            get { return m_byte65535; }
        }
        public byte[] Byte65536
        {
            get { return m_byte65536; }
        }
        
        public pdxEnumTest PdxEnum
        {
            get { return m_pdxEnum; }
        }

        public override string ToString()
        {
            return "PdxVersioned 1 : " + m_string;
        }
        #endregion
    }

    public class InnerPdx : IPdxSerializable
    {
      string inner = "inner1";
      string inner2 = "inner1";
      string age = "21";

      public InnerPdx()
      {
      }

      public void FromData(IPdxReader reader)
      {
        this.inner = reader.ReadString("inner");
        this.inner2 = reader.ReadString("inner2");
        this.age = reader.ReadString("age");
      }

      public void ToData(IPdxWriter writer)
      {
        string str = new string('a', 240);
        writer.WriteString("inner1", str);
        writer.WriteString("inner2", this.inner2);
        writer.WriteString("age", this.age);
      }
    }

    public class Pdx1 : IPdxSerializable
    {
      string name = "name ";

      InnerPdx innerPdx = new InnerPdx();
      string age = "199";

      public Pdx1()
      {
        
      }

      public void FromData(IPdxReader reader)
      {
        this.name = reader.ReadString("name1");
        this.name = reader.ReadString("name2");
        this.name = reader.ReadString("name3");
        this.name = reader.ReadString("name4");
        this.name = reader.ReadString("name5");
        this.innerPdx = (InnerPdx)reader.ReadObject("innerPdx");
        this.age = reader.ReadString("age1");
        this.age = reader.ReadString("age2");
        this.age = reader.ReadString("age3");
        this.age = reader.ReadString("age4");
        this.age = reader.ReadString("age5");
        this.age = reader.ReadString("age6");

      }

      public void ToData(IPdxWriter writer)
      {
        string str = new string('a', 240);
        writer.WriteString("name1", str);
        writer.WriteString("name2", this.name);
        writer.WriteString("name3", this.name);
        writer.WriteString("name4", this.name);
        writer.WriteString("name5", this.name);

        writer.WriteObject("innerPdx", this.innerPdx);
        writer.WriteString("age1", this.age);
        writer.WriteString("age2", this.age);
        writer.WriteString("age3", this.age);
        writer.WriteString("age4", this.age);
        writer.WriteString("age5", this.age);
        writer.WriteString("age6", this.age);
      }

      public override string ToString()
      {
        return this.name + " " + this.age;
      }
    }
}