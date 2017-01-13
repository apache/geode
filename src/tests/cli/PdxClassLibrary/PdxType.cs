using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using GemStone.GemFire.Cache.Generic;
using GemStone.GemFire.Cache.Generic.Internal;

namespace PdxTests
{

  public enum pdxEnumTest { pdx1, pdx2, pdx3};

  public class Address : IPdxSerializable
  {
    int _aptNumber;
    string _street;
    string _city;

    public Address()
    { }
    public override string ToString()
    {
      return _aptNumber + " :"  + _street + " : "+ _city;
    }
    public Address(int aptN, string street, string city)
    {
      _aptNumber = aptN;
      _street = street;
      _city = city;
    }

    public override bool Equals(object obj)
    {
      Console.WriteLine("in addredd equal");
      if (obj == null)
        return false;
      Address other = obj as Address;
      if (other == null)
        return false;
      Console.WriteLine("in addredd equal2 " + this.ToString() + " : : " + other.ToString());
      if (_aptNumber == other._aptNumber
          && _street == other._street
            && _city == other._city)
        return true;
      return false;
    }
    public override int GetHashCode()
    {
        return base.GetHashCode();
    }

    #region IPdxSerializable Members

    public void FromData(IPdxReader reader)
    {
      _aptNumber = reader.ReadInt("_aptNumber");
      _street = reader.ReadString("_street");
      _city = reader.ReadString("_city");
    }

    public void ToData(IPdxWriter writer)
    {
      writer.WriteInt("_aptNumber", _aptNumber);
      writer.WriteString("_street", _street);
      writer.WriteString("_city", _city);
    }

    #endregion
  }
  [Serializable]
  public class PdxType : IPdxSerializable
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
    Hashtable m_hashtable = new Hashtable();
    ArrayList m_vector = new ArrayList();

    CacheableHashSet m_chs = CacheableHashSet.Create();
    CacheableLinkedHashSet m_clhs = CacheableLinkedHashSet.Create();

    byte[] m_byte252 = new byte[252];
    byte[] m_byte253 = new byte[253];
    byte[] m_byte65535 = new byte[65535];
    byte[] m_byte65536 = new byte[65536];
    pdxEnumTest m_pdxEnum = pdxEnumTest.pdx2;

    Address[] m_address;
    List<object> m_objectArray = new List<object>();
    public void Init()
    {
      m_char = 'C';
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
      m_byteArray = new byte[] { 0x34, 0x64 };
      m_sbyteArray = new byte[] { 0x34, 0x64 };

      m_charArray = new char[] { 'c', 'v' };

      DateTime n = new DateTime((62135596800000/*epoch*/ + 1310447869154 ) * 10000, DateTimeKind.Utc);
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

      m_hashtable = new Hashtable();
      m_hashtable.Add(1, "1111111111111111");
      m_hashtable.Add(2, "2222222222221111111111111111");

      m_vector = new ArrayList();
      m_vector.Add(1);
      m_vector.Add(2);
      m_vector.Add(3);

      m_chs.Add(1);
      m_clhs.Add(1);
      m_clhs.Add(2);

      m_pdxEnum = pdxEnumTest.pdx2;

      m_address = new Address[10];

      for (int i = 0; i < 10; i++)
      {
        m_address[i] = new Address(i + 1, "street" + i.ToString(), "city" + i.ToString());
      }

      m_objectArray = new List<object>();
      for (int i = 0; i < 10; i++)
      {
        m_objectArray.Add(new Address(i + 1, "street" + i.ToString(), "city" + i.ToString()));
      }      
    }

    public PdxType()
    {
      Init();

    }

    public static IPdxSerializable CreateDeserializable()
    {
      return new PdxType();
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

    /*
    public static LinkedList<String> compareCompareCollection(LinkedList<String> a, LinkedList<String> a2)
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
    */
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
      throw new IllegalStateException("Not got expected value for type: " + b2.GetType().ToString() + " : values " + b.ToString() +  ": "  + b2.ToString());
    }
    public override bool Equals(object obj)
    {
      if (obj == null)
        return false;
      
      PdxType other = obj as PdxType;
      if (other == null)
        return false;

      if (other == this)
        return true;

      compareByteByteArray(other.m_byteByteArray, m_byteByteArray);
      GenericValCompare(other.m_char, m_char);

      GenericValCompare(other.m_bool, m_bool);
      GenericCompare(other.m_boolArray, m_boolArray);

      GenericValCompare(other.m_byte, m_byte);
      GenericCompare(other.m_byteArray, m_byteArray);
      GenericCompare(other.m_charArray, m_charArray);

      compareCompareCollection(other.m_arraylist, m_arraylist);

      if (other.m_map.Count != m_map.Count)
        throw new IllegalStateException("Not got expected value for type: " + m_map.GetType().ToString());

      if (other.m_hashtable.Count != m_hashtable.Count)
        throw new IllegalStateException("Not got expected value for type: " + m_hashtable.GetType().ToString());

      if (other.m_vector.Count != m_vector.Count)
        throw new IllegalStateException("Not got expected value for type: " + m_vector.GetType().ToString());

      if (other.m_chs.Count != m_chs.Count)
        throw new IllegalStateException("Not got expected value for type: " + m_chs.GetType().ToString());

      if (other.m_clhs.Count != m_clhs.Count)
        throw new IllegalStateException("Not got expected value for type: " + m_clhs.GetType().ToString());


      GenericValCompare(other.m_string, m_string);

      compareData(other.m_dateTime, m_dateTime);

      GenericValCompare(other.m_double, m_double);

      GenericCompare(other.m_doubleArray, m_doubleArray);
      GenericValCompare(other.m_float, m_float);
      GenericCompare(other.m_floatArray, m_floatArray);
      GenericValCompare(other.m_int16, m_int16);
      GenericValCompare(other.m_int32, m_int32);
      GenericValCompare(other.m_long, m_long);
      GenericCompare(other.m_int32Array, m_int32Array);
      GenericCompare(other.m_longArray, m_longArray);
      GenericCompare(other.m_int16Array, m_int16Array);
      GenericValCompare(other.m_sbyte, m_sbyte);
      GenericCompare(other.m_sbyteArray, m_sbyteArray);
      GenericCompare(other.m_stringArray, m_stringArray);
      GenericValCompare(other.m_uint16, m_uint16);
      GenericValCompare(other.m_uint32, m_uint32);
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
      if(m_pdxEnum != other.m_pdxEnum)
        throw new Exception("pdx enum is not equal");

      {
        for (int i = 0; i < m_address.Length; i++)
        {
          if (!m_address[i].Equals(other.m_address[i]))
            throw new Exception("Address array not mateched " + i);
        }
      }

      for (int i = 0; i < m_objectArray.Count; i++)
      {
        if (!m_objectArray[i].Equals(other.m_objectArray[i]))
          return false;
      }
        return true;
    }

    public override int GetHashCode()
    {
        return base.GetHashCode();
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

      byte[][] baa = reader.ReadArrayOfByteArrays("m_byteByteArray");
      m_byteByteArray = compareByteByteArray(baa, m_byteByteArray);
      m_char = GenericValCompare(reader.ReadChar("m_char"), m_char);

      bool bl = reader.ReadBoolean("m_bool");
      m_bool = GenericValCompare(bl, m_bool);
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

      Hashtable tmpH = (Hashtable)reader.ReadObject("m_hashtable");

      if (tmpH.Count != m_hashtable.Count)
        throw new IllegalStateException("Not got expected value for type: " + m_hashtable.GetType().ToString());

      ArrayList arrAl = (ArrayList)reader.ReadObject("m_vector");

      if (arrAl.Count != m_vector.Count)
        throw new IllegalStateException("Not got expected value for type: " + m_vector.GetType().ToString());

      CacheableHashSet rmpChs = (CacheableHashSet)reader.ReadObject("m_chs");

      if (rmpChs.Count != m_chs.Count)
        throw new IllegalStateException("Not got expected value for type: " + m_chs.GetType().ToString());

      CacheableLinkedHashSet rmpClhs = (CacheableLinkedHashSet)reader.ReadObject("m_clhs");

      if (rmpClhs.Count != m_clhs.Count)
        throw new IllegalStateException("Not got expected value for type: " + m_clhs.GetType().ToString());


      m_string = GenericValCompare(reader.ReadString("m_string"), m_string);

      m_dateTime = compareData(reader.ReadDate("m_dateTime"), m_dateTime);

      m_double = GenericValCompare(reader.ReadDouble("m_double"), m_double);

      m_doubleArray = GenericCompare(reader.ReadDoubleArray("m_doubleArray"), m_doubleArray);
      m_float = GenericValCompare(reader.ReadFloat("m_float"), m_float);
      m_floatArray = GenericCompare(reader.ReadFloatArray("m_floatArray"), m_floatArray);
      m_int16 = GenericValCompare(reader.ReadShort("m_int16"), m_int16);
      m_int32 = GenericValCompare(reader.ReadInt("m_int32"), m_int32);
      m_long = GenericValCompare(reader.ReadLong("m_long"), m_long);
      m_int32Array = GenericCompare(reader.ReadIntArray("m_int32Array"), m_int32Array);
      m_longArray = GenericCompare(reader.ReadLongArray("m_longArray"), m_longArray);
      m_int16Array = GenericCompare(reader.ReadShortArray("m_int16Array"), m_int16Array);
      m_sbyte = GenericValCompare(reader.ReadByte("m_sbyte"), m_sbyte);
      m_sbyteArray = GenericCompare(reader.ReadByteArray("m_sbyteArray"), m_sbyteArray);
      m_stringArray = GenericCompare(reader.ReadStringArray("m_stringArray"), m_stringArray);
      m_uint16 = GenericValCompare(reader.ReadShort("m_uint16"), m_uint16);
      m_uint32 = GenericValCompare(reader.ReadInt("m_uint32"), m_uint32);
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

      pdxEnumTest retenum = (pdxEnumTest)reader.ReadObject("m_pdxEnum");
      if (retenum != m_pdxEnum)
        throw new Exception("Enum is not equal");
      //byte[] m_byte252 = new byte[252];
      //byte[] m_byte253 = new byte[253];
      //byte[] m_byte65535 = new byte[65535];
      //byte[] m_byte65536 = new byte[65536];

      Address[] addressArray = (Address[])reader.ReadObject("m_address");

      {
        for (int i = 0; i < m_address.Length; i++)
        {
          if (!m_address[i].Equals(addressArray[i]))
          {
            Console.WriteLine(m_address[i]);
            Console.WriteLine(addressArray[i]);
            throw new Exception("Address array not mateched " + i);
          }
        }
      }

      List<object> retoa = reader.ReadObjectArray("m_objectArray");
      for (int i = 0; i < m_objectArray.Count; i++)
      { 
        if(!m_objectArray[i].Equals(retoa[i]))
          throw new Exception("Object array not mateched " + i);
      }
    }
    public string PString
    {
      get { return m_string; }
    }
    public void ToData(IPdxWriter writer)
    {
      writer.WriteArrayOfByteArrays("m_byteByteArray", m_byteByteArray);
      writer.WriteChar("m_char", m_char);
      writer.WriteBoolean("m_bool", m_bool);
      writer.WriteBooleanArray("m_boolArray", m_boolArray);
      writer.WriteByte("m_byte", m_byte);
      writer.WriteByteArray("m_byteArray", m_byteArray);
      writer.WriteCharArray("m_charArray", m_charArray);
      //writer.WriteCollection("m_list", m_list);

      writer.WriteObject("m_arraylist", m_arraylist);
      writer.WriteObject("m_map", m_map);
      writer.WriteObject("m_hashtable", m_hashtable);
      writer.WriteObject("m_vector", m_vector);

      writer.WriteObject("m_chs", m_chs);
      writer.WriteObject("m_clhs", m_clhs);
      
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
      writer.WriteObject("m_pdxEnum", m_pdxEnum);
      writer.WriteObject("m_address", m_address);
      writer.WriteObjectArray("m_objectArray", m_objectArray);
      //byte[] m_byte252 = new byte[252];
      //byte[] m_byte253 = new byte[253];
      //byte[] m_byte65535 = new byte[65535];
      //byte[] m_byte65536 = new byte[65536];
    }

    public char Char
    {
      get { return m_char; }
    }
    public bool Bool
    {
      get { return m_bool; }
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
    public Int32 Uint32
    {
      get { return m_uint32; }
    }
    public long Long
    {
      get { return m_long; }
    }
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

    public string String
    {
      get { return m_string; }
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
    public Hashtable Hashtable
    {
      get { return m_hashtable; }
    }

    public ArrayList Vector
    {
      get { return m_vector; }
    }

    public CacheableHashSet Chs
    {
      get { return m_chs; }
    }
    public CacheableLinkedHashSet Clhs
    {
      get { return m_clhs; }
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
    public Address[] AddressArray
    {
      get { return m_address; }
    }
    public List<object> ObjectArray
    {
      get { return m_objectArray; }
    }
    #endregion
  }
}
