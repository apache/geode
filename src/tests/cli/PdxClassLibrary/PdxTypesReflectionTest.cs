using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using GemStone.GemFire.Cache.Generic;
using GemStone.GemFire.Cache.Generic.Internal;

namespace PdxTests
{
  public class AddressR 
  {
    int _aptNumber;
    string _street;
    string _city;

    public AddressR()
    { }
    public override string ToString()
    {
      return _aptNumber + " :" + _street + " : " + _city;
    }
    public AddressR(int aptN, string street, string city)
    {
      _aptNumber = aptN;
      _street = street;
      _city = city;
    }

    public override bool Equals(object obj)
    {
      Console.WriteLine("in addreddR equal");
      if (obj == null)
        return false;
      AddressR other = obj as AddressR;
      if (other == null)
        return false;
      Console.WriteLine("in addreddr equal2 " + this.ToString() + " : : " + other.ToString());
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
  }

  public class PdxTypesReflectionTest 
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

    pdxEnumTest m_pdxEnum = pdxEnumTest.pdx3;
    AddressR[] m_address = new AddressR[10];

    LinkedList<Object> m_LinkedList = new LinkedList<Object>();

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

      long ticks = 634460644691540000L;
      m_dateTime = new DateTime(ticks);
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

      m_LinkedList = new LinkedList<Object>();
      m_LinkedList.AddFirst("Item1");
      m_LinkedList.AddLast("Item2");
      m_LinkedList.AddLast("Item3");


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
      m_pdxEnum = pdxEnumTest.pdx3;

      m_address = new AddressR[10];

      for (int i = 0; i < m_address.Length; i++)
      {
        m_address[i] = new AddressR(i, "street" + i, "city" + i);
      }
    }

    public PdxTypesReflectionTest()
    {
      

    }

    public PdxTypesReflectionTest(bool initialize)
    {
      if (initialize)
        Init();

    }

    public override bool Equals(object obj)
    {
      if (obj == null)
        return false;
      
      PdxTypesReflectionTest other = obj as PdxTypesReflectionTest;

      if (other == null)
        return false;

      this.checkEquality(other);
      return true;
    }
    public override int GetHashCode()
    {
        return base.GetHashCode();
    }
    #region IPdxSerializable Members
    byte[][] compareByteByteArray(byte[][] baa, byte[][] baa2)
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
    byte[] compareByteArray(byte[] a, byte[] a2)
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
    List<object> compareCompareCollection(List<object> a, List<object> a2)
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

    LinkedList<object> compareCompareCollection(LinkedList<object> a, LinkedList<object> a2)
    {
        if (a.Count == a2.Count)
        {
            LinkedList<Object>.Enumerator e1 = a.GetEnumerator();
            LinkedList<Object>.Enumerator e2 = a2.GetEnumerator();
            while (e1.MoveNext() && e2.MoveNext())
            {
                if (!e1.Current.Equals(e2.Current))
                    break;
            }
            return a2;
        }
        throw new IllegalStateException("Not got expected value for type: " + a2.GetType().ToString());
    }

    DateTime compareData(DateTime b, DateTime b2)
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

    T[] GenericCompare<T>(T[] a, T[] a2)
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


    T GenericValCompare<T>(T b, T b2)
    {
      if (b.Equals(b2))
        return b;
      throw new IllegalStateException("Not got expected value for type: " + b2.GetType().ToString());
    }
    public void checkEquality(PdxTypesReflectionTest other)
    {      
      byte[][] baa = other.m_byteByteArray;
      m_byteByteArray = compareByteByteArray(baa, m_byteByteArray);
      m_char = GenericValCompare(other.m_char, m_char);

      m_bool = GenericValCompare(other.m_bool, m_bool);
      m_boolArray = GenericCompare(other.m_boolArray, m_boolArray);

      m_byte = GenericValCompare(other.m_byte, m_byte);
      m_byteArray = GenericCompare(other.m_byteArray, m_byteArray);
      m_charArray = GenericCompare(other.m_charArray, m_charArray);

      List<object> tmpl = new List<object>();
      m_arraylist = compareCompareCollection(other.m_arraylist, m_arraylist);

      m_LinkedList = compareCompareCollection(other.m_LinkedList, m_LinkedList);

      IDictionary<object, object> tmpM = other.m_map;
      if (tmpM.Count != m_map.Count)
        throw new IllegalStateException("Not got expected value for type: " + m_map.GetType().ToString());

      Hashtable tmpH = other.m_hashtable;

      if (tmpH.Count != m_hashtable.Count)
        throw new IllegalStateException("Not got expected value for type: " + m_hashtable.GetType().ToString());

      ArrayList arrAl = other.m_vector;

      if (arrAl.Count != m_vector.Count)
        throw new IllegalStateException("Not got expected value for type: " + m_vector.GetType().ToString());

      CacheableHashSet rmpChs = other.m_chs;

      if (rmpChs.Count != m_chs.Count)
        throw new IllegalStateException("Not got expected value for type: " + m_chs.GetType().ToString());

      CacheableLinkedHashSet rmpClhs = other.m_clhs;

      if (rmpClhs.Count != m_clhs.Count)
        throw new IllegalStateException("Not got expected value for type: " + m_clhs.GetType().ToString());


      m_string = GenericValCompare(other.m_string, m_string);

      m_dateTime = compareData(other.m_dateTime, m_dateTime);

      m_double = GenericValCompare(other.m_double, m_double);

      m_doubleArray = GenericCompare(other.m_doubleArray, m_doubleArray);
      m_float = GenericValCompare(other.m_float, m_float);
      m_floatArray = GenericCompare(other.m_floatArray, m_floatArray);
      m_int16 = GenericValCompare(other.m_int16, m_int16);
      m_int32 = GenericValCompare(other.m_int32, m_int32);
      m_long = GenericValCompare(other.m_long, m_long);
      m_int32Array = GenericCompare(other.m_int32Array, m_int32Array);
      m_longArray = GenericCompare(other.m_longArray, m_longArray);
      m_int16Array = GenericCompare(other.m_int16Array, m_int16Array);
      m_sbyte = GenericValCompare(other.m_sbyte, m_sbyte);
      m_sbyteArray = GenericCompare(other.m_sbyteArray, m_sbyteArray);
      m_stringArray = GenericCompare(other.m_stringArray, m_stringArray);
      m_uint16 = GenericValCompare(other.m_uint16, m_uint16);
      m_uint32 = GenericValCompare(other.m_uint32, m_uint32);
      m_ulong = GenericValCompare(other.m_ulong, m_ulong);
      m_uint32Array = GenericCompare(other.m_uint32Array, m_uint32Array);
      m_ulongArray = GenericCompare(other.m_ulongArray, m_ulongArray);
      m_uint16Array = GenericCompare(other.m_uint16Array, m_uint16Array);

      byte[] ret = other.m_byte252;
      if (ret.Length != 252)
        throw new Exception("Array len 252 not found");

      ret = other.m_byte253;
      if (ret.Length != 253)
        throw new Exception("Array len 253 not found");

      ret = other.m_byte65535 ;
      if (ret.Length != 65535)
        throw new Exception("Array len 65535 not found");

      ret = other.m_byte65536;
      if (ret.Length != 65536)
        throw new Exception("Array len 65536 not found");
      if(other.m_pdxEnum !=  m_pdxEnum )
        throw new Exception("Pdx enum is not equal");
      //byte[] m_byte252 = new byte[252];
      //byte[] m_byte253 = new byte[253];
      //byte[] m_byte65535 = new byte[65535];
      //byte[] m_byte65536 = new byte[65536];
      AddressR[] otherA = other.m_address;
      for (int i = 0; i < m_address.Length; i++)
      { 
        if(!m_address[i].Equals(otherA[i]))
          throw new Exception("AddressR array is not equal " + i);
      }
    }



    #endregion
  }
}
