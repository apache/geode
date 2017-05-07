using System;
using System.Collections.Generic;
using System.Text;
using GemStone.GemFire.Cache.Generic;
using System.Collections;
namespace PdxTests
{
 [Serializable]
  public class PdxTypes1 : IPdxSerializable
  {
    int m_i1 = 34324;
    int m_i2 = 2144;
    int m_i3 = 4645734;
    int m_i4 = 73567;

    public PdxTypes1()
    { 
    
    }

    public static IPdxSerializable CreateDeserializable()
    {
      return new PdxTypes1();
    }

    public override int GetHashCode()
    {
      return base.GetHashCode();
    }
    public override bool Equals(object obj)
    {
      if (obj == null)
        return false;
      PdxTypes1 pap = obj as PdxTypes1;

      if (pap == null)
        return false;

      //if (pap == this)
        //return true;

      if (m_i1 == pap.m_i1
         && m_i2 == pap.m_i2
          && m_i3 == pap.m_i3
           && m_i4 == pap.m_i4)
        return true;

      return false;
    }

    #region IPdxSerializable Members

    public void FromData(IPdxReader reader)
    {
      m_i1 = reader.ReadInt("i1");
      m_i2 = reader.ReadInt("i2");
      m_i3 = reader.ReadInt("i3");
      m_i4 = reader.ReadInt("i4");
    }

    public void ToData(IPdxWriter writer)
    {
      writer.WriteInt("i1", m_i1);
      writer.WriteInt("i2", m_i2);
      writer.WriteInt("i3", m_i3);
      writer.WriteInt("i4", m_i4);
    }

    #endregion
  }
    [Serializable]
  public class PdxTypes2 : IPdxSerializable
  {
    string m_s1 = "one";
    int m_i1 = 34324;
    int m_i2 = 2144;
    int m_i3 = 4645734;
    int m_i4 = 73567;

    public PdxTypes2()
    {

    }
    public static IPdxSerializable CreateDeserializable()
    {
      return new PdxTypes2();
    }
    public override int GetHashCode()
    {
      return base.GetHashCode();
    }
    public override bool Equals(object obj)
    {
      if (obj == null)
        return false;
      PdxTypes2 pap = obj as PdxTypes2;

      if (pap == null)
        return false;

      //if (pap == this)
        //return true;

      if (m_i1 == pap.m_i1
         && m_i2 == pap.m_i2
          && m_i3 == pap.m_i3
           && m_i4 == pap.m_i4
           && m_s1 == pap.m_s1)
        return true;

      return false;
    }

    #region IPdxSerializable Members

    public void FromData(IPdxReader reader)
    {
      m_s1 = reader.ReadString("s1");
      m_i1 = reader.ReadInt("i1");
      m_i2 = reader.ReadInt("i2");
      m_i3 = reader.ReadInt("i3");
      m_i4 = reader.ReadInt("i4");
    }

    public void ToData(IPdxWriter writer)
    {
      writer.WriteString("s1", m_s1);
      writer.WriteInt("i1", m_i1);
      writer.WriteInt("i2", m_i2);
      writer.WriteInt("i3", m_i3);
      writer.WriteInt("i4", m_i4);
    }

    #endregion
  }
    [Serializable]
  public class PdxTypes3 : IPdxSerializable
  {
    string m_s1 = "one";
    int m_i1 = 34324;
    int m_i2 = 2144;
    int m_i3 = 4645734;
    int m_i4 = 73567;

    public PdxTypes3()
    {

    }
    public static IPdxSerializable CreateDeserializable()
    {
      return new PdxTypes3();
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

      //if (pap == this)
        //return true;

      if (m_i1 == pap.m_i1
         && m_i2 == pap.m_i2
          && m_i3 == pap.m_i3
           && m_i4 == pap.m_i4
           && m_s1 == pap.m_s1)
        return true;

      return false;
    }

    #region IPdxSerializable Members

    public void FromData(IPdxReader reader)
    {      
      m_i1 = reader.ReadInt("i1");
      m_i2 = reader.ReadInt("i2");
      m_i3 = reader.ReadInt("i3");
      m_i4 = reader.ReadInt("i4");
      m_s1 = reader.ReadString("s1");
    }

    public void ToData(IPdxWriter writer)
    {      
      writer.WriteInt("i1", m_i1);
      writer.WriteInt("i2", m_i2);
      writer.WriteInt("i3", m_i3);
      writer.WriteInt("i4", m_i4);
      writer.WriteString("s1", m_s1);
    }

    #endregion
  }
    [Serializable]
  public class PdxTypes4 : IPdxSerializable
  {
    string m_s1 = "one";
    int m_i1 = 34324;
    int m_i2 = 2144;
    int m_i3 = 4645734;
    int m_i4 = 73567;

    public PdxTypes4()
    {

    }

    public static IPdxSerializable CreateDeserializable()
    {
      return new PdxTypes4();
    }

    public override int GetHashCode()
    {
      return base.GetHashCode();
    }
    public override bool Equals(object obj)
    {
      if (obj == null)
        return false;
      PdxTypes4 pap = obj as PdxTypes4;

      if (pap == null)
        return false;

      //if (pap == this)
        //return true;

      if (m_i1 == pap.m_i1
         && m_i2 == pap.m_i2
          && m_i3 == pap.m_i3
           && m_i4 == pap.m_i4
           && m_s1 == pap.m_s1)
        return true;

      return false;
    }

    #region IPdxSerializable Members

    public void FromData(IPdxReader reader)
    {
      m_i1 = reader.ReadInt("i1");
      m_i2 = reader.ReadInt("i2");
      m_s1 = reader.ReadString("s1");
      m_i3 = reader.ReadInt("i3");
      m_i4 = reader.ReadInt("i4");
      
    }

    public void ToData(IPdxWriter writer)
    {
      writer.WriteInt("i1", m_i1);
      writer.WriteInt("i2", m_i2);
      writer.WriteString("s1", m_s1);
      writer.WriteInt("i3", m_i3);
      writer.WriteInt("i4", m_i4);      
    }

    #endregion
  }
    [Serializable]
  public class PdxTypes5 : IPdxSerializable
  {
    string m_s1 = "one";
    string m_s2 = "two";
    int m_i1 = 34324;
    int m_i2 = 2144;
    int m_i3 = 4645734;
    int m_i4 = 73567;

    public PdxTypes5()
    {

    }
    public static IPdxSerializable CreateDeserializable()
    {
      return new PdxTypes5();
    }
    public override int GetHashCode()
    {
      return base.GetHashCode();
    }

    public override bool Equals(object obj)
    {
      if (obj == null)
        return false;
      PdxTypes5 pap = obj as PdxTypes5;

      if (pap == null)
        return false;

      //if (pap == this)
        //return true;

      if (m_i1 == pap.m_i1
         && m_i2 == pap.m_i2
          && m_i3 == pap.m_i3
           && m_i4 == pap.m_i4
           && m_s1 == pap.m_s1
            && m_s2 == pap.m_s2)
        return true;

      return false;
    }

    #region IPdxSerializable Members

    public void FromData(IPdxReader reader)
    {
      m_s1 = reader.ReadString("s1");
      m_s2 = reader.ReadString("s2");
      m_i1 = reader.ReadInt("i1");
      m_i2 = reader.ReadInt("i2");      
      m_i3 = reader.ReadInt("i3");
      m_i4 = reader.ReadInt("i4");

    }

    public void ToData(IPdxWriter writer)
    {
      writer.WriteString("s1", m_s1);
      writer.WriteString("s2", m_s2);
      writer.WriteInt("i1", m_i1);
      writer.WriteInt("i2", m_i2);      
      writer.WriteInt("i3", m_i3);
      writer.WriteInt("i4", m_i4);
    }

    #endregion
  }
    [Serializable]
  public class PdxTypes6 : IPdxSerializable
  {
    string m_s1 = "one";
    string m_s2 = "two";
    byte[] bytes128 = new byte[128]; 
    int m_i1 = 34324;
    int m_i2 = 2144;
    int m_i3 = 4645734;
    int m_i4 = 73567;

    public PdxTypes6()
    {

    }
    public static IPdxSerializable CreateDeserializable()
    {
      return new PdxTypes6();
    }
    public override int GetHashCode()
    {
      return base.GetHashCode();
    }
    public override bool Equals(object obj)
    {
      if (obj == null)
        return false;
      PdxTypes6 pap = obj as PdxTypes6;

      if (pap == null)
        return false;

      //if (pap == this)
        //return true;

      if (m_i1 == pap.m_i1
         && m_i2 == pap.m_i2
          && m_i3 == pap.m_i3
           && m_i4 == pap.m_i4
           && m_s1 == pap.m_s1
            && m_s2 == pap.m_s2)
      {
        if(bytes128.Length == pap.bytes128.Length)
          return true;
      }

      return false;
    }

    #region IPdxSerializable Members

    public void FromData(IPdxReader reader)
    {
      m_s1 = reader.ReadString("s1");      
      m_i1 = reader.ReadInt("i1");
      bytes128 = reader.ReadByteArray("bytes128");
      m_i2 = reader.ReadInt("i2");
      m_i3 = reader.ReadInt("i3");
      m_i4 = reader.ReadInt("i4");
      m_s2 = reader.ReadString("s2");
    }

    public void ToData(IPdxWriter writer)
    {
      writer.WriteString("s1", m_s1);      
      writer.WriteInt("i1", m_i1);
      writer.WriteByteArray("bytes128", bytes128);
      writer.WriteInt("i2", m_i2);
      writer.WriteInt("i3", m_i3);
      writer.WriteInt("i4", m_i4);
      writer.WriteString("s2", m_s2);
    }

    #endregion
  }
    [Serializable]
  public class PdxTypes7 : IPdxSerializable
  {
    string m_s1 = "one";
    string m_s2 = "two";
    int m_i1 = 34324;
    byte[] bytes38000 = new byte[38000];
    int m_i2 = 2144;
    int m_i3 = 4645734;
    int m_i4 = 73567;

    public PdxTypes7()
    {

    }
    public static IPdxSerializable CreateDeserializable()
    {
      return new PdxTypes7();
    }
    public override int GetHashCode()
    {
      return base.GetHashCode();
    }
    public override bool Equals(object obj)
    {
      if (obj == null)
        return false;
      PdxTypes7 pap = obj as PdxTypes7;

      if (pap == null)
        return false;

      //if (pap == this)
        //return true;

      if (m_i1 == pap.m_i1
         && m_i2 == pap.m_i2
          && m_i3 == pap.m_i3
           && m_i4 == pap.m_i4
           && m_s1 == pap.m_s1
            && m_s2 == pap.m_s2)
      {
        if(bytes38000.Length == pap.bytes38000.Length)
          return true;
      }

      return false;
    }

    #region IPdxSerializable Members

    public void FromData(IPdxReader reader)
    {      
      m_i1 = reader.ReadInt("i1");
      m_i2 = reader.ReadInt("i2");
      m_s1 = reader.ReadString("s1");
      bytes38000 = reader.ReadByteArray("bytes38000");
      m_i3 = reader.ReadInt("i3");
      m_i4 = reader.ReadInt("i4");
      m_s2 = reader.ReadString("s2");
    }

    public void ToData(IPdxWriter writer)
    {      
      writer.WriteInt("i1", m_i1);
      writer.WriteInt("i2", m_i2);
      writer.WriteString("s1", m_s1);
      writer.WriteByteArray("bytes38000", bytes38000);
      writer.WriteInt("i3", m_i3);
      writer.WriteInt("i4", m_i4);
      writer.WriteString("s2", m_s2);
    }

    #endregion
  }
    [Serializable]
  public class PdxTypes8 : IPdxSerializable
  {
    string m_s1 = "one";
    string m_s2 = "two";
    int m_i1 = 34324;
    byte[] bytes300 = new byte[300];
    pdxEnumTest _enum = pdxEnumTest.pdx2;
    int m_i2 = 2144;
    int m_i3 = 4645734;
    int m_i4 = 73567;

    public PdxTypes8()
    {

    }
    public static IPdxSerializable CreateDeserializable()
    {
      return new PdxTypes8();
    }
    public override int GetHashCode()
    {
      return base.GetHashCode();
    }

    public override bool Equals(object obj)
    {
      if (obj == null)
        return false;
      PdxTypes8 pap = obj as PdxTypes8;

      if (pap == null)
        return false;

      //if (pap == this)
        //return true;

      if (m_i1 == pap.m_i1
         && m_i2 == pap.m_i2
          && m_i3 == pap.m_i3
           && m_i4 == pap.m_i4
           && m_s1 == pap.m_s1
            && m_s2 == pap.m_s2
             && _enum == pap._enum)
      {
        if(bytes300.Length == pap.bytes300.Length)
          return true;
      }

      return false;
    }

    #region IPdxSerializable Members

    public void FromData(IPdxReader reader)
    {
      m_i1 = reader.ReadInt("i1");
      m_i2 = reader.ReadInt("i2");
      m_s1 = reader.ReadString("s1");
      bytes300 = reader.ReadByteArray("bytes300");
      _enum = (pdxEnumTest)reader.ReadObject("_enum");
      m_s2 = reader.ReadString("s2");
      m_i3 = reader.ReadInt("i3");
      m_i4 = reader.ReadInt("i4");      
    }

    public void ToData(IPdxWriter writer)
    {
      writer.WriteInt("i1", m_i1);
      writer.WriteInt("i2", m_i2);
      writer.WriteString("s1", m_s1);
      writer.WriteByteArray("bytes300", bytes300);
      writer.WriteObject("_enum", _enum);
      writer.WriteString("s2", m_s2);
      writer.WriteInt("i3", m_i3);
      writer.WriteInt("i4", m_i4);      
    }

    #endregion
  }
    [Serializable]
  public class PdxTypes9 : IPdxSerializable
  {
    string m_s1 = "one";
    string m_s2 = "two";
    string m_s3 = "three";
    byte[] m_bytes66000 = new byte[66000];
    string m_s4 = "four";
    string m_s5 = "five";
    
    public PdxTypes9()
    {

    }
    public static IPdxSerializable CreateDeserializable()
    {
      return new PdxTypes9();
    }
    public override int GetHashCode()
    {
      return base.GetHashCode();
    }

    public override bool Equals(object obj)
    {
      if (obj == null)
        return false;
      PdxTypes9 pap = obj as PdxTypes9;

      if (pap == null)
        return false;

      //if (pap == this)
        //return true;

      if (m_s1 == pap.m_s1
         && m_s2 == pap.m_s2
          && m_s3 == pap.m_s3
           && m_s4 == pap.m_s4
           && m_s5 == pap.m_s5)
      {
        if(m_bytes66000.Length == pap.m_bytes66000.Length )
          return true;
      }

      return false;
    }

    #region IPdxSerializable Members

    public void FromData(IPdxReader reader)
    {
      m_s1 = reader.ReadString("s1");
      m_s2 = reader.ReadString("s2");
      m_bytes66000 = reader.ReadByteArray("bytes66000");
      m_s3 = reader.ReadString("s3");
      m_s4 = reader.ReadString("s4");
      m_s5 = reader.ReadString("s5");
    }

    public void ToData(IPdxWriter writer)
    {
      writer.WriteString("s1", m_s1);
      writer.WriteString("s2", m_s2);
      writer.WriteByteArray("bytes66000", m_bytes66000);
      writer.WriteString("s3", m_s3);
      writer.WriteString("s4", m_s4);
      writer.WriteString("s5", m_s5);
    }

    #endregion
  }
    [Serializable]
  public class PdxTypes10 : IPdxSerializable
  {
    string m_s1 = "one";
    string m_s2 = "two";
    string m_s3 = "three";
    byte[] m_bytes66000 = new byte[66000];
    string m_s4 = "four";
    string m_s5 = "five";

    public PdxTypes10()
    {

    }
    public override int GetHashCode()
    {
      return base.GetHashCode();
    }

    public override bool Equals(object obj)
    {
      if (obj == null)
        return false;
      PdxTypes10 pap = obj as PdxTypes10;

      if (pap == null)
        return false;

      //if (pap == this)
      //return true;

      if (m_s1 == pap.m_s1
         && m_s2 == pap.m_s2
          && m_s3 == pap.m_s3
           && m_s4 == pap.m_s4
           && m_s5 == pap.m_s5)
      {
        if (m_bytes66000.Length == pap.m_bytes66000.Length)
          return true;
      }

      return false;
    }

    #region IPdxSerializable Members

    public void FromData(IPdxReader reader)
    {
      m_s1 = reader.ReadString("s1");
      m_s2 = reader.ReadString("s2");
      m_bytes66000 = reader.ReadByteArray("bytes66000");
      m_s3 = reader.ReadString("s3");
      m_s4 = reader.ReadString("s4");
      m_s5 = reader.ReadString("s5");
    }

    public void ToData(IPdxWriter writer)
    {
      writer.WriteString("s1", m_s1);
      writer.WriteString("s2", m_s2);
      writer.WriteByteArray("bytes66000", m_bytes66000);
      writer.WriteString("s3", m_s3);
      writer.WriteString("s4", m_s4);
      writer.WriteString("s5", m_s5);
    }

    #endregion
  }
  [Serializable]
  public class NestedPdx : IPdxSerializable
  {
    PdxTypes1 m_pd1 = new PdxTypes1();
    PdxTypes2 m_pd2 = new PdxTypes2();
    string m_s1 = "one";
    string m_s2 = "two";
    int m_i1 = 34324;
    int m_i2 = 2144;
    int m_i3 = 4645734;
    int m_i4 = 73567;

    public NestedPdx()
    {

    }
    public static IPdxSerializable CreateDeserializable()
    {
      return new NestedPdx();
    }
    public override int GetHashCode()
    {
      return base.GetHashCode();
    }

    public override bool Equals(object obj)
    {
      if (obj == null)
        return false;
      NestedPdx pap = obj as NestedPdx;

      if (pap == null)
        return false;

      //if (pap == this)
        //return true;

      if (m_i1 == pap.m_i1
         && m_i2 == pap.m_i2
          && m_i3 == pap.m_i3
           && m_i4 == pap.m_i4
           && m_s1 == pap.m_s1
            && m_s2 == pap.m_s2
             && m_pd1.Equals(pap.m_pd1)
              && m_pd2.Equals(pap.m_pd2))
        return true;

      return false;
    }

    #region IPdxSerializable Members

    public void FromData(IPdxReader reader)
    {
      m_i1 = reader.ReadInt("i1");
      m_pd1 = (PdxTypes1)reader.ReadObject("pd1");
      m_i2 = reader.ReadInt("i2");
      m_s1 = reader.ReadString("s1");
      m_s2 = reader.ReadString("s2");
      m_pd2 = (PdxTypes2)reader.ReadObject("pd2");
      m_i3 = reader.ReadInt("i3");
      m_i4 = reader.ReadInt("i4");      
    }

    public void ToData(IPdxWriter writer)
    {
      writer.WriteInt("i1", m_i1);
      writer.WriteObject("pd1", m_pd1);
      writer.WriteInt("i2", m_i2);
      writer.WriteString("s1", m_s1);
      writer.WriteString("s2", m_s2);
      writer.WriteObject("pd2", m_pd2);
      writer.WriteInt("i3", m_i3);
      writer.WriteInt("i4", m_i4);      
    }

    #endregion
  }
    [Serializable]
  public class PdxInsideIGFSerializable : IGFSerializable
  {
    NestedPdx m_npdx = new NestedPdx();
    PdxTypes3 m_pdx3 = new PdxTypes3();
    string m_s1 = "one";
    string m_s2 = "two";
    int m_i1 = 34324;
    int m_i2 = 2144;
    int m_i3 = 4645734;
    int m_i4 = 73567;


    public static IGFSerializable CreateDeserializable()
    {
      return new PdxInsideIGFSerializable();
    }
    public override int GetHashCode()
    {
      return base.GetHashCode();
    }

    public override bool Equals(object obj)
    {
      if (obj == null)
        return false;
      PdxInsideIGFSerializable pap = obj as PdxInsideIGFSerializable;

      if (pap == null)
        return false;

      //if (pap == this)
        //return true;

      if (m_i1 == pap.m_i1
         && m_i2 == pap.m_i2
          && m_i3 == pap.m_i3
           && m_i4 == pap.m_i4
           && m_s1 == pap.m_s1
            && m_s2 == pap.m_s2
             && m_npdx.Equals(pap.m_npdx)
              && m_pdx3.Equals(pap.m_pdx3))
        return true;

      return false;
    }

    #region IGFSerializable Members

    public uint ClassId
    {
      get { return 5005; }
    }

    public IGFSerializable FromData(DataInput input)
    {
      m_i1 = input.ReadInt32();
      m_npdx = (NestedPdx)input.ReadObject();
      m_i2 = input.ReadInt32();
      m_s1 = input.ReadUTF();
      m_s2 = input.ReadUTF();
      m_pdx3 = (PdxTypes3)input.ReadObject();
      m_i3 = input.ReadInt32();
      m_i4 = input.ReadInt32();
      return this;
    }

    public uint ObjectSize
    {
      get { return 0; }
    }

    public void ToData(DataOutput output)
    {
      output.WriteInt32( m_i1);
      output.WriteObject( m_npdx);
      output.WriteInt32( m_i2);
      output.WriteUTF(m_s1);
      output.WriteUTF(m_s2);
      output.WriteObject(m_pdx3);
      output.WriteInt32(m_i3);
      output.WriteInt32(m_i4); 
    }

    #endregion
  }

  #region Test class for all primitives types array
  public class AllPdxTypes : IPdxSerializable
  {
    private string _asciiNULL = null;
    private string _ascii0 = "";
    private string _ascii255 = new string('a', 255);
    private string _ascii35000 = new string('a', 35000);
    private string _ascii89000 = new string('a', 89000);

    private string _utf10 = new string((char)0x2345, 10);
    private string _utf255 = new string((char)0x2345, 255);
    private string _utf2000 = new string((char)0x2345, 2000);
    private string _utf4000 = new string((char)0x2345, 4000);

    private List<object> _listNULL = null;
    private List<object> _list0 = new List<object>();
    private List<object> _list252 = new List<object>();
    private List<object> _list253 = new List<object>();
    private List<object> _list35000 = new List<object>();
    private List<object> _list70000 = new List<object>();

    private List<object> _oalistNULL = null;
    private List<object> _oalist0 = new List<object>();
    private List<object> _oalist252 = new List<object>();
    private List<object> _oalist253 = new List<object>();
    private List<object> _oalist35000 = new List<object>();
    private List<object> _oalist70000 = new List<object>();

    private ArrayList _arraylistNULL = null;
    private ArrayList _arraylist0 = new ArrayList();
    private ArrayList _arraylist252 = new ArrayList();
    private ArrayList _arraylist253 = new ArrayList();
    private ArrayList _arraylist35000 = new ArrayList();
    private ArrayList _arraylist70000 = new ArrayList();

    private Hashtable _hashtableNULL = null;
    private Hashtable _hashtable0 = new Hashtable();
    private Hashtable _hashtable252 = new Hashtable();
    private Hashtable _hashtable253 = new Hashtable();
    private Hashtable _hashtable35000 = new Hashtable();
    private Hashtable _hashtable70000 = new Hashtable();

    private Dictionary<object, object> _dictNULL = null;
    private Dictionary<object, object> _dict0 = new Dictionary<object, object>();
    private Dictionary<object, object> _dict252 = new Dictionary<object, object>();
    private Dictionary<object, object> _dict253 = new Dictionary<object, object>();
    private Dictionary<object, object> _dict35000 = new Dictionary<object, object>();
    private Dictionary<object, object> _dict70000 = new Dictionary<object, object>();

    private string[] _stringArrayNULL = null;
    private string[] _stringArrayEmpty = new string[0];
    private string[] _stringArray252 = new string[252];
    private string[] _stringArray253 = new string[253];
    private string[] _stringArray255 = new string[255];
    private string[] _stringArray40000 = new string[40000];
    private string[] _stringArray70000 = new string[70000];

    private byte[] _byteArrayNULL = null;
    private byte[] _byteArrayEmpty = new byte[0];
    private byte[] _byteArray252 = new byte[252];
    private byte[] _byteArray253 = new byte[253];
    private byte[] _byteArray255 = new byte[255];
    private byte[] _byteArray40000 = new byte[40000];
    private byte[] _byteArray70000 = new byte[70000];

    private short[] _shortArrayNULL = null;
    private short[] _shortArrayEmpty = new short[0];
    private short[] _shortArray252 = new short[252];
    private short[] _shortArray253 = new short[253];
    private short[] _shortArray255 = new short[255];
    private short[] _shortArray40000 = new short[40000];
    private short[] _shortArray70000 = new short[70000];

    //int
    private int[] _intArrayNULL = null;
    private int[] _intArrayEmpty = new int[0];
    private int[] _intArray252 = new int[252];
    private int[] _intArray253 = new int[253];
    private int[] _intArray255 = new int[255];
    private int[] _intArray40000 = new int[40000];
    private int[] _intArray70000 = new int[70000];

    //long
    private long[] _longArrayNULL = null;
    private long[] _longArrayEmpty = new long[0];
    private long[] _longArray252 = new long[252];
    private long[] _longArray253 = new long[253];
    private long[] _longArray255 = new long[255];
    private long[] _longArray40000 = new long[40000];
    private long[] _longArray70000 = new long[70000];

    //double
    private double[] _doubleArrayNULL = null;
    private double[] _doubleArrayEmpty = new double[0];
    private double[] _doubleArray252 = new double[252];
    private double[] _doubleArray253 = new double[253];
    private double[] _doubleArray255 = new double[255];
    private double[] _doubleArray40000 = new double[40000];
    private double[] _doubleArray70000 = new double[70000];

    //float
    private float[] _floatArrayNULL = null;
    private float[] _floatArrayEmpty = new float[0];
    private float[] _floatArray252 = new float[252];
    private float[] _floatArray253 = new float[253];
    private float[] _floatArray255 = new float[255];
    private float[] _floatArray40000 = new float[40000];
    private float[] _floatArray70000 = new float[70000];

    //char
    private char[] _charArrayNULL = null;
    private char[] _charArrayEmpty = new char[0];
    private char[] _charArray252 = new char[252];
    private char[] _charArray253 = new char[253];
    private char[] _charArray255 = new char[255];
    private char[] _charArray40000 = new char[40000];
    private char[] _charArray70000 = new char[70000];

    private byte[][] _bytebytearrayNULL = null;
    private byte[][] _bytebytearrayEmpty = null;
    private byte[][] _bytebyteArray252 = null;
    private byte[][] _bytebyteArray253 = null;
    private byte[][] _bytebyteArray255 = null;
    private byte[][] _bytebyteArray40000 = null;
    private byte[][] _bytebyteArray70000 = null;

    public AllPdxTypes() { }
    public static IPdxSerializable Create() {
      return new AllPdxTypes();
    }
    public AllPdxTypes(bool initialize)
    {
      if (initialize)
        init();
    }
    private void init()
    {
      _list252 = new List<object>();
      for (int i = 0; i < 252; i++)
        _list252.Add(i);

      _list253 = new List<object>();
      for (int i = 0; i < 253; i++)
        _list253.Add(i);
      _list35000 = new List<object>();
      for (int i = 0; i < 35000; i++)
        _list35000.Add(i);

      _list70000 = new List<object>();
      for (int i = 0; i < 70000; i++)
        _list70000.Add(i);

      _oalist252 = new List<object>();
      for (int i = 0; i < 252; i++)
        _oalist252.Add(i);
      _oalist253 = new List<object>();
      for (int i = 0; i < 253; i++)
        _oalist253.Add(i);
      _oalist35000 = new List<object>();
      for (int i = 0; i < 35000; i++)
        _oalist35000.Add(i);
      _oalist70000 = new List<object>();
      for (int i = 0; i < 70000; i++)
        _oalist70000.Add(i);

      _arraylist252 = new ArrayList();
      for (int i = 0; i < 252; i++)
        _arraylist252.Add(i);
      _arraylist253 = new ArrayList();
      for (int i = 0; i < 253; i++)
        _arraylist253.Add(i);
      _arraylist35000 = new ArrayList();
      for (int i = 0; i < 35000; i++)
        _arraylist35000.Add(i);
      _arraylist70000 = new ArrayList();
      for (int i = 0; i < 70000; i++)
        _arraylist70000.Add(i);

      _hashtable252 = new Hashtable();
      for (int i = 0; i < 252; i++)
        _hashtable252.Add(i, i);
      _hashtable253 = new Hashtable();
      for (int i = 0; i < 253; i++)
        _hashtable253.Add(i, i);
      _hashtable35000 = new Hashtable();
      for (int i = 0; i < 35000; i++)
        _hashtable35000.Add(i, i);
      _hashtable70000 = new Hashtable();
      for (int i = 0; i < 70000; i++)
        _hashtable70000.Add(i, i);

      _dict252 = new Dictionary<object, object>();
      for (int i = 0; i < 252; i++)
        _dict252.Add(i, i);
      _dict253 = new Dictionary<object, object>();
      for (int i = 0; i < 253; i++)
        _dict253.Add(i, i);
      _dict35000 = new Dictionary<object, object>();
      for (int i = 0; i < 35000; i++)
        _dict35000.Add(i, i);
      _dict70000 = new Dictionary<object, object>();
      for (int i = 0; i < 70000; i++)
        _dict70000.Add(i, i);

      _stringArray252 = new string[252];
      for (int i = 0; i < 252; i++)
      {
        if (i % 2 == 0)
          _stringArray252[i] = new string('1', 1);
        else
          _stringArray252[i] = "";
      }
      _stringArray253 = new string[253];
      for (int i = 0; i < 253; i++)
      {
        if (i % 2 == 0)
          _stringArray253[i] = new string('1', 1);
        else
          _stringArray253[i] = "";
      }
      _stringArray255 = new string[255];
      for (int i = 0; i < 255; i++)
      {
        if (i % 2 == 0)
          _stringArray255[i] = new string('1', 1);
        else
          _stringArray255[i] = "";
      }
      _stringArray40000 = new string[40000];
      for (int i = 0; i < 40000; i++)
      {
        if (i % 2 == 0)
          _stringArray40000[i] = new string('1', 1);
        else
          _stringArray40000[i] = "";
      }
      _stringArray70000 = new string[70000];
      for (int i = 0; i < 70000; i++)
      {
        if (i % 2 == 0)
          _stringArray70000[i] = new string('1', 1);
        else
          _stringArray70000[i] = "";
      }


      _bytebytearrayEmpty = new byte[0][];
      _bytebyteArray252 = new byte[252][];
      _bytebyteArray253 = new byte[253][];
      _bytebyteArray255 = new byte[255][];
      _bytebyteArray40000 = new byte[40000][];
      _bytebyteArray70000 = new byte[70000][];
    }

    public override bool Equals(object obj)
    {
      AllPdxTypes other = obj as AllPdxTypes;
      if (other == null)
        return false;

      if (_asciiNULL != null || _asciiNULL != other._asciiNULL) return false;
      if (_ascii0 != "" || !_ascii0.Equals(other._ascii0)) return false;
      if (_ascii255.Length != 255 || !_ascii255.Equals(other._ascii255)) return false;
      if (_ascii35000.Length != 35000 || !_ascii35000.Equals(other._ascii35000)) return false;
      if (_ascii89000.Length != 89000 || !_ascii89000.Equals(other._ascii89000)) return false;


      if (!_utf10.Equals(other._utf10)) return false;
      if (!_utf255.Equals(other._utf255)) return false;
      if (!_utf2000.Equals(other._utf2000)) return false;
      if (!_utf4000.Equals(other._utf4000)) return false;

      if (_listNULL != null || _listNULL != other._listNULL) return false;
      if (_list0.Count != 0 || _list0.Count != other._list0.Count) return false;
      //_list252 
      if (_list252.Count != 252 || _list252.Count != other._list252.Count) return false;
      //_list253 
      if (_list253.Count != 253 || _list253.Count != other._list253.Count) return false;
      //_list35000
      if (_list35000.Count != 35000 || _list35000.Count != other._list35000.Count) return false;
      //_list70000
      if (_list70000.Count != 70000 || _list70000.Count != other._list70000.Count) return false;

      //_oalistNULL
      if (_oalistNULL != null || _oalistNULL != other._oalistNULL) return false;
      //_oalist0
      if (_oalist0.Count != 0 || _oalist0.Count != other._oalist0.Count) return false;
      //_oalist252 
      if (_oalist252.Count != 252 || _oalist252.Count != other._oalist252.Count) return false;
      //_oalist253 
      if (_oalist253.Count != 253 || _oalist253.Count != other._oalist253.Count) return false;
      //_oalist35000
      if (_oalist35000.Count != 35000 || _oalist35000.Count != other._oalist35000.Count) return false;
      //_oalist70000
      if (_oalist70000.Count != 70000 || _oalist70000.Count != other._oalist70000.Count) return false;

      //    _arraylistNULL
      if (_arraylistNULL != null || _arraylistNULL != other._arraylistNULL) return false;
      //_arraylist0
      if (_arraylist0.Count != 0 || _arraylist0.Count != other._arraylist0.Count) return false;
      //_arraylist252
      if (_arraylist252.Count != 252 || _arraylist252.Count != other._arraylist252.Count) return false;
      //_arraylist253
      if (_arraylist253.Count != 253 || _arraylist253.Count != other._arraylist253.Count) return false;
      //_arraylist35000
      if (_arraylist35000.Count != 35000 || _arraylist35000.Count != other._arraylist35000.Count) return false;
      //_arraylist70000
      if (_arraylist70000.Count != 70000 || _arraylist70000.Count != other._arraylist70000.Count) return false;

      //_hashtableNULL
      if (_hashtableNULL != null || _hashtableNULL != other._hashtableNULL) return false;
      //_hashtable0 
      if (_hashtable0.Count != 0 || _hashtable0.Count != other._hashtable0.Count) return false;
      //_hashtable252 
      if (_hashtable252.Count != 252 || _hashtable252.Count != other._hashtable252.Count) return false;
      //_hashtable253 
      if (_hashtable253.Count != 253 || _hashtable253.Count != other._hashtable253.Count) return false;
      //_hashtable35000 
      if (_hashtable35000.Count != 35000 || _hashtable35000.Count != other._hashtable35000.Count) return false;
      //_hashtable70000 
      if (_hashtable70000.Count != 70000 || _hashtable70000.Count != other._hashtable70000.Count) return false;

      //_dictNULL 
      if (_dictNULL != null || _dictNULL != other._dictNULL) return false;
      //_dict0 
      if (_dict0.Count != 0 || _dict0.Count != other._dict0.Count) return false;
      //_dict252 
      if (_dict252.Count != 252 || _dict252.Count != other._dict252.Count) return false;
      //_dict253 
      if (_dict253.Count != 253 || _dict253.Count != other._dict253.Count) return false;
      //_dict35000 
      if (_dict35000.Count != 35000 || _dict35000.Count != other._dict35000.Count) return false;
      //_dict70000 
      if (_dict70000.Count != 70000 || _dict70000.Count != other._dict70000.Count) return false;

      //_stringArrayNULL 
      if (_stringArrayNULL != null || _stringArrayNULL != other._stringArrayNULL) return false;
      //_stringArrayEmpty 
      if (_stringArrayEmpty.Length != 0 || _stringArrayEmpty.Length != other._stringArrayEmpty.Length) return false;
      //_stringArray252 
      if (_stringArray252.Length != 252 || _stringArray252.Length != other._stringArray252.Length) return false;
      //_stringArray253 
      if (_stringArray253.Length != 253 || _stringArray253.Length != other._stringArray253.Length) return false;
      //_stringArray255 
      if (_stringArray255.Length != 255 || _stringArray255.Length != other._stringArray255.Length) return false;
      //_stringArray40000 
      if (_stringArray40000.Length != 40000 || _stringArray40000.Length != other._stringArray40000.Length) return false;
      //_stringArray70000 
      if (_stringArray70000.Length != 70000 || _stringArray70000.Length != other._stringArray70000.Length) return false;

      //_byteArrayNULL 
      if (_byteArrayNULL != null && _byteArrayNULL != other._byteArrayNULL) return false;
      //_byteArrayEmpty 
      if (_byteArrayEmpty.Length != 0 || _byteArrayEmpty.Length != other._byteArrayEmpty.Length) return false;
      //_byteArray252 
      if (_byteArray252.Length != 252 || _byteArray252.Length != other._byteArray252.Length) return false;
      //_byteArray253 
      if (_byteArray253.Length != 253 || _byteArray253.Length != other._byteArray253.Length) return false;
      //_byteArray255 
      if (_byteArray255.Length != 255 || _byteArray255.Length != other._byteArray255.Length) return false;
      //_byteArray40000 
      if (_byteArray40000.Length != 40000 || _byteArray40000.Length != other._byteArray40000.Length) return false;
      //_byteArray70000 
      if (_byteArray70000.Length != 70000 || _byteArray70000.Length != other._byteArray70000.Length) return false;

      //_shortArrayNULL 
      if (_shortArrayNULL != null || _shortArrayNULL != other._shortArrayNULL) return false;
      //_shortArrayEmpty 
      if (_shortArrayEmpty.Length != 0 || _shortArrayEmpty.Length != other._shortArrayEmpty.Length) return false;
      //_shortArray252 
      if (_shortArray252.Length != 252 || _shortArray252.Length != other._shortArray252.Length) return false;
      //_shortArray253 
      if (_shortArray253.Length != 253 || _shortArray253.Length != other._shortArray253.Length) return false;
      //_shortArray255 
      if (_shortArray255.Length != 255 || _shortArray255.Length != other._shortArray255.Length) return false;
      //_shortArray40000 
      if (_shortArray40000.Length != 40000 || _shortArray40000.Length != other._shortArray40000.Length) return false;
      //_shortArray70000 
      if (_shortArray70000.Length != 70000 || _shortArray70000.Length != other._shortArray70000.Length) return false;

      //int
      //_intArrayNULL 
      if (_intArrayNULL != null || _intArrayNULL != other._intArrayNULL) return false;
      //_intArrayEmpty 
      if (_intArrayEmpty.Length != 0 || _intArrayEmpty.Length != other._intArrayEmpty.Length) return false;
      //_intArray252 
      if (_intArray252.Length != 252 || _intArray252.Length != other._intArray252.Length) return false;
      //_intArray253 
      if (_intArray253.Length != 253 || _intArray253.Length != other._intArray253.Length) return false;
      //_intArray255 
      if (_intArray255.Length != 255 || _intArray255.Length != other._intArray255.Length) return false;
      //_intArray40000 
      if (_intArray40000.Length != 40000 || _intArray40000.Length != other._intArray40000.Length) return false;
      //_intArray70000 
      if (_intArray70000.Length != 70000 || _intArray70000.Length != other._intArray70000.Length) return false;

      //long
      //_longArrayNULL 
      if (_longArrayNULL != null || _longArrayNULL != other._longArrayNULL) return false;
      //_longArrayEmpty 
      if (_longArrayEmpty.Length != 0 || _longArrayEmpty.Length != other._longArrayEmpty.Length) return false;
      //_longArray252 
      if (_longArray252.Length != 252 || _longArray252.Length != other._longArray252.Length) return false;
      //_longArray253 
      if (_longArray253.Length != 253 || _longArray253.Length != other._longArray253.Length) return false;
      //_longArray255 
      if (_longArray255.Length != 255 || _longArray255.Length != other._longArray255.Length) return false;
      //_longArray40000 
      if (_longArray40000.Length != 40000 || _longArray40000.Length != other._longArray40000.Length) return false;
      //_longArray70000 
      if (_longArray70000.Length != 70000 || _longArray70000.Length != other._longArray70000.Length) return false;

      //double
      //_doubleArrayNULL 
      if (_doubleArrayNULL != null || _doubleArrayNULL != other._doubleArrayNULL) return false;
      //_doubleArrayEmpty 
      if (_doubleArrayEmpty.Length != 0 || _doubleArrayEmpty.Length != other._doubleArrayEmpty.Length) return false;
      //_doubleArray252 
      if (_doubleArray252.Length != 252 || _doubleArray252.Length != other._doubleArray252.Length) return false;
      //_doubleArray253 
      if (_doubleArray253.Length != 253 || _doubleArray253.Length != other._doubleArray253.Length) return false;
      //_doubleArray255 
      if (_doubleArray255.Length != 255 || _doubleArray255.Length != other._doubleArray255.Length) return false;
      //_doubleArray40000 
      if (_doubleArray40000.Length != 40000 || _doubleArray40000.Length != other._doubleArray40000.Length) return false;
      //_doubleArray70000 
      if (_doubleArray70000.Length != 70000 || _doubleArray70000.Length != other._doubleArray70000.Length) return false;

      //float
      //_floatArrayNULL 
      if (_floatArrayNULL != null || _floatArrayNULL != other._floatArrayNULL) return false;
      //_floatArrayEmpty 
      if (_floatArrayEmpty.Length != 0 || _floatArrayEmpty.Length != other._floatArrayEmpty.Length) return false;
      //_floatArray252 
      if (_floatArray252.Length != 252 || _floatArray252.Length != other._floatArray252.Length) return false;
      //_floatArray253 
      if (_floatArray253.Length != 253 || _floatArray253.Length != other._floatArray253.Length) return false;
      //_floatArray255 
      if (_floatArray255.Length != 255 || _floatArray255.Length != other._floatArray255.Length) return false;
      //_floatArray40000 
      if (_floatArray40000.Length != 40000 || _floatArray40000.Length != other._floatArray40000.Length) return false;
      //_floatArray70000 
      if (_floatArray70000.Length != 70000 || _floatArray70000.Length != other._floatArray70000.Length) return false;

      //char
      //_charArrayNULL 
      if (_charArrayNULL != null || _charArrayNULL != other._charArrayNULL) return false;
      //_charArrayEmpty 
      if (_charArrayEmpty.Length != 0 || _charArrayEmpty.Length != other._charArrayEmpty.Length) return false;
      //_charArray252 
      if (_charArray252.Length != 252 || _charArray252.Length != other._charArray252.Length) return false;
      //_charArray253 
      if (_charArray253.Length != 253 || _charArray253.Length != other._charArray253.Length) return false;

      //_charArray255 
      if (_charArray255.Length != 255 || _charArray255.Length != other._charArray255.Length) return false;
      //_charArray40000 
      if (_charArray40000.Length != 40000 || _charArray40000.Length != other._charArray40000.Length) return false;
      //_charArray70000 
      if (_charArray70000.Length != 70000 || _charArray70000.Length != other._charArray70000.Length) return false;

      //_bytebytearrayNULL
      if (_bytebytearrayNULL != null || _bytebytearrayNULL != other._bytebytearrayNULL) return false;
      //_bytebytearrayEmpty 
      if (_bytebytearrayEmpty.Length != 0 || _bytebytearrayEmpty.Length != other._bytebytearrayEmpty.Length) return false;
      //_bytebyteArray252 
      if (_bytebyteArray252.Length != 252 || _bytebyteArray252.Length != other._bytebyteArray252.Length) return false;
      //_bytebyteArray253 
      if (_bytebyteArray253.Length != 253 || _bytebyteArray253.Length != other._bytebyteArray253.Length) return false;
      //_bytebyteArray255 
      if (_bytebyteArray255.Length != 255 || _bytebyteArray255.Length != other._bytebyteArray255.Length) return false;
      //_bytebyteArray40000 
      if (_bytebyteArray40000.Length != 40000 || _bytebyteArray40000.Length != other._bytebyteArray40000.Length) return false;
      //_bytebyteArray70000 
      if (_bytebyteArray70000.Length != 70000 || _bytebyteArray70000.Length != other._bytebyteArray70000.Length) return false;

      return true;
    }

    #region IPdxSerializable Members

    public void FromData(IPdxReader reader)
    {
      _asciiNULL = reader.ReadString("_asciiNULL");
      _ascii0 = reader.ReadString("_ascii0");
      _ascii255 = reader.ReadString("_ascii255");
      _ascii35000 = reader.ReadString("_ascii35000");
      _ascii89000 = reader.ReadString("_ascii89000");

      _utf10 = reader.ReadString("_utf10");
      _utf255 = reader.ReadString("_utf255");
      _utf2000 = reader.ReadString("_utf2000");
      _utf4000 = reader.ReadString("_utf4000");

      _listNULL = (List<object>)reader.ReadObject("_listNULL");
      _list0 = (List<object>)reader.ReadObject("_list0");
      _list252 = (List<object>)reader.ReadObject("_list252");
      _list253 = (List<object>)reader.ReadObject("_list253");
      _list35000 = (List<object>)reader.ReadObject("_list35000");
      _list70000 = (List<object>)reader.ReadObject("_list70000");

      _oalistNULL = reader.ReadObjectArray("_oalistNULL");
      _oalist0 = reader.ReadObjectArray("_oalist0");
      _oalist252 = reader.ReadObjectArray("_oalist252");
      _oalist253 = reader.ReadObjectArray("_oalist253");
      _oalist35000 = reader.ReadObjectArray("_oalist35000");
      _oalist70000 = reader.ReadObjectArray("_oalist70000");

      _arraylistNULL = (ArrayList)reader.ReadObject("_arraylistNULL");
      _arraylist0 = (ArrayList)reader.ReadObject("_arraylist0");
      _arraylist252 = (ArrayList)reader.ReadObject("_arraylist252");
      _arraylist253 = (ArrayList)reader.ReadObject("_arraylist253");
      _arraylist35000 = (ArrayList)reader.ReadObject("_arraylist35000");
      _arraylist70000 = (ArrayList)reader.ReadObject("_arraylist70000");

      _hashtableNULL = (Hashtable)reader.ReadObject("_hashtableNULL");
      _hashtable0 = (Hashtable)reader.ReadObject("_hashtable0");
      _hashtable252 = (Hashtable)reader.ReadObject("_hashtable252");
      _hashtable253 = (Hashtable)reader.ReadObject("_hashtable253");
      _hashtable35000 = (Hashtable)reader.ReadObject("_hashtable35000");
      _hashtable70000 = (Hashtable)reader.ReadObject("_hashtable70000");

      _dictNULL = (Dictionary<object, object>)reader.ReadObject("_dictNULL");
      _dict0 = (Dictionary<object, object>)reader.ReadObject("_dict0");
      _dict252 = (Dictionary<object, object>)reader.ReadObject("_dict252");
      _dict253 = (Dictionary<object, object>)reader.ReadObject("_dict253");
      _dict35000 = (Dictionary<object, object>)reader.ReadObject("_dict35000");
      _dict70000 = (Dictionary<object, object>)reader.ReadObject("_dict70000");

      _stringArrayNULL = reader.ReadStringArray("_stringArrayNULL");
      _stringArrayEmpty = reader.ReadStringArray("_stringArrayEmpty");
      _stringArray252 = reader.ReadStringArray("_stringArray252");
      _stringArray253 = reader.ReadStringArray("_stringArray253");
      _stringArray255 = reader.ReadStringArray("_stringArray255");
      _stringArray40000 = reader.ReadStringArray("_stringArray40000");
      _stringArray70000 = reader.ReadStringArray("_stringArray70000");

      _byteArrayNULL = reader.ReadByteArray("_byteArrayNULL");
      _byteArrayEmpty = reader.ReadByteArray("_byteArrayEmpty");
      _byteArray252 = reader.ReadByteArray("_byteArray252");
      _byteArray253 = reader.ReadByteArray("_byteArray253");
      _byteArray255 = reader.ReadByteArray("_byteArray255");
      _byteArray40000 = reader.ReadByteArray("_byteArray40000");
      _byteArray70000 = reader.ReadByteArray("_byteArray70000");

      _shortArrayNULL = reader.ReadShortArray("_shortArrayNULL");
      _shortArrayEmpty = reader.ReadShortArray("_shortArrayEmpty");
      _shortArray252 = reader.ReadShortArray("_shortArray252");
      _shortArray253 = reader.ReadShortArray("_shortArray253");
      _shortArray255 = reader.ReadShortArray("_shortArray255");
      _shortArray40000 = reader.ReadShortArray("_shortArray40000");
      _shortArray70000 = reader.ReadShortArray("_shortArray70000");

      //int
      _intArrayNULL = reader.ReadIntArray("_intArrayNULL");
      _intArrayEmpty = reader.ReadIntArray("_intArrayEmpty");
      _intArray252 = reader.ReadIntArray("_intArray252");
      _intArray253 = reader.ReadIntArray("_intArray253");
      _intArray255 = reader.ReadIntArray("_intArray255");
      _intArray40000 = reader.ReadIntArray("_intArray40000");
      _intArray70000 = reader.ReadIntArray("_intArray70000");

      //long
      _longArrayNULL = reader.ReadLongArray("_longArrayNULL");
      _longArrayEmpty = reader.ReadLongArray("_longArrayEmpty");
      _longArray252 = reader.ReadLongArray("_longArray252");
      _longArray253 = reader.ReadLongArray("_longArray253");
      _longArray255 = reader.ReadLongArray("_longArray255");
      _longArray40000 = reader.ReadLongArray("_longArray40000");
      _longArray70000 = reader.ReadLongArray("_longArray70000");

      //double
      _doubleArrayNULL = reader.ReadDoubleArray("_doubleArrayNULL");
      _doubleArrayEmpty = reader.ReadDoubleArray("_doubleArrayEmpty");
      _doubleArray252 = reader.ReadDoubleArray("_doubleArray252");
      _doubleArray253 = reader.ReadDoubleArray("_doubleArray253");
      _doubleArray255 = reader.ReadDoubleArray("_doubleArray255");
      _doubleArray40000 = reader.ReadDoubleArray("_doubleArray40000");
      _doubleArray70000 = reader.ReadDoubleArray("_doubleArray70000");

      //float
      _floatArrayNULL = reader.ReadFloatArray("_floatArrayNULL");
      _floatArrayEmpty = reader.ReadFloatArray("_floatArrayEmpty");
      _floatArray252 = reader.ReadFloatArray("_floatArray252");
      _floatArray253 = reader.ReadFloatArray("_floatArray253");
      _floatArray255 = reader.ReadFloatArray("_floatArray255");
      _floatArray40000 = reader.ReadFloatArray("_floatArray40000");
      _floatArray70000 = reader.ReadFloatArray("_floatArray70000");

      //char
      _charArrayNULL = reader.ReadCharArray("_charArrayNULL");
      _charArrayEmpty = reader.ReadCharArray("_charArrayEmpty");
      _charArray252 = reader.ReadCharArray("_charArray252");
      _charArray253 = reader.ReadCharArray("_charArray253");
      _charArray255 = reader.ReadCharArray("_charArray255");
      _charArray40000 = reader.ReadCharArray("_charArray40000");
      _charArray70000 = reader.ReadCharArray("_charArray70000");

      _bytebytearrayNULL = reader.ReadArrayOfByteArrays("_bytebytearrayNULL");
      _bytebytearrayEmpty = reader.ReadArrayOfByteArrays("_bytebytearrayEmpty");
      _bytebyteArray252 = reader.ReadArrayOfByteArrays("_bytebyteArray252");
      _bytebyteArray253 = reader.ReadArrayOfByteArrays("_bytebyteArray253");
      _bytebyteArray255 = reader.ReadArrayOfByteArrays("_bytebyteArray255");
      _bytebyteArray40000 = reader.ReadArrayOfByteArrays("_bytebyteArray40000");
      _bytebyteArray70000 = reader.ReadArrayOfByteArrays("_bytebyteArray70000");
    }

    public void ToData(IPdxWriter writer)
    {
      writer.WriteString("_asciiNULL", _asciiNULL);
      writer.WriteString("_ascii0", _ascii0);
      writer.WriteString("_ascii255", _ascii255);
      writer.WriteString("_ascii35000", _ascii35000);
      writer.WriteString("_ascii89000", _ascii89000);

      writer.WriteString("_utf10", _utf10);
      writer.WriteString("_utf255", _utf255);
      writer.WriteString("_utf2000", _utf2000);
      writer.WriteString("_utf4000", _utf4000);

      writer.WriteObject("_listNULL", _listNULL);
      writer.WriteObject("_list0", _list0);
      writer.WriteObject("_list252", _list252);
      writer.WriteObject("_list253", _list253);
      writer.WriteObject("_list35000", _list35000);
      writer.WriteObject("_list70000", _list70000);

      writer.WriteObjectArray("_oalistNULL", _oalistNULL);
      writer.WriteObjectArray("_oalist0", _oalist0);
      writer.WriteObjectArray("_oalist252", _oalist252);
      writer.WriteObjectArray("_oalist253", _oalist253);
      writer.WriteObjectArray("_oalist35000", _oalist35000);
      writer.WriteObjectArray("_oalist70000", _oalist70000);

      writer.WriteObject("_arraylistNULL", _arraylistNULL);
      writer.WriteObject("_arraylist0", _arraylist0);
      writer.WriteObject("_arraylist252", _arraylist252);
      writer.WriteObject("_arraylist253", _arraylist253);
      writer.WriteObject("_arraylist35000", _arraylist35000);
      writer.WriteObject("_arraylist70000", _arraylist70000);

      writer.WriteObject("_hashtableNULL", _hashtableNULL);
      writer.WriteObject("_hashtable0", _hashtable0);
      writer.WriteObject("_hashtable252", _hashtable252);
      writer.WriteObject("_hashtable253", _hashtable253);
      writer.WriteObject("_hashtable35000", _hashtable35000);
      writer.WriteObject("_hashtable70000", _hashtable70000);

      writer.WriteObject("_dictNULL", _dictNULL);
      writer.WriteObject("_dict0", _dict0);
      writer.WriteObject("_dict252", _dict252);
      writer.WriteObject("_dict253", _dict253);
      writer.WriteObject("_dict35000", _dict35000);
      writer.WriteObject("_dict70000", _dict70000);

      writer.WriteStringArray("_stringArrayNULL", _stringArrayNULL);
      writer.WriteStringArray("_stringArrayEmpty", _stringArrayEmpty);
      writer.WriteStringArray("_stringArray252", _stringArray252);
      writer.WriteStringArray("_stringArray253", _stringArray253);
      writer.WriteStringArray("_stringArray255", _stringArray255);
      writer.WriteStringArray("_stringArray40000", _stringArray40000);
      writer.WriteStringArray("_stringArray70000", _stringArray70000);

      writer.WriteByteArray("_byteArrayNULL", _byteArrayNULL);
      writer.WriteByteArray("_byteArrayEmpty", _byteArrayEmpty);
      writer.WriteByteArray("_byteArray252", _byteArray252);
      writer.WriteByteArray("_byteArray253", _byteArray253);
      writer.WriteByteArray("_byteArray255", _byteArray255);
      writer.WriteByteArray("_byteArray40000", _byteArray40000);
      writer.WriteByteArray("_byteArray70000", _byteArray70000);

      writer.WriteShortArray("_shortArrayNULL", _shortArrayNULL);
      writer.WriteShortArray("_shortArrayEmpty", _shortArrayEmpty);
      writer.WriteShortArray("_shortArray252", _shortArray252);
      writer.WriteShortArray("_shortArray253", _shortArray253);
      writer.WriteShortArray("_shortArray255", _shortArray255);
      writer.WriteShortArray("_shortArray40000", _shortArray40000);
      writer.WriteShortArray("_shortArray70000", _shortArray70000);

      //int
      writer.WriteIntArray("_intArrayNULL", _intArrayNULL);
      writer.WriteIntArray("_intArrayEmpty", _intArrayEmpty);
      writer.WriteIntArray("_intArray252", _intArray252);
      writer.WriteIntArray("_intArray253", _intArray253);
      writer.WriteIntArray("_intArray255", _intArray255);
      writer.WriteIntArray("_intArray40000", _intArray40000);
      writer.WriteIntArray("_intArray70000", _intArray70000);

      //long
      writer.WriteLongArray("_longArrayNULL", _longArrayNULL);
      writer.WriteLongArray("_longArrayEmpty", _longArrayEmpty);
      writer.WriteLongArray("_longArray252", _longArray252);
      writer.WriteLongArray("_longArray253", _longArray253);
      writer.WriteLongArray("_longArray255", _longArray255);
      writer.WriteLongArray("_longArray40000", _longArray40000);
      writer.WriteLongArray("_longArray70000", _longArray70000);

      //double
      writer.WriteDoubleArray("_doubleArrayNULL", _doubleArrayNULL);
      writer.WriteDoubleArray("_doubleArrayEmpty", _doubleArrayEmpty);
      writer.WriteDoubleArray("_doubleArray252", _doubleArray252);
      writer.WriteDoubleArray("_doubleArray253", _doubleArray253);
      writer.WriteDoubleArray("_doubleArray255", _doubleArray255);
      writer.WriteDoubleArray("_doubleArray40000", _doubleArray40000);
      writer.WriteDoubleArray("_doubleArray70000", _doubleArray70000);

      //float
      writer.WriteFloatArray("_floatArrayNULL", _floatArrayNULL);
      writer.WriteFloatArray("_floatArrayEmpty", _floatArrayEmpty);
      writer.WriteFloatArray("_floatArray252", _floatArray252);
      writer.WriteFloatArray("_floatArray253", _floatArray253);
      writer.WriteFloatArray("_floatArray255", _floatArray255);
      writer.WriteFloatArray("_floatArray40000", _floatArray40000);
      writer.WriteFloatArray("_floatArray70000", _floatArray70000);

      //char
      writer.WriteCharArray("_charArrayNULL", _charArrayNULL);
      writer.WriteCharArray("_charArrayEmpty", _charArrayEmpty);
      writer.WriteCharArray("_charArray252", _charArray252);
      writer.WriteCharArray("_charArray253", _charArray253);
      writer.WriteCharArray("_charArray255", _charArray255);
      writer.WriteCharArray("_charArray40000", _charArray40000);
      writer.WriteCharArray("_charArray70000", _charArray70000);

      writer.WriteArrayOfByteArrays("_bytebytearrayNULL", _bytebytearrayNULL);
      writer.WriteArrayOfByteArrays("_bytebytearrayEmpty", _bytebytearrayEmpty);
      writer.WriteArrayOfByteArrays("_bytebyteArray252", _bytebyteArray252);
      writer.WriteArrayOfByteArrays("_bytebyteArray253", _bytebyteArray253);
      writer.WriteArrayOfByteArrays("_bytebyteArray255", _bytebyteArray255);
      writer.WriteArrayOfByteArrays("_bytebyteArray40000", _bytebyteArray40000);
      writer.WriteArrayOfByteArrays("_bytebyteArray70000", _bytebyteArray70000);
    }

    #endregion
  }
  #endregion
}
