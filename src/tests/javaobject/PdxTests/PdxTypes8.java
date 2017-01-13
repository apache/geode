/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package PdxTests;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;
public class PdxTypes8 implements PdxSerializable
{
  //public enum pdxEnumTest { pdx1, pdx2, pdx3};
  String m_s1 = "one";
  String m_s2 = "two";
  int m_i1 = 34324;
  byte[] bytes300 = new byte[300];
  pdxEnumTest _enum = pdxEnumTest.pdx2;
  int m_i2 = 2144;
  int m_i3 = 4645734;
  int m_i4 = 73567;

  public PdxTypes8()
  {

  }
  public static PdxSerializable CreateDeserializable()
  {
    return new PdxTypes8();
  }
  
  @Override
  public boolean equals(Object obj)
  {
    if (obj == null)
      return false;
    if(!(obj instanceof PdxTypes8))
      return false;
    PdxTypes8 pap = (PdxTypes8)obj;

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
      if(bytes300.length == pap.bytes300.length)
        return true;
    }

    return false;
  }

  @Override
  public void fromData(PdxReader reader)
  {
    m_i1 = reader.readInt("i1");
    m_i2 = reader.readInt("i2");
    m_s1 = reader.readString("s1");
    bytes300 = reader.readByteArray("bytes300");
    _enum = (pdxEnumTest)reader.readObject("_enum");
    m_s2 = reader.readString("s2");
    m_i3 = reader.readInt("i3");
    m_i4 = reader.readInt("i4");      
  }

  @Override
  public void toData(PdxWriter writer)
  {
    writer.writeInt("i1", m_i1);
    writer.writeInt("i2", m_i2);
    writer.writeString("s1", m_s1);
    writer.writeByteArray("bytes300", bytes300);
    writer.writeObject("_enum", _enum);
    writer.writeString("s2", m_s2);
    writer.writeInt("i3", m_i3);
    writer.writeInt("i4", m_i4);      
  }
 
}
