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
public class PdxTypes7 implements PdxSerializable
{
  String m_s1 = "one";
  String m_s2 = "two";
  int m_i1 = 34324;
  byte[] bytes38000 = new byte[38000];
  int m_i2 = 2144;
  int m_i3 = 4645734;
  int m_i4 = 73567;

  public PdxTypes7()
  {

  }
  public static PdxSerializable CreateDeserializable()
  {
    return new PdxTypes7();
  }
  
  @Override
  public boolean equals(Object obj)
  {
    if (obj == null)
      return false;
    if(!(obj instanceof PdxTypes7))
      return false;
    PdxTypes7 pap = (PdxTypes7)obj;

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
      if(bytes38000.length == pap.bytes38000.length)
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
    bytes38000 = reader.readByteArray("bytes38000");
    m_i3 = reader.readInt("i3");
    m_i4 = reader.readInt("i4");
    m_s2 = reader.readString("s2");
  }

  @Override
  public void toData(PdxWriter writer)
  {      
    writer.writeInt("i1", m_i1);
    writer.writeInt("i2", m_i2);
    writer.writeString("s1", m_s1);
    writer.writeByteArray("bytes38000", bytes38000);
    writer.writeInt("i3", m_i3);
    writer.writeInt("i4", m_i4);
    writer.writeString("s2", m_s2);
  }

  
}
