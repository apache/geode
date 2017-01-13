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

public class PdxTypes1 implements PdxSerializable
{
  int m_i1 = 34324;
  int m_i2 = 2144;
  int m_i3 = 4645734;
  int m_i4 = 73567;

  public PdxTypes1()
  { 
  
  }

  public static PdxSerializable CreateDeserializable()
  {
    return new PdxTypes1();
  }

  @Override
  public int hashCode()
  {
    return super.hashCode();
  }
  
  @Override
  public boolean equals(Object obj)
  {
    if (obj == null)
      return false;
    
    if(!(obj instanceof PdxTypes1))
      return false;
    
    PdxTypes1 pap = (PdxTypes1)obj ;

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

  @Override
  public void fromData(PdxReader reader)
  {
    m_i1 = reader.readInt("i1");
    m_i2 = reader.readInt("i2");
    m_i3 = reader.readInt("i3");
    m_i4 = reader.readInt("i4");
  }

  @Override
  public void toData(PdxWriter writer)
  {
    writer.writeInt("i1", m_i1);
    writer.writeInt("i2", m_i2);
    writer.writeInt("i3", m_i3);
    writer.writeInt("i4", m_i4);
  }

 
}
