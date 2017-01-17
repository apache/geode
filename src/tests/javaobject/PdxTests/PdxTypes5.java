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
package PdxTests;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;
public class PdxTypes5 implements PdxSerializable
{
  String m_s1 = "one";
  String m_s2 = "two";
  int m_i1 = 34324;
  int m_i2 = 2144;
  int m_i3 = 4645734;
  int m_i4 = 73567;

  public PdxTypes5()
  {

  }
  public static PdxSerializable CreateDeserializable()
  {
    return new PdxTypes5();
  }
  
  @Override
  public boolean equals(Object obj)
  {
    if (obj == null)
      return false;
    if(!(obj instanceof PdxTypes5))
      return false;
    PdxTypes5 pap = (PdxTypes5)obj;

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

  @Override
  public void fromData(PdxReader reader)
  {
    m_s1 = reader.readString("s1");
    m_s2 = reader.readString("s2");
    m_i1 = reader.readInt("i1");
    m_i2 = reader.readInt("i2");      
    m_i3 = reader.readInt("i3");
    m_i4 = reader.readInt("i4");

  }

  @Override
  public void toData(PdxWriter writer)
  {
    writer.writeString("s1", m_s1);
    writer.writeString("s2", m_s2);
    writer.writeInt("i1", m_i1);
    writer.writeInt("i2", m_i2);      
    writer.writeInt("i3", m_i3);
    writer.writeInt("i4", m_i4);
  }

  
}
