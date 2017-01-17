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
public class PdxTypes9 implements PdxSerializable
{
  String m_s1 = "one";
  String m_s2 = "two";
  String m_s3 = "three";
  byte[] m_bytes66000 = new byte[66000];
  String m_s4 = "four";
  String m_s5 = "five";
  
  public PdxTypes9()
  {

  }
  
  @Override
  public boolean equals(Object obj)
  {    
    if (obj == null)
      return false;
    if(!(obj instanceof PdxTypes9))
      return false;
    PdxTypes9 pap = (PdxTypes9)obj;

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
      if(m_bytes66000.length == pap.m_bytes66000.length )
        return true;
    }

    return false;
  }

  @Override
  public void fromData(PdxReader reader)
  {
    m_s1 = reader.readString("s1");
    m_s2 = reader.readString("s2");
    m_bytes66000 = reader.readByteArray("bytes66000");
    m_s3 = reader.readString("s3");
    m_s4 = reader.readString("s4");
    m_s5 = reader.readString("s5");
  }

  @Override
  public void toData(PdxWriter writer)
  {
    writer.writeString("s1", m_s1);
    writer.writeString("s2", m_s2);
    writer.writeByteArray("bytes66000", m_bytes66000);
    writer.writeString("s3", m_s3);
    writer.writeString("s4", m_s4);
    writer.writeString("s5", m_s5);
  }

}
