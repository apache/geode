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


public class Address implements PdxSerializable
  {
    int _aptNumber;
    String _street;
    String _city;

    public Address()
    { }
    public Address(int aptN, String street, String city)
    {
      _aptNumber = aptN;
      _street = street;
      _city = city;
    }
    
     @Override
    public String toString() {
    // TODO Auto-generated method stub
    return String.valueOf(_aptNumber) + _street + _city;
    }

    public boolean equals(Object obj)
    {
      if (obj == null)
        return false;
      if(!(obj instanceof Address))
        return false;
      Address other = (Address)obj;
      if (other == null)
        return false;
      if (_aptNumber == other._aptNumber
          && _street.equals( other._street)
            && _city.equals(other._city))
        return true;
      return false;
    }

    
    public void fromData(PdxReader reader)
    {
      _aptNumber = reader.readInt("_aptNumber");
      _street = reader.readString("_street");
      _city = reader.readString("_city");
    }

    public void toData(PdxWriter writer)
    {
      writer.writeInt("_aptNumber", _aptNumber);
      writer.writeString("_street", _street);
      writer.writeString("_city", _city);
    }
  }
