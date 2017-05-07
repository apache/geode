/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package PdxTests;
import com.gemstone.gemfire.pdx.PdxReader;
import com.gemstone.gemfire.pdx.PdxSerializable;
import com.gemstone.gemfire.pdx.PdxWriter;


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
      System.out.println("hitesh in address equals ");
      if (obj == null)
        return false;
      System.out.println("hitesh in address equals1 ");
      if(!(obj instanceof Address))
        return false;
      System.out.println("hitesh in address equals2 ");
      Address other = (Address)obj;
      if (other == null)
        return false;
      System.out.println("hitesh in address equals3 ");
      System.out.println("hitesh " + this.toString());
      System.out.println("hitesh " + other.toString());
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