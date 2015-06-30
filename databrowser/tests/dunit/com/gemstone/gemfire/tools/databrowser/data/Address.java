package com.gemstone.gemfire.tools.databrowser.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;

public class Address implements DataSerializable {

  private String street1;
  private String city;
  private int postalCode; 
  
  
  public Address() {
    this.street1 = null;
    this.city = null;
    this.postalCode = 0;
  }
   
  public Address(String street1 , String city , int postalCode) {
    super();
    this.city = city;
    this.postalCode = postalCode;
    this.street1 = street1;
  }

  public String getStreet1() {
    return street1;
  }

  public String getCity() {
    return city;
  }

  public int getPostalCode() {
    return postalCode;
  }

  public void fromData(DataInput arg0) throws IOException,
      ClassNotFoundException {
    this.street1 = DataSerializer.readString(arg0) ;
    this.city = DataSerializer.readString(arg0) ;
    this.postalCode = DataSerializer.readPrimitiveInt(arg0);
  }
  
  public void toData(DataOutput arg0) throws IOException {
    DataSerializer.writeString(this.street1, arg0);
    DataSerializer.writeString(this.city, arg0);
    DataSerializer.writePrimitiveInt(this.postalCode, arg0);
  }

}
