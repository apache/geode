package com.gemstone.gemfire.tools.databrowser.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;

/**
 * Test objects.
 * 
 * @author vreddy
 * 
 */
public class Customer implements DataSerializable {

  //State of this object.
  private Integer id;
  private String name;
  private String address;  

  public Customer() {
    //Do nothing.
  }

  public Customer(int cid, String cnm, String cads) {
    this.id = Integer.valueOf(cid);
    this.name = cnm;
    this.address = cads;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setAddress(String address) {
    this.address = address;
  }

  public Integer getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public String getAddress() {
    return address;
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeInteger(id, out);
    DataSerializer.writeString(name, out);
    DataSerializer.writeString(address, out);
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.id = DataSerializer.readInteger(in);
    this.name = DataSerializer.readString(in);
    this.address = DataSerializer.readString(in);
  }

  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("Customer [ name :"+this.name+" , address :"+this.address+"]");
    return buffer.toString();
  }
}
