package com.gemstone.databrowser.objects;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;

/**
 * customer ObjectType.
 * 
 * @author vreddy
 * 
 */
public class Customer implements DataSerializable {

  private String cust_name;

  private Address address;

  private int[] contact;

  public Customer() {

  }

  public Customer(String cust_name, Address address, int[] contact) {
    this.cust_name = cust_name;
    this.address = address;
    this.contact = contact;
  }

  public Address getAddress() {
    return address;
  }

  public void setAddress(Address address) {
    this.address = address;
  }

  public int[] getContact() {
    return contact;
  }

  public void setContact(int[] contact) {
    this.contact = contact;
  }

  public String getName() {
    return cust_name;
  }

  public void setName(String cust_name) {
    this.cust_name = cust_name;
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.cust_name = DataSerializer.readString(in);
    this.address = (Address)DataSerializer.readObject(in);
    this.contact = DataSerializer.readIntArray(in);
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.cust_name, out);
    DataSerializer.writeObject(this.address, out);
    DataSerializer.writeIntArray(this.contact, out);
  }

  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("Customer [ cust_name :" + this.cust_name + " , address :"
        + this.address + " , contact : " + this.contact + "]");
    return buffer.toString();
  }

}
