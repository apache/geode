package com.gemstone.gemfire.tools.databrowser.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;

public class GemStoneCustomer implements DataSerializable {
  
  private int id;
  private String name;
  private Address address;
  private ArrayList<Product> products;
  
  public GemStoneCustomer() {
    super();
    this.id = 0;
    this.name = null;
    this.address = null;
    this.products = null;
  }
    
  public GemStoneCustomer(int id, String name,Address address,
      ArrayList<Product> products) {
    super();
    this.address = address;
    this.id = id;
    this.name = name;
    this.products = products;
  }

  public List<Product> getProducts() {
    return this.products;
  }
    
  public int getId() {
    return this.id;
  }

  public String getName() {
    return this.name;
  }

  public Address getAddress() {
    return this.address;
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeInteger(id, out);
    DataSerializer.writeString(name, out);
    DataSerializer.writeObject(this.address, out);
    DataSerializer.writeArrayList(this.products, out);
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.id = DataSerializer.readInteger(in);
    this.name = DataSerializer.readString(in);
    this.address = (Address)DataSerializer.readObject(in);
    this.products = DataSerializer.readArrayList(in);
  }
}
