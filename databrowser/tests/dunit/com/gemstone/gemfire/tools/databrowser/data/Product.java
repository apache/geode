package com.gemstone.gemfire.tools.databrowser.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;

public class Product implements DataSerializable {

  private int id;
  private String name;
  private float price;
  
  public Product() {
    //Do nothing.
  }
  
  public Product(int id, String name, float price) {
    this.id = id;
    this.name = name;
    this.price = price;
  }
    
  public int getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public float getPrice() {
    return price;
  }

  public void fromData(DataInput arg0) throws IOException,
      ClassNotFoundException {
    this.id = DataSerializer.readPrimitiveInt(arg0);
    this.name = DataSerializer.readString(arg0);
    this.price = DataSerializer.readPrimitiveFloat(arg0);
  }

  public void toData(DataOutput arg0) throws IOException {
    DataSerializer.writePrimitiveInt(this.id, arg0);
    DataSerializer.writeString(this.name, arg0);
    DataSerializer.writeFloat(this.price, arg0);
  }

}
