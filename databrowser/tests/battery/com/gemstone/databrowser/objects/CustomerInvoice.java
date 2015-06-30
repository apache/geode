package com.gemstone.databrowser.objects;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;

/**
 * CustomerInvoice ObjectType.
 * 
 * @author vreddy
 * 
 */
public class CustomerInvoice implements DataSerializable {

  private int id;

  private String name;

  private String address;

  public CustomerInvoice() {
    // Do Nothing.
  }

  public CustomerInvoice(int cid, String cnm, String cads) {
    this.id = cid;
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

  public int getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public String getAddress() {
    return address;
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.id = DataSerializer.readPrimitiveInt(in);
    this.name = DataSerializer.readString(in);
    this.address = DataSerializer.readString(in);
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writePrimitiveInt(this.id, out);
    DataSerializer.writeString(this.name, out);
    DataSerializer.writeString(this.address, out);
  }
}
