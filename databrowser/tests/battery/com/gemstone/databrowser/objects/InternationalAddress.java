package com.gemstone.databrowser.objects;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;

/**
 * InternationalAddress ObjectType.
 * 
 * @author vreddy
 * 
 */
public class InternationalAddress extends Address implements DataSerializable {
  protected String state;

  protected String country;

  public InternationalAddress() {
    super();
  }

  public InternationalAddress(Address address) {
    super(address);
    this.state = "Minnesota";
    this.country = "USA";
  }

  public InternationalAddress(String street1, String city, int postalCode,
      String state, String country) {
    super(street1, city, postalCode);
    this.state = state;
    this.country = country;
  }

  public String getState() {
    return state;
  }

  public String getCountry() {
    return country;
  }

  public void fromData(DataInput arg0) throws IOException,
      ClassNotFoundException {
    super.fromData(arg0);
    this.state = DataSerializer.readString(arg0);
    this.country = DataSerializer.readString(arg0);
  }

  public void toData(DataOutput arg0) throws IOException {
    super.toData(arg0);
    DataSerializer.writeString(this.state, arg0);
    DataSerializer.writeString(this.country, arg0);
  }

  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append(" InternationalAddress [ state = " + this.state
        + " , country = " + this.country + "]");
    return buffer.toString();
  }
}
