package com.gemstone.databrowser.objects;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;

/**
 * GemStoneCustomer ObjectType.
 * 
 * @author vreddy
 * 
 */
public class GemStoneCustomer extends Customer implements DataSerializable {

  private String[] roles;

  public GemStoneCustomer() {
    super();
  }

  public GemStoneCustomer(String cust_name, Address address, int[] contact,
      String[] roles) {
    super(cust_name, address, contact);
    this.roles = roles;
  }

  public String[] getRoles() {
    return roles;
  }

  public void setRoles(String[] roles) {
    this.roles = roles;
  }

  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeStringArray(roles, out);
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.roles = DataSerializer.readStringArray(in);
  }

  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("GemStoneCustomer [ roles = " + this.roles + "]");
    return buffer.toString();
  }
}
