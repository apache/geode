package com.gemstone.databrowser.objects;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;

/**
 * SubEmployee ObjectType.
 * 
 * @author vreddy
 * 
 */
public class SubEmployee extends Employee implements DataSerializable {

  public byte empId;

  public SubEmployee() {
    super();
  }

  public SubEmployee(String ename, boolean eisMale, long esalary, byte eId,
      char eBlock, Work eWork) {
    super(ename, eisMale, esalary, eId, eBlock, eWork);
    super.empId = (byte)15;
    this.empId = eId;
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.name = DataSerializer.readString(in);
    this.male = DataSerializer.readBoolean(in);
    this.salary = DataSerializer.readLong(in);
    this.empId = DataSerializer.readByte(in);
    this.empWork = (Work)DataSerializer.readObject(in);
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.name, out);
    DataSerializer.writeBoolean(this.male, out);
    DataSerializer.writeLong(this.salary, out);
    DataSerializer.writeByte(this.empId, out);
    DataSerializer.writeObject(empWork, out);
  }

  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append(" SubEmployee [ empId = " + this.empId + "]");
    return buffer.toString();
  }

}
