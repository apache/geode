package com.gemstone.databrowser.objects;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;

public class Work implements DataSerializable {
  private String Designation;

  private byte empCode;

  private String Department;

  public Work() {
    this.Designation = null;
    this.empCode = 0;
    this.Department = null;

  }

  public Work(String Designation, byte empCode, String Department) {
    super();
    this.Designation = Designation;
    this.empCode = empCode;
    this.Department = Department;
  }

  public String getDesignation() {
    return Designation;
  }

  public byte getempCode() {
    return empCode;
  }

  public String getDepartment() {
    return Department;
  }

  public void fromData(DataInput arg0) throws IOException,
      ClassNotFoundException {
    this.Designation = DataSerializer.readString(arg0);
    this.empCode = DataSerializer.readByte(arg0);
    this.Department = DataSerializer.readString(arg0);
  }

  public void toData(DataOutput arg0) throws IOException {
    DataSerializer.writeString(this.Designation, arg0);
    DataSerializer.writeByte(this.empCode, arg0);
    DataSerializer.writeString(this.Department, arg0);
  }

  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("Work [ Designation :" + this.Designation + " , empCode :"
        + this.empCode + " , Department : " + this.Department + "]");
    return buffer.toString();
  }
}
