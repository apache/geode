package com.gemstone.databrowser.objects;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;

/**
 * Employee ObjectType.
 * 
 * @author vreddy
 */
public class Employee implements DataSerializable {

  protected String name;

  protected boolean male;

  public long salary;

  public byte empId;

  protected Work empWork;

  public Employee() {
  }

  /** Creates a new instance of Employee */
  public Employee(String ename, boolean eisMale, long esalary, byte eId,
      char eBlock, Work eWork) {
    this.name = ename;
    this.male = eisMale;
    this.salary = esalary;
    this.empId = eId;
    this.empWork = eWork;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public boolean isMale() {
    return male;
  }

  public void setMale(boolean isMale) {
    this.male = isMale;
  }

  public long getSalary() {
    return salary * 1000;
  }

  public void setSalary(long salary) {
    this.salary = salary;
  }

  public Work getEmpWork() {
    return empWork;
  }

  public void setEmpWork(Work empWork) {
    this.empWork = empWork;
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
    buffer.append("Employee [ name = " + this.name + " male = " + this.male
        + " salary =" + this.salary + " empId =" + this.empId + " empWork= "
        + this.empWork + "]");
    return buffer.toString();
  }
}
