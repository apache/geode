package ittest.io.pivotal.gemfire.spark.connector;

import java.io.Serializable;

public class Employee implements Serializable {

  private String name;

  private int age;

  public Employee(String n, int a) {
    name = n;
    age = a;
  }

  public String getName() {
    return name;
  }

  public int getAge() {
    return age;
  }

  public String toString() {
    return new StringBuilder().append("Employee[name=").append(name).
            append(", age=").append(age).
            append("]").toString();
  }
  
  public boolean equals(Object o) {
    if (o instanceof Employee) {
      return ((Employee) o).name.equals(name) && ((Employee) o).age == age;
    }
    return false;
  }

}

