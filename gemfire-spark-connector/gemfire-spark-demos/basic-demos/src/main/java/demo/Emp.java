package demo;

import java.io.Serializable;

/**
 * This is a demo class used in doc/?.md
 */
public class Emp implements Serializable {

  private int id;
  
  private String lname;

  private String fname;

  private int age;

  private String loc;

  public Emp(int id, String lname, String fname, int age, String loc) {
    this.id = id;
    this.lname = lname;
    this.fname = fname;
    this.age = age;
    this.loc = loc;
  }

  public int getId() {
    return id;
  }

  public String getLname() {
    return lname;
  }

  public String getFname() {
    return fname;
  }

  public int getAge() {
    return age;
  }

  public String getLoc() {
    return loc;
  }

  @Override
  public String toString() {
    return "Emp(" + id + ", " + lname + ", " + fname + ", " + age + ", " + loc + ")";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Emp emp = (Emp) o;

    if (age != emp.age) return false;
    if (id != emp.id) return false;
    if (fname != null ? !fname.equals(emp.fname) : emp.fname != null) return false;
    if (lname != null ? !lname.equals(emp.lname) : emp.lname != null) return false;
    if (loc != null ? !loc.equals(emp.loc) : emp.loc != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = id;
    result = 31 * result + (lname != null ? lname.hashCode() : 0);
    result = 31 * result + (fname != null ? fname.hashCode() : 0);
    result = 31 * result + age;
    result = 31 * result + (loc != null ? loc.hashCode() : 0);
    return result;
  }

}
