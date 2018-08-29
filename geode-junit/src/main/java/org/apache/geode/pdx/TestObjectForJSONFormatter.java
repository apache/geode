/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.pdx;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;

import org.json.JSONException;
import org.json.JSONObject;

enum Day {
  Sunday, Monday, Tuesday, Wednesday, Thursday, Friday, Saturday
}


public class TestObjectForJSONFormatter implements PdxSerializable {

  private boolean p_bool;
  private byte p_byte;
  private short p_short;
  private int p_int;
  private long p_long;
  private float p_float;
  private double p_double;

  // wrapper
  private Boolean w_bool;
  private Byte w_byte;
  private Short w_short;
  private Integer w_int;
  private Long w_long;
  private BigInteger w_bigInt;
  private Float w_float;
  private BigDecimal w_bigDec;
  private Double w_double;
  private String w_string;

  // Primitive_Arrays
  private boolean[] p_boolArray;
  private byte[] p_byteArray;
  private short[] p_shortArray;
  private int[] p_intArray;
  private long[] p_longArray;
  private float[] p_floatArray;
  private double[] p_doubleArray;

  // Wrapper_Arrays
  private Boolean[] w_boolArray;
  private Byte[] w_byteArray;
  private Short[] w_shortArray;
  private Integer[] w_intArray;
  private Long[] w_longArray;
  private BigInteger[] w_bigIntArray;
  private Float[] w_floatArray;
  private BigDecimal[] w_bigDecArray;
  private Double[] w_doubleArray;
  private String[] w_strArray;

  // Collection Type: List, Set, Queue, Deque
  private List<String> c_list;
  private Set<Object> c_set;
  private Queue<String> c_queue;
  private Deque<Integer> c_deque;
  private Stack<String> c_stack;

  // Map - Classify Person objects by city
  Map<String, List<Employee>> m_empByCity;

  // Enum
  private Day day;

  private Employee employee;

  public TestObjectForJSONFormatter() {}

  public String addClassTypeToJson(String json) throws JSONException {
    JSONObject jsonObj = new JSONObject(json);
    jsonObj.put("@type", "org.apache.geode.pdx.TestObjectForJSONFormatter");
    return jsonObj.toString();
  }

  public void defaultInitialization() {

    employee = new Employee(1010L, "NilkanthKumar", "Patel");

    // Initialize Map type member
    Employee e1 = new Employee(1L, "Nilkanth", "Patel");
    Employee e2 = new Employee(2L, "Amey", "Barve");
    Employee e3 = new Employee(3L, "Shankar", "Hundekar");
    Employee e4 = new Employee(4L, "Avinash", "Dongre");
    Employee e5 = new Employee(5L, "supriya", "Patil");
    Employee e6 = new Employee(6L, "Rajesh", null);
    Employee e7 = new Employee(7L, "Vishal", "Rao");
    Employee e8 = new Employee(8L, "Hitesh", "Khamesara");
    Employee e9 = new Employee(9L, "Sudhir", "Menon");

    m_empByCity = new HashMap<String, List<Employee>>();
    List<Employee> list1 = new ArrayList<Employee>();
    List<Employee> list2 = new ArrayList<Employee>();
    List<Employee> list3 = new ArrayList<Employee>();

    list1.add(e1);
    list1.add(e2);
    list1.add(e3);

    list2.add(e4);
    list2.add(e5);
    list2.add(e6);

    list3.add(e7);
    list3.add(e8);
    list3.add(e9);

    m_empByCity.put("Ahmedabad", list1);
    m_empByCity.put("mumbai", list2);
    m_empByCity.put("Pune", list3);


    // Initialize Collection types members
    c_list = new ArrayList<String>();
    c_list.add("Java");
    c_list.add("scala");
    c_list.add("closure");

    c_set = new HashSet<Object>();
    c_set.add("element 0");
    c_set.add("element 1");
    c_set.add("element 2");

    c_queue = new PriorityQueue<String>(3);
    c_queue.add("short");
    c_queue.add("very long indeed");
    c_queue.add("medium");

    c_deque = new ArrayDeque<Integer>(4);
    c_deque.add(15);
    c_deque.add(30);
    c_deque.add(20);
    c_deque.add(18);

    c_stack = new Stack();
    c_stack.push("bat");
    c_stack.push("cat");
    c_stack.push("dog");

    // Initialize primitive types members
    p_bool = true;
    p_byte = 101;
    p_short = 32001;
    p_int = 100001;
    p_long = 1234567898765432L;
    p_float = 123.456f;
    p_double = 98765.12345d;

    // Wrapper type member initialization
    w_bool = new Boolean(false);
    w_byte = new Byte((byte) 11);
    w_short = new Short((short) 101);
    w_int = new Integer(1001);
    w_long = new Long(987654321234567L);
    w_bigInt = new BigInteger("12345678910");
    w_float = new Float(789.456f);
    w_bigDec = new BigDecimal(8866333);
    w_double = new Double(123456.9876d);
    w_string = new String("Nilkanth Patel");

    // Initialization for members of type primitive arrays
    p_boolArray = new boolean[] {true, false, false};
    p_byteArray = new byte[] {10, 11, 12};
    p_shortArray = new short[] {101, 102, 103};
    p_intArray = new int[] {1001, 1002, 1003, 1004, 1005, 1006};
    p_longArray = new long[] {12345678910L, 12345678911L, 12345678912L};
    p_floatArray = new float[] {123.45f, 456.78f, -91011.123f};
    p_doubleArray = new double[] {1234.5678d, -91011.1213d, 1415.1617d};

    // Initialization for members of type wrapper arrays
    w_boolArray = new Boolean[3];
    w_byteArray = new Byte[3];
    w_shortArray = new Short[3];
    w_intArray = new Integer[3];
    w_longArray = new Long[3];
    w_floatArray = new Float[3];
    w_doubleArray = new Double[3];
    w_strArray = new String[3];

    for (int i = 0; i < 3; i++) {
      w_boolArray[i] = p_boolArray[i];
      w_byteArray[i] = p_byteArray[i];
      w_shortArray[i] = p_shortArray[i];
      w_intArray[i] = p_intArray[i];
      w_longArray[i] = p_longArray[i];
      w_floatArray[i] = p_floatArray[i];
      w_doubleArray[i] = p_doubleArray[i];
    }

    w_bigIntArray =
        new BigInteger[] {BigInteger.ZERO, BigInteger.ONE, new BigInteger("12345678910")};
    w_bigDecArray =
        new BigDecimal[] {BigDecimal.TEN, new BigDecimal("143.145"), new BigDecimal("10.01")};
    w_strArray = new String[] {"Nilkanth", "Vishal", "Hitesh"};

    // Enum type initialization
    day = Day.Thursday;
  }

  public TestObjectForJSONFormatter(boolean p_bool, byte p_byte, short p_short, int p_int,
      long p_long, float p_float, double p_double, Boolean w_bool, Byte w_byte, Short w_short,
      Integer w_int, Long w_long, BigInteger w_bigInt, Float w_float, BigDecimal w_bigDec,
      Double w_double, String w_string) {
    super();
    this.p_bool = p_bool;
    this.p_byte = p_byte;
    this.p_short = p_short;
    this.p_int = p_int;
    this.p_long = p_long;
    this.p_float = p_float;
    this.p_double = p_double;
    this.w_bool = w_bool;
    this.w_byte = w_byte;
    this.w_short = w_short;
    this.w_int = w_int;
    this.w_long = w_long;
    this.w_bigInt = w_bigInt;
    this.w_float = w_float;
    this.w_bigDec = w_bigDec;
    this.w_double = w_double;
    this.w_string = w_string;
  }

  public Employee getEmployee() {
    return employee;
  }

  public void setEmployee(Employee employee) {
    this.employee = employee;
  }

  public List<String> getC_list() {
    return c_list;
  }

  public void setC_list(List<String> c_list) {
    this.c_list = c_list;
  }

  public Set<Object> getC_set() {
    return c_set;
  }

  public void setC_set(Set<Object> c_set) {
    this.c_set = c_set;
  }

  public Queue<String> getC_queue() {
    return c_queue;
  }

  public void setC_queue(Queue<String> c_queue) {
    this.c_queue = c_queue;
  }

  public Deque<Integer> getC_deque() {
    return c_deque;
  }

  public void setC_deque(Deque<Integer> c_deque) {
    this.c_deque = c_deque;
  }

  public Map<String, List<Employee>> getM_empByCity() {
    return m_empByCity;
  }

  public void setM_empByCity(Map<String, List<Employee>> m_empByCity) {
    this.m_empByCity = m_empByCity;
  }

  public Day getDay() {
    return day;
  }

  public void setDay(Day day) {
    this.day = day;
  }

  public boolean isP_bool() {
    return p_bool;
  }

  public void setP_bool(boolean p_bool) {
    this.p_bool = p_bool;
  }

  public byte getP_byte() {
    return p_byte;
  }

  public void setP_byte(byte p_byte) {
    this.p_byte = p_byte;
  }

  public short getP_short() {
    return p_short;
  }

  public void setP_short(short p_short) {
    this.p_short = p_short;
  }

  public int getP_int() {
    return p_int;
  }

  public void setP_int(int p_int) {
    this.p_int = p_int;
  }

  public long getP_long() {
    return p_long;
  }

  public void setP_long(long p_long) {
    this.p_long = p_long;
  }

  public float getP_float() {
    return p_float;
  }

  public void setP_float(float p_float) {
    this.p_float = p_float;
  }

  public double getP_double() {
    return p_double;
  }

  public void setP_double(double p_double) {
    this.p_double = p_double;
  }

  public Boolean getW_bool() {
    return w_bool;
  }

  public void setW_bool(Boolean w_bool) {
    this.w_bool = w_bool;
  }

  public Byte getW_byte() {
    return w_byte;
  }

  public void setW_byte(Byte w_byte) {
    this.w_byte = w_byte;
  }

  public Short getW_short() {
    return w_short;
  }

  public void setW_short(Short w_short) {
    this.w_short = w_short;
  }

  public Integer getW_int() {
    return w_int;
  }

  public void setW_int(Integer w_int) {
    this.w_int = w_int;
  }

  public Long getW_long() {
    return w_long;
  }

  public void setW_long(Long w_long) {
    this.w_long = w_long;
  }

  public BigInteger getW_bigInt() {
    return w_bigInt;
  }

  public void setW_bigInt(BigInteger w_bigInt) {
    this.w_bigInt = w_bigInt;
  }

  public Float getW_float() {
    return w_float;
  }

  public void setW_float(Float w_float) {
    this.w_float = w_float;
  }

  public BigDecimal getW_bigDec() {
    return w_bigDec;
  }

  public void setW_bigDec(BigDecimal w_bigDec) {
    this.w_bigDec = w_bigDec;
  }

  public Double getW_double() {
    return w_double;
  }

  public void setW_double(Double w_double) {
    this.w_double = w_double;
  }

  public String getW_string() {
    return w_string;
  }

  public void setW_string(String w_string) {
    this.w_string = w_string;
  }

  public boolean[] getP_boolArray() {
    return p_boolArray;
  }

  public void setP_boolArray(boolean[] p_boolArray) {
    this.p_boolArray = p_boolArray;
  }

  public byte[] getP_byteArray() {
    return p_byteArray;
  }

  public void setP_byteArray(byte[] p_byteArray) {
    this.p_byteArray = p_byteArray;
  }

  public short[] getP_shortArray() {
    return p_shortArray;
  }

  public void setP_shortArray(short[] p_shortArray) {
    this.p_shortArray = p_shortArray;
  }

  public int[] getP_intArray() {
    return p_intArray;
  }

  public void setP_intArray(int[] p_intArray) {
    this.p_intArray = p_intArray;
  }

  public long[] getP_longArray() {
    return p_longArray;
  }

  public void setP_longArray(long[] p_longArray) {
    this.p_longArray = p_longArray;
  }

  public float[] getP_floatArray() {
    return p_floatArray;
  }

  public void setP_floatArray(float[] p_floatArray) {
    this.p_floatArray = p_floatArray;
  }

  public double[] getP_doubleArray() {
    return p_doubleArray;
  }

  public void setP_doubleArray(double[] p_doubleArray) {
    this.p_doubleArray = p_doubleArray;
  }

  public Boolean[] getW_boolArray() {
    return w_boolArray;
  }

  public void setW_boolArray(Boolean[] w_boolArray) {
    this.w_boolArray = w_boolArray;
  }

  public Byte[] getW_byteArray() {
    return w_byteArray;
  }

  public void setW_byteArray(Byte[] w_byteArray) {
    this.w_byteArray = w_byteArray;
  }

  public Short[] getW_shortArray() {
    return w_shortArray;
  }

  public void setW_shortArray(Short[] w_shortArray) {
    this.w_shortArray = w_shortArray;
  }

  public Integer[] getW_intArray() {
    return w_intArray;
  }

  public void setW_intArray(Integer[] w_intArray) {
    this.w_intArray = w_intArray;
  }

  public Long[] getW_longArray() {
    return w_longArray;
  }

  public void setW_longArray(Long[] w_longArray) {
    this.w_longArray = w_longArray;
  }

  public BigInteger[] getW_bigIntArray() {
    return w_bigIntArray;
  }

  public void setW_bigIntArray(BigInteger[] w_bigIntArray) {
    this.w_bigIntArray = w_bigIntArray;
  }

  public Float[] getW_floatArray() {
    return w_floatArray;
  }

  public void setW_floatArray(Float[] w_floatArray) {
    this.w_floatArray = w_floatArray;
  }

  public BigDecimal[] getW_bigDecArray() {
    return w_bigDecArray;
  }

  public void setW_bigDecArray(BigDecimal[] w_bigDecArray) {
    this.w_bigDecArray = w_bigDecArray;
  }

  public Double[] getW_doubleArray() {
    return w_doubleArray;
  }

  public void setW_doubleArray(Double[] w_doubleArray) {
    this.w_doubleArray = w_doubleArray;
  }

  public String[] getW_strArray() {
    return w_strArray;
  }

  public void setW_strArray(String[] w_strArray) {
    this.w_strArray = w_strArray;
  }

  public Stack<String> getC_stack() {
    return c_stack;
  }

  public void setC_stack(Stack<String> c_stack) {
    this.c_stack = c_stack;
  }

  // Getters for returning field names
  public String getP_boolFN() {
    return "p_bool";
  }

  public String getP_byteFN() {
    return "p_byte";
  }

  public String getP_shortFN() {
    return "p_short";
  }

  public String getP_intFN() {
    return "p_int";
  }

  public String getP_longFN() {
    return "p_long";
  }

  public String getP_floatFn() {
    return "p_float";
  }

  public String getP_doubleFN() {
    return "p_double";
  }

  public String getW_boolFN() {
    return "w_bool";
  }

  public String getW_byteFN() {
    return "w_byte";
  }

  public String getW_shortFN() {
    return "w_short";
  }

  public String getW_intFN() {
    return "w_int";
  }

  public String getW_longFN() {
    return "w_long";
  }

  public String getW_bigIntFN() {
    return "w_bigInt";
  }

  public String getW_floatFN() {
    return "w_float";
  }

  public String getW_bigDecFN() {
    return "w_bigDec";
  }

  public String getW_doubleFN() {
    return "w_double";
  }

  public String getW_stringFN() {
    return "w_string";
  }

  public String getP_boolArrayFN() {
    return "p_boolArray";
  }

  public String getP_byteArrayFN() {
    return "p_byteArray";
  }

  public String getP_shortArrayFN() {
    return "p_shortArray";
  }

  public String getP_intArrayFN() {
    return "p_intArray";
  }

  public String getP_longArrayFN() {
    return "p_longArray";
  }

  public String getP_floatArrayFN() {
    return "p_floatArray";
  }

  public String getP_doubleArrayFN() {
    return "p_doubleArray";
  }

  public String getW_boolArrayFN() {
    return "w_boolArray";
  }

  public String getW_byteArrayFN() {
    return "w_byteArray";
  }

  public String getW_shortArrayFN() {
    return "w_shortArray";
  }

  public String getW_intArrayFN() {
    return "w_intArray";
  }

  public String getW_longArrayFN() {
    return "w_longArray";
  }

  public String getW_bigIntArrayFN() {
    return "w_bigIntArray";
  }

  public String getW_floatArrayFN() {
    return "w_floatArray";
  }

  public String getW_bigDecArrayFN() {
    return "w_bigDecArray";
  }

  public String getW_doubleArrayFN() {
    return "w_doubleArray";
  }

  public String getW_strArrayFN() {
    return "w_strArray";
  }

  public String getC_listFN() {
    return "c_list";
  }

  public String getC_setFN() {
    return "c_set";
  }

  public String getC_queueFN() {
    return "c_queue";
  }

  public String getC_dequeFN() {
    return "c_deque";
  }

  public String getC_stackFN() {
    return "c_stack";
  }

  public String getM_empByCityFN() {
    return "m_empByCity";
  }

  public String getDayFN() {
    return "day";
  }

  @Override
  public void fromData(PdxReader in) {
    this.p_bool = in.readBoolean("p_bool");
    this.p_byte = in.readByte("p_byte");
    this.p_short = in.readShort("p_short");
    this.p_int = in.readInt("p_int");
    this.p_long = in.readLong("p_long");
    this.p_float = in.readFloat("p_float");
    this.p_double = in.readDouble("p_double");
    this.w_bool = in.readBoolean("w_bool");
    this.w_byte = in.readByte("w_byte");
    this.w_short = in.readShort("w_short");
    this.w_int = in.readInt("w_int");
    this.w_long = in.readLong("w_long");
    this.w_float = in.readFloat("w_float");
    this.w_double = in.readDouble("w_double");
    this.w_string = in.readString("w_string");
    this.w_bigInt = (BigInteger) in.readObject("w_bigInt");
    this.w_bigDec = (BigDecimal) in.readObject("w_bigDec");

    // P_Arrays
    this.p_boolArray = in.readBooleanArray("p_boolArray");
    this.p_byteArray = in.readByteArray("p_byteArray");
    this.p_shortArray = in.readShortArray("p_shortArray");
    this.p_intArray = in.readIntArray("p_intArray");
    this.p_longArray = in.readLongArray("p_longArray");
    this.p_floatArray = in.readFloatArray("p_floatArray");
    this.p_doubleArray = in.readDoubleArray("p_doubleArray");

    // W_Arrays
    this.w_boolArray = (Boolean[]) in.readObjectArray("w_boolArray");
    this.w_byteArray = (Byte[]) in.readObjectArray("w_byteArray");
    this.w_shortArray = (Short[]) in.readObjectArray("w_shortArray");
    this.w_intArray = (Integer[]) in.readObjectArray("w_intArray");
    this.w_longArray = (Long[]) in.readObjectArray("w_longArray");
    this.w_floatArray = (Float[]) in.readObjectArray("w_floatArray");
    this.w_doubleArray = (Double[]) in.readObjectArray("w_doubleArray");
    this.w_strArray = in.readStringArray("w_strArray");
    this.w_bigIntArray = (BigInteger[]) in.readObjectArray("w_bigIntArray");
    this.w_bigDecArray = (BigDecimal[]) in.readObjectArray("w_bigDecArray");

    // Collections
    this.c_list = (List<String>) in.readObject("c_list");
    this.c_set = (Set<Object>) in.readObject("c_set");
    this.c_queue = (Queue<String>) in.readObject("c_queue");
    this.c_deque = (Deque<Integer>) in.readObject("c_deque");
    this.c_stack = (Stack<String>) in.readObject("c_stack");

    // Map
    this.m_empByCity = (Map<String, List<Employee>>) in.readObject("m_empByCity");

    // Enum
    this.day = (Day) (in.readObject("day"));

    // User Object
    this.employee = (Employee) in.readObject("employee");
    // String type= in.readString("@type");
  }

  @Override
  public void toData(PdxWriter out) {
    // if(m_unreadFields != null){ out.writeUnreadFields(m_unreadFields); }
    out.writeBoolean("p_bool", this.p_bool);
    out.writeByte("p_byte", this.p_byte);
    out.writeShort("p_short", p_short);
    out.writeInt("p_int", p_int);
    out.writeLong("p_long", p_long);
    out.writeFloat("p_float", p_float);
    out.writeDouble("p_double", p_double);
    out.writeBoolean("w_bool", w_bool);
    out.writeByte("w_byte", w_byte);
    out.writeShort("w_short", w_short);
    out.writeInt("w_int", w_int);
    out.writeLong("w_long", w_long);
    out.writeFloat("w_float", w_float);
    out.writeDouble("w_double", w_double);
    out.writeString("w_string", w_string);
    out.writeObject("w_bigInt", w_bigInt);
    out.writeObject("w_bigDec", w_bigDec);

    // P_Arrays
    out.writeBooleanArray("p_boolArray", p_boolArray);
    out.writeByteArray("p_byteArray", p_byteArray);
    out.writeShortArray("p_shortArray", p_shortArray);
    out.writeIntArray("p_intArray", p_intArray);
    out.writeLongArray("p_longArray", p_longArray);
    out.writeFloatArray("p_floatArray", p_floatArray);
    out.writeDoubleArray("p_doubleArray", p_doubleArray);

    // W_Arrays
    out.writeObjectArray("w_boolArray", w_boolArray);
    out.writeObjectArray("w_byteArray", w_byteArray);
    out.writeObjectArray("w_shortArray", w_shortArray);
    out.writeObjectArray("w_intArray", w_intArray);
    out.writeObjectArray("w_longArray", w_longArray);
    out.writeObjectArray("w_floatArray", w_floatArray);
    out.writeObjectArray("w_doubleArray", w_doubleArray);
    out.writeStringArray("w_strArray", w_strArray);
    out.writeObjectArray("w_bigIntArray", w_bigIntArray);
    out.writeObjectArray("w_bigDecArray", w_bigDecArray);

    // Collections
    out.writeObject("c_list", c_list);
    out.writeObject("c_set", c_set);
    out.writeObject("c_queue", c_queue);
    out.writeObject("c_deque", c_deque);
    out.writeObject("c_stack", c_stack);

    // Map
    out.writeObject("m_empByCity", m_empByCity);

    // Enum
    out.writeObject("day", day);

    out.writeObject("employee", this.employee);
    // out.writeString("@type", "org.apache.geode.pdx.TestObjectForJSONFormatter");
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;

    TestObjectForJSONFormatter other = (TestObjectForJSONFormatter) obj;

    // primitive type
    if (p_bool != other.p_bool)
      return false;
    if (p_byte != other.p_byte)
      return false;
    if (p_short != other.p_short)
      return false;
    if (p_int != other.p_int)
      return false;
    if (p_long != other.p_long)
      return false;
    if (p_float != other.p_float)
      return false;
    if (p_double != other.p_double)
      return false;

    // wrapper type
    if (w_bool.booleanValue() != other.w_bool.booleanValue())
      return false;
    if (w_byte.byteValue() != other.w_byte.byteValue())
      return false;
    if (w_short.shortValue() != other.w_short.shortValue())
      return false;
    if (w_int.intValue() != other.w_int.intValue())
      return false;
    if (w_long.longValue() != other.w_long.longValue())
      return false;
    if (w_float.floatValue() != other.w_float.floatValue())
      return false;
    if (w_double.doubleValue() != other.w_double.doubleValue())
      return false;
    if (!w_string.equals(other.w_string))
      return false;
    if (w_bigInt.longValue() != other.w_bigInt.longValue())
      return false;
    if (w_bigDec.longValue() != other.w_bigDec.longValue())
      return false;

    // Primitive arrays
    if (!Arrays.equals(p_boolArray, other.p_boolArray))
      return false;
    if (!Arrays.equals(p_byteArray, other.p_byteArray))
      return false;
    if (!Arrays.equals(p_shortArray, other.p_shortArray))
      return false;
    if (!Arrays.equals(p_intArray, other.p_intArray))
      return false;
    if (!Arrays.equals(p_longArray, other.p_longArray))
      return false;
    if (!Arrays.equals(p_floatArray, other.p_floatArray))
      return false;
    if (!Arrays.equals(p_doubleArray, other.p_doubleArray))
      return false;

    // wrapper Arrays
    if (!Arrays.equals(w_boolArray, other.w_boolArray))
      return false;
    if (!Arrays.equals(w_byteArray, other.w_byteArray))
      return false;
    if (!Arrays.equals(w_shortArray, other.w_shortArray))
      return false;
    if (!Arrays.equals(w_intArray, other.w_intArray))
      return false;
    if (!Arrays.equals(w_longArray, other.w_longArray))
      return false;
    if (!Arrays.equals(w_floatArray, other.w_floatArray))
      return false;
    if (!Arrays.equals(w_doubleArray, other.w_doubleArray))
      return false;
    if (!Arrays.equals(w_strArray, other.w_strArray))
      return false;
    if (!Arrays.equals(w_bigIntArray, other.w_bigIntArray))
      return false;
    if (!Arrays.equals(w_bigDecArray, other.w_bigDecArray))
      return false;

    // comparing Collections based on content, order not considered
    if (!(c_list.size() == other.c_list.size() && c_list.containsAll(other.c_list)
        && other.c_list.containsAll(c_list)))
      return false;
    if (!(c_set.size() == other.c_set.size() && c_set.containsAll(other.c_set)
        && other.c_set.containsAll(c_set)))
      return false;
    if (!(c_queue.size() == other.c_queue.size() && c_queue.containsAll(other.c_queue)
        && other.c_queue.containsAll(c_queue)))
      return false;
    if (!(c_deque.size() == other.c_deque.size() && c_deque.containsAll(other.c_deque)
        && other.c_deque.containsAll(c_deque)))
      return false;

    // map comparision.
    if (!(compareMaps(m_empByCity, other.m_empByCity)))
      return false;

    // Enum validation
    if (!(day.equals(other.day)))
      return false;

    return true;
  }

  boolean compareMaps(Map m1, Map m2) {
    if (m1.size() != m2.size())
      return false;
    for (Object key : m1.keySet())
      if (!m1.get(key).equals(m2.get(key)))
        return false;
    return true;
  }

  @Override
  public String toString() {
    return "TestObjectForJSONFormatter [p_bool=" + p_bool + ", p_byte=" + p_byte + ", p_short="
        + p_short + ", p_int=" + p_int + ", p_long=" + p_long + ", p_float=" + p_float
        + ", p_double=" + p_double + ", w_bool=" + w_bool + ", w_byte=" + w_byte + ", w_short="
        + w_short + ", w_int=" + w_int + ", w_long=" + w_long + ", w_bigInt=" + w_bigInt
        + ", w_float=" + w_float + ", w_bigDec=" + w_bigDec + ", w_double=" + w_double
        + ", w_string=" + w_string + ", p_boolArray=" + Arrays.toString(p_boolArray)
        + ", p_byteArray=" + Arrays.toString(p_byteArray) + ", p_shortArray="
        + Arrays.toString(p_shortArray) + ", p_intArray=" + Arrays.toString(p_intArray)
        + ", p_longArray=" + Arrays.toString(p_longArray) + ", p_floatArray="
        + Arrays.toString(p_floatArray) + ", p_doubleArray=" + Arrays.toString(p_doubleArray)
        + ", w_boolArray=" + Arrays.toString(w_boolArray) + ", w_byteArray="
        + Arrays.toString(w_byteArray) + ", w_shortArray=" + Arrays.toString(w_shortArray)
        + ", w_intArray=" + Arrays.toString(w_intArray) + ", w_longArray="
        + Arrays.toString(w_longArray) + ", w_bigIntArray=" + Arrays.toString(w_bigIntArray)
        + ", w_floatArray=" + Arrays.toString(w_floatArray) + ", w_bigDecArray="
        + Arrays.toString(w_bigDecArray) + ", w_doubleArray=" + Arrays.toString(w_doubleArray)
        + ", w_strArray=" + Arrays.toString(w_strArray) + ", c_list=" + c_list + ", c_set=" + c_set
        + ", c_queue=" + c_queue + ", c_deque=" + c_deque + ", c_stack=" + c_stack
        + ", m_empByCity=" + m_empByCity + ", day=" + day + ", employee=" + employee + "]";
  }


}
