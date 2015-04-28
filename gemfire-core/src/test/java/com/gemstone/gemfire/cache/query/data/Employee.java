/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * Employee.java
 *
 * Created on March 2, 2005, 12:29 PM
 */

package com.gemstone.gemfire.cache.query.data;
import java.util.*;
/**
 *
 * @author  vikramj
 */
public class Employee {
    private String name;
    private int age;
    private int empId;
    private Set addresses;
    private String title;
    private int salary;
    private PhoneNo phoneNo= null;
   
   // private Set subordinates;
    
    public String name(){return name;}
    public int getAge(){return age;}
    public int empId(){return empId;}
    public String getTitle(){return title;}
    public int salary(){return salary;}
    
    /** Creates a new instance of Employee */
    public Employee(String name, int age, int empId, String title, int salary, Set addresses) {
        this.name=name;
        this.age=age;
        this.empId=empId;
        this.title=title;
        this.salary=salary;
        this.addresses=addresses;
        this.phoneNo = new PhoneNo(111, 222, 333, 444); 
        
    }
    // Added for the Test IUMRCompositeIteratorTest
    public Set getPhoneNo(String zipCode){
        Set ph = new HashSet();
        ph.add(this.phoneNo);
        return ph;
    }
    
    public Set getPh(int empId){
       Set ph = new HashSet();
        ph.add(this.phoneNo);
        return ph;
    }
    
    }// end of employee class
    

