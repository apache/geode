/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * Manager.java
 *
 * Created on April 7, 2005, 6:05 PM
 */
package com.gemstone.gemfire.cache.query.data;

import java.util.Set;

/**
 * @author vikramj
 */
public class Manager extends Employee {

  public int manager_id;

  /** Creates a new instance of Manager */
  public Manager(String name, int age, int empId, String title, int salary,
      Set addresses, int mgrId) {
    super(name, age, empId, title, salary, addresses);
    this.manager_id = mgrId;
  }

  public int getManager_id() {
    return this.manager_id;
  }
}
