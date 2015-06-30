/*=========================================================================
 * Copyright (c) 2002-2014, Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package hibe;

import hydra.*;
import hydra.blackboard.Blackboard;

/**
 * A Hydra blackboard that keeps track of what the various task
 * threads in an {@link HibernateTest} do.  
 *
 * @author lhughes
 * @since 6.5
 */
public class HibernateBB extends Blackboard {
   
// Blackboard creation variables
static String HIBERNATE_BB_NAME = "Hibernate_Blackboard";
static String HIBERNATE_BB_TYPE = "RMI";

// number of invocations for each task type
// just to show counters definition + use
public static int STARTTASK;
public static int INITTASK;
public static int TASK;
public static int CLOSETASK;
public static int ENDTASK;

// singleton instance of this Blackboard
public static HibernateBB bbInstance = null;

 /**
  *  Get the HibernateBB
  */
 public static HibernateBB getBB() {
    if (bbInstance == null) {
       synchronized ( HibernateBB.class ) {
          if (bbInstance == null) 
             bbInstance = new HibernateBB(HIBERNATE_BB_NAME, HIBERNATE_BB_TYPE);
       }
    }
    return bbInstance;
 }
    
 /**
  *  Zero-arg constructor for remote method invocations.
  */
 public HibernateBB() {
 }
    
 /**
  *  Creates a sample blackboard using the specified name and transport type.
  */
 public HibernateBB(String name, String type) {
    super(name, type, HibernateBB.class);
 }
   
}
