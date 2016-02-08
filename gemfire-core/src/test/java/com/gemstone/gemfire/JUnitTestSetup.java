/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire;

import junit.extensions.TestDecorator;
import junit.framework.Test;
import junit.framework.TestSuite;
import junit.framework.TestResult;
import java.util.ArrayList;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;


/*
 * This class is a decorator for junit.framework.Test to enable class wide setUp() and tearDown() 
 * In order to use this decorator, the Testcase must contain this code:
 * <code>
 * public static Test suite() {
 *   return JUnitTestSetup.createJUnitTestSetup(ExampleTestCase.class);
 * }
 * </code>
 * If a class being added to this suite has any static methods like this
 * <code>static void caseSetUp()</code>
 * then it will be called once for that test case before any of its test methods are invoked.
 * If a class being added to this suite has any static methods like this
 * <code>static void caseTearDown()</code>
 * then it will be called once for that test case after all of its test methods are invoked.
 * For both of these <em>case</em> methods the subclass one is called first
 * followed by any found in the parent classes.
 *
 * @see Test
 * @deprecated use @BeforeClass and @AfterClass instead
 */
public class JUnitTestSetup extends TestDecorator {

   /**   * List of Method instances to call once on a testCase before any of its test methods
   */
  private ArrayList fCaseSetUp = null;
  /**
   * List of Method instances to call once on a testCase after all of its test methods
   */
  private ArrayList fCaseTearDown = null;

   /**
   * Find a static method on the specified class with the specified name
   * and puts it in the specified list
   * given class
   */
  private void findCaseMethod(Class c, String methName, ArrayList l) {
    Method m = null;
    try {
      m = c.getDeclaredMethod(methName, new Class[]{});
      if (!Modifier.isStatic(m.getModifiers())) {
        m = null;
      }
    } catch (NoSuchMethodException ex) {
    }
    if (m != null) {
      l.add(m);
    }
  }

  /*
   * Factory method to use this Junit TestDecorator
   * @params theClass this is the test you want to decorate
   */
  public static Test createJUnitTestSetup(final Class theClass) {
    TestSuite ts = new TestSuite();
    ts.addTestSuite(theClass);
    return new JUnitTestSetup(ts, theClass);
  }

  public JUnitTestSetup(Test ts, Class theClass) {
    super(ts);
    Class superClass = theClass;
    ArrayList caseSetUp = new ArrayList();
    ArrayList caseTearDown = new ArrayList();
    while (Test.class.isAssignableFrom(superClass)) { 
//      Method[] methods= superClass.getDeclaredMethods();
      //for (int i= 0; i < methods.length; i++) {
      //  addTestMethod(methods[i], names, theClass);
      //} 
      findCaseMethod(superClass, "caseSetUp", caseSetUp);       
      findCaseMethod(superClass, "caseTearDown", caseTearDown);  
      superClass= superClass.getSuperclass();
      if (this.countTestCases() != 0) {
        if (caseSetUp.size() > 0) {
          fCaseSetUp = caseSetUp;
        }
        if (caseTearDown.size() > 0) {
          fCaseTearDown = caseTearDown;
        }
      }
    }
  }

  public void run(TestResult result) {
    try {
      callCaseMethods(fCaseSetUp, result);    
      basicRun(result);
      //getTest().run(result);
      //for (Enumeration e= tests(); e.hasMoreElements(); ) {
      //  if (result.shouldStop() )
      //    break;
      //    Test test= (Test)e.nextElement();
      //    test.run(result);
      //  }
    } finally {
      callCaseMethods(fCaseTearDown, result);
    }
  }
  
  /**
   * Used to invoke a caseSetUp method once, if one exists,
   * before all the test methods have been invoked
   */
  private void callCaseMethods(ArrayList l, TestResult result) {
    if (l != null) {
      for (int i=0; i < l.size(); i++) {
        try {
          Method m = (Method)l.get(i);
          m.invoke(null, new Object[]{});
        } catch (Exception ex) {
          result.addError(this, ex);
        }
      }
    }
  }
}
