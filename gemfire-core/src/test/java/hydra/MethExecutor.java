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

package hydra;

//import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;

import com.gemstone.gemfire.SystemFailure;

/**
* 
* A class specialized for executing (via reflection) the receiver/selector
* pairs found in TestTasks. 
*
*/
public class MethExecutor {

   // @todo lises add static args method

  /**
   * Helper method that searches a class (and its superclasses) for a
   * method with the given name and parameter types.
   *
   * @throws NoSuchMethodException
   *         If the method cannot be found
   */
  public static Method getMethod(Class c, String methodName, Class[] paramTypes)
  throws NoSuchMethodException {

    ArrayList matchingMethods  = new ArrayList();
    for (Class q = c; q != null; q = q.getSuperclass()) {
      Method[] methods = q.getDeclaredMethods();
    NEXT_METHOD:
      for (int i = 0; i < methods.length; i++) {
        Method m = methods[i];
        if (!m.getName().equals(methodName)) {
          continue;
        }

        Class[] argTypes = m.getParameterTypes();
        if (argTypes.length != paramTypes.length) {
          continue;
        }

        for (int j = 0; j < argTypes.length; j++) {
          if(paramTypes[j] == null) {
            if(argTypes[j].isPrimitive()) {
              //this parameter is not ok, the parameter is a primative and the value is null
              continue NEXT_METHOD;
            } else {
              //this parameter is ok, the argument is an object and the value is null
              continue;
            }
          }
          if (!argTypes[j].isAssignableFrom(paramTypes[j])) {
            Class argType = argTypes[j];
            Class paramType = paramTypes[j];

            if (argType.isPrimitive()) {
              if ((argType.equals(boolean.class) &&
                   paramType.equals(Boolean.class)) ||
                  (argType.equals(short.class) &&
                   paramType.equals(Short.class)) ||
                  (argType.equals(int.class) &&
                   paramType.equals(Integer.class)) ||
                  (argType.equals(long.class) &&
                   paramType.equals(Long.class)) ||
                  (argType.equals(float.class) &&
                   paramType.equals(Float.class)) ||
                  (argType.equals(double.class) &&
                   paramType.equals(Double.class)) ||
                  (argType.equals(char.class) &&
                   paramType.equals(Character.class)) ||
                  (argType.equals(byte.class) &&
                   paramType.equals(Byte.class)) ||
                  false) {

                // This parameter is okay, try the next arg
                continue;
              }
            }
            continue NEXT_METHOD;
          }
        }

        matchingMethods.add(m);
      }
      
      //We want to check to make sure there aren't two
      //ambiguous methods on the same class. But a subclass
      //can still override a method on a super class, so we'll stop
      //if we found a method on the subclass.
      if(matchingMethods.size() > 0) {
        break;
      }
    }
    
    if(matchingMethods.isEmpty()) {
      StringBuffer sb = new StringBuffer();
      sb.append("Could not find method ");
      sb.append(methodName);
      sb.append(" with ");
      sb.append(paramTypes.length);
      sb.append(" parameters [");
      for (int i = 0; i < paramTypes.length; i++) {
        String name = paramTypes[i] == null ? null : paramTypes[i].getName();
        sb.append(name);
        if (i < paramTypes.length - 1) {
          sb.append(", ");
        }
      }
      sb.append("] in class ");
      sb.append(c.getName());
      throw new NoSuchMethodException(sb.toString());
    }
    if(matchingMethods.size() > 1) {
      StringBuffer sb = new StringBuffer();
      sb.append("Method is ambiguous ");
      sb.append(methodName);
      sb.append(" with ");
      sb.append(paramTypes.length);
      sb.append(" parameters [");
      for (int i = 0; i < paramTypes.length; i++) {
        String name = paramTypes[i] == null ? null : paramTypes[i].getName();
        sb.append(name);
        if (i < paramTypes.length - 1) {
          sb.append(", ");
        }
      }
      sb.append("] in class ");
      sb.append(c.getName());
      throw new NoSuchMethodException(sb.toString());
    }
    else return (Method) matchingMethods.get(0);
  }

  /**
  *
  * Send the message "selector" to the class named "receiver".
  * Return the result, including stack trace (if any).
  *
  */
  public static MethExecutorResult execute(String receiver, String selector) {
    return execute(receiver, selector, null);
  }

  /**
   * Executes the given static method on the given class with the
   * given arguments.
   */
  public static MethExecutorResult execute(String receiver, 
                                           String selector, 
                                           Object[] args) {
    try {
      // get the class
      Class receiverClass = Class.forName(receiver);

      // invoke the method
      Object res = null;
      try {
        Class[] paramTypes;
        if (args == null) {
          paramTypes = new Class[0];

        } else {
          paramTypes = new Class[args.length];
          for (int i = 0; i < args.length; i++) {
            if (args[i] == null) {
              paramTypes[i] = null;

            } else {
              paramTypes[i] = args[i].getClass();
            }
          }
        }

        Method theMethod =
          getMethod(receiverClass, selector, paramTypes);
        theMethod.setAccessible(true);
        res = theMethod.invoke(receiverClass, args);
        return new MethExecutorResult( res );

      } catch (InvocationTargetException invTargEx) {
        Throwable targEx = invTargEx.getTargetException();
        if ( targEx == null ) {
          return new MethExecutorResult( res );

        } else {
          return new MethExecutorResult(targEx);
        }
      }

    } 
    catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    }
    catch (Throwable t) {
//       String s = "While trying to invoke " + receiver + "." +
//         selector;
//       t = new HydraConfigException(s, t);
      return new MethExecutorResult(t);
    } 
  }

  /**
  *
  * Send the message "selector" to the object "target".
  * Return the result, including stack trace (if any).
  *
  */
  public static MethExecutorResult executeObject(Object target, String selector) {
    return executeObject(target, selector, null);
  }

  /**
   * Executes the given instance method on the given object with the
   * given arguments.
   */
  public static MethExecutorResult executeObject(Object target, 
                                           String selector, 
                                           Object[] args) {
    try {
      // get the class
      Class receiverClass = target.getClass();

      // invoke the method
      Object res = null;
      try {
        Class[] paramTypes;
        if (args == null) {
          paramTypes = new Class[0];

        } else {
          paramTypes = new Class[args.length];
          for (int i = 0; i < args.length; i++) {
            if (args[i] == null) {
              paramTypes[i] = Object.class;

            } else {
              paramTypes[i] = args[i].getClass();
            }
          }
        }

        Method theMethod =
          getMethod(receiverClass, selector, paramTypes);
        theMethod.setAccessible(true);
        res = theMethod.invoke(target, args);
        return new MethExecutorResult( res );

      } catch (InvocationTargetException invTargEx) {
        Throwable targEx = invTargEx.getTargetException();
        if ( targEx == null ) {
          return new MethExecutorResult( res );

        } else {
          return new MethExecutorResult(targEx);
        }
      }

    } 
    catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    }
    catch (Throwable t) {
      return new MethExecutorResult(t);
    } 
  }

  /**
  *
  * Send the message "selector" to an instance of the class named "receiver".
  * Return the result, including stack trace (if any).
  *
  */
  public static MethExecutorResult executeInstance( String receiver, String selector ) {

    try {
      // get the class
      Class receiverClass = Class.forName(receiver);
      Object target = receiverClass.newInstance();

      // invoke the method
      Object res = null;
      try {
        Method theMethod =
          getMethod(receiverClass, selector, new Class[0]);
        res = theMethod.invoke(target, new Object[0] );
        return new MethExecutorResult( res );

      } catch (InvocationTargetException invTargEx) {
        Throwable targEx = invTargEx.getTargetException();
        if ( targEx == null ) {
          return new MethExecutorResult( res );
        } else {
          return new MethExecutorResult(targEx);
        }
      }

    } 
    catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    }
    catch (Throwable t) {
      return new MethExecutorResult(t);
    } 
  }

  /**
  *
  * Send the message "selector" to an instance of the class named "receiver".
  * Return the result, including stack trace (if any).
  *
  */
  public static MethExecutorResult executeInstance( String receiver, String selector,
                                                    Class[] types, Object[] args ) {

    try {
      // get the class
      Class receiverClass = Class.forName(receiver);
      Constructor init =
        receiverClass.getDeclaredConstructor(new Class[0]);
      init.setAccessible(true);
      Object target = init.newInstance(new Object[0]);

      // invoke the method
      Object res = null;
      try {
        Method theMethod = getMethod(receiverClass, selector, types);
        res = theMethod.invoke(target, args );
        return new MethExecutorResult( res );

      } catch (InvocationTargetException invTargEx) {
        Throwable targEx = invTargEx.getTargetException();
        if ( targEx == null ) {
          return new MethExecutorResult( res );

        } else {
          return new MethExecutorResult(targEx);
        }
      }

    } 
    catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    }
    catch (Throwable t) {
      return new MethExecutorResult(t);
    } 
  }

  /** 
  *
  * A small program for testing this class.
  *
  */
  public static String testMethod1() {
    return "The result is: " + System.currentTimeMillis(); 
  }
  public static String testMethod2() {
    throw new ArrayIndexOutOfBoundsException("frip");
  }
  public static void main(String[] args) {
    MethExecutorResult result = null;
    result = MethExecutor.execute( "hydra.MethExecutor", "testMethod1" );
    System.out.println(result.toString());
    result = MethExecutor.execute( "hydra.MethExecutor", "testMethod2" );
    System.out.println(result.toString());
  }
}
