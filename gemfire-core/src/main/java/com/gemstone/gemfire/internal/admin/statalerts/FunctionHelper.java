/*
 * ========================================================================= 
 * (c)Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved. 
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * =========================================================================
 */
package com.gemstone.gemfire.internal.admin.statalerts;

import com.gemstone.gemfire.SystemFailure;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * This class acts as a helper for the AlertManager & AlertAggregator for the
 * execution of the user specified functions
 * 
 * This class also keeps a registry of all the functions which are supported,
 * which should be used during creation of alert definition.
 * 
 * @author Hrishi
 */

public class FunctionHelper {

  private static final short FUN_AVG = 1;

  private static final short FUN_MIN = 2;

  private static final short FUN_MAX = 3;

  private static final short FUN_SUM = 4;

  private static final String STR_AVG = "Average";

  private static final String STR_MIN = "Min Value";

  private static final String STR_MAX = "Max Value";

  private static final String STR_ADD = "Sum";

  /**
   * This function returns the available function names.
   * 
   * @return List of the function names.
   */
  public static String[] getFunctionNames() {
    return new String[] { STR_ADD, STR_AVG, STR_MIN, STR_MAX };
  }

  /**
   * This method returns the function's name for the requested function
   * identifier.
   * 
   * @param functionId
   *                Identifier of the function
   * @return Function name.
   */
  public static String getFunctionName(short functionId) {
    switch (functionId) {
      case FUN_AVG:
        return STR_AVG;
      case FUN_MIN:
        return STR_MIN;
      case FUN_MAX:
        return STR_MAX;
      case FUN_SUM:
        return STR_ADD;
      default:
        return null;
    }
  }

  /**
   * This function returns the function identifier for the requested function
   * name.
   * 
   * @param qFunctionName
   *                Name of the function
   * @return Function identifier.
   */
  public static short getFunctionIdentifier(String qFunctionName) {

    if (qFunctionName == null)
      return -1;

    if (qFunctionName.equalsIgnoreCase(STR_ADD))
      return FUN_SUM;
    if (qFunctionName.equalsIgnoreCase(STR_AVG))
      return FUN_AVG;
    if (qFunctionName.equalsIgnoreCase(STR_MIN))
      return FUN_MIN;
    if (qFunctionName.equalsIgnoreCase(STR_MAX))
      return FUN_MAX;

    return -1;
  }

  /**
   * Apply the given function of the given list of numbers and returns result
   * 
   * @param functorId
   *                Id of function to be applied
   * @param vals
   *                List of number on which function will be applied
   * 
   */
  public static Number[] applyFunction(short functorId, Number[] vals) {
    Number[] res = new Number[1];
    switch (functorId) {
      case FUN_SUM:
        res[0] = SUM(vals);
        return res;
      case FUN_AVG:
        res[0] = AVG(vals);
        return res;
      case FUN_MIN:
        res[0] = MIN(vals);
        return res;
      case FUN_MAX:
        res[0] = MAX(vals);
        return res;
      default:
        return null;
    }
  }

  /**
   * Apply the SUM function on given list of number
   * 
   * @param vals
   *                Array of number
   */
  public static final Number SUM(Number[] vals) {
    try {
      double sum = 0.0;
      for (int i = 0; i < vals.length; i++) {
        sum = sum + vals[i].doubleValue();
      }
      return Double.valueOf(sum);
    }
    catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    }
    catch (Throwable t) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      return null;
    }
  }

  /**
   * Apply the Average function on given list of number
   * 
   * @param vals
   *                Array of number
   */
  public static final Number AVG(Number[] vals) {
    try {
      return Double.valueOf(SUM(vals).doubleValue() / vals.length);
    }
    catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    }
    catch (Throwable ex) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      return null;
    }
  }

  /**
   * Apply the Minimum function on given list of number
   * 
   * @param vals
   *                Array of number
   */
  public static final Number MIN(Number[] vals) {
    try {
      Collection col = Arrays.asList(vals);
      Number min = (Number)Collections.max(col);

      return min;
    }
    catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    }
    catch (Throwable t) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      return null;
    }
  }

  /**
   * Apply the Maximum function on given list of number
   * 
   * @param vals
   *                Array of number
   */
  public static final Number MAX(Number[] vals) {
    try {
      Collection col = Arrays.asList(vals);
      Number max = (Number)Collections.max(col);

      return max;
    }
    catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    }
    catch (Throwable t) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      return null;
    }
  }

}
