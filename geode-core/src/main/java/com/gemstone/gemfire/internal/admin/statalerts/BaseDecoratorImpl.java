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
package com.gemstone.gemfire.internal.admin.statalerts;

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.internal.admin.StatAlert;
import com.gemstone.gemfire.internal.admin.StatAlertDefinition;

/**
 * Base class for all the decorators
 * 
 * @see FunctionDecoratorImpl
 * @see GaugeThresholdDecoratorImpl
 * @see NumberThresholdDecoratorImpl
 * 
 */
public abstract class BaseDecoratorImpl implements StatAlertDefinition {

  protected StatAlertDefinition _def;

  public BaseDecoratorImpl() {
    super();
  }

  /**
   * @param definition
   */
  public BaseDecoratorImpl(StatAlertDefinition definition) {
    super();
    this._def = definition;
  }

  public final int getId() {
    return _def.getId();
  }

  public boolean verify(StatisticsFactory factory) {
    return _def.verify(factory);
  }

  /**
   * This method returns the name of this stat alert definition.
   * 
   * @return Name of the StatAlertDefinition
   */
  public String getName() {
    return _def.getName();
  }

  /**
   * This method sets the name of this stat alert definition.
   * 
   * @param name
   *                name to be set for this StatAlertDefinition.
   */
  public void setName(String name) {
    _def.setName(name);
  }

  public final StatisticInfo[] getStatisticInfo() {
    return _def.getStatisticInfo();
  }

  public final void setStatisticInfo(StatisticInfo[] info) {
    _def.setStatisticInfo(info);
  }

  @Override
  public String toString() {
    return _def.toString();
  }

  public String getStringRepresentation() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("StatAlertDefinition [\n");
    buffer.append(toString());
    buffer.append("]");

    return buffer.toString();
  }

  public boolean evaluate(Number[] params) {
    return _def.evaluate(params);
  }

  public boolean evaluate() {
    return _def.evaluate();
  }

  public StatAlert evaluateAndAlert(Number[] params) {
    return _def.evaluateAndAlert(params);
  }

  public StatAlert evaluateAndAlert() {
    // TODO Auto-generated method stub
    return _def.evaluateAndAlert();
  }

  public Number[] getValue() {
    return _def.getValue();
  }

  public Number[] getValue(Number[] vals) {
    return _def.getValue(vals);
  }

  public boolean hasDecorator(String decoratorID) {
    return _def.hasDecorator(decoratorID);
  }

  public StatAlertDefinition getDecorator(String decoratorID) {
    return _def.getDecorator(decoratorID);
  }

  public static boolean isGreaterThan(Number param, Number threshold) {
    try {
      int eval = compare(param, threshold);
      return eval > 0;
    }
    catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    }
    catch (Throwable e) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      return false;
    }
  }

  public static boolean isLessThan(Number param, Number threshold) {
    try {
      int eval = compare(param, threshold);
      return eval < 0;
    }
    catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    }
    catch (Throwable e) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      return false;
    }
  }

  public static int compare(Number param, Number threshold) throws Throwable {
    try {
      int eval = 0;
      if (threshold instanceof Double) {
        eval = Double.compare(param.doubleValue(), threshold.doubleValue());
      }
      else if (threshold instanceof Float) {
        eval = Float.compare(param.floatValue(), threshold.floatValue());
      }
      else if (threshold instanceof Long) {
        eval = (Long.valueOf(param.longValue())).compareTo(Long.valueOf(threshold
            .longValue()));
      }
      else if (threshold instanceof Integer) {
        eval = param.intValue() > threshold.intValue() ? 1 : -1;
      }
      return eval;
    }
    catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    }
    catch (Throwable e) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      throw e;
    }
  }


  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeObject(this._def, out);
  }

  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    this._def = (StatAlertDefinition) DataSerializer.readObject(in);
  }
}
