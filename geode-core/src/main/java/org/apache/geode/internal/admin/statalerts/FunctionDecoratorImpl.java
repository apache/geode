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
package org.apache.geode.internal.admin.statalerts;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializer;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.internal.admin.StatAlert;
import org.apache.geode.internal.admin.StatAlertDefinition;

/**
 * Implementation of {@link StatAlertDefinition}, which defines function Function to be applied on
 * all the statistic Threshold is valid for value evaluated by function
 *
 */
public class FunctionDecoratorImpl extends BaseDecoratorImpl {
  private static final long serialVersionUID = -4857857489413081553L;

  protected short functorId = -1;

  public FunctionDecoratorImpl() {}

  public FunctionDecoratorImpl(StatAlertDefinition definition, short functorId) {
    super(definition);
    this.functorId = functorId;
  }

  public short getFunctionID() {
    return functorId;
  }

  public boolean isSystemWide() {
    return false;
  }

  @Override
  public boolean verify(StatisticsFactory factory) {
    return (super.verify(factory) && (-1 != functorId));
  }

  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer(super.toString());
    buffer.append("Function ID:" + functorId + "\n");

    return buffer.toString();
  }

  /**
   * This eval just applies to a single value or the 1st value in params
   */
  @Override
  public boolean evaluate(Number[] params) {
    return super.evaluate(params) && canApplyFunction(params);

  }

  @Override
  public boolean evaluate() {
    return evaluate(getValue());
  }

  @Override
  public StatAlert evaluateAndAlert(Number[] params) {
    return evaluate(params) ? super.evaluateAndAlert(params) : null;
  }

  @Override
  public StatAlert evaluateAndAlert() {
    return evaluate() ? super.evaluateAndAlert() : null;
  }

  @Override
  public Number[] getValue() {
    return FunctionHelper.applyFunction(functorId, super.getValue());
  }

  @Override
  public Number[] getValue(Number[] vals) {
    return FunctionHelper.applyFunction(functorId, vals);
  }

  protected boolean canApplyFunction(Number[] vals) {
    return FunctionHelper.applyFunction(functorId, vals) != null;
  }

  @Override
  public boolean hasDecorator(String decoratorID) {
    return ID.equalsIgnoreCase(decoratorID) || super.hasDecorator(decoratorID);
  }

  @Override
  public StatAlertDefinition getDecorator(String decoratorID) {
    return ID.equalsIgnoreCase(decoratorID) ? this : super.getDecorator(decoratorID);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writePrimitiveShort(functorId, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    functorId = DataSerializer.readPrimitiveShort(in);
  }

  public static final String ID = "NonSystemFunction";

}
