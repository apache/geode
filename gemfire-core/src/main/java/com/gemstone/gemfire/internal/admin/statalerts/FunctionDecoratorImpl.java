/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.admin.statalerts;

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.internal.admin.StatAlert;
import com.gemstone.gemfire.internal.admin.StatAlertDefinition;

/**
 * Implementation of {@link StatAlertDefinition}, which defines function
 * Function to be applied on all the statistic Threshold is valid for value
 * evaluated by function
 * 
 * @author Harsh Khanna
 */
public final class FunctionDecoratorImpl extends BaseDecoratorImpl {
  private static final long serialVersionUID = -4857857489413081553L;

  protected short functorId = -1;

  public FunctionDecoratorImpl() {
  }

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
    return ID.equalsIgnoreCase(decoratorID) ? this : super
        .getDecorator(decoratorID);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writePrimitiveShort(this.functorId, out);
  }

  @Override
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.functorId = DataSerializer.readPrimitiveShort(in);
  }

  public static final String ID = "NonSystemFunction";

}
