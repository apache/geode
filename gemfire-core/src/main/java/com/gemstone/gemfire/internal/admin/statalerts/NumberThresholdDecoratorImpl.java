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
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.admin.StatAlert;
import com.gemstone.gemfire.internal.admin.StatAlertDefinition;

/**
 * Implementation of {@link StatAlertDefinition}, represents threshold as
 * number
 * 
 * @author hgadre
 */
public final class NumberThresholdDecoratorImpl extends BaseDecoratorImpl 
  implements DataSerializableFixedID {

  private static final long serialVersionUID = -1799140125261894306L;

  protected Number threshold;
  protected boolean evalForGtThan;

  public NumberThresholdDecoratorImpl() {
  }

  public NumberThresholdDecoratorImpl(StatAlertDefinition definition,
      Number threshold, boolean evalForGtThan) {
    super(definition);
    this.threshold = threshold;
    this.evalForGtThan = evalForGtThan;
  }

  public int getDSFID() {
    return DataSerializableFixedID.STAT_ALERT_DEFN_NUM_THRESHOLD;
  }

  public Number getThreshold() {
    return threshold;
  }

  public boolean isGauge() {
    return false;
  }
  
  public boolean isEvalForGreaterThan() {
    return evalForGtThan;
  }
  
  @Override
  public boolean verify(StatisticsFactory factory) {
    return (super.verify(factory) && (null != threshold));
  }

  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer(super.toString());
    buffer.append("Threshold Value:" + threshold + "\n");

    return buffer.toString();
  }

  /**
   * This eval just applies to a single value or the 1st value in params
   */
  @Override
  public boolean evaluate(Number[] params) {
    if (this.evalForGtThan)
      return super.evaluate(params)
            && isGreaterThan(getValue(params)[0], threshold);
    else
      return super.evaluate(params)
      && isLessThan(getValue(params)[0], threshold);      
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
    return super.getValue();
  }

  @Override
  public Number[] getValue(Number[] vals) {
    return super.getValue(vals);
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
    DataSerializer.writeObject(this.threshold, out);
    DataSerializer.writePrimitiveBoolean(this.evalForGtThan, out);
  }

  @Override
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.threshold = (Number)DataSerializer.readObject(in);
    this.evalForGtThan = DataSerializer.readPrimitiveBoolean(in);
  }

  public static final String ID = "NumberThreshold";

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }
}
