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
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.Version;

/**
 * Implementation of {@link StatAlertDefinition}, represents threshold as number
 *
 */
public class NumberThresholdDecoratorImpl extends BaseDecoratorImpl
    implements DataSerializableFixedID {

  private static final long serialVersionUID = -1799140125261894306L;

  protected Number threshold;
  protected boolean evalForGtThan;

  public NumberThresholdDecoratorImpl() {}

  public NumberThresholdDecoratorImpl(StatAlertDefinition definition, Number threshold,
      boolean evalForGtThan) {
    super(definition);
    this.threshold = threshold;
    this.evalForGtThan = evalForGtThan;
  }

  @Override
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
    if (this.evalForGtThan) {
      return super.evaluate(params) && isGreaterThan(getValue(params)[0], threshold);
    } else {
      return super.evaluate(params) && isLessThan(getValue(params)[0], threshold);
    }
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
    return ID.equalsIgnoreCase(decoratorID) ? this : super.getDecorator(decoratorID);
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out);
    context.getSerializer().writeObject(this.threshold, out);
    DataSerializer.writePrimitiveBoolean(this.evalForGtThan, out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.threshold = (Number) context.getDeserializer().readObject(in);
    this.evalForGtThan = DataSerializer.readPrimitiveBoolean(in);
  }

  public static final String ID = "NumberThreshold";

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }
}
