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
package org.apache.geode.management.cli;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.geode.DataSerializer;
import org.apache.geode.management.AbstractFunctionResult;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;

public class CliFunctionResult extends AbstractFunctionResult {
  private XmlEntity xmlEntity;

  /**
   * Remove elements from the list that are not instances of CliFunctionResult and then sort the
   * results.
   *
   * @param results The results to clean.
   * @return The cleaned results.
   */
  public static List<CliFunctionResult> cleanResults(List<?> results) {
    List<CliFunctionResult> returnResults = new ArrayList<CliFunctionResult>(results.size());
    for (Object result : results) {
      if (result instanceof CliFunctionResult) {
        returnResults.add((CliFunctionResult) result);
      }
    }

    Collections.sort(returnResults);
    return returnResults;
  }

  @Deprecated
  public CliFunctionResult() {}

  @Deprecated
  public CliFunctionResult(final String memberIdOrName) {
    this.memberIdOrName = memberIdOrName;
    this.state = StatusState.OK;
  }

  @Deprecated
  public CliFunctionResult(final String memberIdOrName, final Serializable[] serializables) {
    this.memberIdOrName = memberIdOrName;
    this.serializables = serializables;
    this.state = StatusState.OK;
  }

  @Deprecated
  public CliFunctionResult(final String memberIdOrName, final XmlEntity xmlEntity) {
    this.memberIdOrName = memberIdOrName;
    this.xmlEntity = xmlEntity;
    this.state = StatusState.OK;
  }

  @Deprecated
  public CliFunctionResult(final String memberIdOrName, final XmlEntity xmlEntity,
      final Serializable[] serializables) {
    this.memberIdOrName = memberIdOrName;
    this.xmlEntity = xmlEntity;
    this.serializables = serializables;
    this.state = StatusState.OK;
  }

  @Deprecated
  public CliFunctionResult(final String memberIdOrName, XmlEntity xmlEntity, final String message) {
    this.memberIdOrName = memberIdOrName;
    this.xmlEntity = xmlEntity;
    if (message != null) {
      this.serializables = new String[] {message};
    }
    this.state = StatusState.OK;
  }

  /**
   * @deprecated Use {@code CliFunctionResult(String, StatusState, String)} instead
   */
  @Deprecated
  public CliFunctionResult(final String memberIdOrName, final boolean successful,
      final String message) {
    this(memberIdOrName, successful ? StatusState.OK : StatusState.ERROR, message);
  }

  public CliFunctionResult(final String memberIdOrName, final StatusState state,
      final String message) {
    this.memberIdOrName = memberIdOrName;
    this.state = state;
    if (message != null) {
      this.serializables = new String[] {message};
    }
  }

  public CliFunctionResult(final String memberIdOrName, final Object resultObject,
      final String message) {
    this.memberIdOrName = memberIdOrName;
    this.resultObject = resultObject;
    if (message != null) {
      this.serializables = new String[] {message};
    }
    if (resultObject instanceof Throwable) {
      this.state = StatusState.ERROR;
    } else {
      this.state = StatusState.OK;
    }
  }

  public CliFunctionResult(final String memberIdOrName, final Object resultObject) {
    this(memberIdOrName, resultObject, null);
  }

  public String getStatus(boolean skipIgnore) {
    if (state == StatusState.IGNORABLE) {
      return skipIgnore ? "IGNORED" : "ERROR";
    }

    return state.name();
  }

  /**
   * This can be removed once all commands are using ResultModel.
   */
  @Deprecated
  public String getLegacyStatus() {
    String message = getMessage();

    if (isSuccessful()) {
      return message;
    }

    String errorMessage = "ERROR: ";
    if (message != null
        && (resultObject == null || !((Throwable) resultObject).getMessage().contains(message))) {
      errorMessage += message;
    }

    if (resultObject != null) {
      errorMessage = errorMessage.trim() + " " + ((Throwable) resultObject).getClass().getName()
          + ": " + ((Throwable) resultObject).getMessage();
    }

    return errorMessage;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    toDataPre_GEODE_1_6_0_0(out);
    DataSerializer.writeEnum(this.state, out);
  }

  public void toDataPre_GEODE_1_6_0_0(DataOutput out) throws IOException {
    DataSerializer.writeString(this.memberIdOrName, out);
    DataSerializer.writePrimitiveBoolean(this.isSuccessful(), out);
    DataSerializer.writeObject(this.xmlEntity, out);
    DataSerializer.writeObjectArray(this.serializables, out);
    DataSerializer.writeObject(this.resultObject, out);
    DataSerializer.writeByteArray(this.byteData, out);
  }

  public void toDataPre_GFE_8_0_0_0(DataOutput out) throws IOException {
    DataSerializer.writeString(this.memberIdOrName, out);
    DataSerializer.writeObjectArray(this.serializables, out);
    DataSerializer.writeObject(this.resultObject, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    fromDataPre_GEODE_1_6_0_0(in);
    this.state = DataSerializer.readEnum(StatusState.class, in);
  }

  public void fromDataPre_GEODE_1_6_0_0(DataInput in) throws IOException, ClassNotFoundException {
    this.memberIdOrName = DataSerializer.readString(in);
    this.state = DataSerializer.readPrimitiveBoolean(in) ? StatusState.OK : StatusState.ERROR;
    this.xmlEntity = DataSerializer.readObject(in);
    this.serializables = (Serializable[]) DataSerializer.readObjectArray(in);
    this.resultObject = DataSerializer.readObject(in);
    this.byteData = DataSerializer.readByteArray(in);
  }

  public void fromDataPre_GFE_8_0_0_0(DataInput in) throws IOException, ClassNotFoundException {
    this.memberIdOrName = DataSerializer.readString(in);
    this.resultObject = DataSerializer.readObject(in);
    this.serializables = (Serializable[]) DataSerializer.readObjectArray(in);
  }

  public boolean isIgnorableFailure() {
    return this.state == StatusState.IGNORABLE;
  }

  @Deprecated
  public XmlEntity getXmlEntity() {
    return this.xmlEntity;
  }

  @Override
  public String toString() {
    return "CliFunctionResult [memberId=" + this.memberIdOrName + ", successful="
        + this.isSuccessful() + ", xmlEntity=" + this.xmlEntity + ", serializables="
        + Arrays.toString(this.serializables) + ", throwable=" + this.resultObject + ", byteData="
        + Arrays.toString(this.byteData) + "]";
  }
}
