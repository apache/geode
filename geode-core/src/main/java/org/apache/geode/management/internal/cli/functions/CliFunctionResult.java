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
package org.apache.geode.management.internal.cli.functions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.Version;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;

public class CliFunctionResult implements Comparable<CliFunctionResult>, DataSerializableFixedID {
  private String memberIdOrName;
  private Serializable[] serializables = new String[0];
  private Object resultObject;
  private boolean successful;
  private XmlEntity xmlEntity;
  private byte[] byteData = new byte[0];

  @Deprecated
  public CliFunctionResult() {}

  @Deprecated
  public CliFunctionResult(final String memberIdOrName) {
    this.memberIdOrName = memberIdOrName;
    this.successful = true;
  }

  @Deprecated
  public CliFunctionResult(final String memberIdOrName, final Serializable[] serializables) {
    this.memberIdOrName = memberIdOrName;
    this.serializables = serializables;
    this.successful = true;
  }

  @Deprecated
  public CliFunctionResult(final String memberIdOrName, final XmlEntity xmlEntity) {
    this.memberIdOrName = memberIdOrName;
    this.xmlEntity = xmlEntity;
    this.successful = true;
  }

  @Deprecated
  public CliFunctionResult(final String memberIdOrName, final XmlEntity xmlEntity,
      final Serializable[] serializables) {
    this.memberIdOrName = memberIdOrName;
    this.xmlEntity = xmlEntity;
    this.serializables = serializables;
    this.successful = true;
  }

  @Deprecated
  public CliFunctionResult(final String memberIdOrName, XmlEntity xmlEntity, final String message) {
    this.memberIdOrName = memberIdOrName;
    this.xmlEntity = xmlEntity;
    if (message != null) {
      this.serializables = new String[] {message};
    }
    this.successful = true;
  }

  public CliFunctionResult(final String memberIdOrName, final boolean successful,
      final String message) {
    this.memberIdOrName = memberIdOrName;
    this.successful = successful;
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
      this.successful = false;
    } else {
      this.successful = true;
    }
  }

  public CliFunctionResult(final String memberIdOrName, final Object resultObject) {
    this(memberIdOrName, resultObject, null);
  }

  public String getMemberIdOrName() {
    return this.memberIdOrName;
  }

  @Deprecated
  public String getMessage() {
    if (this.serializables.length == 0 || !(this.serializables[0] instanceof String)) {
      return null;
    }

    return (String) this.serializables[0];
  }

  public String getStatus() {
    String message = getMessage();

    if (successful) {
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

  @Deprecated
  public Serializable[] getSerializables() {
    return this.serializables;
  }

  @Deprecated
  public Throwable getThrowable() {
    if (successful) {
      return null;
    }
    return ((Throwable) resultObject);
  }

  public Object getResultObject() {
    return resultObject;
  }

  @Override
  public int getDSFID() {
    return DataSerializableFixedID.CLI_FUNCTION_RESULT;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.memberIdOrName, out);
    DataSerializer.writePrimitiveBoolean(this.successful, out);
    DataSerializer.writeObject(this.xmlEntity, out);
    DataSerializer.writeObjectArray(this.serializables, out);
    DataSerializer.writeObject(this.resultObject, out);
    DataSerializer.writeByteArray(this.byteData, out);
  }

  public void toDataPre_8_0_0_0(DataOutput out) throws IOException {
    DataSerializer.writeString(this.memberIdOrName, out);
    DataSerializer.writeObjectArray(this.serializables, out);
    DataSerializer.writeObject(this.resultObject, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.memberIdOrName = DataSerializer.readString(in);
    this.successful = DataSerializer.readPrimitiveBoolean(in);
    this.xmlEntity = DataSerializer.readObject(in);
    this.serializables = (Serializable[]) DataSerializer.readObjectArray(in);
    this.resultObject = DataSerializer.readObject(in);
    this.byteData = DataSerializer.readByteArray(in);
  }

  public void fromDataPre_8_0_0_0(DataInput in) throws IOException, ClassNotFoundException {
    this.memberIdOrName = DataSerializer.readString(in);
    this.resultObject = DataSerializer.readObject(in);
    this.serializables = (Serializable[]) DataSerializer.readObjectArray(in);
  }

  public boolean isSuccessful() {
    return this.successful;
  }

  @Deprecated
  public XmlEntity getXmlEntity() {
    return this.xmlEntity;
  }

  @Deprecated
  public byte[] getByteData() {
    return this.byteData;
  }

  @Override
  public int compareTo(CliFunctionResult o) {
    if (this.memberIdOrName == null && o.memberIdOrName == null) {
      return 0;
    }
    if (this.memberIdOrName == null && o.memberIdOrName != null) {
      return -1;
    }
    if (this.memberIdOrName != null && o.memberIdOrName == null) {
      return 1;
    }
    return getMemberIdOrName().compareTo(o.memberIdOrName);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((this.memberIdOrName == null) ? 0 : this.memberIdOrName.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    CliFunctionResult other = (CliFunctionResult) obj;
    if (this.memberIdOrName == null) {
      if (other.memberIdOrName != null)
        return false;
    } else if (!this.memberIdOrName.equals(other.memberIdOrName))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "CliFunctionResult [memberId=" + this.memberIdOrName + ", successful=" + this.successful
        + ", xmlEntity=" + this.xmlEntity + ", serializables=" + Arrays.toString(this.serializables)
        + ", throwable=" + this.resultObject + ", byteData=" + Arrays.toString(this.byteData) + "]";
  }

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

  @Override
  public Version[] getSerializationVersions() {
    return new Version[] {Version.GFE_80};
  }
}
