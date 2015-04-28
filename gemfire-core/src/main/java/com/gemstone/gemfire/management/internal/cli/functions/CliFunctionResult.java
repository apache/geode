/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.functions;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.management.internal.configuration.domain.XmlEntity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class CliFunctionResult implements Comparable<CliFunctionResult>, DataSerializableFixedID {
  private String memberIdOrName;
  private Serializable[] serializables = new String[0];
  private Throwable throwable;
  private boolean successful;
  private XmlEntity xmlEntity;
  private byte[] byteData = new byte[0];

  public CliFunctionResult() {
  }

  public CliFunctionResult(final String memberIdOrName) {
    this.memberIdOrName = memberIdOrName;
    
    this.successful = true;
  }
  
  public CliFunctionResult(final String memberIdOrName, final Serializable[] serializables) {
    this.memberIdOrName = memberIdOrName;
    this.serializables = serializables;
    
    this.successful = true;
  }
  
  public CliFunctionResult(final String memberIdOrName, final byte[] byteData, final Serializable[] serializables) {
    this.byteData = byteData;
    this.serializables = serializables;
    this.successful = true;
  }
  
  public CliFunctionResult(final String memberIdOrName, final XmlEntity xmlEntity) {
    this.memberIdOrName = memberIdOrName;
    this.xmlEntity = xmlEntity;
    
    this.successful = true;
  }
  
  
  public CliFunctionResult(final String memberIdOrName, final XmlEntity xmlEntity, final Serializable[] serializables) {
    this.memberIdOrName = memberIdOrName;
    this.xmlEntity = xmlEntity;
    this.serializables = serializables;
    
    this.successful = true;
  }

  public CliFunctionResult(final String memberIdOrName, XmlEntity xmlEntity, final String message) {
    this.memberIdOrName = memberIdOrName;
    this.xmlEntity = xmlEntity;
    if (message != null) {
      this.serializables = new String[] { message };
    }
    
    this.successful = true;
  }
  
  public CliFunctionResult(final String memberIdOrName, final boolean successful, final String message) {
    this.memberIdOrName = memberIdOrName;
    this.successful = successful;
    if (message != null) {
      this.serializables = new String[] { message };
    }
  }
  
  public CliFunctionResult(final String memberIdOrName, final Throwable throwable, final String message) {
    this.memberIdOrName = memberIdOrName;
    this.throwable = throwable;
    if (message != null) {
      this.serializables = new String[] { message };
    }
    
    this.successful = false;
  }
  
  public String getMemberIdOrName() {
    return this.memberIdOrName;
  }

  public String getMessage() {
    if (this.serializables.length == 0 || !(this.serializables[0] instanceof String)) {
      return null;
    }
    
    return (String) this.serializables[0];
  }

  public Serializable[] getSerializables() {
    return this.serializables;
  }
  
  public Throwable getThrowable() {
    return this.throwable;
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
    DataSerializer.writeObject(this.throwable, out);
    DataSerializer.writeByteArray(this.byteData, out);
  }

  public void toDataPre_GFE_8_0_0_0(DataOutput out) throws IOException {
    DataSerializer.writeString(this.memberIdOrName, out);
    DataSerializer.writeObjectArray(this.serializables, out);
    DataSerializer.writeObject(this.throwable, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.memberIdOrName = DataSerializer.readString(in);
    this.successful = DataSerializer.readPrimitiveBoolean(in);
    this.xmlEntity = DataSerializer.readObject(in);
    this.serializables = (Serializable[]) DataSerializer.readObjectArray(in);
    this.throwable = DataSerializer.readObject(in);
    this.byteData = DataSerializer.readByteArray(in);
  }

  public void fromDataPre_GFE_8_0_0_0(DataInput in) throws IOException, ClassNotFoundException {
    this.memberIdOrName = DataSerializer.readString(in);
    this.throwable = DataSerializer.readObject(in);
    this.serializables = (Serializable[]) DataSerializer.readObjectArray(in);
  }
  
  public boolean isSuccessful() {
    return this.successful;
  }
  
  public XmlEntity getXmlEntity() {
    return this.xmlEntity;
  }
  
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
    return "CliFunctionResult [memberId=" + this.memberIdOrName
        + ", successful=" + this.successful
        + ", xmlEntity=" + this.xmlEntity
        + ", serializables=" + Arrays.toString(this.serializables)
        + ", throwable=" + this.throwable
        + ", byteData=" + Arrays.toString(this.byteData) +"]";
  }
  
  /** 
   * Remove elements from the list that are not instances of CliFunctionResult and then
   * sort the results.
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
