/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.configuration.domain;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;

public class ConfigurationChangeResult implements DataSerializable{
  private boolean isSuccessful = true;
  private String errorMessage;
  private Exception exception;
  
  
  private static final long serialVersionUID = 1L;
  
  public ConfigurationChangeResult() {
    
  }
  public ConfigurationChangeResult(boolean isSuccessful) {
    this.isSuccessful = isSuccessful;
  }
  
  
  public ConfigurationChangeResult(String errorMessage, Exception exception) {
    this.isSuccessful = false;
    this.errorMessage = errorMessage;
    this.exception = exception;
  }
  
  public void setIsSuccessful(boolean isSuccessful) {
    this.isSuccessful = isSuccessful;
  }
  
  public boolean isSuccessful() {
    return this.isSuccessful;
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((errorMessage == null) ? 0 : errorMessage.hashCode());
    result = prime * result + ((exception == null) ? 0 : exception.hashCode());
    result = prime * result + (isSuccessful ? 1231 : 1237);
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
    ConfigurationChangeResult other = (ConfigurationChangeResult) obj;
    if (errorMessage == null) {
      if (other.errorMessage != null)
        return false;
    } else if (!errorMessage.equals(other.errorMessage))
      return false;
    if (exception == null) {
      if (other.exception != null)
        return false;
    } else if (!exception.equals(other.exception))
      return false;
    if (isSuccessful != other.isSuccessful)
      return false;
    return true;
  }
  @Override
  public String toString() {
    return "ConfigurationChangeResult [isSuccessful=" + isSuccessful
        + ", errorMessage=" + errorMessage + ", exception=" + exception + "]";
  }
  public void setException(Exception exception) {
    this.exception = exception;
    this.isSuccessful = false;
  }
  
  public Exception getException() {
    return this.exception;
  }
  
  public void setErrorMessage(String errorMessage) {
    this.isSuccessful = false;
    this.errorMessage = errorMessage;
  }
  
  public String getErrorMessage() {
    return this.errorMessage;
  }
  
  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeBoolean(this.isSuccessful, out);
    DataSerializer.writeString(this.errorMessage, out);
    DataSerializer.writeObject(exception, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.isSuccessful = DataSerializer.readBoolean(in);
    this.errorMessage = DataSerializer.readString(in);
    this.exception = DataSerializer.readObject(in);
  }

}
