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
package org.apache.geode.management.internal.configuration.domain;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;

public class ConfigurationChangeResult implements DataSerializable {
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
    result = prime * result + ((errorMessage == null) ? 0 : errorMessage.hashCode());
    result = prime * result + ((exception == null) ? 0 : exception.hashCode());
    result = prime * result + (isSuccessful ? 1231 : 1237);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    ConfigurationChangeResult other = (ConfigurationChangeResult) obj;
    if (errorMessage == null) {
      if (other.errorMessage != null) {
        return false;
      }
    } else if (!errorMessage.equals(other.errorMessage)) {
      return false;
    }
    if (exception == null) {
      if (other.exception != null) {
        return false;
      }
    } else if (!exception.equals(other.exception)) {
      return false;
    }
    if (isSuccessful != other.isSuccessful) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "ConfigurationChangeResult [isSuccessful=" + isSuccessful + ", errorMessage="
        + errorMessage + ", exception=" + exception + "]";
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
