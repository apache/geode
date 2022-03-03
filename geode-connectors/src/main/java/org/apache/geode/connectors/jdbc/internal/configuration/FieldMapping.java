/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.connectors.jdbc.internal.configuration;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;

@SuppressWarnings("serial")
@XmlAccessorType(XmlAccessType.FIELD)
public class FieldMapping implements Serializable {
  @XmlAttribute(name = "pdx-name")
  private String pdxName;
  @XmlAttribute(name = "pdx-type")
  private String pdxType;
  @XmlAttribute(name = "jdbc-name")
  private String jdbcName;
  @XmlAttribute(name = "jdbc-type")
  private String jdbcType;
  @XmlAttribute(name = "jdbc-nullable")
  private boolean jdbcNullable;

  public FieldMapping() {}

  public FieldMapping(String pdxName, String pdxType, String jdbcName, String jdbcType,
      boolean jdbcNullable) {
    this.pdxName = pdxName;
    this.pdxType = pdxType;
    this.jdbcName = jdbcName;
    this.jdbcType = jdbcType;
    this.jdbcNullable = jdbcNullable;
  }

  public String getPdxName() {
    return pdxName;
  }

  public void setPdxName(String value) {
    pdxName = value;
  }

  public String getPdxType() {
    return pdxType;
  }

  public void setPdxType(String value) {
    pdxType = value;
  }

  public String getJdbcName() {
    return jdbcName;
  }

  public void setJdbcName(String value) {
    jdbcName = value;
  }

  public String getJdbcType() {
    return jdbcType;
  }

  public boolean isJdbcNullable() {
    return jdbcNullable;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((jdbcName == null) ? 0 : jdbcName.hashCode());
    result = prime * result + (jdbcNullable ? 1231 : 1237);
    result = prime * result + ((jdbcType == null) ? 0 : jdbcType.hashCode());
    result = prime * result + ((pdxName == null) ? 0 : pdxName.hashCode());
    result = prime * result + ((pdxType == null) ? 0 : pdxType.hashCode());
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
    FieldMapping other = (FieldMapping) obj;
    if (jdbcName == null) {
      if (other.jdbcName != null) {
        return false;
      }
    } else if (!jdbcName.equals(other.jdbcName)) {
      return false;
    }
    if (jdbcNullable != other.jdbcNullable) {
      return false;
    }
    if (jdbcType == null) {
      if (other.jdbcType != null) {
        return false;
      }
    } else if (!jdbcType.equals(other.jdbcType)) {
      return false;
    }
    if (pdxName == null) {
      if (other.pdxName != null) {
        return false;
      }
    } else if (!pdxName.equals(other.pdxName)) {
      return false;
    }
    if (pdxType == null) {
      return other.pdxType == null;
    } else
      return pdxType.equals(other.pdxType);
  }

  @Override
  public String toString() {
    return "FieldMapping [pdxName=" + pdxName + ", pdxType=" + pdxType + ", jdbcName=" + jdbcName
        + ", jdbcType=" + jdbcType + ", jdbcNullable=" + jdbcNullable + "]";
  }


}
