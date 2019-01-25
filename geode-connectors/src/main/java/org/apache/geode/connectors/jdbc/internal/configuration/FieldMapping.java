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
  @Override
  public String toString() {
    return "FieldMapping [name=" + name + ", jdbcType=" + jdbcType + ", pdxType=" + pdxType + "]";
  }

  @XmlAttribute(name = "name")
  private String name;
  @XmlAttribute(name = "pdx-type")
  private String pdxType;
  @XmlAttribute(name = "jdbc-type")
  private String jdbcType;

  public FieldMapping() {}

  public FieldMapping(String name, String pdxType, String jdbcType) {
    this.name = name;
    this.pdxType = pdxType;
    this.jdbcType = jdbcType;
  }

  public String getName() {
    return name;
  }

  public void setName(String value) {
    this.name = value;
  }

  public String getPdxType() {
    return pdxType;
  }

  public void setPdxType(String pdxType) {
    this.pdxType = pdxType;
  }

  public String getJdbcType() {
    return jdbcType;
  }

  public void setJdbcType(String jdbcType) {
    this.jdbcType = jdbcType;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + ((jdbcType == null) ? 0 : jdbcType.hashCode());
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
    if (name == null) {
      if (other.name != null) {
        return false;
      }
    } else if (!name.equals(other.name)) {
      return false;
    }
    if (jdbcType == null) {
      if (other.jdbcType != null) {
        return false;
      }
    } else if (!jdbcType.equals(other.jdbcType)) {
      return false;
    }
    if (pdxType == null) {
      if (other.pdxType != null) {
        return false;
      }
    } else if (!pdxType.equals(other.pdxType)) {
      return false;
    }
    return true;
  }

}
