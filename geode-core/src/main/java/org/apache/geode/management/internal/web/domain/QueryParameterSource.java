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
package org.apache.geode.management.internal.web.domain;

import java.io.Serializable;

import javax.management.ObjectName;
import javax.management.QueryExp;

/**
 * The QueryParameterSource class encapsulates details in order to perform a query on an JMX MBean
 * server.
 * <p/>
 *
 * @see java.io.Serializable
 * @see javax.management.ObjectName
 * @see javax.management.QueryExp
 * @since GemFire 8.0
 */
@SuppressWarnings("unused")
public class QueryParameterSource implements Serializable {

  private static final long serialVersionUID = 34131123582155l;

  private final ObjectName objectName;

  private final QueryExp queryExpression;

  public QueryParameterSource(final ObjectName objectName, final QueryExp queryExpression) {
    this.objectName = objectName;
    this.queryExpression = queryExpression;
  }

  public ObjectName getObjectName() {
    return objectName;
  }

  public QueryExp getQueryExpression() {
    return queryExpression;
  }

  @Override
  public String toString() {
    return "{" + "{ objectName = " + getObjectName()
        + ", queryExpression = " + getQueryExpression()
        + " }";
  }

}
