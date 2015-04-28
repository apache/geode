/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.web.domain;

import java.io.Serializable;
import javax.management.ObjectName;
import javax.management.QueryExp;

/**
 * The QueryParameterSource class encapsulates details in order to perform a query on an JMX MBean server.
 * <p/>
 * @author John Blum
 * @see java.io.Serializable
 * @see javax.management.ObjectName
 * @see javax.management.QueryExp
 * @since 8.0
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
    final StringBuilder buffer = new StringBuilder("{");
    buffer.append("{ objectName = ").append(String.valueOf(getObjectName()));
    buffer.append(", queryExpression = ").append(String.valueOf(getQueryExpression()));
    buffer.append(" }");
    return buffer.toString();
  }

}
