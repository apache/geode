/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.web.domain;

import static org.junit.Assert.*;

import java.io.IOException;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.Query;
import javax.management.QueryExp;

import com.gemstone.gemfire.internal.util.IOUtils;
import com.gemstone.junit.UnitTest;

import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * The QueryParameterSourceJUnitTest class is a test suite of test cases testing the contract and functionality of the
 * QueryParameterSource class.
 * <p/>
 * @author John Blum
 * @see com.gemstone.gemfire.management.internal.web.domain.QueryParameterSource
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since 8.0
 */
@Category(UnitTest.class)
public class QueryParameterSourceJUnitTest {

  @Test
  public void testCreateQueryParameterSource() throws MalformedObjectNameException {
    final ObjectName expectedObjectName = ObjectName.getInstance("GemFire:type=Member,*");
    
    final QueryExp expectedQueryExpression = Query.eq(Query.attr("id"), Query.value("12345"));

    final QueryParameterSource query = new QueryParameterSource(expectedObjectName, expectedQueryExpression);

    assertNotNull(query);
    assertSame(expectedObjectName, query.getObjectName());
    assertSame(expectedQueryExpression, query.getQueryExpression());
  }

  @Test
  public void testSerialization() throws ClassNotFoundException, IOException, MalformedObjectNameException {
    final ObjectName expectedObjectName = ObjectName.getInstance("GemFire:type=Member,*");
    
    final QueryExp expectedQueryExpression = Query.or(
      Query.eq(Query.attr("name"), Query.value("myName")),
      Query.eq(Query.attr("id"), Query.value("myId"))
    );

    final QueryParameterSource expectedQuery = new QueryParameterSource(expectedObjectName, expectedQueryExpression);

    assertNotNull(expectedQuery);
    assertSame(expectedObjectName, expectedQuery.getObjectName());
    assertSame(expectedQueryExpression, expectedQuery.getQueryExpression());

    final byte[] queryBytes = IOUtils.serializeObject(expectedQuery);

    assertNotNull(queryBytes);
    assertTrue(queryBytes.length != 0);

    final Object queryObj = IOUtils.deserializeObject(queryBytes);

    assertTrue(queryObj instanceof QueryParameterSource);

    final QueryParameterSource actualQuery = (QueryParameterSource) queryObj;

    assertNotSame(expectedQuery, actualQuery);
    assertNotNull(actualQuery.getObjectName());
    assertEquals(expectedQuery.getObjectName().toString(), actualQuery.getObjectName().toString());
    assertNotNull(actualQuery.getQueryExpression());
    assertEquals(expectedQuery.getQueryExpression().toString(), actualQuery.getQueryExpression().toString());
  }

}
