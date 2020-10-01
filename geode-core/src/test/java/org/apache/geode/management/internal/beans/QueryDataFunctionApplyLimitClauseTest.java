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
 *
 */

package org.apache.geode.management.internal.beans;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Before;
import org.junit.Test;



public class QueryDataFunctionApplyLimitClauseTest {

  private String selectQuery;
  private int limit_0;
  private int limit_10;
  private int queryResultSetLimit_100;

  @Before
  public void setUp() throws Exception {
    this.selectQuery = "SELECT * FROM " + SEPARATOR + "MyRegion";
    this.limit_0 = 0;
    this.limit_10 = 10;
    this.queryResultSetLimit_100 = 100;
  }

  @Test
  public void applyLimitClauseDoesNothingIfLimitClauseSpecified() {
    String limitClause = " LIMIT 50";
    String selectQueryWithLimit = selectQuery + limitClause;
    assertThat(
        QueryDataFunction.applyLimitClause(selectQueryWithLimit, limit_10, queryResultSetLimit_100))
            .isEqualTo(selectQueryWithLimit);
  }

  @Test
  public void applyLimitClauseAddsQueryResultSetLimit() {
    assertThat(QueryDataFunction.applyLimitClause(selectQuery, limit_0, queryResultSetLimit_100))
        .isEqualTo(selectQuery + " LIMIT " + queryResultSetLimit_100);
  }

  @Test
  public void applyLimitClausePrefersLimitOverQueryResultSetLimit() {
    assertThat(QueryDataFunction.applyLimitClause(selectQuery, limit_10, queryResultSetLimit_100))
        .isEqualTo(selectQuery + " LIMIT " + limit_10);
  }

  @Test // GEODE-1907
  public void applyLimitClauseAddsQueryResultSetLimitIfMissingSpaceAfterFrom() {
    String selectQueryMissingSpaceAfterFrom = "SELECT * FROM" + SEPARATOR + "MyRegion";
    assertThat(QueryDataFunction.applyLimitClause(selectQueryMissingSpaceAfterFrom, limit_0,
        queryResultSetLimit_100))
            .isEqualTo(selectQueryMissingSpaceAfterFrom + " LIMIT " + queryResultSetLimit_100);
  }

  @Test
  public void applyLimitClauseDoesNotAddQueryResultSetLimitIfMissingSpaceAfterFromButLimitIsPresent() {
    String selectQueryMissingSpaceAfterFromWithLimit =
        "SELECT * FROM" + SEPARATOR + "MyRegion LIMIT " + limit_10;
    assertThat(QueryDataFunction.applyLimitClause(selectQueryMissingSpaceAfterFromWithLimit,
        limit_0, queryResultSetLimit_100)).isEqualTo(selectQueryMissingSpaceAfterFromWithLimit);
  }

  @Test
  public void applyLimitClauseShouldTrimQuery() throws Exception {
    String query = selectQuery + System.lineSeparator();
    assertThat(QueryDataFunction.applyLimitClause(query, limit_10, queryResultSetLimit_100))
        .isEqualTo(selectQuery + " LIMIT " + limit_10);
  }

  @Test
  public void applyLimitClauseShouldTrimQueryAndUseDefaultLimit() throws Exception {
    String query = selectQuery + System.lineSeparator();
    assertThat(QueryDataFunction.applyLimitClause(query, limit_0, queryResultSetLimit_100))
        .isEqualTo(selectQuery + " LIMIT " + queryResultSetLimit_100);
  }

  @Test
  public void applyLimitClauseShouldIgnoreComments() throws Exception {
    String query = "--comment" + System.lineSeparator() + selectQuery + System.lineSeparator()
        + "--comment" + System.lineSeparator();
    assertThat(QueryDataFunction.applyLimitClause(query, limit_10, queryResultSetLimit_100))
        .isEqualTo(selectQuery + " LIMIT " + limit_10);
  }

  @Test
  public void applyLimitShouldIgnoreNewLinesBetweenAndAfterQuery() throws Exception {
    String query = "select" + System.lineSeparator() + " * from" + System.lineSeparator()
        + "/testRegion" + System.lineSeparator();
    assertThat(QueryDataFunction.applyLimitClause(query, limit_0, queryResultSetLimit_100))
        .isEqualTo("select  * from /testRegion LIMIT 100");
  }

  @Test
  public void shouldFailAtEmptyQuery() throws Exception {
    assertThatThrownBy(() -> QueryDataFunction.applyLimitClause("", 0, 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("invalid query");
  }

  @Test
  public void shouldFailWithCommentOnly() throws Exception {
    assertThatThrownBy(() -> QueryDataFunction.applyLimitClause("--comment", 0, 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("invalid query");
  }

  @Test
  public void brokenSelectAndBrokenFromClauseShouldAddLimitAsWell() throws Exception {
    String query = "select r.name," + System.lineSeparator() + "r.id from" + System.lineSeparator()
        + "/testRegion" + System.lineSeparator() + "r" + System.lineSeparator();
    assertThat(QueryDataFunction.applyLimitClause(query, limit_0, queryResultSetLimit_100))
        .isEqualTo("select r.name, r.id from /testRegion r LIMIT 100");
  }
}
