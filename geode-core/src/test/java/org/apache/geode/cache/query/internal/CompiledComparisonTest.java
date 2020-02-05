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
package org.apache.geode.cache.query.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.index.IndexProtocol;
import org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes;

public class CompiledComparisonTest {
  private CompiledComparison compiledComparison;
  private QueryExecutionContext queryExecutionContext;

  @Before
  public void setUp() {
    CompiledValue aliasId = new CompiledID("Batman");
    CompiledValue clarkLiteral = new CompiledLiteral("BruceWayne");
    compiledComparison =
        spy(new CompiledComparison(aliasId, clarkLiteral, OQLLexerTokenTypes.TOK_EQ));
    queryExecutionContext = mock(QueryExecutionContext.class);
  }

  @Test
  public void getSizeEstimateShouldReturnZeroWhenBothFieldsAreIndexed()
      throws NameResolutionException, TypeMismatchException, FunctionDomainException,
      QueryInvocationTargetException {
    IndexInfo[] indexInfos = new IndexInfo[] {mock(IndexInfo.class), mock(IndexInfo.class)};
    doReturn(indexInfos).when(compiledComparison).getIndexInfo(queryExecutionContext);

    assertThat(compiledComparison.getSizeEstimate(queryExecutionContext)).isEqualTo(0);
  }

  @Test
  public void getSizeEstimateShouldReturnZeroWhenTheIndexKeyIsUndefined()
      throws NameResolutionException, TypeMismatchException, FunctionDomainException,
      QueryInvocationTargetException {
    IndexInfo indexInfo = mock(IndexInfo.class);
    when(indexInfo.evaluateIndexKey(queryExecutionContext)).thenReturn(QueryService.UNDEFINED);
    IndexInfo[] indexInfos = new IndexInfo[] {indexInfo};
    doReturn(indexInfos).when(compiledComparison).getIndexInfo(queryExecutionContext);

    assertThat(compiledComparison.getSizeEstimate(queryExecutionContext)).isEqualTo(0);
  }

  @Test
  public void getSizeEstimateShouldReturnOneWhenThereAreNoIndexes() throws NameResolutionException,
      TypeMismatchException, FunctionDomainException, QueryInvocationTargetException {
    assertThat(compiledComparison.getSizeEstimate(queryExecutionContext)).isEqualTo(1);
  }

  @Test
  public void getSizeEstimateShouldReturnHintSizeWhenTheIndexIsHinted()
      throws NameResolutionException, TypeMismatchException, FunctionDomainException,
      QueryInvocationTargetException {
    String indexName = "MyIndex";
    when(queryExecutionContext.isHinted(indexName)).thenReturn(true);
    when(queryExecutionContext.getHintSize(indexName)).thenReturn(10);
    IndexProtocol indexProtocol = mock(IndexProtocol.class);
    when(indexProtocol.getName()).thenReturn(indexName);
    IndexInfo indexInfo = spy(new IndexInfo(null, null, indexProtocol, 0, null, 0));
    doReturn("Key1").when(indexInfo).evaluateIndexKey(queryExecutionContext);
    IndexInfo[] indexInfos = new IndexInfo[] {indexInfo};
    doReturn(indexInfos).when(compiledComparison).getIndexInfo(queryExecutionContext);

    assertThat(compiledComparison.getSizeEstimate(queryExecutionContext)).isEqualTo(10);
  }

  @Test
  public void getSizeEstimateShouldReturnIndexSizeEstimate() throws NameResolutionException,
      TypeMismatchException, FunctionDomainException, QueryInvocationTargetException {
    String indexName = "MyIndex";
    IndexProtocol indexProtocol = mock(IndexProtocol.class);
    when(indexProtocol.getName()).thenReturn(indexName);
    when(indexProtocol.getSizeEstimate(any(), anyInt(), anyInt())).thenReturn(15);
    IndexInfo indexInfo = spy(new IndexInfo(null, null, indexProtocol, 0, null, 0));
    doReturn("Key1").when(indexInfo).evaluateIndexKey(queryExecutionContext);
    IndexInfo[] indexInfos = new IndexInfo[] {indexInfo};
    doReturn(indexInfos).when(compiledComparison).getIndexInfo(queryExecutionContext);

    assertThat(compiledComparison.getSizeEstimate(queryExecutionContext)).isEqualTo(15);
  }
}
