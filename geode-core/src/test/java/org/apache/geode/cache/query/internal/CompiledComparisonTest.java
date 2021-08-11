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

import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.index.AbstractMapIndex;
import org.apache.geode.cache.query.internal.index.IndexData;
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

  @Test
  public void evaluateHandlesEntryDestroyedExceptionThrownByRegionEntryGetValue()
      throws NameResolutionException, TypeMismatchException, QueryInvocationTargetException,
      FunctionDomainException {
    CompiledValue left = mock(CompiledValue.class);
    CompiledValue right = mock(CompiledValue.class);

    ExecutionContext context = mock(ExecutionContext.class);
    when(context.isCqQueryContext()).thenReturn(true);
    Region.Entry<?, ?> leftEntry = mock(Region.Entry.class);
    when(left.evaluate(context)).thenReturn(leftEntry);
    when(leftEntry.getValue()).thenThrow(new EntryDestroyedException());
    Region.Entry<?, ?> rightEntry = mock(Region.Entry.class);
    when(right.evaluate(context)).thenReturn(rightEntry);
    when(rightEntry.getValue()).thenThrow(new EntryDestroyedException());

    CompiledComparison comparison =
        spy(new CompiledComparison(left, right, OQLLexerTokenTypes.TOK_EQ));

    comparison.evaluate(context);
  }

  @Test
  public void cannotUseIndexReturnsFalse_WithNullIndex() {
    CompiledValue left = mock(CompiledLiteral.class);
    CompiledValue right = mock(CompiledLiteral.class);
    IndexData indexData = mock(IndexData.class);
    when(indexData.getIndex()).thenReturn(null);
    ExecutionContext context = mock(ExecutionContext.class);

    CompiledComparison comparison =
        spy(new CompiledComparison(left, right, OQLLexerTokenTypes.TOK_NE));

    assertThat(comparison.indexCannotBeUsed(context, indexData)).isFalse();
  }

  @Test
  public void cannotUseIndexReturnsFalse_WithoutAllKeysIndex() {
    CompiledValue left = mock(CompiledLiteral.class);
    CompiledValue right = mock(CompiledLiteral.class);
    AbstractMapIndex indexProtocol = mock(AbstractMapIndex.class);
    when(indexProtocol.getIsAllKeys()).thenReturn(false);
    IndexData indexData = mock(IndexData.class);
    when(indexData.getIndex()).thenReturn(indexProtocol);
    ExecutionContext context = mock(ExecutionContext.class);

    CompiledComparison comparison =
        spy(new CompiledComparison(left, right, OQLLexerTokenTypes.TOK_NE));

    assertThat(comparison.indexCannotBeUsed(context, indexData)).isFalse();
  }

  @Test
  public void cannotUseIndexReturnsTrue_WithAllKeysIndexAndNE()
      throws NameResolutionException, TypeMismatchException, QueryInvocationTargetException,
      FunctionDomainException {
    CompiledValue left = mock(CompiledLiteral.class);
    CompiledValue right = mock(CompiledLiteral.class);
    AbstractMapIndex indexProtocol = mock(AbstractMapIndex.class);
    when(indexProtocol.getIsAllKeys()).thenReturn(true);
    IndexData indexData = mock(IndexData.class);
    when(indexData.getIndex()).thenReturn(indexProtocol);
    ExecutionContext context = mock(ExecutionContext.class);
    when(left.evaluate(context)).thenReturn(null);

    CompiledComparison comparison =
        spy(new CompiledComparison(left, right, OQLLexerTokenTypes.TOK_NE));

    assertThat(comparison.indexCannotBeUsed(context, indexData)).isTrue();
  }

  @Test
  public void cannotUseIndexReturnsTrue_WithAllKeysIndexAndEQWithLeftNull()
      throws NameResolutionException, TypeMismatchException, QueryInvocationTargetException,
      FunctionDomainException {
    CompiledValue left = mock(CompiledLiteral.class);
    CompiledValue right = mock(CompiledLiteral.class);
    AbstractMapIndex indexProtocol = mock(AbstractMapIndex.class);
    when(indexProtocol.getIsAllKeys()).thenReturn(true);
    IndexData indexData = mock(IndexData.class);
    when(indexData.getIndex()).thenReturn(indexProtocol);
    ExecutionContext context = mock(ExecutionContext.class);
    Region.Entry<?, ?> rightEntry = mock(Region.Entry.class);
    when(left.evaluate(context)).thenReturn(rightEntry);
    when(left.evaluate(context)).thenReturn(null);


    CompiledComparison comparison =
        spy(new CompiledComparison(left, right, OQLLexerTokenTypes.TOK_EQ));

    assertThat(comparison.indexCannotBeUsed(context, indexData)).isTrue();
  }

  @Test
  public void cannotUseIndexReturnsTrue_WithAllKeysIndexAndEQWithRightNull()
      throws NameResolutionException, TypeMismatchException, QueryInvocationTargetException,
      FunctionDomainException {
    CompiledValue left = mock(CompiledLiteral.class);
    CompiledValue right = mock(CompiledLiteral.class);
    AbstractMapIndex indexProtocol = mock(AbstractMapIndex.class);
    when(indexProtocol.getIsAllKeys()).thenReturn(true);
    IndexData indexData = mock(IndexData.class);
    when(indexData.getIndex()).thenReturn(indexProtocol);
    ExecutionContext context = mock(ExecutionContext.class);
    Region.Entry<?, ?> leftEntry = mock(Region.Entry.class);
    when(left.evaluate(context)).thenReturn(leftEntry);
    when(right.evaluate(context)).thenReturn(null);

    CompiledComparison comparison =
        spy(new CompiledComparison(left, right, OQLLexerTokenTypes.TOK_EQ));

    assertThat(comparison.indexCannotBeUsed(context, indexData)).isTrue();
  }

  @Test
  public void cannotUseIndexReturnsFalse_WithAllKeysIndexAndEQWithoutNulls()
      throws NameResolutionException, TypeMismatchException, QueryInvocationTargetException,
      FunctionDomainException {
    CompiledValue left = mock(CompiledLiteral.class);
    CompiledValue right = mock(CompiledLiteral.class);
    AbstractMapIndex indexProtocol = mock(AbstractMapIndex.class);
    when(indexProtocol.getIsAllKeys()).thenReturn(true);
    IndexData indexData = mock(IndexData.class);
    when(indexData.getIndex()).thenReturn(indexProtocol);
    ExecutionContext context = mock(ExecutionContext.class);
    Region.Entry<?, ?> leftEntry = mock(Region.Entry.class);
    when(left.evaluate(context)).thenReturn(leftEntry);
    Region.Entry<?, ?> rightEntry = mock(Region.Entry.class);
    when(right.evaluate(context)).thenReturn(rightEntry);

    CompiledComparison comparison =
        spy(new CompiledComparison(left, right, OQLLexerTokenTypes.TOK_EQ));

    assertThat(comparison.indexCannotBeUsed(context, indexData)).isFalse();
  }
}
