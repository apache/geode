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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes;

public class GroupJunctionTest {
  private GroupJunction groupJunction;
  private CompiledValue nameIsClark;
  private CompiledValue aliasNotBatman;
  private CompiledValue aliasNotEqualsNameFilter;
  private CompiledValue aliasNotEqualsNameFilterWithPreferredFlag;
  private CompiledValue aliasNotEqualsNameFilterWithMultipleIndexes;

  @Mock
  private QueryExecutionContext queryExecutionContext;
  @Captor
  private ArgumentCaptor<Integer> indexCaptor;
  @Captor
  private ArgumentCaptor<List<Object>> evalOperandsCaptor;

  @Before
  public void setUp() throws NameResolutionException, TypeMismatchException,
      QueryInvocationTargetException, FunctionDomainException {
    MockitoAnnotations.initMocks(this);
    nameIsClark = spy(new CompiledComparison(new CompiledID("name"), new CompiledLiteral("Clark"),
        OQLLexerTokenTypes.TOK_EQ));
    doReturn(mock(PlanInfo.class)).when(nameIsClark).getPlanInfo(any());
    doReturn(true).when(nameIsClark).isDependentOnCurrentScope(any());

    aliasNotBatman = spy(new CompiledComparison(new CompiledID("alias"),
        new CompiledLiteral("Batman"), OQLLexerTokenTypes.TOK_NE));
    doReturn(mock(PlanInfo.class)).when(aliasNotBatman).getPlanInfo(any());
    doReturn(true).when(aliasNotBatman).isDependentOnCurrentScope(any());

    aliasNotEqualsNameFilter = spy(new CompiledComparison(new CompiledID("alias"),
        new CompiledID("name"), OQLLexerTokenTypes.TOK_NE));
    PlanInfo planInfo = new PlanInfo();
    planInfo.evalAsFilter = true;
    planInfo.indexes = Collections.singletonList("singleIndex");
    doReturn(planInfo).when(aliasNotEqualsNameFilter).getPlanInfo(any());
    doReturn(true).when(aliasNotEqualsNameFilter).isDependentOnCurrentScope(any());

    aliasNotEqualsNameFilterWithPreferredFlag = spy(new CompiledComparison(new CompiledID("alias"),
        new CompiledID("name"), OQLLexerTokenTypes.TOK_NE));
    PlanInfo planInfo2 = new PlanInfo();
    planInfo2.evalAsFilter = true;
    planInfo2.indexes = Collections.singletonList("singleIndex");
    planInfo2.isPreferred = true;
    doReturn(planInfo2).when(aliasNotEqualsNameFilterWithPreferredFlag).getPlanInfo(any());
    doReturn(true).when(aliasNotEqualsNameFilterWithPreferredFlag).isDependentOnCurrentScope(any());

    aliasNotEqualsNameFilterWithMultipleIndexes =
        spy(new CompiledComparison(new CompiledID("alias"), new CompiledID("name"),
            OQLLexerTokenTypes.TOK_NE));
    PlanInfo planInfo3 = new PlanInfo();
    planInfo3.evalAsFilter = true;
    planInfo3.indexes = Arrays.asList("multipleIndex1", "multipleIndex2");
    doReturn(planInfo3).when(aliasNotEqualsNameFilterWithMultipleIndexes).getPlanInfo(any());
    doReturn(true).when(aliasNotEqualsNameFilterWithMultipleIndexes)
        .isDependentOnCurrentScope(any());
  }

  @Test
  public void organizeOperandsForORTheOperationShouldUseAllOperands()
      throws NameResolutionException, TypeMismatchException, QueryInvocationTargetException,
      FunctionDomainException {
    groupJunction = spy(new GroupJunction(OQLLexerTokenTypes.LITERAL_or, new RuntimeIterator[] {},
        true, new CompiledValue[] {nameIsClark, aliasNotBatman}));
    OrganizedOperands organizedOperands = groupJunction.organizeOperands(queryExecutionContext);

    assertThat(organizedOperands).isNotNull();
    verify(groupJunction, times(1)).createOrganizedOperandsObject(indexCaptor.capture(),
        evalOperandsCaptor.capture());
    assertThat(indexCaptor.getValue()).isEqualTo(2);
    assertThat(evalOperandsCaptor.getValue()).isEqualTo(Arrays.asList(nameIsClark, aliasNotBatman));
  }

  @Test
  public void organizeOperandsForANDOperationShouldUseAllOperandsAndFiltersWhenHintsHaveBeenProvided()
      throws NameResolutionException, TypeMismatchException, QueryInvocationTargetException,
      FunctionDomainException {
    groupJunction = spy(new GroupJunction(OQLLexerTokenTypes.LITERAL_and, new RuntimeIterator[] {},
        true, new CompiledValue[] {nameIsClark, aliasNotBatman, aliasNotEqualsNameFilter}));
    when(queryExecutionContext.hasHints()).thenReturn(true);
    when(queryExecutionContext.hasMultiHints()).thenReturn(true);
    OrganizedOperands organizedOperands = groupJunction.organizeOperands(queryExecutionContext);

    assertThat(organizedOperands).isNotNull();
    verify(groupJunction, times(1)).createOrganizedOperandsObject(indexCaptor.capture(),
        evalOperandsCaptor.capture());
    assertThat(indexCaptor.getValue()).isEqualTo(1);
    assertThat(evalOperandsCaptor.getValue())
        .isEqualTo(Arrays.asList(aliasNotEqualsNameFilter, nameIsClark, aliasNotBatman));
  }

  @Test
  public void organizeOperandsForANDOperationShouldUseSingleIndexOptimization()
      throws NameResolutionException, TypeMismatchException, QueryInvocationTargetException,
      FunctionDomainException {
    groupJunction = spy(new GroupJunction(OQLLexerTokenTypes.LITERAL_and, new RuntimeIterator[] {},
        true, new CompiledValue[] {aliasNotBatman, aliasNotEqualsNameFilter}));
    OrganizedOperands organizedOperands = groupJunction.organizeOperands(queryExecutionContext);

    assertThat(organizedOperands).isNotNull();
    verify(groupJunction, times(1)).createOrganizedOperandsObject(indexCaptor.capture(),
        evalOperandsCaptor.capture());
    assertThat(indexCaptor.getValue()).isEqualTo(1);
    assertThat(evalOperandsCaptor.getValue())
        .isEqualTo(Arrays.asList(aliasNotEqualsNameFilter, aliasNotBatman));
  }

  @Test
  public void organizeOperandsForANDOperationShouldUseSingleIndexOptimizationAndOverrideBestFilterWithPreferredOne()
      throws NameResolutionException, TypeMismatchException, QueryInvocationTargetException,
      FunctionDomainException {
    groupJunction = spy(new GroupJunction(OQLLexerTokenTypes.LITERAL_and, new RuntimeIterator[] {},
        true, new CompiledValue[] {aliasNotBatman, aliasNotEqualsNameFilter,
            aliasNotEqualsNameFilterWithPreferredFlag}));
    OrganizedOperands organizedOperands = groupJunction.organizeOperands(queryExecutionContext);

    assertThat(organizedOperands).isNotNull();
    verify(groupJunction, times(1)).createOrganizedOperandsObject(indexCaptor.capture(),
        evalOperandsCaptor.capture());
    assertThat(indexCaptor.getValue()).isEqualTo(1);
    assertThat(evalOperandsCaptor.getValue()).isEqualTo(Arrays.asList(
        aliasNotEqualsNameFilterWithPreferredFlag, aliasNotBatman, aliasNotEqualsNameFilter));
  }

  @Test
  public void organizeOperandsForANDOperationShouldUseAllOperandsAndFiltersWhenHintsHaveNotBeenProvidedAndSingleIndexOptimizationCanNotBeApplied()
      throws NameResolutionException, TypeMismatchException, QueryInvocationTargetException,
      FunctionDomainException {
    groupJunction = spy(new GroupJunction(OQLLexerTokenTypes.LITERAL_and, new RuntimeIterator[] {},
        true, new CompiledValue[] {nameIsClark, aliasNotEqualsNameFilterWithMultipleIndexes}));
    OrganizedOperands organizedOperands = groupJunction.organizeOperands(queryExecutionContext);

    assertThat(organizedOperands).isNotNull();
    verify(groupJunction, times(1)).createOrganizedOperandsObject(indexCaptor.capture(),
        evalOperandsCaptor.capture());
    assertThat(indexCaptor.getValue()).isEqualTo(1);
    assertThat(evalOperandsCaptor.getValue())
        .isEqualTo(Arrays.asList(aliasNotEqualsNameFilterWithMultipleIndexes, nameIsClark));
  }
}
