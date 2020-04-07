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
package org.apache.geode.internal.cache.partitioned.rebalance;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.internal.cache.partitioned.rebalance.model.PartitionedRegionLoadModel;

@RunWith(JUnitParamsRunner.class)
public class RestoreRedundancyDirectorTest {
  RemoveOverRedundancy removeOverRedundancy;
  SatisfyRedundancy satisfyRedundancy;
  MovePrimaries reassignPrimaries;
  PartitionedRegionLoadModel loadModel;

  @Before
  public void setUp() {
    removeOverRedundancy = mock(RemoveOverRedundancy.class);
    satisfyRedundancy = mock(SatisfyRedundancy.class);
    reassignPrimaries = mock(MovePrimaries.class);
    loadModel = mock(PartitionedRegionLoadModel.class);
  }

  @Test
  @Parameters({
      "true, true, true",
      "false, true, true",
      "false, false, false",
      "true, false, true"
  })
  public void isRebalanceNecessaryReturnsCorrectly(boolean redundancyImpaired,
      boolean shouldReassignPrimaries, boolean expectedResult) {
    RestoreRedundancyDirector director =
        spy(new RestoreRedundancyDirector(shouldReassignPrimaries));

    assertThat(director.isRebalanceNecessary(redundancyImpaired, false), is(expectedResult));
  }

  @Test
  public void initializeInitializesInternalDirectors() {
    RestoreRedundancyDirector director =
        spy(new RestoreRedundancyDirector(true, removeOverRedundancy, satisfyRedundancy,
            reassignPrimaries));

    director.initialize(loadModel);

    verify(removeOverRedundancy, times(1)).initialize(loadModel);
    verify(satisfyRedundancy, times(1)).initialize(loadModel);
    verify(reassignPrimaries, times(1)).initialize(loadModel);
  }

  @Test
  public void nextStepReturnsFalseIfNoOperationAttempted() {
    RestoreRedundancyDirector director =
        spy(new RestoreRedundancyDirector(true, removeOverRedundancy, satisfyRedundancy,
            reassignPrimaries));

    director.initialize(loadModel);

    when(removeOverRedundancy.nextStep()).thenReturn(false);
    when(satisfyRedundancy.nextStep()).thenReturn(false);
    when(reassignPrimaries.nextStep()).thenReturn(false);

    assertThat(director.nextStep(), is(false));
  }

  @Test
  public void nextStepPerformsStepsInCorrectOrderOneAtATime() {
    // Correct order is: remove over redundancy, satisfy redundancy, reassign primaries
    RestoreRedundancyDirector director =
        spy(new RestoreRedundancyDirector(true, removeOverRedundancy, satisfyRedundancy,
            reassignPrimaries));

    director.initialize(loadModel);

    when(removeOverRedundancy.nextStep()).thenReturn(true).thenReturn(false);
    when(satisfyRedundancy.nextStep()).thenReturn(true).thenReturn(false);
    when(reassignPrimaries.nextStep()).thenReturn(true).thenReturn(false);

    director.nextStep();
    verify(removeOverRedundancy, times(1)).nextStep();
    verify(satisfyRedundancy, times(0)).nextStep();
    verify(reassignPrimaries, times(0)).nextStep();

    director.nextStep();
    // We expect to see another call to removeOverRedundancy.nextStep() here, which will return
    // false and cause the director to move on to satisfying redundancy
    verify(removeOverRedundancy, times(2)).nextStep();
    verify(satisfyRedundancy, times(1)).nextStep();
    verify(reassignPrimaries, times(0)).nextStep();

    director.nextStep();
    // We expect to see another call to satisfyRedundancy.nextStep() here, which will return false
    // and cause the director to move on to reassigning primaries
    verify(removeOverRedundancy, times(2)).nextStep();
    verify(satisfyRedundancy, times(2)).nextStep();
    verify(reassignPrimaries, times(1)).nextStep();
  }

  @Test
  public void nextStepReturnsTrueIfOperationWasAttempted() {
    RestoreRedundancyDirector director =
        spy(new RestoreRedundancyDirector(true, removeOverRedundancy, satisfyRedundancy,
            reassignPrimaries));

    director.initialize(loadModel);

    // Return true for the first call, then false
    when(removeOverRedundancy.nextStep()).thenReturn(true).thenReturn(false);
    when(satisfyRedundancy.nextStep()).thenReturn(true).thenReturn(false);
    when(reassignPrimaries.nextStep()).thenReturn(true).thenReturn(false);

    // Remove over redundancy
    assertThat(director.nextStep(), is(true));
    // Satisfy redundancy
    assertThat(director.nextStep(), is(true));
    // Reassign primaries
    assertThat(director.nextStep(), is(true));
    // No work left to do
    assertThat(director.nextStep(), is(false));
  }

  @Test
  public void nextStepDoesNotAttemptToReassignPrimariesWhenReassignPrimariesIsFalse() {
    RestoreRedundancyDirector director =
        spy(new RestoreRedundancyDirector(false, removeOverRedundancy, satisfyRedundancy,
            reassignPrimaries));

    director.initialize(loadModel);

    director.nextStep();

    verify(removeOverRedundancy, times(1)).nextStep();
    verify(satisfyRedundancy, times(1)).nextStep();
    verify(reassignPrimaries, times(0)).nextStep();
  }
}
