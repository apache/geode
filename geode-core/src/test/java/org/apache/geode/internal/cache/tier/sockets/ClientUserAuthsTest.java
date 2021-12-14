/*
 *
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

package org.apache.geode.internal.cache.tier.sockets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.STRICT_STUBS;

import org.apache.shiro.subject.Subject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.junit.jupiter.MockitoSettings;

@MockitoSettings(strictness = STRICT_STUBS)
class ClientUserAuthsTest {
  @Nested
  class PutSubject {
    @ParameterizedTest(name = "{displayName} givenId={0}")
    // IDs 0 and -1 both request a generated unique ID.
    @ValueSource(longs = {0, -1})
    void associatesSubjectWithGeneratedIdIfGivenIdRequestsGeneratedId(long givenId) {
      SubjectIdGenerator idGenerator = mock(SubjectIdGenerator.class);
      ClientUserAuths auths = new ClientUserAuths(idGenerator);

      long idFromGenerator = -234950983L;
      when(idGenerator.generateId())
          .thenReturn(idFromGenerator);

      Subject subject = mock(Subject.class);

      long returnedId = auths.putSubject(subject, givenId);

      Subject subjectWithGeneratedId = auths.getSubject(idFromGenerator);
      assertThat(subjectWithGeneratedId)
          .as("subject with generated ID")
          .isSameAs(subject);

      assertThat(returnedId)
          .as("returned ID")
          .isEqualTo(idFromGenerator);

      Subject subjectWithGivenId = auths.getSubject(givenId);
      assertThat(subjectWithGivenId)
          .as("subject with given ID")
          .isNull();

      verifyNoInteractions(subject);
    }

    @ParameterizedTest(name = "{displayName} givenId={0}")
    // IDs 0 and -1 both request a generated unique ID.
    @ValueSource(longs = {0, -1})
    void associatesSubjectWithSecondGeneratedIdIfFirstGeneratedIdIsZero(long givenId) {
      SubjectIdGenerator idGenerator = mock(SubjectIdGenerator.class);
      ClientUserAuths auths = new ClientUserAuths(idGenerator);

      long secondIdFromGenerator = -9341L;
      when(idGenerator.generateId())
          .thenReturn(0L)
          .thenReturn(secondIdFromGenerator);

      Subject subject = mock(Subject.class);

      long returnedId = auths.putSubject(subject, givenId);

      Subject subjectWithSecondGeneratedId = auths.getSubject(secondIdFromGenerator);
      assertThat(subjectWithSecondGeneratedId)
          .as("subject with second generated ID")
          .isSameAs(subject);

      assertThat(returnedId)
          .as("returned ID")
          .isEqualTo(secondIdFromGenerator);

      Subject subjectWithGivenId = auths.getSubject(givenId);
      assertThat(subjectWithGivenId)
          .as("subject with given ID")
          .isNull();

      verifyNoInteractions(subject);
    }

    @Test
    void associatesSubjectWithGivenIdIfGivenIdDoesNotRequestGeneratedId() {
      SubjectIdGenerator idGenerator = mock(SubjectIdGenerator.class);
      ClientUserAuths auths = new ClientUserAuths(idGenerator);

      // Neither 0 nor -1, and so gives the ID to associate with this subject
      long givenId = 3L;
      Subject subject = mock(Subject.class);

      long returnedId = auths.putSubject(subject, givenId);

      Subject subjectWithGivenId = auths.getSubject(givenId);
      assertThat(subjectWithGivenId)
          .as("subject with given ID")
          .isSameAs(subject);

      assertThat(returnedId)
          .as("returned ID")
          .isEqualTo(givenId);

      verifyNoInteractions(idGenerator);
    }

    @Test
    void doesNotInteractWithAddedSubject() {
      ClientUserAuths auths = new ClientUserAuths(null);

      Subject subject = mock(Subject.class);

      auths.putSubject(subject, 674928374L);

      verifyNoInteractions(subject);
    }

    @Test
    void doesNotInteractWithSubjectsPreviouslyAssociatedWithSameId() {
      ClientUserAuths auths = new ClientUserAuths(null);

      Subject previousSubject1 = mock(Subject.class);
      Subject previousSubject2 = mock(Subject.class);
      Subject previousSubject3 = mock(Subject.class);
      Subject previousSubject4 = mock(Subject.class);
      Subject previousSubject5 = mock(Subject.class);

      Subject mostRecentlyAddedSubject = mock(Subject.class);

      long givenId = 34149L;
      auths.putSubject(previousSubject1, givenId);
      auths.putSubject(previousSubject2, givenId);
      auths.putSubject(previousSubject3, givenId);
      auths.putSubject(previousSubject4, givenId);
      auths.putSubject(previousSubject5, givenId);
      auths.putSubject(mostRecentlyAddedSubject, givenId);

      verifyNoInteractions(
          previousSubject1,
          previousSubject2,
          previousSubject3,
          previousSubject4,
          previousSubject5);
    }
  }

  @Nested
  class GetSubjectById {
    @Test
    void returnsNullIfNoSubjectIsAssociatedWithId() {
      ClientUserAuths auths = new ClientUserAuths(null);

      long subjectId = 9234L;

      auths.putSubject(mock(Subject.class), 92L); // Different ID #1
      auths.putSubject(mock(Subject.class), 923L); // Different ID #2
      auths.putSubject(mock(Subject.class), 92345L); // Different ID #3
      // Note: We did not add a subject with subjectId

      assertThat(auths.getSubject(subjectId))
          .isNull();
    }

    @Test
    void returnsTheSubjectAssociatedWithId() {
      ClientUserAuths auths = new ClientUserAuths(null);

      long subjectId = 238947L;
      long differentId1 = 23894L;
      long differentId2 = 2389L;
      long differentId3 = 238L;

      Subject subjectAssociatedWithId = mock(Subject.class);
      auths.putSubject(subjectAssociatedWithId, subjectId);

      auths.putSubject(mock(Subject.class), differentId1);
      auths.putSubject(mock(Subject.class), differentId2);
      auths.putSubject(mock(Subject.class), differentId3);

      assertThat(auths.getSubject(subjectId))
          .isSameAs(subjectAssociatedWithId);
    }

    @Test
    void returnsTheMostRecentlyAddedSubjectAssociatedWithId() {
      ClientUserAuths auths = new ClientUserAuths(null);

      long subjectId = 927834L;
      auths.putSubject(mock(Subject.class), subjectId);
      auths.putSubject(mock(Subject.class), subjectId);
      auths.putSubject(mock(Subject.class), subjectId);
      auths.putSubject(mock(Subject.class), subjectId);
      auths.putSubject(mock(Subject.class), subjectId);
      auths.putSubject(mock(Subject.class), subjectId);

      Subject mostRecentlyAddedWithSpecifiedId = mock(Subject.class);
      auths.putSubject(mostRecentlyAddedWithSpecifiedId, subjectId);

      assertThat(auths.getSubject(subjectId))
          .isSameAs(mostRecentlyAddedWithSpecifiedId);
    }
  }

  @Nested
  class RemoveSubject {
    @Test
    void removesAssociationOfAllSubjectsWithId() {
      ClientUserAuths auths = new ClientUserAuths(null);

      long givenId = -487023L;
      auths.putSubject(mock(Subject.class), givenId);
      auths.putSubject(mock(Subject.class), givenId);
      auths.putSubject(mock(Subject.class), givenId);
      auths.putSubject(mock(Subject.class), givenId);
      auths.putSubject(mock(Subject.class), givenId);

      auths.removeSubject(givenId);

      assertThat(auths.getSubject(givenId))
          .isNull();
    }

    @Test
    void logsOutAllSubjectsAssociatedWithId() {
      ClientUserAuths auths = new ClientUserAuths(null);

      long givenId = 8963947L;
      Subject subject1 = mock(Subject.class);
      Subject subject2 = mock(Subject.class);
      Subject subject3 = mock(Subject.class);
      Subject subject4 = mock(Subject.class);
      Subject subject5 = mock(Subject.class);

      auths.putSubject(subject1, givenId);
      auths.putSubject(subject2, givenId);
      auths.putSubject(subject3, givenId);
      auths.putSubject(subject4, givenId);
      auths.putSubject(subject5, givenId);

      auths.removeSubject(givenId);

      verify(subject1).logout();
      verify(subject2).logout();
      verify(subject3).logout();
      verify(subject4).logout();
      verify(subject5).logout();
    }

    @Test
    void retainsAssociationsOfAllSubjectsWithOtherIds() {
      ClientUserAuths auths = new ClientUserAuths(null);

      long doomedSubjectId = 98347L;
      long retainedSubject1Id = doomedSubjectId + 1;
      long retainedSubject2Id = doomedSubjectId + 2;
      long retainedSubject3Id = doomedSubjectId + 3;

      Subject retainedSubject1 = mock(Subject.class);
      Subject retainedSubject2 = mock(Subject.class);
      Subject retainedSubject3 = mock(Subject.class);

      auths.putSubject(retainedSubject1, retainedSubject1Id);
      auths.putSubject(retainedSubject2, retainedSubject2Id);
      auths.putSubject(retainedSubject3, retainedSubject3Id);

      auths.putSubject(mock(Subject.class), doomedSubjectId);

      auths.removeSubject(doomedSubjectId);

      assertThat(auths.getSubject(retainedSubject1Id))
          .isSameAs(retainedSubject1);
      assertThat(auths.getSubject(retainedSubject2Id))
          .isSameAs(retainedSubject2);
      assertThat(auths.getSubject(retainedSubject3Id))
          .isSameAs(retainedSubject3);
    }

    @Test
    void doesNotInteractWithSubjectsAssociatedWithOtherIds() {
      ClientUserAuths auths = new ClientUserAuths(null);

      long doomedSubjectId = 78974L;

      long retainedSubjectId1 = doomedSubjectId + 1;
      long retainedSubjectId2 = doomedSubjectId + 2;
      long retainedSubjectId3 = doomedSubjectId + 3;
      Subject retainedSubject1a = mock(Subject.class);
      Subject retainedSubject1b = mock(Subject.class);
      Subject retainedSubject1c = mock(Subject.class);
      Subject retainedSubject2a = mock(Subject.class);
      Subject retainedSubject2b = mock(Subject.class);
      Subject retainedSubject2c = mock(Subject.class);
      Subject retainedSubject3a = mock(Subject.class);
      Subject retainedSubject3b = mock(Subject.class);
      Subject retainedSubject3c = mock(Subject.class);

      auths.putSubject(retainedSubject1a, retainedSubjectId1);
      auths.putSubject(retainedSubject1b, retainedSubjectId1);
      auths.putSubject(retainedSubject1c, retainedSubjectId1);
      auths.putSubject(retainedSubject2a, retainedSubjectId2);
      auths.putSubject(retainedSubject2b, retainedSubjectId2);
      auths.putSubject(retainedSubject2c, retainedSubjectId2);
      auths.putSubject(retainedSubject3a, retainedSubjectId3);
      auths.putSubject(retainedSubject3b, retainedSubjectId3);
      auths.putSubject(retainedSubject3c, retainedSubjectId3);

      auths.putSubject(mock(Subject.class), doomedSubjectId);

      auths.removeSubject(doomedSubjectId);

      verifyNoInteractions(
          retainedSubject1a,
          retainedSubject1b,
          retainedSubject1c,
          retainedSubject2a,
          retainedSubject2b,
          retainedSubject2c,
          retainedSubject3a,
          retainedSubject3b,
          retainedSubject3c);
    }

    @Test
    public void doesNotThrowIfNoSubjectsAssociatedWithId() {
      ClientUserAuths auths = new ClientUserAuths(null);

      assertThatNoException().isThrownBy(() -> auths.removeSubject(5678L));
    }
  }

  @Nested
  class ContinuousQueryAuths {
    @Test
    public void setUserAttributesForCqAssociatesSubjectWithCqName() {
      ClientUserAuths auths = new ClientUserAuths(null);

      long id = 39874L;
      String cqName = "cq4132";
      Subject subject = mock(Subject.class);
      auths.putSubject(subject, id);

      auths.setUserAuthAttributesForCq(cqName, id, true);

      assertThat(auths.getSubject(cqName))
          .isSameAs(subject);
    }

    @Test
    public void removeSubjectRemovesAssociationWithCqName() {
      ClientUserAuths auths = new ClientUserAuths(null);

      long id = 33L;
      String cqName = "cq23497" + id;
      auths.putSubject(mock(Subject.class), id);
      auths.setUserAuthAttributesForCq(cqName, id, true);

      auths.removeSubject(id);

      assertThat(auths.getSubject(cqName))
          .isNull();
    }

    @Test
    public void removeUserAuthAttributesForCqRemovesAssociationOfSubjectToCqName() {
      ClientUserAuths auths = new ClientUserAuths(null);

      long id = -43780L;
      String cqName = "cq98740";
      auths.putSubject(mock(Subject.class), id);
      auths.setUserAuthAttributesForCq(cqName, id, true);

      auths.removeUserAuthAttributesForCq(cqName, true);

      assertThat(auths.getSubject(cqName))
          .isNull();
    }
  }
}
