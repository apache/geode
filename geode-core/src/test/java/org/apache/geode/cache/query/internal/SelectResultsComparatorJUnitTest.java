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
/*
 * Created on Nov 14, 2005
 */
package org.apache.geode.cache.query.internal;

import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.Iterator;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.Test;

import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes;

public class SelectResultsComparatorJUnitTest implements OQLLexerTokenTypes {

  @Test
  public void testComparatorForSortedSet() throws Exception {
    int sameSizeVar = 0;
    boolean sameSizeVarSetFlag = false;
    SortedSet testSet =
        Collections.synchronizedSortedSet(new TreeSet(new SelectResultsComparator()));
    for (int i = 0; i < 10; i++) {
      Random rand = new Random();
      SelectResults resultsSet = new ResultsSet();
      int size = rand.nextInt();
      if (size < 0) {
        size = 0 - size;
      }
      size = size % 20;
      if (!sameSizeVarSetFlag) {
        sameSizeVar = size;
        sameSizeVarSetFlag = true;
      }
      for (int j = 0; j < size; j++) {
        resultsSet.add(new Object());
      }
      testSet.add(resultsSet);
    }

    SelectResults resultsSet = new ResultsSet();
    for (int j = 0; j < sameSizeVar; j++) {
      resultsSet.add(new Object());
    }
    testSet.add(resultsSet);
    if (testSet.size() != 11) {
      fail("Same size resultSets were overwritten");
    }
    Iterator iter1 = testSet.iterator();
    Iterator iter2 = testSet.iterator();
    iter2.next();

    while (iter2.hasNext()) {
      SelectResults sr1 = (SelectResults) iter1.next();
      SelectResults sr2 = (SelectResults) iter2.next();
      if (sr1.size() > sr2.size()) {
        fail("This is not expected behaviour");
      }
    }
  }
}
