/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.cache.lucene.internal.cli;

import java.io.Serializable;

public class LuceneSearchResults<K,V> implements Comparable<LuceneSearchResults>, Serializable {

  private String key;
  private String value;
  private float score;
  private boolean exceptionFlag = false;
  private String exceptionMessage;


  public LuceneSearchResults(final String key, final String value, final float score) {
    this.key = key;
    this.value = value;
    this.score = score;
  }

  public LuceneSearchResults(final String key) {
    this.key = key;
  }

  public LuceneSearchResults(final boolean exceptionFlag, final String exceptionMessage) {
    this.exceptionFlag=exceptionFlag;
    this.exceptionMessage=exceptionMessage;
  }

  public String getKey() {
    return key;
  }

  public String getValue() {
    return value;
  }

  public float getScore() {
    return score;
  }

  @Override
  public int compareTo(final LuceneSearchResults searchResults) {
    return Float.compare(getScore(),searchResults.getScore());
  }

  public boolean getExeptionFlag() { return exceptionFlag; }

  public String getExceptionMessage() { return exceptionMessage; }

  @Override public String toString() {
    return "LuceneSearchResults{" +
      "key='" + key + '\'' +
      ", value='" + value + '\'' +
      ", score=" + score +
      ", exceptionFlag=" + exceptionFlag +
      ", exceptionMessage='" + exceptionMessage + '\'' +
      '}';
  }
}
