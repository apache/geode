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

package org.apache.geode.management.internal.configuration.converters;

import static org.assertj.core.api.Assertions.assertThat;

import org.assertj.core.api.SoftAssertions;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.management.configuration.Index;
import org.apache.geode.management.configuration.IndexType;

public class IndexConverterTest {
  private IndexConverter indexConverter;

  @Before
  public void init() {
    indexConverter = new IndexConverter();
  }

  @Test
  public void fromXMLRangetoConfigOkay() {
    RegionConfig.Index xmlIndex = new RegionConfig.Index();
    xmlIndex.setType("range");
    xmlIndex.setName("testRange");
    xmlIndex.setExpression("age > 65");
    xmlIndex.setKeyIndex(false);

    Index index = indexConverter.fromNonNullXmlObject(xmlIndex);
    SoftAssertions.assertSoftly(softly -> {
      softly.assertThat(index.getExpression()).as("expression").isEqualTo("age > 65");
      softly.assertThat(index.getKeyIndex()).as("keyIndex").isFalse();
      softly.assertThat(index.getIndexType()).as("indexType").isEqualTo(IndexType.FUNCTIONAL);
      softly.assertThat(index.getName()).as("name").isEqualTo("testRange");
    });
  }

  @Test
  public void fromXMLHashtoConfigOkay() {
    RegionConfig.Index xmlIndex = new RegionConfig.Index();
    xmlIndex.setType("hash");
    xmlIndex.setName("testHash");
    xmlIndex.setExpression("age > 65");
    xmlIndex.setKeyIndex(false);

    Index index = indexConverter.fromNonNullXmlObject(xmlIndex);
    assertThat(index.getExpression()).isEqualTo("age > 65");
    assertThat(index.getKeyIndex()).isFalse();
    assertThat(index.getIndexType()).isEqualTo(IndexType.HASH_DEPRECATED);
    assertThat(index.getName()).isEqualTo("testHash");
  }

  @Test
  public void fromXMLKeytoConfigOkay() {
    RegionConfig.Index xmlIndex = new RegionConfig.Index();
    xmlIndex.setType("key");
    xmlIndex.setName("testKey");
    xmlIndex.setExpression("key");
    xmlIndex.setKeyIndex(true);

    Index index = indexConverter.fromNonNullXmlObject(xmlIndex);
    assertThat(index.getExpression()).isEqualTo("key");
    assertThat(index.getKeyIndex()).isTrue();
    assertThat(index.getIndexType()).isEqualTo(IndexType.PRIMARY_KEY);
    assertThat(index.getName()).isEqualTo("testKey");
  }

  @Test
  public void fromConfigRangetoXMLOkay() {
    Index index = new Index();
    index.setIndexType(IndexType.FUNCTIONAL);
    index.setExpression("age < 65");
    index.setKeyIndex(false);
    index.setName("testRange");

    RegionConfig.Index xmlIndex = indexConverter.fromNonNullConfigObject(index);
    assertThat(xmlIndex.getExpression()).isEqualTo("age < 65");
    assertThat(xmlIndex.getName()).isEqualTo("testRange");
    assertThat(xmlIndex.getType()).isEqualTo("range");
    assertThat(xmlIndex.isKeyIndex()).isFalse();
  }

  @Test
  public void fromConfigKeytoXMLOkay() {
    Index index = new Index();
    index.setIndexType(IndexType.PRIMARY_KEY);
    index.setExpression("key");
    index.setKeyIndex(true);
    index.setName("testKey");

    RegionConfig.Index xmlIndex = indexConverter.fromNonNullConfigObject(index);
    assertThat(xmlIndex.getExpression()).isEqualTo("key");
    assertThat(xmlIndex.getName()).isEqualTo("testKey");
    assertThat(xmlIndex.getType()).isEqualTo("key");
    assertThat(xmlIndex.isKeyIndex()).isTrue();
  }

  @Test
  public void fromConfigHashtoXMLOkay() {
    Index index = new Index();
    index.setIndexType(IndexType.HASH_DEPRECATED);
    index.setExpression("key");
    index.setKeyIndex(false);
    index.setName("testHash");

    RegionConfig.Index xmlIndex = indexConverter.fromNonNullConfigObject(index);
    assertThat(xmlIndex.getExpression()).isEqualTo("key");
    assertThat(xmlIndex.getName()).isEqualTo("testHash");
    assertThat(xmlIndex.getType()).isEqualTo("hash");
    assertThat(xmlIndex.isKeyIndex()).isFalse();
  }

  @Test
  public void fromXMLtoConfigFail() {
//    RegionConfig.Index xmlIndex = new RegionConfig.Index();
//    xmlIndex.setType("krindex");
//    xmlIndex.setName("testFail");
//    xmlIndex.setExpression("key");
//    xmlIndex.setKeyIndex(false);
//
//    Index index = indexConverter.fromNonNullXmlObject(xmlIndex);
//    assertThat(index.getExpression()).isEqualTo("key");
//    assertThat(index.getKeyIndex()).isTrue();
//    assertThat(index.getIndexType()).isEqualTo(IndexType.PRIMARY_KEY);
//    assertThat(index.getName()).isEqualTo("testKey");
  }

  @Test
  public void fromConfigtoXMLFail() {

  }

}