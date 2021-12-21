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

import static org.apache.geode.management.configuration.IndexType.HASH_DEPRECATED;
import static org.apache.geode.management.configuration.IndexType.KEY;
import static org.apache.geode.management.configuration.IndexType.RANGE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;

import org.junit.Test;

import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.management.configuration.Index;

public class IndexConverterTest {
  private final IndexConverter indexConverter = new IndexConverter();

  @Test
  public void fromNonNullConfigObject_copiesSimpleProperties() {
    Index index = new Index();
    index.setExpression("index expression");
    index.setName("index name");
    index.setRegionPath("index region path");

    RegionConfig.Index regionConfigIndex = indexConverter.fromNonNullConfigObject(index);

    assertSoftly(softly -> {
      softly.assertThat(regionConfigIndex.getExpression())
          .isEqualTo(index.getExpression());
      softly.assertThat(regionConfigIndex.getName())
          .isEqualTo(index.getName());
      softly.assertThat(regionConfigIndex.getFromClause())
          .isEqualTo(index.getRegionPath());
    });
  }

  @Test
  public void fromNonNullConfigObject_mapsFunctionalIndexTypeToRangeIndexType() {
    Index index = new Index();
    index.setIndexType(RANGE);

    RegionConfig.Index regionConfigIndex = indexConverter.fromNonNullConfigObject(index);

    assertSoftly(softly -> {
      softly.assertThat(regionConfigIndex.getType())
          .as("type")
          .isEqualTo("range");
      softly.assertThat(regionConfigIndex.isKeyIndex())
          .as("is key index")
          .isFalse();
    });
  }

  @Test
  public void fromNonNullConfigObject_mapsPrimaryKeyIndexTypeToKeyIndexType() {
    Index index = new Index();
    index.setIndexType(KEY);

    RegionConfig.Index regionConfigIndex = indexConverter.fromNonNullConfigObject(index);

    assertSoftly(softly -> {
      softly.assertThat(regionConfigIndex.getType())
          .as("type")
          .isEqualTo("key");
      softly.assertThat(regionConfigIndex.isKeyIndex())
          .as("is key index")
          .isTrue();
    });

  }

  @Test
  public void fromNonNullConfigObject_mapsHashDeprecatedIndexTypeToHashIndexType() {
    Index index = new Index();
    index.setIndexType(HASH_DEPRECATED);

    RegionConfig.Index regionConfigIndex = indexConverter.fromNonNullConfigObject(index);

    assertSoftly(softly -> {
      softly.assertThat(regionConfigIndex.getType())
          .as("type")
          .isEqualTo("hash");
      softly.assertThat(regionConfigIndex.isKeyIndex())
          .as("is key index")
          .isFalse();
    });
  }

  @Test
  public void fromNonNullXmlObject_copiesSimpleProperties() {
    RegionConfig.Index regionConfigIndex = new RegionConfig.Index();
    regionConfigIndex.setName("region config index name");
    regionConfigIndex.setExpression("region config index expression");
    regionConfigIndex.setFromClause("region config index from clause");

    Index index = indexConverter.fromNonNullXmlObject(regionConfigIndex);

    assertSoftly(softly -> {
      softly.assertThat(index.getExpression())
          .isEqualTo(regionConfigIndex.getExpression());
      softly.assertThat(index.getName())
          .isEqualTo(regionConfigIndex.getName());
      softly.assertThat(index.getRegionPath())
          .isEqualTo(regionConfigIndex.getFromClause());
    });
  }

  @Test
  public void fromNonNullXmlObject_mapsHashIndexTypeToHashDeprecatedIndexType() {
    RegionConfig.Index regionConfigIndex = new RegionConfig.Index();
    regionConfigIndex.setType("hash");

    Index index = indexConverter.fromNonNullXmlObject(regionConfigIndex);

    assertThat(index.getIndexType())
        .as("type")
        .isEqualTo(HASH_DEPRECATED);
  }

  @Test
  public void fromNonNullXmlObject_mapsRangeIndexTypeToFunctionalIndexType() {
    RegionConfig.Index regionConfigIndex = new RegionConfig.Index();
    regionConfigIndex.setType("range");

    Index index = indexConverter.fromNonNullXmlObject(regionConfigIndex);

    assertThat(index.getIndexType())
        .as("type")
        .isEqualTo(RANGE);
  }

  @Test
  public void fromNonNullXmlObject_mapsKeyIndexTypeToPrimaryKeyIndexType() {
    RegionConfig.Index regionConfigIndex = new RegionConfig.Index();
    regionConfigIndex.setType("key");

    Index index = indexConverter.fromNonNullXmlObject(regionConfigIndex);

    assertThat(index.getIndexType())
        .as("type")
        .isEqualTo(KEY);
  }
}
