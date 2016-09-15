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

package org.apache.geode.management.internal.cli.domain;

import java.io.Serializable;

import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexStatistics;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.lang.ObjectUtils;
import org.apache.geode.internal.lang.StringUtils;

/**
 * The IndexDetails class encapsulates information for an Index on a Region in the GemFire Cache.
 * </p>
 * @see org.apache.geode.cache.query.Index
 * @see org.apache.geode.cache.query.IndexStatistics
 * @see org.apache.geode.cache.query.IndexType
 * @see org.apache.geode.distributed.DistributedMember
 * @since GemFire 7.0
 */
@SuppressWarnings("unused")
public class IndexDetails implements Comparable<IndexDetails>, Serializable {

  private IndexStatisticsDetails indexStatisticsDetails;

  private IndexType indexType;

  private String indexedExpression;
  private String fromClause;
  private String projectionAttributes;
  private String memberName;
  private String regionName;

  private final String indexName;
  private final String memberId;
  private final String regionPath;

  protected static void assertValidArgument(final boolean valid, final String message, final Object... args) {
    if (!valid) {
      throw new IllegalArgumentException(String.format(message, args));
    }
  }

  protected static <T extends Comparable<T>> int compare(final T obj1, final T obj2) {
    return (obj1 == null && obj2 == null ? 0 : (obj1 == null ? 1 : (obj2 == null ? -1 : obj1.compareTo(obj2))));
  }

  protected static IndexStatisticsDetails createIndexStatisticsDetails(final IndexStatistics indexStatistics) {
    final IndexStatisticsDetails indexStatisticsDetails = new IndexStatisticsDetails();

    indexStatisticsDetails.setNumberOfKeys(indexStatistics.getNumberOfKeys());
    indexStatisticsDetails.setNumberOfUpdates(indexStatistics.getNumUpdates());
    indexStatisticsDetails.setNumberOfValues(indexStatistics.getNumberOfValues());
    indexStatisticsDetails.setTotalUpdateTime(indexStatistics.getTotalUpdateTime());
    indexStatisticsDetails.setTotalUses(indexStatistics.getTotalUses());

    return indexStatisticsDetails;
  }

  public IndexDetails(final DistributedMember member, final Index index) {
    this(member.getId(), index.getRegion().getFullPath(), index.getName());

    setFromClause(index.getFromClause());
    setIndexedExpression(index.getIndexedExpression());
    setIndexType(index.getType());
    setMemberName(member.getName());
    setProjectionAttributes(index.getProjectionAttributes());
    setRegionName(index.getRegion().getName());

    if (index.getStatistics() != null) {
      setIndexStatisticsDetails(createIndexStatisticsDetails(index.getStatistics()));
    }
  }

  public IndexDetails(final String memberId, final String regionPath, final String indexName) {
    assertValidArgument(!StringUtils.isBlank(memberId), "The member having a region with an index must be specified!");
    assertValidArgument(!StringUtils.isBlank(regionPath), "The region in member (%1$s) with an index must be specified!", memberId);
    assertValidArgument(!StringUtils.isBlank(indexName), "The name of the index on region (%1$s) of member (%2$s) must be specified!", regionPath, memberId);
    this.memberId = memberId;
    this.regionPath = regionPath;
    this.indexName = indexName;
  }

  public String getFromClause() {
    return fromClause;
  }

  public void setFromClause(final String fromClause) {
    this.fromClause = fromClause;
  }

  public String getIndexedExpression() {
    return indexedExpression;
  }

  public void setIndexedExpression(final String indexedExpression) {
    this.indexedExpression = indexedExpression;
  }

  public String getIndexName() {
    return indexName;
  }

  public IndexStatisticsDetails getIndexStatisticsDetails() {
    return indexStatisticsDetails;
  }

  public void setIndexStatisticsDetails(final IndexStatisticsDetails indexStatisticsDetails) {
    this.indexStatisticsDetails = indexStatisticsDetails;
  }

  public IndexType getIndexType() {
    return indexType;
  }

  public void setIndexType(final IndexType indexType) {
    this.indexType = indexType;
  }

  public void setIndexType(final org.apache.geode.cache.query.IndexType indexType) {
    setIndexType(IndexType.valueOf(indexType));
  }

  public String getMemberId() {
    return memberId;
  }

  public String getMemberName() {
    return memberName;
  }

  public void setMemberName(final String memberName) {
    this.memberName = memberName;
  }

  public String getProjectionAttributes() {
    return projectionAttributes;
  }

  public void setProjectionAttributes(final String projectionAttributes) {
    this.projectionAttributes = projectionAttributes;
  }

  public String getRegionName() {
    return regionName;
  }

  public void setRegionName(final String regionName) {
    this.regionName = regionName;
  }

  public String getRegionPath() {
    return regionPath;
  }

  public int compareTo(final IndexDetails indexDetails) {
    int comparisonValue = compare(getMemberName(), indexDetails.getMemberName());
    comparisonValue = (comparisonValue != 0 ? comparisonValue : compare(getMemberId(), indexDetails.getMemberId()));
    comparisonValue = (comparisonValue != 0 ? comparisonValue : compare(getRegionPath(), indexDetails.getRegionPath()));
    return (comparisonValue != 0 ? comparisonValue : compare(getIndexName(), indexDetails.getIndexName()));
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == this) {
      return true;
    }

    if (!(obj instanceof IndexDetails)) {
      return false;
    }

    final IndexDetails that = (IndexDetails) obj;

    return ObjectUtils.equals(getMemberId(), that.getMemberId())
      && ObjectUtils.equals(getRegionPath(), that.getRegionPath())
      && ObjectUtils.equals(getIndexName(), that.getIndexName());
  }

  @Override
  public int hashCode() {
    int hashValue = 17;
    hashValue = 37 * hashValue + ObjectUtils.hashCode(getMemberId());
    hashValue = 37 * hashValue + ObjectUtils.hashCode(getRegionPath());
    hashValue = 37 * hashValue + ObjectUtils.hashCode(getIndexName());
    return hashValue;
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder(getClass().getSimpleName());

    buffer.append(" {fromClause = ").append(getFromClause());
    buffer.append(", indexExpression = ").append(getIndexedExpression());
    buffer.append(", indexName = ").append(getIndexName());
    buffer.append(", indexType = ").append(getIndexType());
    buffer.append(", memberId = ").append(getMemberId());
    buffer.append(", memberName = ").append(getMemberName());
    buffer.append(", regionName = ").append(getRegionName());
    buffer.append(", regionPath = ").append(getRegionPath());
    buffer.append(", projectAttributes = ").append(getProjectionAttributes());
    buffer.append("}");

    return buffer.toString();
  }

  public static class IndexStatisticsDetails implements Serializable {

    private Long numberOfKeys;
    private Long numberOfUpdates;
    private Long numberOfValues;
    private Long totalUpdateTime;
    private Long totalUses;

    public Long getNumberOfKeys() {
      return numberOfKeys;
    }

    public void setNumberOfKeys(final Long numberOfKeys) {
      this.numberOfKeys = numberOfKeys;
    }

    public Long getNumberOfUpdates() {
      return numberOfUpdates;
    }

    public void setNumberOfUpdates(final Long numberOfUpdates) {
      this.numberOfUpdates = numberOfUpdates;
    }

    public Long getNumberOfValues() {
      return numberOfValues;
    }

    public void setNumberOfValues(final Long numberOfValues) {
      this.numberOfValues = numberOfValues;
    }

    public Long getTotalUpdateTime() {
      return totalUpdateTime;
    }

    public void setTotalUpdateTime(final Long totalUpdateTime) {
      this.totalUpdateTime = totalUpdateTime;
    }

    public Long getTotalUses() {
      return totalUses;
    }

    public void setTotalUses(final Long totalUses) {
      this.totalUses = totalUses;
    }

    @Override
    public String toString() {
      final StringBuilder buffer = new StringBuilder(getClass().getSimpleName());

      buffer.append(" {numberOfKeys = ").append(getNumberOfKeys());
      buffer.append(", numberOfUpdates = ").append(getNumberOfUpdates());
      buffer.append(", numberOfValues = ").append(getNumberOfValues());
      buffer.append(", totalUpdateTime = ").append(getTotalUpdateTime());
      buffer.append(", totalUses").append(getTotalUses());
      buffer.append("}");

      return buffer.toString();
    }
  }

  public static enum IndexType {
    FUNCTIONAL("RANGE"),
    HASH("HASH"),
    PRIMARY_KEY("KEY");

    private final String description;

    public static IndexType valueOf(final org.apache.geode.cache.query.IndexType indexType) {
      for (final IndexType type : values()) {
        if (type.name().equalsIgnoreCase(ObjectUtils.toString(indexType))) {
          return type;
        }
      }
      return null;
    }

    IndexType(final String description) {
      assertValidArgument(!StringUtils.isBlank(description), "The description for the IndexType must be specified!");
      this.description = description;
    }

    public String getDescription() {
      return this.description;
    }

    public org.apache.geode.cache.query.IndexType getType() {
      switch (this) {
        case HASH:
          return null;
        case PRIMARY_KEY:
          return org.apache.geode.cache.query.IndexType.PRIMARY_KEY;
        case FUNCTIONAL:
        default:
          return org.apache.geode.cache.query.IndexType.FUNCTIONAL;
      }
    }

    @Override
    public String toString() {
      return getDescription();
    }
  }

}
