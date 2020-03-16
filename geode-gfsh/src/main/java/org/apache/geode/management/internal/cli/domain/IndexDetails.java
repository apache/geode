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

package org.apache.geode.management.internal.cli.domain;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexStatistics;
import org.apache.geode.cache.query.internal.index.AbstractIndex;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.lang.ObjectUtils;

/**
 * The IndexDetails class encapsulates information for an Index on a Region in the GemFire Cache.
 * </p>
 *
 * @see org.apache.geode.cache.query.Index
 * @see org.apache.geode.cache.query.IndexStatistics
 * @see org.apache.geode.cache.query.IndexType
 * @see org.apache.geode.distributed.DistributedMember
 * @since GemFire 7.0
 */
@SuppressWarnings({"unused", "deprecation"})
public class IndexDetails implements Comparable<IndexDetails>, Serializable {

  private static final long serialVersionUID = -2198907141534201288L;
  private IndexStatisticsDetails indexStatisticsDetails;

  private org.apache.geode.cache.query.IndexType indexType;

  private String indexedExpression;
  private String fromClause;
  private String projectionAttributes;
  private String memberName;
  private String regionName;
  private boolean isValid;

  private final String indexName;
  private final String memberId;
  private final String regionPath;

  protected static void assertValidArgument(final boolean valid, final String message,
      final Object... args) {
    if (!valid) {
      throw new IllegalArgumentException(String.format(message, args));
    }
  }

  protected static <T extends Comparable<T>> int compare(final T obj1, final T obj2) {
    return (obj1 == null && obj2 == null ? 0
        : (obj1 == null ? 1 : (obj2 == null ? -1 : obj1.compareTo(obj2))));
  }

  protected static IndexStatisticsDetails createIndexStatisticsDetails(
      final IndexStatistics indexStatistics) {
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
    if (index instanceof AbstractIndex) {
      setIsValid(index.isValid());
    } else {
      setIsValid(false);
    }
    if (index.getStatistics() != null) {
      setIndexStatisticsDetails(createIndexStatisticsDetails(index.getStatistics()));
    }
  }

  public void setIsValid(boolean valid) {
    this.isValid = valid;
  }

  public IndexDetails(final String memberId, final String regionPath, final String indexName) {
    assertValidArgument(StringUtils.isNotBlank(memberId),
        "The member having a region with an index must be specified!");
    assertValidArgument(StringUtils.isNotBlank(regionPath),
        "The region in member (%1$s) with an index must be specified!", memberId);
    assertValidArgument(StringUtils.isNotBlank(indexName),
        "The name of the index on region (%1$s) of member (%2$s) must be specified!", regionPath,
        memberId);
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

  public org.apache.geode.cache.query.IndexType getIndexType() {
    return indexType;
  }


  public void setIndexType(final org.apache.geode.cache.query.IndexType indexType) {
    this.indexType = indexType;
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

  public boolean getIsValid() {
    return this.isValid;
  }

  @Override
  public int compareTo(final IndexDetails indexDetails) {
    int comparisonValue = compare(getMemberName(), indexDetails.getMemberName());
    comparisonValue = (comparisonValue != 0 ? comparisonValue
        : compare(getMemberId(), indexDetails.getMemberId()));
    comparisonValue = (comparisonValue != 0 ? comparisonValue
        : compare(getRegionPath(), indexDetails.getRegionPath()));
    return (comparisonValue != 0 ? comparisonValue
        : compare(getIndexName(), indexDetails.getIndexName()));
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
    return getClass().getSimpleName() + " {fromClause = " + getFromClause() +
        ", indexExpression = " + getIndexedExpression() +
        ", indexName = " + getIndexName() +
        ", indexType = " + getIndexType().getName() +
        ", memberId = " + getMemberId() +
        ", memberName = " + getMemberName() +
        ", regionName = " + getRegionName() +
        ", regionPath = " + getRegionPath() +
        ", isValid = " + getIsValid() +
        ", projectAttributes = " + getProjectionAttributes() +
        "}";
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
      return getClass().getSimpleName() + " {numberOfKeys = " + getNumberOfKeys() +
          ", numberOfUpdates = " + getNumberOfUpdates() +
          ", numberOfValues = " + getNumberOfValues() +
          ", totalUpdateTime = " + getTotalUpdateTime() +
          ", totalUses" + getTotalUses() +
          "}";
    }
  }
}
