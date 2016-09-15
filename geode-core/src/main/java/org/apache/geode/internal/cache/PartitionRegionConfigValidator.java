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
package org.apache.geode.internal.cache;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DuplicatePrimaryPartitionException;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.FixedPartitionAttributes;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;

/**
 * 
 * 
 */
public class PartitionRegionConfigValidator {

  private static final Logger logger = LogService.getLogger();
  
  private final PartitionedRegion pr;

  // Incompatible LRU memory eviction attributes maximum message fragment, for
  // tests
  public static final String EVICTION_ATTRIBUTE_MAXIMUM_MEMORY_MESSAGE = " the Eviction Attribute for maximum memory, ";

  // Incompatible LRU entry eviction attributes maximum message fragment, for
  // tests
  public static final String EVICTION_ATTRIBUTE_MAXIMUM_ENTRIES_MESSAGE = " the Eviction Attribute for maximum entries, ";

  // Incompatible eviction attributes exception message fragment, for tests
  public static final String EVICTION_ATTRIBUTES_ARE_INCOMPATIBLE_MESSAGE = " is incompatible with other VMs which have EvictionAttributes ";

  public PartitionRegionConfigValidator(PartitionedRegion pr) {
    this.pr = pr;
  }

  /**
   * This method validates the PartitionedAttributes that user provided PR
   * Attributes with PR Attributes set in PR Config obtained from global
   * meta-data allPartitionedRegion region
   */
  void validatePartitionAttrsFromPRConfig(PartitionRegionConfig prconf) {
    final PartitionAttributes prconfPA = prconf.getPartitionAttrs();
    final PartitionAttributes userPA = pr.getAttributes()
        .getPartitionAttributes();

    if (userPA.getTotalSize() != prconfPA.getTotalSize()) {
      throw new IllegalStateException(
          LocalizedStrings.PartitionedRegion_TOTAL_SIZE_IN_PARTITIONATTRIBUTES_IS_INCOMPATIBLE_WITH_GLOBALLY_SET_TOTAL_SIZE_SET_THE_TOTAL_SIZE_TO_0MB
              .toLocalizedString(Long.valueOf(prconfPA.getTotalSize())));
    }
    if (userPA.getRedundantCopies() != prconfPA.getRedundantCopies()) {
      throw new IllegalStateException(
          LocalizedStrings.PartitionedRegion_REQUESTED_REDUNDANCY_0_IS_INCOMPATIBLE_WITH_EXISTING_REDUNDANCY_1
              .toLocalizedString(new Object[] {
                  Integer.valueOf(userPA.getRedundantCopies()),
                  Integer.valueOf(prconfPA.getRedundantCopies()) }));
    }

    if (prconf.isFirstDataStoreCreated() && pr.isDataStore()) {
      validateDistributedEvictionAttributes(prconf.getEvictionAttributes());
    }

    Scope prconfScope = prconf.getScope();
    Scope myScope = pr.getScope();
    if (!myScope.equals(prconfScope)) {
      throw new IllegalStateException(
          LocalizedStrings.PartitionedRegion_SCOPE_IN_PARTITIONATTRIBUTES_IS_INCOMPATIBLE_WITH_ALREADY_SET_SCOPESET_THE_SCOPE_TO_0
              .toLocalizedString(prconfScope));
    }

    final int prconfTotalNumBuckets = prconfPA.getTotalNumBuckets();
    if (userPA.getTotalNumBuckets() != prconfTotalNumBuckets) {
      throw new IllegalStateException(
          LocalizedStrings.PartitionedRegion_THE_TOTAL_NUMBER_OF_BUCKETS_FOUND_IN_PARTITIONATTRIBUTES_0_IS_INCOMPATIBLE_WITH_THE_TOTAL_NUMBER_OF_BUCKETS_USED_BY_OTHER_DISTRIBUTED_MEMBERS_SET_THE_NUMBER_OF_BUCKETS_TO_1
              .toLocalizedString(new Object[] {
                  Integer.valueOf(userPA.getTotalNumBuckets()),
                  Integer.valueOf(prconfTotalNumBuckets) }));
    }
     validatePartitionListeners(prconf, userPA);
     validatePartitionResolver(prconf, userPA);
     validateColocatedWith(prconf, userPA);
     validateExpirationAttributes(pr.getAttributes(), prconf);
  }
  
  private void validatePartitionListeners(final PartitionRegionConfig prconf,
      final PartitionAttributes userPA) {

    ArrayList<String> prconfList = prconf.getPartitionListenerClassNames();

    if (userPA.getPartitionListeners() == null
        && userPA.getPartitionListeners().length == 0 && prconfList != null) {
      throw new IllegalStateException(
          LocalizedStrings.PartitionRegionConfigValidator_INCOMPATIBLE_PARTITION_LISTENER
              .toLocalizedString(new Object[] { null, prconfList }));
    }
    if (userPA.getPartitionListeners() != null && prconfList != null) {
      ArrayList<String> userPRList = new ArrayList<String>();
      for (int i = 0; i < userPA.getPartitionListeners().length; i++) {
        userPRList.add(userPA.getPartitionListeners()[i].getClass().getName());
      }

      if (userPA.getPartitionListeners().length != prconfList.size()) {
        throw new IllegalStateException(
            LocalizedStrings.PartitionRegionConfigValidator_INCOMPATIBLE_PARTITION_LISTENER
                .toLocalizedString(new Object[] { userPRList, prconfList }));
      }

      for (String listener : prconfList) {
        if (!(userPRList.contains(listener))) {
          throw new IllegalStateException(
              LocalizedStrings.PartitionRegionConfigValidator_INCOMPATIBLE_PARTITION_LISTENER
                  .toLocalizedString(new Object[] { userPRList, prconfList }));
        }
      }
    }
  }

  private void validatePartitionResolver(final PartitionRegionConfig prconf,
      final PartitionAttributes userPA) {
    /* if (userPA.getPartitionResolver() == null
        && prconf.getPartitionResolverClassName() != null) {
      throw new IllegalStateException(
          LocalizedStrings.PartitionRegionConfigValidator_INCOMPATIBLE_PARTITION_RESOLVER
              .toLocalizedString(new Object[] { "null",
                  prconf.getPartitionResolverClassName() }));
    }*/ 
	if (userPA.getPartitionResolver() != null
        && prconf.getResolverClassName() != null) {
      if (!(prconf.getResolverClassName().equals(userPA
          .getPartitionResolver().getClass().getName()))) {
        throw new IllegalStateException(
            LocalizedStrings.PartitionRegionConfigValidator_INCOMPATIBLE_PARTITION_RESOLVER
                .toLocalizedString(new Object[] {
                    userPA.getPartitionResolver().getClass().getName(),
                    prconf.getResolverClassName() }));
      }
    }
  }

  private void validateColocatedWith(final PartitionRegionConfig prconf,
      final PartitionAttributes userPA) {
    if (userPA.getColocatedWith() == null && prconf.getColocatedWith() != null) {
      throw new IllegalStateException(
          LocalizedStrings.PartitionRegionConfigValidator_INCOMPATIBLE_COLOCATED_WITH
              .toLocalizedString(new Object[] { "null",
                  prconf.getColocatedWith() }));

    }
    if (userPA.getColocatedWith() != null && prconf.getColocatedWith() != null) {
      if (!(prconf.getColocatedWith().equals(userPA.getColocatedWith()))) {
        throw new IllegalStateException(
            LocalizedStrings.PartitionRegionConfigValidator_INCOMPATIBLE_COLOCATED_WITH
                .toLocalizedString(new Object[] { userPA.getColocatedWith(),
                    prconf.getColocatedWith() }));
      }
    }
  }

  private void validateExpirationAttributes(final RegionAttributes userRA,
      final PartitionRegionConfig prconf) {
    if (!userRA.getRegionIdleTimeout().equals(prconf.getRegionIdleTimeout())) {
      throw new IllegalStateException(
          LocalizedStrings.PartitionRegionConfigValidator_INCOMPATIBLE_EXPIRATION_ATTRIBUETS
              .toLocalizedString(new Object[] { " region idle timout " }));
    }
    if (!userRA.getRegionTimeToLive().equals(prconf.getRegionTimeToLive())) {
      throw new IllegalStateException(
          LocalizedStrings.PartitionRegionConfigValidator_INCOMPATIBLE_EXPIRATION_ATTRIBUETS
              .toLocalizedString(new Object[] { " region time to live " }));
    }
    if (!userRA.getEntryIdleTimeout().equals(prconf.getEntryIdleTimeout())) {
      throw new IllegalStateException(
          LocalizedStrings.PartitionRegionConfigValidator_INCOMPATIBLE_EXPIRATION_ATTRIBUETS
              .toLocalizedString(new Object[] { " entry idle timout " }));
    }
    if (!userRA.getEntryTimeToLive().equals(prconf.getEntryTimeToLive())) {
      throw new IllegalStateException(
          LocalizedStrings.PartitionRegionConfigValidator_INCOMPATIBLE_EXPIRATION_ATTRIBUETS
              .toLocalizedString(new Object[] { " entry time to live " }));
    }
  }
  
  /**
   * The 2nd step of Eviction Attributes validation to ensure that all VMs are
   * reasonably similar to prevent weird config. issues.
   * 
   * @param prconfEa
   *                the eviction attributes currently used by other VMs
   * @see AttributesFactory#validateAttributes(RegionAttributes)
   * @see #validateEvictionAttributesAgainstLocalMaxMemory()
   */
  private void validateDistributedEvictionAttributes(
      final EvictionAttributes prconfEa) {
    final EvictionAttributes ea = pr.getAttributes().getEvictionAttributes();
    // there is no such thing as null EvictionAttributes, assert that is true
    Assert.assertTrue(ea != null);
    Assert.assertTrue(prconfEa != null);

    // Enforce that all VMs with this PR have the same Eviction Attributes
    // Even an accessor should do this to stay consistent with all other
    // accessor validation/enforcement
    // *and* because an accessor can set the first/global eviction attributes
    // If this is an accessor with Evicitonttributes, log an info message
    // indicating that no eviction will
    // occur in this VM (duh)
    // Further validation should occur for datastores to ensure consistent
    // behavior wrt local max memory and
    // total number of buckets
    final boolean equivAlgoAndAction = ea.getAlgorithm().equals(
        prconfEa.getAlgorithm())
        && ea.getAction().equals(prconfEa.getAction());

    if (!equivAlgoAndAction) {
      throw new IllegalStateException("For Partitioned Region "
          + pr.getFullPath() + " the configured EvictionAttributes " + ea
          + EVICTION_ATTRIBUTES_ARE_INCOMPATIBLE_MESSAGE + prconfEa);
    }
    else {
      // Same algo, action...
      // It is ok to have disparate heap or memory sizes since different
      // VMs may have different heap or memory sizes, particularly if
      // the action is overflow, but...
      // It *is* dangerous to locally destroy entries if all VMs don't have the
      // same maximum
      // basically the VM with the smallest maximum may cause erroneous misses
      // to occur. Warn the user,
      // but allow the configuration.
      if (ea.getAction().isLocalDestroy()) {
        // LRUHeap doesn't support maximum, but other eviction algos do
        if (! ea.getAlgorithm().isLRUHeap() && ea.getMaximum() != prconfEa.getMaximum()) {
          logger.warn(LocalizedMessage.create(
              LocalizedStrings.PartitionedRegion_0_EVICTIONATTRIBUTES_1_DO_NOT_MATCH_WITH_OTHER_2,
              new Object[] {pr.getFullPath(), ea, prconfEa}));
        }
      }
    } // end Same algo, action...
  }

   /**
   * The 3rd step of EvictionAttributes validation, where mutation is acceptible
   * This should be done before buckets are created. Validate EvictionAttributes
   * with respect to localMaxMemory potentially changing the eviction
   * attributes.
   * 
   * @see AttributesFactory#validateAttributes(RegionAttributes)
   * @see #validateDistributedEvictionAttributes(EvictionAttributes)
   */
  void validateEvictionAttributesAgainstLocalMaxMemory() {
    final EvictionAttributes ea = pr.getEvictionAttributes();
    if (pr.getLocalMaxMemory()==0 && !ea.getAction().isNone()) {
      // This is an accessor which won't ever do eviction, say so
      logger.info(LocalizedMessage.create(
          LocalizedStrings.PartitionedRegion_EVICTIONATTRIBUTES_0_WILL_HAVE_NO_EFFECT_1_2,
          new Object[] { ea, pr.getFullPath(), Integer.valueOf(pr.localMaxMemory)}));
    }
  }

  /**
   * validates the persistence for datastores should match
   * between members
   */
  void validatePersistentMatchBetweenDataStores(PartitionRegionConfig prconf) {
    final boolean isPersistent = pr.getAttributes().getDataPolicy() == DataPolicy.PERSISTENT_PARTITION;
    if (pr.getLocalMaxMemory()==0 || prconf==null) {
      return;
    }
    Set<Node> nodes = prconf.getNodes();
    Iterator itor = nodes.iterator();
    while (itor.hasNext()) {
      Node n = (Node)itor.next();
      if (n.getPRType() != Node.ACCESSOR_DATASTORE) {
        continue;
      } else {
        if (n.isPersistent() != (pr.getAttributes().getDataPolicy() == DataPolicy.PERSISTENT_PARTITION)) {
          throw new IllegalStateException(
              "DataPolicy for Datastore members should all be persistent or not.");
        }
      }
    }
  }

  void validateColocation() {
    final PartitionAttributesImpl userPA = (PartitionAttributesImpl) pr.getAttributes()
        .getPartitionAttributes();
    
    userPA.validateColocation(); // do this here to fix bug 47197

    PartitionedRegion colocatedPR = ColocationHelper.getColocatedRegion(pr);
    if (colocatedPR != null) {
      if (colocatedPR.getPartitionAttributes().getTotalNumBuckets()!= userPA.getTotalNumBuckets()){
        throw new IllegalStateException(
            "Colocated regions should have same number of total-num-buckets");
      }
      if (colocatedPR.getPartitionAttributes().getRedundantCopies()!= userPA.getRedundantCopies()){
        throw new IllegalStateException(
            "Colocated regions should have same number of redundant-copies");
      }
      if ((colocatedPR.getPartitionAttributes().getLocalMaxMemory() == 0)
          && (userPA.getLocalMaxMemory() != 0)) {
        throw new IllegalStateException(
            "Colocated regions should have accessors at the same node");
      }
      if ((colocatedPR.getLocalMaxMemory() != 0)
          && (userPA.getLocalMaxMemory() == 0)) {
        throw new IllegalStateException(
            "Colocated regions should have accessors at the same node");
      }
      if (!pr.isShadowPR()) {
        if (pr.getAttributes().getDataPolicy().withPersistence()) {
          if (!colocatedPR.getDataPolicy().withPersistence()) {
            throw new IllegalStateException(
                "Cannot colocate a persistent region with a non persistent region");
          }
        }
      }
    }
  }

  public void validateCacheLoaderWriterBetweenDataStores(
      PartitionRegionConfig prconf) {
    if (pr.getLocalMaxMemory() == 0 || prconf == null) {
      return;
    }
    Set<Node> nodes = prconf.getNodes();
    Iterator itor = nodes.iterator();
    while (itor.hasNext()) {
      Node n = (Node)itor.next();
      if (n.getPRType() != Node.ACCESSOR_DATASTORE) {
        continue;
      }
      else {
        if (n.isCacheLoaderAttached()
            && pr.getAttributes().getCacheLoader() == null) {
          throw new IllegalStateException(
              LocalizedStrings.PartitionRegionConfigValidator_CACHE_LOADER_IS_NOTNULL_IN_PARTITIONED_REGION_0_ON_OTHER_DATASTORE
                  .toLocalizedString(new Object[] { this.pr.getName() }));
        }
        if (!n.isCacheLoaderAttached()
            && pr.getAttributes().getCacheLoader() != null) {
          throw new IllegalStateException(
              LocalizedStrings.PartitionRegionConfigValidator_CACHE_LOADER_IS_NULL_IN_PARTITIONED_REGION_0_ON_OTHER_DATASTORE
                  .toLocalizedString(new Object[] { this.pr.getName() }));
        }
        if (n.isCacheWriterAttached()
            && pr.getAttributes().getCacheWriter() == null) {
          throw new IllegalStateException(
              LocalizedStrings.PartitionRegionConfigValidator_CACHE_WRITER_IS_NOTNULL_IN_PARTITIONED_REGION_0_ON_OTHER_DATASTORE
                  .toLocalizedString(new Object[] { this.pr.getName() }));
        }
        if (!n.isCacheWriterAttached()
            && pr.getAttributes().getCacheWriter() != null) {
          throw new IllegalStateException(
              LocalizedStrings.PartitionRegionConfigValidator_CACHE_WRITER_IS_NULL_IN_PARTITIONED_REGION_0_ON_OTHER_DATASTORE
                  .toLocalizedString(new Object[] { this.pr.getName() }));
        }
      }
    }
  }

  void validateFixedPartitionAttributes() {
    if (this.pr.getFixedPartitionAttributesImpl() != null) {
      validatePrimaryFixedPartitionAttributes();
      validateFixedPartitionAttributesAgainstRedundantCopies();
      validateFixedPartitionAttributesAgainstTotalNumberBuckets();
    }
  }

  /**
   * validate that for all partitions defined across all datastores, sum of
   * num-buckets is not more than total-num-buckets defined
   */
  private void validateFixedPartitionAttributesAgainstTotalNumberBuckets() {
    for (FixedPartitionAttributesImpl fpa : this.pr
        .getFixedPartitionAttributesImpl()) {
      int numBuckets = 0;

      Set<FixedPartitionAttributesImpl> allFPAs = new HashSet<FixedPartitionAttributesImpl>(
          this.pr.getRegionAdvisor().adviseAllFixedPartitionAttributes());
      allFPAs.add(fpa);

      for (FixedPartitionAttributes samefpa : allFPAs) {
        numBuckets = numBuckets + samefpa.getNumBuckets();
      }
      if (numBuckets > this.pr.getTotalNumberOfBuckets()) {
        Object[] prms = new Object[] { this.pr.getName(), numBuckets,
            this.pr.getTotalNumberOfBuckets() };
        throw new IllegalStateException(
            LocalizedStrings.PartitionedRegionConfigValidator_FOR_REGION_0_SUM_OF_NUM_BUCKETS_1_FOR_DIFFERENT_PRIMARY_PARTITIONS_SHOULD_NOT_BE_GREATER_THAN_TOTAL_NUM_BUCKETS_2
                .toString(prms));
      }
    }
  }
  
  /**
   * Validate that for the given partition, number if secondaries are never
   * exceed redundant copies defined Validate that the num-buckets defined for a
   * partition are same across all datastores
   */
  private void validateFixedPartitionAttributesAgainstRedundantCopies() {
    for (FixedPartitionAttributesImpl fpa : this.pr.getFixedPartitionAttributesImpl()) {
      List<FixedPartitionAttributesImpl> allSameFPAs = this.pr.getRegionAdvisor().adviseSameFPAs(fpa);
      allSameFPAs.add(fpa);
      
      if (!allSameFPAs.isEmpty()) {
        int numSecondaries = 0;
        for (FixedPartitionAttributes otherfpa : allSameFPAs) {
          if (fpa.getNumBuckets() != otherfpa.getNumBuckets()) {
            Object[] prms = new Object[] { this.pr.getName(),
                fpa.getPartitionName(), fpa.getNumBuckets(),
                otherfpa.getNumBuckets() };
            throw new IllegalStateException(
                LocalizedStrings.PartitionedRegionConfigValidator_FOR_REGION_0_FOR_PARTITION_1_NUM_BUCKETS_ARE_NOT_SAME_ACROSS_NODES
                    .toString(prms));
          }
          
          if (!otherfpa.isPrimary()) {
            if (++numSecondaries > (this.pr.getRedundantCopies())) {
              Object[] prms = new Object[] { this.pr.getName(), numSecondaries,
                  fpa.getPartitionName(), this.pr.getRedundantCopies() };
              throw new IllegalStateException(
                  LocalizedStrings.PartitionedRegionConfigValidator_FOR_REGION_0_NUMBER_OF_SECONDARY_PARTITIONS_1_OF_A_PARTITION_2_SHOULD_NEVER_EXCEED_NUMBER_OF_REDUNDANT_COPIES_3
                      .toString(prms));
            }
          }
        }
      }
    }
  }
  
  /**
   * Validate that same partition is not defined as primary on more that one
   * datastore
   */
  private void validatePrimaryFixedPartitionAttributes() {
    List<FixedPartitionAttributesImpl> remotePrimaryFPAs = this.pr.getRegionAdvisor().adviseRemotePrimaryFPAs();
    
    for (FixedPartitionAttributes fpa : this.pr.getFixedPartitionAttributesImpl()) {
      if (fpa.isPrimary() && remotePrimaryFPAs.contains(fpa)) {
        Object[] prms = new Object[]{this.pr.getName(), fpa.getPartitionName()};
        throw new DuplicatePrimaryPartitionException(
            LocalizedStrings.PartitionedRegionConfigValidator_FOR_REGION_0_SAME_PARTITION_NAME_1_CANNOT_BE_DEFINED_AS_PRIMARY_ON_MORE_THAN_ONE_NODE
                .toString(prms));
      }
    }
  }

  /**
   * @param prconf
   */
  public void validateFixedPABetweenDataStores(PartitionRegionConfig prconf) {
    boolean isDataStore = this.pr.localMaxMemory > 0;
    boolean isFixedPR = this.pr.fixedPAttrs != null;

    Set<Node> nodes = prconf.getNodes();
    Iterator<Node> itr = nodes.iterator();
    while (itr.hasNext()) {     
      Node n = itr.next();
      if (isFixedPR) {
        if (n.getPRType() == Node.DATASTORE
            || n.getPRType() == Node.ACCESSOR_DATASTORE) {
          // throw exception
          Object[] prms = new Object[] { pr.getName() };
          throw new IllegalStateException(
              LocalizedStrings.PartitionedRegionConfigValidator_FIXED_PARTITION_REGION_ONE_DATASTORE_IS_WITHOUTFPA
                  .toLocalizedString(prms));
        }
      } else {
        if (isDataStore) {
          if (n.getPRType() == Node.FIXED_PR_ACCESSOR
              || n.getPRType() == Node.FIXED_PR_DATASTORE) {
            // throw Exception
            Object[] prms = new Object[] { pr.getName() };
            throw new IllegalStateException(
                LocalizedStrings.PartitionedRegionConfigValidator_FIXED_PARTITION_REGION_ONE_DATASTORE_IS_WITHOUTFPA
                    .toLocalizedString(prms));
          }
        }
      }
    }
  }
}
