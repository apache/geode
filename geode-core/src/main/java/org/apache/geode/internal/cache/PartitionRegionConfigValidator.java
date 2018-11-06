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
import org.apache.geode.internal.logging.LogService;

public class PartitionRegionConfigValidator {

  private static final Logger logger = LogService.getLogger();

  private final PartitionedRegion pr;

  // Incompatible LRU memory eviction attributes maximum message fragment, for
  // tests
  public static final String EVICTION_ATTRIBUTE_MAXIMUM_MEMORY_MESSAGE =
      " the Eviction Attribute for maximum memory, ";

  // Incompatible LRU entry eviction attributes maximum message fragment, for
  // tests
  public static final String EVICTION_ATTRIBUTE_MAXIMUM_ENTRIES_MESSAGE =
      " the Eviction Attribute for maximum entries, ";

  // Incompatible eviction attributes exception message fragment, for tests
  public static final String EVICTION_ATTRIBUTES_ARE_INCOMPATIBLE_MESSAGE =
      " is incompatible with other VMs which have EvictionAttributes ";

  public PartitionRegionConfigValidator(PartitionedRegion pr) {
    this.pr = pr;
  }

  /**
   * This method validates the PartitionedAttributes that user provided PR Attributes with PR
   * Attributes set in PR Config obtained from global meta-data allPartitionedRegion region
   */
  void validatePartitionAttrsFromPRConfig(PartitionRegionConfig prconf) {
    final PartitionAttributes prconfPA = prconf.getPartitionAttrs();
    final PartitionAttributes userPA = pr.getAttributes().getPartitionAttributes();

    if (userPA.getTotalSize() != prconfPA.getTotalSize()) {
      throw new IllegalStateException(
          String.format(
              "Total size in PartitionAttributes is incompatible with globally set total size. Set the total size to %sMB.",
              Long.valueOf(prconfPA.getTotalSize())));
    }
    if (userPA.getRedundantCopies() != prconfPA.getRedundantCopies()) {
      throw new IllegalStateException(
          String.format("Requested redundancy %s is incompatible with existing redundancy %s",
              new Object[] {Integer.valueOf(userPA.getRedundantCopies()),
                  Integer.valueOf(prconfPA.getRedundantCopies())}));
    }

    if (prconf.isFirstDataStoreCreated() && pr.isDataStore()) {
      validateDistributedEvictionAttributes(prconf.getEvictionAttributes());
    }

    Scope prconfScope = prconf.getScope();
    Scope myScope = pr.getScope();
    if (!myScope.equals(prconfScope)) {
      throw new IllegalStateException(
          String.format(
              "Scope in PartitionAttributes is incompatible with already set scope.Set the scope to %s .",
              prconfScope));
    }

    final int prconfTotalNumBuckets = prconfPA.getTotalNumBuckets();
    if (userPA.getTotalNumBuckets() != prconfTotalNumBuckets) {
      throw new IllegalStateException(
          String.format(
              "The total number of buckets found in PartitionAttributes ( %s ) is incompatible with the total number of buckets used by other distributed members. Set the number of buckets to %s",
              new Object[] {Integer.valueOf(userPA.getTotalNumBuckets()),
                  Integer.valueOf(prconfTotalNumBuckets)}));
    }
    validatePartitionListeners(prconf, userPA);
    validatePartitionResolver(prconf, userPA);
    validateColocatedWith(prconf, userPA);
    validateExpirationAttributes(pr.getAttributes(), prconf);
  }

  private void validatePartitionListeners(final PartitionRegionConfig prconf,
      final PartitionAttributes userPA) {

    ArrayList<String> prconfList = prconf.getPartitionListenerClassNames();

    if (userPA.getPartitionListeners() == null && userPA.getPartitionListeners().length == 0
        && prconfList != null) {
      throw new IllegalStateException(
          String.format(
              "The PartitionListeners=%s are incompatible with the PartitionListeners=%s used by other distributed members.",
              new Object[] {null, prconfList}));
    }
    if (userPA.getPartitionListeners() != null && prconfList != null) {
      ArrayList<String> userPRList = new ArrayList<String>();
      for (int i = 0; i < userPA.getPartitionListeners().length; i++) {
        userPRList.add(userPA.getPartitionListeners()[i].getClass().getName());
      }

      if (userPA.getPartitionListeners().length != prconfList.size()) {
        throw new IllegalStateException(
            String.format(
                "The PartitionListeners=%s are incompatible with the PartitionListeners=%s used by other distributed members.",
                new Object[] {userPRList, prconfList}));
      }

      for (String listener : prconfList) {
        if (!(userPRList.contains(listener))) {
          throw new IllegalStateException(
              String.format(
                  "The PartitionListeners=%s are incompatible with the PartitionListeners=%s used by other distributed members.",
                  new Object[] {userPRList, prconfList}));
        }
      }
    }
  }

  private void validatePartitionResolver(final PartitionRegionConfig prconf,
      final PartitionAttributes userPA) {
    /*
     * if (userPA.getPartitionResolver() == null && prconf.getPartitionResolverClassName() != null)
     * { throw new IllegalStateException(
     * String.
     * format("The PartitionResolver=%s is incompatible with the PartitionResolver=%s used by other distributed members."
     * ,
     * new Object[] { "null", prconf.getPartitionResolverClassName() })); }
     */
    if (userPA.getPartitionResolver() != null && prconf.getResolverClassName() != null) {
      if (!(prconf.getResolverClassName()
          .equals(userPA.getPartitionResolver().getClass().getName()))) {
        throw new IllegalStateException(
            String.format(
                "The PartitionResolver=%s is incompatible with the PartitionResolver=%s used by other distributed members.",
                new Object[] {userPA.getPartitionResolver().getClass().getName(),
                    prconf.getResolverClassName()}));
      }
    }
  }

  private void validateColocatedWith(final PartitionRegionConfig prconf,
      final PartitionAttributes userPA) {
    if (userPA.getColocatedWith() == null && prconf.getColocatedWith() != null) {
      throw new IllegalStateException(
          String.format(
              "The colocatedWith=%s found in PartitionAttributes is incompatible with the colocatedWith=%s used by other distributed members.",
              new Object[] {"null", prconf.getColocatedWith()}));

    }
    if (userPA.getColocatedWith() != null && prconf.getColocatedWith() != null) {
      if (!(prconf.getColocatedWith().equals(userPA.getColocatedWith()))) {
        throw new IllegalStateException(
            String.format(
                "The colocatedWith=%s found in PartitionAttributes is incompatible with the colocatedWith=%s used by other distributed members.",

                new Object[] {userPA.getColocatedWith(), prconf.getColocatedWith()}));
      }
    }
  }

  private void validateExpirationAttributes(final RegionAttributes userRA,
      final PartitionRegionConfig prconf) {
    if (!userRA.getRegionIdleTimeout().equals(prconf.getRegionIdleTimeout())) {
      throw new IllegalStateException(
          String.format(
              "The %1$s set in RegionAttributes is incompatible with %1$s used by other distributed members.",
              new Object[] {" region idle timout "}));
    }
    if (!userRA.getRegionTimeToLive().equals(prconf.getRegionTimeToLive())) {
      throw new IllegalStateException(
          String.format(
              "The %1$s set in RegionAttributes is incompatible with %1$s used by other distributed members.",
              new Object[] {" region time to live "}));
    }
    if (!userRA.getEntryIdleTimeout().equals(prconf.getEntryIdleTimeout())) {
      throw new IllegalStateException(
          String.format(
              "The %1$s set in RegionAttributes is incompatible with %1$s used by other distributed members.",
              new Object[] {" entry idle timout "}));
    }
    if (!userRA.getEntryTimeToLive().equals(prconf.getEntryTimeToLive())) {
      throw new IllegalStateException(
          String.format(
              "The %1$s set in RegionAttributes is incompatible with %1$s used by other distributed members.",
              new Object[] {" entry time to live "}));
    }
  }

  /**
   * The 2nd step of Eviction Attributes validation to ensure that all VMs are reasonably similar to
   * prevent weird config. issues.
   *
   * @param prconfEa the eviction attributes currently used by other VMs
   * @see AttributesFactory#validateAttributes(RegionAttributes)
   * @see #validateEvictionAttributesAgainstLocalMaxMemory()
   */
  private void validateDistributedEvictionAttributes(final EvictionAttributes prconfEa) {
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
    final boolean equivAlgoAndAction = ea.getAlgorithm().equals(prconfEa.getAlgorithm())
        && ea.getAction().equals(prconfEa.getAction());

    if (!equivAlgoAndAction) {
      throw new IllegalStateException(
          "For Partitioned Region " + pr.getFullPath() + " the configured EvictionAttributes " + ea
              + EVICTION_ATTRIBUTES_ARE_INCOMPATIBLE_MESSAGE + prconfEa);
    } else {
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
        if (!ea.getAlgorithm().isLRUHeap() && ea.getMaximum() != prconfEa.getMaximum()) {
          logger.warn(
              "For Partitioned Region {} the locally configured EvictionAttributes {} do not match with other EvictionAttributes {} and may cause misses during reads from VMs with smaller maximums.",
              new Object[] {pr.getFullPath(), ea, prconfEa});
        }
      }
    } // end Same algo, action...
  }

  /**
   * The 3rd step of EvictionAttributes validation, where mutation is acceptible This should be done
   * before buckets are created. Validate EvictionAttributes with respect to localMaxMemory
   * potentially changing the eviction attributes.
   *
   * @see AttributesFactory#validateAttributes(RegionAttributes)
   * @see #validateDistributedEvictionAttributes(EvictionAttributes)
   */
  void validateEvictionAttributesAgainstLocalMaxMemory() {
    final EvictionAttributes ea = pr.getEvictionAttributes();
    if (pr.getLocalMaxMemory() == 0 && !ea.getAction().isNone()) {
      // This is an accessor which won't ever do eviction, say so
      logger.info(
          "EvictionAttributes {} will have no effect for Partitioned Region {} on this VM because localMaxMemory is {}.",
          new Object[] {ea, pr.getFullPath(), Integer.valueOf(pr.localMaxMemory)});
    }
  }

  /**
   * validates the persistence for datastores should match between members
   */
  void validatePersistentMatchBetweenDataStores(PartitionRegionConfig prconf) {
    final boolean isPersistent =
        pr.getAttributes().getDataPolicy() == DataPolicy.PERSISTENT_PARTITION;
    if (pr.getLocalMaxMemory() == 0 || prconf == null) {
      return;
    }
    Set<Node> nodes = prconf.getNodes();
    Iterator itor = nodes.iterator();
    while (itor.hasNext()) {
      Node n = (Node) itor.next();
      if (n.getPRType() != Node.ACCESSOR_DATASTORE) {
        continue;
      } else {
        if (n.isPersistent() != (pr.getAttributes()
            .getDataPolicy() == DataPolicy.PERSISTENT_PARTITION)) {
          throw new IllegalStateException(
              "DataPolicy for Datastore members should all be persistent or not.");
        }
      }
    }
  }

  void validateColocation() {
    final PartitionAttributesImpl userPA =
        (PartitionAttributesImpl) pr.getAttributes().getPartitionAttributes();

    userPA.validateColocation(pr.getCache()); // do this here to fix bug 47197

    PartitionedRegion colocatedPR = ColocationHelper.getColocatedRegion(pr);
    if (colocatedPR != null) {
      if (colocatedPR.getPartitionAttributes().getTotalNumBuckets() != userPA
          .getTotalNumBuckets()) {
        throw new IllegalStateException(
            "Colocated regions should have same number of total-num-buckets");
      }
      if (colocatedPR.getPartitionAttributes().getRedundantCopies() != userPA
          .getRedundantCopies()) {
        throw new IllegalStateException(
            "Colocated regions should have same number of redundant-copies");
      }
      if ((colocatedPR.getPartitionAttributes().getLocalMaxMemory() == 0)
          && (userPA.getLocalMaxMemory() != 0)) {
        throw new IllegalStateException("Colocated regions should have accessors at the same node");
      }
      if ((colocatedPR.getLocalMaxMemory() != 0) && (userPA.getLocalMaxMemory() == 0)) {
        throw new IllegalStateException("Colocated regions should have accessors at the same node");
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

  public void validateCacheLoaderWriterBetweenDataStores(PartitionRegionConfig prconf) {
    if (pr.getLocalMaxMemory() == 0 || prconf == null) {
      return;
    }
    Set<Node> nodes = prconf.getNodes();
    Iterator itor = nodes.iterator();
    while (itor.hasNext()) {
      Node n = (Node) itor.next();
      if (n.getPRType() != Node.ACCESSOR_DATASTORE) {
        continue;
      } else {
        if (n.isCacheLoaderAttached() && pr.getAttributes().getCacheLoader() == null) {
          throw new IllegalStateException(
              String.format(
                  "Incompatible CacheLoader. CacheLoader is not null in partitionedRegion %s on another datastore.",
                  new Object[] {this.pr.getName()}));
        }
        if (!n.isCacheLoaderAttached() && pr.getAttributes().getCacheLoader() != null) {
          throw new IllegalStateException(
              String.format(
                  "Incompatible CacheLoader. CacheLoader is null in partitionedRegion %s on another datastore.",
                  new Object[] {this.pr.getName()}));
        }
        if (n.isCacheWriterAttached() && pr.getAttributes().getCacheWriter() == null) {
          throw new IllegalStateException(
              String.format(
                  "Incompatible CacheWriter. CacheWriter is not null in partitionedRegion %s on another datastore.",
                  new Object[] {this.pr.getName()}));
        }
        if (!n.isCacheWriterAttached() && pr.getAttributes().getCacheWriter() != null) {
          throw new IllegalStateException(
              String.format(
                  "Incompatible CacheWriter. CacheWriter is null in partitionedRegion %s on another datastore.",
                  new Object[] {this.pr.getName()}));
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
   * validate that for all partitions defined across all datastores, sum of num-buckets is not more
   * than total-num-buckets defined
   */
  private void validateFixedPartitionAttributesAgainstTotalNumberBuckets() {
    for (FixedPartitionAttributesImpl fpa : this.pr.getFixedPartitionAttributesImpl()) {
      int numBuckets = 0;

      Set<FixedPartitionAttributesImpl> allFPAs = new HashSet<FixedPartitionAttributesImpl>(
          this.pr.getRegionAdvisor().adviseAllFixedPartitionAttributes());
      allFPAs.add(fpa);

      for (FixedPartitionAttributes samefpa : allFPAs) {
        numBuckets = numBuckets + samefpa.getNumBuckets();
      }
      if (numBuckets > this.pr.getTotalNumberOfBuckets()) {
        throw new IllegalStateException(
            String.format(
                "For region %s,sum of num-buckets %s for different primary partitions should not be greater than total-num-buckets %s.",
                this.pr.getName(), numBuckets, this.pr.getTotalNumberOfBuckets()));
      }
    }
  }

  /**
   * Validate that for the given partition, number if secondaries are never exceed redundant copies
   * defined Validate that the num-buckets defined for a partition are same across all datastores
   */
  private void validateFixedPartitionAttributesAgainstRedundantCopies() {
    for (FixedPartitionAttributesImpl fpa : this.pr.getFixedPartitionAttributesImpl()) {
      List<FixedPartitionAttributesImpl> allSameFPAs =
          this.pr.getRegionAdvisor().adviseSameFPAs(fpa);
      allSameFPAs.add(fpa);

      if (!allSameFPAs.isEmpty()) {
        int numSecondaries = 0;
        for (FixedPartitionAttributes otherfpa : allSameFPAs) {
          if (fpa.getNumBuckets() != otherfpa.getNumBuckets()) {
            throw new IllegalStateException(
                String.format(
                    "For region %s,for partition %s, num-buckets are not same (%s, %s)across nodes.",
                    this.pr.getName(), fpa.getPartitionName(),
                    fpa.getNumBuckets(), otherfpa.getNumBuckets()));
          }

          if (!otherfpa.isPrimary()) {
            if (++numSecondaries > (this.pr.getRedundantCopies())) {
              throw new IllegalStateException(
                  String.format(
                      "For region %s, number of secondary partitions %s of a partition %s should never exceed number of redundant copies %s.",
                      this.pr.getName(), numSecondaries,
                      fpa.getPartitionName(), this.pr.getRedundantCopies()));
            }
          }
        }
      }
    }
  }

  /**
   * Validate that same partition is not defined as primary on more that one datastore
   */
  private void validatePrimaryFixedPartitionAttributes() {
    List<FixedPartitionAttributesImpl> remotePrimaryFPAs =
        this.pr.getRegionAdvisor().adviseRemotePrimaryFPAs();

    for (FixedPartitionAttributes fpa : this.pr.getFixedPartitionAttributesImpl()) {
      if (fpa.isPrimary() && remotePrimaryFPAs.contains(fpa)) {
        throw new DuplicatePrimaryPartitionException(
            String.format(
                "For region %s, same partition name %s can not be defined as primary on more than one node.",
                this.pr.getName(), fpa.getPartitionName()));
      }
    }
  }

  public void validateFixedPABetweenDataStores(PartitionRegionConfig prconf) {
    boolean isDataStore = this.pr.localMaxMemory > 0;
    boolean isFixedPR = this.pr.fixedPAttrs != null;

    Set<Node> nodes = prconf.getNodes();
    Iterator<Node> itr = nodes.iterator();
    while (itr.hasNext()) {
      Node n = itr.next();
      if (isFixedPR) {
        if (n.getPRType() == Node.DATASTORE || n.getPRType() == Node.ACCESSOR_DATASTORE) {
          // throw exception
          Object[] prms = new Object[] {pr.getName()};
          throw new IllegalStateException(
              String.format(
                  "Region %s uses fixed partitioning but at least one datastore node (localMaxMemory > 0) has no fixed partitions. Please make sure that each datastore creating this region is configured with at least one fixed partition.",
                  prms));
        }
      } else {
        if (isDataStore) {
          if (n.getPRType() == Node.FIXED_PR_ACCESSOR || n.getPRType() == Node.FIXED_PR_DATASTORE) {
            // throw Exception
            Object[] prms = new Object[] {pr.getName()};
            throw new IllegalStateException(
                String.format(
                    "Region %s uses fixed partitioning but at least one datastore node (localMaxMemory > 0) has no fixed partitions. Please make sure that each datastore creating this region is configured with at least one fixed partition.",
                    prms));
          }
        }
      }
    }
  }
}
