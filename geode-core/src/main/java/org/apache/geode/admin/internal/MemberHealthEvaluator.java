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
package org.apache.geode.admin.internal;

import org.apache.geode.CancelException;
import org.apache.geode.admin.*;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.internal.*;
import org.apache.geode.internal.*;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.statistics.GemFireStatSampler;
import org.apache.geode.internal.statistics.platform.ProcessStats;

import java.util.*;

/**
 * Contains the logic for evaluating the health of a GemFire
 * distributed system member according to the thresholds provided in a
 * {@link MemberHealthConfig}.  
 *
 * @see VMStats
 * @see ProcessStats
 * @see DMStats
 *
 *
 * @since GemFire 3.5
 */
/**
 *
 */
class MemberHealthEvaluator extends AbstractHealthEvaluator {

  /** The config from which we get the evaluation criteria */
  private MemberHealthConfig config;

  /** The description of the member being evaluated */
  private String description;

//  /** Statistics about this VM (may be null) */
//  private VMStatsContract vmStats;

  /** Statistics about this process (may be null) */
  private ProcessStats processStats;

  /** Statistics about the distribution manager */
  private DMStats dmStats;

  /** The previous value of the reply timeouts stat */
  private long prevReplyTimeouts;

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>MemberHealthEvaluator</code>
   */
  MemberHealthEvaluator(GemFireHealthConfig config,
                        DM dm) {
    super(config, dm);

    this.config = config;
    InternalDistributedSystem system = dm.getSystem();

    GemFireStatSampler sampler = system.getStatSampler();
    if (sampler != null) {
      // Sampling is enabled
//      this.vmStats = sampler.getVMStats();
      this.processStats = sampler.getProcessStats();
    }

    this.dmStats = dm.getStats();
    
    StringBuffer sb = new StringBuffer();
    sb.append("Application VM member ");
    sb.append(dm.getId());
    int pid = OSProcess.getId();
    if (pid != 0) {
      sb.append(" with pid ");
      sb.append(pid);
    }
    this.description = sb.toString();
  }

  ////////////////////  Instance Methods  ////////////////////

  @Override
  protected String getDescription() {
    return this.description;
  }

  /**
   * Checks to make sure that the {@linkplain
   * ProcessStats#getProcessSize VM's process size} is less than the
   * {@linkplain MemberHealthConfig#getMaxVMProcessSize threshold}.
   * If not, the status is "okay" health.
   */
  void checkVMProcessSize(List status) {
    // There is no need to check isFirstEvaluation()
    if (this.processStats == null) {
      return;
    }

    long vmSize = this.processStats.getProcessSize();
    long threshold = this.config.getMaxVMProcessSize();
    if (vmSize > threshold) {
      String s = LocalizedStrings.MemberHealthEvaluator_THE_SIZE_OF_THIS_VM_0_MEGABYTES_EXCEEDS_THE_THRESHOLD_1_MEGABYTES.toLocalizedString(new Object[] {Long.valueOf(vmSize), Long.valueOf(threshold)});
      status.add(okayHealth(s));
    }
  }

  /**
   * Checks to make sure that the size of the distribution manager's
   * {@linkplain DMStats#getOverflowQueueSize() overflow} message
   * queue does not exceed the {@linkplain
   * MemberHealthConfig#getMaxMessageQueueSize threshold}.  If not,
   * the status is "okay" health.
   */
  void checkMessageQueueSize(List status) {
    long threshold = this.config.getMaxMessageQueueSize();
    long overflowSize = this.dmStats.getOverflowQueueSize();
    if (overflowSize > threshold) {
      String s = LocalizedStrings.MemberHealthEvaluator_THE_SIZE_OF_THE_OVERFLOW_QUEUE_0_EXCEEDS_THE_THRESHOLD_1.toLocalizedString(new Object[] { Long.valueOf(overflowSize), Long.valueOf(threshold)});
      status.add(okayHealth(s));
    }
  }

  /**
   * Checks to make sure that the number of {@linkplain
   * DMStats#getReplyTimeouts reply timeouts} does not exceed the
   * {@linkplain MemberHealthConfig#getMaxReplyTimeouts threshold}.
   * If not, the status is "okay" health.
   */
  void checkReplyTimeouts(List status) {
    if (isFirstEvaluation()) {
      return;
    }

    long threshold = this.config.getMaxReplyTimeouts();
    long deltaReplyTimeouts =
      this.dmStats.getReplyTimeouts() - prevReplyTimeouts;
    if (deltaReplyTimeouts > threshold) {
      String s = LocalizedStrings.MemberHealthEvaluator_THE_NUMBER_OF_MESSAGE_REPLY_TIMEOUTS_0_EXCEEDS_THE_THRESHOLD_1.toLocalizedString(new Object[] { Long.valueOf(deltaReplyTimeouts), Long.valueOf(threshold)}); 
      status.add(okayHealth(s));
    }
  }

  /**
   * See if the multicast retransmission ratio is okay
   */
  void checkRetransmissionRatio(List status) {
    double threshold = this.config.getMaxRetransmissionRatio();
    int mcastMessages = this.dmStats.getMcastWrites();
    if (mcastMessages > 100000) { // avoid initial state & int overflow
      // the ratio we actually use here is (retransmit requests) / (mcast datagram writes)
      // a single retransmit request may include multiple missed messages
      double ratio = (this.dmStats.getMcastRetransmits() * 1.0) /
                    (this.dmStats.getMcastWrites() * 1.0);
      if (ratio > threshold) {
        String s = "The number of message retransmissions (" +
          ratio + ") exceeds the threshold (" + threshold + ")";
        status.add(okayHealth(s));
      }
    }
  }
  
/**
 * The function keeps updating the health of the cache based on 
 * roles required by the regions and their reliablity policies.
 * 
 * */
  
  void checkCacheRequiredRolesMeet(List status){
	// will have to call here okeyHealth() or poorHealth()
	//GemFireCache cache = (GemFireCache)CacheFactory.getAnyInstance();
	
	//CachePerfStats cPStats= null;
	try{
		GemFireCacheImpl cache = (GemFireCacheImpl)CacheFactory.getAnyInstance();
		CachePerfStats cPStats= null;
		cPStats= cache.getCachePerfStats();
	
		if(cPStats.getReliableRegionsMissingFullAccess()> 0){
			// health is okay.
			int numRegions = cPStats.getReliableRegionsMissingFullAccess();
			status.add(okayHealth(LocalizedStrings.MemberHealthEvaluator_THERE_ARE_0_REGIONS_MISSING_REQUIRED_ROLES_BUT_ARE_CONFIGURED_FOR_FULL_ACCESS.toLocalizedString(Integer.valueOf(numRegions))));
		}else if(cPStats.getReliableRegionsMissingLimitedAccess() > 0){
			// health is poor
			int numRegions = cPStats.getReliableRegionsMissingLimitedAccess();
			status.add(poorHealth(LocalizedStrings.MemberHealthEvaluator_THERE_ARE_0_REGIONS_MISSING_REQUIRED_ROLES_AND_CONFIGURED_WITH_LIMITED_ACCESS.toLocalizedString(Integer.valueOf(numRegions))));
		}else if (cPStats.getReliableRegionsMissingNoAccess() > 0){
			// health is poor
			int numRegions = cPStats.getReliableRegionsMissingNoAccess();
			status.add(poorHealth(LocalizedStrings.MemberHealthEvaluator_THERE_ARE_0_REGIONS_MISSING_REQUIRED_ROLES_AND_CONFIGURED_WITHOUT_ACCESS.toLocalizedString(Integer.valueOf(numRegions))));
		}//else{
			// health is good/okay
		//	status.add(okayHealth("All regions have there required roles meet"));
		//}
	}
	catch (CancelException ignore) {
	}
  }

    
  /**
   * Updates the previous values of statistics
   */
  private void updatePrevious() {
    this.prevReplyTimeouts = this.dmStats.getReplyTimeouts();
  }

  @Override
  protected void check(List status) {
    checkVMProcessSize(status);
    checkMessageQueueSize(status);
    checkReplyTimeouts(status);
    // will have to add another call to check for roles 
    // missing and reliablity attributed.
    checkCacheRequiredRolesMeet(status);

    updatePrevious();
  }

  @Override
  void close() {

  }
}
