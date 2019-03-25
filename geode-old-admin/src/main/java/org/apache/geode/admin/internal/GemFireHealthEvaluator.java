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
package org.apache.geode.admin.internal;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.logging.log4j.Logger;

import org.apache.geode.admin.GemFireHealth;
import org.apache.geode.admin.GemFireHealthConfig;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.logging.LogService;

/**
 * Evaluates the health of various GemFire components in the VM according to a
 * {@link GemFireHealthConfig}.
 *
 * <P>
 *
 * Note that evaluators never reside in the administration VM, they only in member VMs. They are not
 * <code>Serializable</code> and aren't meant to be.
 *
 * @see MemberHealthEvaluator
 * @see CacheHealthEvaluator
 *
 *
 * @since GemFire 3.5
 */
public class GemFireHealthEvaluator {

  private static final Logger logger = LogService.getLogger();

  /** Determines how the health of GemFire is determined */
  private GemFireHealthConfig config;

  /** Evaluates the health of this member of the distributed system */
  private MemberHealthEvaluator memberHealth;

  /** Evaluates the health of the Cache hosted in this VM */
  private CacheHealthEvaluator cacheHealth;

  /**
   * The most recent <code>OKAY_HEALTH</code> diagnoses of the GemFire system
   */
  private List okayDiagnoses;

  /**
   * The most recent <code>POOR_HEALTH</code> diagnoses of the GemFire system
   */
  private List poorDiagnoses;

  /////////////////////// Constructors ///////////////////////

  /**
   * Creates a new <code>GemFireHealthEvaluator</code>
   *
   * @param config The configuration that determines whether or GemFire is healthy
   * @param dm The distribution manager
   */
  public GemFireHealthEvaluator(GemFireHealthConfig config, ClusterDistributionManager dm) {
    if (config == null) {
      throw new NullPointerException(
          "Null GemFireHealthConfig");
    }

    this.config = config;
    this.memberHealth = new MemberHealthEvaluator(config, dm);
    this.cacheHealth = new CacheHealthEvaluator(config, dm);
    this.okayDiagnoses = new ArrayList();
    this.poorDiagnoses = new ArrayList();
  }

  ////////////////////// Instance Methods //////////////////////

  /**
   * Evaluates the health of the GemFire components in this VM.
   *
   * @return The aggregate health code (such as {@link GemFireHealth#OKAY_HEALTH}) of the GemFire
   *         components.
   */
  public GemFireHealth.Health evaluate() {
    List status = new ArrayList();
    this.memberHealth.evaluate(status);
    this.cacheHealth.evaluate(status);

    GemFireHealth.Health overallHealth = GemFireHealth.GOOD_HEALTH;
    this.okayDiagnoses.clear();
    this.poorDiagnoses.clear();

    for (Iterator iter = status.iterator(); iter.hasNext();) {
      AbstractHealthEvaluator.HealthStatus health =
          (AbstractHealthEvaluator.HealthStatus) iter.next();
      if (overallHealth == GemFireHealth.GOOD_HEALTH) {
        if ((health.getHealthCode() != GemFireHealth.GOOD_HEALTH)) {
          overallHealth = health.getHealthCode();
        }

      } else if (overallHealth == GemFireHealth.OKAY_HEALTH) {
        if (health.getHealthCode() == GemFireHealth.POOR_HEALTH) {
          overallHealth = GemFireHealth.POOR_HEALTH;
        }
      }

      GemFireHealth.Health healthCode = health.getHealthCode();
      if (healthCode == GemFireHealth.OKAY_HEALTH) {
        this.okayDiagnoses.add(health.getDiagnosis());

      } else if (healthCode == GemFireHealth.POOR_HEALTH) {
        this.poorDiagnoses.add(health.getDiagnosis());
      }
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Evaluated health to be {}", overallHealth);
    }
    return overallHealth;
  }

  /**
   * Returns detailed information explaining the current health status. Each array element is a
   * different cause for the current status. An empty array will be returned if the current status
   * is {@link GemFireHealth#GOOD_HEALTH}.
   */
  public String[] getDiagnosis(GemFireHealth.Health healthCode) {
    if (healthCode == GemFireHealth.GOOD_HEALTH) {
      return new String[0];

    } else if (healthCode == GemFireHealth.OKAY_HEALTH) {
      String[] array = new String[this.okayDiagnoses.size()];
      this.okayDiagnoses.toArray(array);
      return array;

    } else {
      Assert.assertTrue(healthCode == GemFireHealth.POOR_HEALTH);
      String[] array = new String[this.poorDiagnoses.size()];
      this.poorDiagnoses.toArray(array);
      return array;
    }
  }

  /**
   * Resets the state of this evaluator
   */
  public void reset() {
    this.okayDiagnoses.clear();
    this.poorDiagnoses.clear();
  }

  /**
   * Returns the heath evaluation interval, in seconds.
   *
   * @see GemFireHealthConfig#getHealthEvaluationInterval
   */
  public int getEvaluationInterval() {
    return this.config.getHealthEvaluationInterval();
  }

  /**
   * Closes this evaluator and releases all of its resources
   */
  public void close() {
    this.memberHealth.close();
    this.cacheHealth.close();
  }

}
