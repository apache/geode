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

import java.util.List;

import org.apache.logging.log4j.Logger;

import org.apache.geode.admin.GemFireHealth;
import org.apache.geode.admin.GemFireHealthConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.logging.LogService;

/**
 * The abstract superclass of all GemFire health evaluators. Basically, this class specifies what
 * the health evaluators need and what they should do.
 *
 * <P>
 *
 * Note that evaluators never reside in the administration VM, they only in member VMs. They are not
 * <code>Serializable</code> and aren't meant to be.
 *
 *
 * @since GemFire 3.5
 */
public abstract class AbstractHealthEvaluator {

  private static final Logger logger = LogService.getLogger();

  /**
   * The number of times this evaluator has been evaluated. Certain checks are not made the first
   * time an evaluation occurs.
   */
  private int numEvaluations;

  ////////////////////// Constructors //////////////////////

  /**
   * Creates a new <code>AbstractHealthEvaluator</code> with the given
   * <code>GemFireHealthConfig</code> and <code>DistributionManager</code>.
   *
   * Originally, this method took an <code>InternalDistributedSystem</code>, but we found there were
   * race conditions during initialization. Namely, that a <code>DistributionMessage</code> can be
   * processed before the <code>InternalDistributedSystem</code>'s <code>DistributionManager</code>
   * is set.
   */
  protected AbstractHealthEvaluator(GemFireHealthConfig config, DistributionManager dm) {
    this.numEvaluations = 0;
  }

  ///////////////////// Instance Methods /////////////////////

  /**
   * Evaluates the health of a component of a GemFire distributed system.
   *
   * @param status A list of {@link AbstractHealthEvaluator.HealthStatus HealthStatus} objects that
   *        is populated when ill health is detected.
   */
  public void evaluate(List status) {
    this.numEvaluations++;
    check(status);
  }

  /**
   * Checks the health of a component of a GemFire distributed system.
   *
   * @see #evaluate
   */
  protected abstract void check(List status);

  /**
   * Returns whether or not this is the first evaluation
   */
  protected boolean isFirstEvaluation() {
    return this.numEvaluations <= 1;
  }

  /**
   * A factory method that creates a {@link AbstractHealthEvaluator.HealthStatus HealthStats} with
   * {@linkplain GemFireHealth#OKAY_HEALTH okay} status.
   */
  protected HealthStatus okayHealth(String diagnosis) {
    logger.info("OKAY_HEALTH:  {}",
        diagnosis);
    return new HealthStatus(GemFireHealth.OKAY_HEALTH, diagnosis);
  }

  /**
   * A factory method that creates a {@link AbstractHealthEvaluator.HealthStatus HealthStats} with
   * {@linkplain GemFireHealth#POOR_HEALTH poor} status.
   */
  protected HealthStatus poorHealth(String diagnosis) {
    logger.info("POOR_HEALTH:  {}",
        diagnosis);
    return new HealthStatus(GemFireHealth.POOR_HEALTH, diagnosis);
  }

  /**
   * Returns a <code>String</code> describing the component whose health is evaluated by this
   * evaluator.
   */
  protected abstract String getDescription();

  /**
   * Closes this evaluator and releases all of its resources
   */
  abstract void close();

  /////////////////////// Inner Classes //////////////////////

  /**
   * Represents the health of a GemFire component.
   */
  public class HealthStatus {
    /** The health of a GemFire component */
    private GemFireHealth.Health healthCode;

    /** The diagnosis of the illness */
    private String diagnosis;

    ////////////////////// Constructors //////////////////////

    /**
     * Creates a new <code>HealthStatus</code> with the give <code>health</code> code and
     * <code>dianosis</code> message.
     *
     * @see GemFireHealth#OKAY_HEALTH
     * @see GemFireHealth#POOR_HEALTH
     */
    HealthStatus(GemFireHealth.Health healthCode, String diagnosis) {
      this.healthCode = healthCode;
      this.diagnosis = "[" + AbstractHealthEvaluator.this.getDescription() + "] " + diagnosis;
    }

    ///////////////////// Instance Methods /////////////////////

    /**
     * Returns the health code
     *
     * @see GemFireHealth#OKAY_HEALTH
     * @see GemFireHealth#POOR_HEALTH
     */
    public GemFireHealth.Health getHealthCode() {
      return this.healthCode;
    }

    /**
     * Returns the diagnosis prepended with a description of the component that is ill.
     */
    public String getDiagnosis() {
      return this.diagnosis;
    }

  }

}
