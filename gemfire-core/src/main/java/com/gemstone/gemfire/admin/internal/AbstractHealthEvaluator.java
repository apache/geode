/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin.internal;

import java.util.List;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.admin.GemFireHealth;
import com.gemstone.gemfire.admin.GemFireHealthConfig;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;

/**
 * The abstract superclass of all GemFire health evaluators.
 * Basically, this class specifies what the health evaluators need and
 * what they should do.
 *
 * <P>
 *
 * Note that evaluators never reside in the administration VM, they
 * only in member VMs.  They are not <code>Serializable</code> and
 * aren't meant to be.
 *
 * @author David Whitlock
 *
 * @since 3.5
 * */
public abstract class AbstractHealthEvaluator  {

  private static final Logger logger = LogService.getLogger();
  
  /** The number of times this evaluator has been evaluated.  Certain
   * checks are not made the first time an evaluation occurs.  */
  private int numEvaluations;

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>AbstractHealthEvaluator</code> with the given
   * <code>GemFireHealthConfig</code> and
   * <code>DistributionManager</code>.  
   *
   * Originally, this method took an
   * <code>InternalDistributedSystem</code>, but we found there were
   * race conditions during initialization.  Namely, that a
   * <code>DistributionMessage</code> can be processed before the
   * <code>InternalDistributedSystem</code>'s
   * <code>DistributionManager</code> is set.
   */
  protected AbstractHealthEvaluator(GemFireHealthConfig config,
                                    DM dm)
  {
    this.numEvaluations = 0;
  }

  /////////////////////  Instance Methods  /////////////////////

  /**
   * Evaluates the health of a component of a GemFire distributed
   * system. 
   *
   * @param status
   *        A list of {@link AbstractHealthEvaluator.HealthStatus
   *        HealthStatus} objects that is populated when ill health is
   *        detected.
   */
  public final void evaluate(List status) {
    this.numEvaluations++;
    check(status);
  }

  /**
   * Checks the health of a component of a GemFire distributed
   * system. 
   *
   * @see #evaluate
   */
  protected abstract void check(List status);

  /**
   * Returns whether or not this is the first evaluation
   */
  protected final boolean isFirstEvaluation() {
    return this.numEvaluations <= 1;
  }

  /**
   * A factory method that creates a {@link
   * AbstractHealthEvaluator.HealthStatus HealthStats} with
   * {@linkplain GemFireHealth#OKAY_HEALTH okay} status.
   */
  protected HealthStatus okayHealth(String diagnosis) {
    logger.info(LocalizedMessage.create(LocalizedStrings.AbstractHealthEvaluator_OKAY_HEALTH__0, diagnosis));
    return new HealthStatus(GemFireHealth.OKAY_HEALTH, diagnosis);
  }

  /**
   * A factory method that creates a {@link
   * AbstractHealthEvaluator.HealthStatus HealthStats} with
   * {@linkplain GemFireHealth#POOR_HEALTH poor} status.
   */
  protected HealthStatus poorHealth(String diagnosis) {
    logger.info(LocalizedMessage.create(LocalizedStrings.AbstractHealthEvaluator_POOR_HEALTH__0, diagnosis));
    return new HealthStatus(GemFireHealth.POOR_HEALTH, diagnosis);
  }

  /**
   * Returns a <code>String</code> describing the component whose
   * health is evaluated by this evaluator.
   */
  protected abstract String getDescription();

  /**
   * Closes this evaluator and releases all of its resources
   */
  abstract void close();

  ///////////////////////  Inner Classes  //////////////////////

  /**
   * Represents the health of a GemFire component.
   */
  public class HealthStatus  {
    /** The health of a GemFire component */
    private GemFireHealth.Health healthCode;

    /** The diagnosis of the illness */
    private String diagnosis;

    //////////////////////  Constructors  //////////////////////

    /**
     * Creates a new <code>HealthStatus</code> with the give
     * <code>health</code> code and <code>dianosis</code> message.
     *
     * @see GemFireHealth#OKAY_HEALTH
     * @see GemFireHealth#POOR_HEALTH
     */
    HealthStatus(GemFireHealth.Health healthCode, String diagnosis) {
      this.healthCode = healthCode;
      this.diagnosis =
        "[" + AbstractHealthEvaluator.this.getDescription() + "] " +
        diagnosis;
    }

    /////////////////////  Instance Methods  /////////////////////

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
     * Returns the diagnosis prepended with a description of the
     * component that is ill.
     */
    public String getDiagnosis() {
      return this.diagnosis;
    }

  }

}
