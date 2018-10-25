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
package org.apache.geode.admin;

import org.apache.geode.internal.Assert;

/**
 * Provides information about the aggregate health of the members of a GemFire distributed system
 * ("components"). The {@link #getHealth getHealth} method provides an indication of the overall
 * health. Health is expressed as one of three levels: {@link #GOOD_HEALTH GOOD_HEALTH},
 * {@link #OKAY_HEALTH OKAY_HEALTH}, and {@link #POOR_HEALTH POOR_HEALTH}. The {@link #getDiagnosis
 * getDiagnosis} method provides a more detailed explanation of the cause of ill health.
 *
 * <P>
 *
 * The aggregate health of the GemFire component is evaluated
 * {@linkplain GemFireHealthConfig#setHealthEvaluationInterval every so often} and if certain
 * criteria are met, then the overall health of the component changes accordingly. If any of the
 * components is in <code>OKAY_HEALTH</code>, then the overall health is <code>OKAY_HEALTH</code>.
 * If any of the components is in <code>POOR_HEALTH</code>, then the overall health is
 * <code>POOR_HEALTH</code>.
 *
 *
 * @since GemFire 3.5
 * @deprecated as of 7.0 use the <code><a href=
 *             "{@docRoot}/org/apache/geode/management/package-summary.html">management</a></code>
 *             package instead
 */
public interface GemFireHealth {

  /**
   * An indicator that the GemFire components are healthy.
   *
   * @see #getHealth
   */
  Health GOOD_HEALTH = new Health(Health.GOOD_STRING);

  /**
   * An indicator that one or more GemFire components is slightly unhealthy. The problem may or may
   * not require configuration changes and may not necessarily lead to poorer component health.
   *
   * @see #getHealth
   */
  Health OKAY_HEALTH = new Health(Health.OKAY_STRING);

  /**
   * An indicator that one or more GemFire components is unhealthy. While it may be possible for the
   * components to recover on their own, it is likely that they will have to be restarted.
   *
   * @see #getHealth
   */
  Health POOR_HEALTH = new Health(Health.POOR_STRING);

  /////////////////////// Instance Methods ///////////////////////

  /**
   * Returns an indicator of the overall health of the GemFire components.
   *
   * @see #GOOD_HEALTH
   * @see #OKAY_HEALTH
   * @see #POOR_HEALTH
   */
  Health getHealth();

  /**
   * Resets the overall health of the GemFire components to {@link #GOOD_HEALTH}. This operation
   * should be invoked when the operator has determined that warnings about the components's health
   * do not need to be regarded.
   */
  void resetHealth();

  /**
   * Returns a message that provides a description of the cause of a component's ill health.
   */
  String getDiagnosis();

  /**
   * Returns the configuration for determining the health of the distributed system itself.
   */
  DistributedSystemHealthConfig getDistributedSystemHealthConfig();

  /**
   * Sets the configuration for determining the health of the distributed system itself.
   */
  void setDistributedSystemHealthConfig(DistributedSystemHealthConfig config);

  /**
   * Returns the <code>GemFireHealthConfig</code> for GemFire components whose configurations are
   * not overridden on a per-host basis. Note that changes made to the returned
   * <code>GemFireHealthConfig</code> will not take effect until
   * {@link #setDefaultGemFireHealthConfig} is invoked.
   */
  GemFireHealthConfig getDefaultGemFireHealthConfig();

  /**
   * Sets the <code>GemFireHealthConfig</code> for GemFire components whose configurations are not
   * overridden on a per-host basis.
   *
   * @throws IllegalArgumentException If <code>config</code> specifies the config for a host
   */
  void setDefaultGemFireHealthConfig(GemFireHealthConfig config);

  /**
   * Returns the <code>GemFireHealthConfig</code> for GemFire components that reside on a given
   * host. This configuration will override the {@linkplain #getDefaultGemFireHealthConfig default}
   * configuration.
   *
   * @param hostName The {@linkplain java.net.InetAddress#getCanonicalHostName canonical} name of
   *        the host.
   */
  GemFireHealthConfig getGemFireHealthConfig(String hostName);

  /**
   * Sets the <code>GemFireHealthConfig</code> for GemFire components that reside on a given host.
   * This configuration will override the {@linkplain #getDefaultGemFireHealthConfig default}
   * configuration. Note that changes made to the returned <code>GemFireHealthConfig</code> will not
   * take effect until {@link #setDefaultGemFireHealthConfig} is invoked.
   *
   * @param hostName The {@linkplain java.net.InetAddress#getCanonicalHostName canonical} name of
   *        the host.
   *
   * @throws IllegalArgumentException If host <code>hostName</code> does not exist or if there are
   *         no GemFire components running on that host or if <code>config</code> does not configure
   *         host <code>hostName</code>.
   */
  void setGemFireHealthConfig(String hostName, GemFireHealthConfig config);

  /**
   * Closes this health monitor and releases all resources associated with it.
   */
  void close();

  /**
   * Returns whether or not this <code>GemFireHealth</code> is {@linkplain #close closed}.
   */
  boolean isClosed();

  ////////////////////// Inner Classes //////////////////////

  /**
   * An enumerated type for the health of GemFire.
   */
  class Health implements java.io.Serializable {
    private static final long serialVersionUID = 3039539430412151801L;
    /** The string for good health */
    static final String GOOD_STRING = "Good";

    /** The string for okay health */
    static final String OKAY_STRING = "Okay";

    /** The string for poor health */
    static final String POOR_STRING = "Poor";

    //////////////////// Instance Fields ////////////////////

    /** The string for this health */
    private String healthString = OKAY_STRING;

    ///////////////////// Constructors //////////////////////

    /**
     * Creates a new <code>Health</code> with the given string
     */
    protected Health(String healthString) {
      this.healthString = healthString;
    }

    //////////////////// Instance Methods ////////////////////

    /**
     * Returns the appropriate canonical instance of <code>Health</code>.
     */
    public Object readResolve() {
      if (this.healthString == null) {
        return null;
      }
      if (this.healthString.equals(GOOD_STRING)) {
        return GemFireHealth.GOOD_HEALTH;

      } else if (this.healthString.equals(OKAY_STRING)) {
        return GemFireHealth.OKAY_HEALTH;

      } else if (this.healthString.equals(POOR_STRING)) {
        return GemFireHealth.POOR_HEALTH;

      } else {
        Assert.assertTrue(false, "Unknown healthString: " + this.healthString);
        return null;
      }
    }

    @Override
    public String toString() {
      return this.healthString;
    }

  }

}
