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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.geode.admin.GemFireHealth;
import org.apache.geode.admin.GemFireHealthConfig;
import org.apache.geode.internal.statistics.GemFireStatSampler;
import org.apache.geode.internal.statistics.OsStatisticsProvider;
import org.apache.geode.internal.statistics.platform.ProcessStats;

/**
 * Contains simple tests for the {@link MemberHealthEvaluator}.
 *
 *
 * @since GemFire 3.5
 */
@SuppressWarnings("deprecation")
public class MemberHealthEvaluatorJUnitTest extends HealthEvaluatorTestCase {

  /**
   * Tests that we are in {@link GemFireHealth#OKAY_HEALTH okay} health if the VM's process size is
   * too big.
   *
   * @see MemberHealthEvaluator#checkVMProcessSize
   */
  @Test
  public void testCheckVMProcessSize() throws InterruptedException {
    if (OsStatisticsProvider.build().osStatsSupported()) {
      GemFireStatSampler sampler = system.getStatSampler();
      assertNotNull(sampler);

      sampler.waitForInitialization(10000); // fix: remove infinite wait

      ProcessStats stats = sampler.getProcessStats();
      assertNotNull(stats);

      List status = new ArrayList();
      long threshold = stats.getProcessSize() * 2;

      if (threshold <= 0) {
        // The process size is zero on some Linux versions
        return;
      }

      GemFireHealthConfig config = new GemFireHealthConfigImpl(null);
      config.setMaxVMProcessSize(threshold);

      MemberHealthEvaluator eval =
          new MemberHealthEvaluator(config, system.getDistributionManager());
      eval.evaluate(status);
      assertTrue(status.isEmpty());

      status = new ArrayList();
      long processSize = stats.getProcessSize();
      threshold = processSize / 2;
      assertTrue("Threshold (" + threshold + ") is > 0.  " + "Process size is " + processSize,
          threshold > 0);

      config = new GemFireHealthConfigImpl(null);
      config.setMaxVMProcessSize(threshold);

      eval = new MemberHealthEvaluator(config, system.getDistributionManager());
      eval.evaluate(status);
      assertEquals(1, status.size());

      AbstractHealthEvaluator.HealthStatus ill =
          (AbstractHealthEvaluator.HealthStatus) status.get(0);
      assertEquals(GemFireHealth.OKAY_HEALTH, ill.getHealthCode());
      assertTrue(ill.getDiagnosis().indexOf("The size of this VM") != -1);
    }
  }
}
