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

package org.apache.geode.cache.lucene.internal.vector.continuous;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.lucene.internal.vector.continuous.ChordalVoronoiRouter.FnrVerificationResult;
import org.apache.geode.cache.lucene.internal.vector.continuous.ChordalVoronoiRouter.RouterConfig;
import org.apache.geode.cache.lucene.internal.vector.continuous.ChordalVoronoiRouter.RouterTelemetry;
import org.apache.geode.cache.lucene.internal.vector.continuous.ChordalVoronoiRouter.RoutingMode;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
public class ChordalVoronoiRouterTest {

  @Test
  public void theorem1ZeroFalseNegativesAcrossThousandsOfRandomAndBoundaryVectors() {
    int[] dimensions = new int[] {16, 64, 128};
    Random rng = new Random(42L);

    for (int dim : dimensions) {
      RouterConfig config = RouterConfig.builder(dim)
          .numClusters(16)
          .expectedSurvivalFraction(0.15d)
          .hysteresisDelta(0.15d)
          .randomSeed(100L + dim)
          .build();
      ChordalVoronoiRouter router = new ChordalVoronoiRouter(config);

      // Build 256 subscriptions drawn from 16 spherical topics + varied thresholds
      int numTopics = 16;
      float[][] topics = new float[numTopics][dim];
      for (int t = 0; t < numTopics; t++) {
        topics[t] = randomUnitVector(rng, dim);
      }

      float subNoise = 0.45f / (float) Math.sqrt(dim);
      float queryNoise = 0.55f / (float) Math.sqrt(dim);

      List<VectorSubscription> subs = new ArrayList<>(256);
      for (int m = 0; m < 256; m++) {
        float[] base = topics[m % numTopics];
        float[] perturbed = perturbVector(rng, base, subNoise);
        float threshold = (m % 20 == 0)
            ? 0.9999f // extreme boundary threshold near 1.0
            : 0.60f + rng.nextFloat() * 0.35f;
        subs.add(new VectorSubscription("sub-" + dim + "-" + m, perturbed, threshold));
      }
      router.addSubscriptions(subs);

      assertThat(router.getCurrentMode())
          .as("256 subscriptions should activate CHORDAL_VORONOI mode for dim=%d", dim)
          .isEqualTo(RoutingMode.CHORDAL_VORONOI);

      // 1. Test 2,000 random unit embeddings (mix of topic-aligned and uniform random)
      int totalBruteMatches = 0;
      for (int i = 0; i < 2000; i++) {
        float[] query = (i % 3 == 0)
            ? perturbVector(rng, topics[i % numTopics], queryNoise)
            : randomUnitVector(rng, dim);
        FnrVerificationResult res = router.verifyAgainstBruteForce(query);
        assertThat(res.getFalseNegatives())
            .as("Theorem 1 violation (FNR > 0) at dim=%d, iter=%d", dim, i)
            .isZero();
        assertThat(res.getFalsePositives())
            .as("False positive detected at dim=%d, iter=%d", dim, i)
            .isZero();
        totalBruteMatches += res.getBruteForceMatchCount();
      }

      // 2. Test 200 adversarial boundary vectors placed directly at cos(x, q_m) == tau_m or 1.0
      for (int i = 0; i < 200; i++) {
        VectorSubscription target = subs.get(i % subs.size());
        float[] q = target.getNormalizedQueryVector();
        float[] query;
        if (i % 2 == 0) {
          // Exact copy of subscription query vector (tests tau near 1.0)
          query = q.clone();
        } else {
          // Construct unit vector with cosine similarity right at threshold tau_m
          query = vectorAtExactCosine(rng, q, target.getSimilarityThreshold());
        }
        FnrVerificationResult res = router.verifyAgainstBruteForce(query);
        assertThat(res.getFalseNegatives())
            .as("Theorem 1 boundary violation at dim=%d, boundaryIter=%d", dim, i)
            .isZero();
        assertThat(res.getFalsePositives()).isZero();
      }

      RouterTelemetry telemetry = router.getTelemetry();
      assertThat(totalBruteMatches).isGreaterThan(0);
      assertThat(telemetry.getFnrVerificationRuns()).isEqualTo(2200L);
      assertThat(telemetry.getFnrVerificationFalseNegatives()).isZero();
      assertThat(telemetry.getPruneRatio())
          .as("Chordal Voronoi router should prune cells for dim=%d", dim)
          .isGreaterThan(0.15d);
    }
  }

  @Test
  public void highSelectivitySubscriptionsAchieveStrongPruningWithZeroFnr() {
    int dim = 64;
    Random rng = new Random(1337L);
    RouterConfig config = RouterConfig.builder(dim)
        .numClusters(16)
        .expectedSurvivalFraction(0.10d)
        .hysteresisDelta(0.15d)
        .randomSeed(2026L)
        .build();
    ChordalVoronoiRouter router = new ChordalVoronoiRouter(config);

    float[][] topics = new float[16][dim];
    for (int t = 0; t < topics.length; t++) {
      topics[t] = randomUnitVector(rng, dim);
    }
    List<VectorSubscription> subs = new ArrayList<>(256);
    for (int m = 0; m < 256; m++) {
      float[] q = perturbVector(rng, topics[m % topics.length], 0.05f);
      float threshold = 0.85f + rng.nextFloat() * 0.10f;
      subs.add(new VectorSubscription("sel-sub-" + m, q, threshold));
    }
    router.addSubscriptions(subs);
    assertThat(router.getCurrentMode()).isEqualTo(RoutingMode.CHORDAL_VORONOI);

    for (int i = 0; i < 1000; i++) {
      float[] query = (i % 4 == 0)
          ? perturbVector(rng, topics[i % topics.length], 0.06f)
          : randomUnitVector(rng, dim);
      FnrVerificationResult res = router.verifyAgainstBruteForce(query);
      assertThat(res.isExactMatch()).isTrue();
    }
    assertThat(router.getTelemetry().getPruneRatio())
        .as("High-selectivity spherical clusters should prune >70% of cells")
        .isGreaterThan(0.70d);
  }

  @Test
  public void proposition1ClosedFormMCritAndHysteresisBandPreventOscillation() {
    // Verify Proposition 1 formula: M_crit = ceil(K * (1 + (c_bound * W)/(c_simd * d)) / (1 - p))
    // d = 64, K = 16, W = 8, c_simd = 1.0, c_bound = 4.0 => eta = 32/64 = 0.5
    // p_bar = 0.20 => M_crit = ceil(16 * 1.5 / 0.80) = ceil(30.0) = 30
    int mCrit = ChordalVoronoiRouter.computeMCrit(64, 16, 0.20d, 8, 1.0d, 4.0d);
    assertThat(mCrit).isEqualTo(30);

    // Hysteresis delta = 0.15 => [0.85 * 30, 1.15 * 30] = [25.5, 34.5] => M_down = 25, M_up = 35
    int mUp = ChordalVoronoiRouter.computeUpperSwitchThreshold(mCrit, 0.15d);
    int mDown = ChordalVoronoiRouter.computeLowerSwitchThreshold(mCrit, 0.15d);
    assertThat(mUp).isEqualTo(35);
    assertThat(mDown).isEqualTo(25);

    RouterConfig config = RouterConfig.builder(64)
        .numClusters(16)
        .simdLaneWidth(8)
        .simdFmaCost(1.0d)
        .boundCheckCost(4.0d)
        .expectedSurvivalFraction(0.20d)
        .hysteresisDelta(0.15d)
        .build();
    ChordalVoronoiRouter router = new ChordalVoronoiRouter(config);

    assertThat(router.getMCrit()).isEqualTo(30);
    assertThat(router.getUpperSwitchThreshold()).isEqualTo(35);
    assertThat(router.getLowerSwitchThreshold()).isEqualTo(25);
    assertThat(router.getCurrentMode()).isEqualTo(RoutingMode.FLAT_SIMD);

    Random rng = new Random(7L);
    // Register up to M_up - 1 = 34 -> must remain in FLAT_SIMD (0 transitions)
    for (int i = 0; i < mUp - 1; i++) {
      router.addSubscription(new VectorSubscription("sub-" + i, randomUnitVector(rng, 64), 0.80f));
      assertThat(router.getCurrentMode()).isEqualTo(RoutingMode.FLAT_SIMD);
    }
    assertThat(router.getTelemetry().getModeTransitionCount()).isZero();

    // Register 35th subscription (M == M_up) -> switches to CHORDAL_VORONOI (1 transition)
    router.addSubscription(
        new VectorSubscription("sub-" + (mUp - 1), randomUnitVector(rng, 64), 0.80f));
    assertThat(router.getCurrentMode()).isEqualTo(RoutingMode.CHORDAL_VORONOI);
    assertThat(router.getTelemetry().getModeTransitionCount()).isEqualTo(1L);

    // Oscillate 50 times inside deadband [M_down + 1, M_up - 1] = [26, 34]
    // Must NEVER switch back to FLAT_SIMD!
    int nextId = 1000;
    for (int cycle = 0; cycle < 50; cycle++) {
      while (router.getSubscriptionCount() > mDown + 1) {
        String idToRemove = "osc-" + (nextId - 1);
        if (!router.removeSubscription(idToRemove)) {
          router.removeSubscription("sub-" + (router.getSubscriptionCount() - 1));
        } else {
          nextId--;
        }
        assertThat(router.getCurrentMode()).isEqualTo(RoutingMode.CHORDAL_VORONOI);
      }
      while (router.getSubscriptionCount() < mUp - 1) {
        router.addSubscription(
            new VectorSubscription("osc-" + (nextId++), randomUnitVector(rng, 64), 0.80f));
        assertThat(router.getCurrentMode()).isEqualTo(RoutingMode.CHORDAL_VORONOI);
      }
    }
    assertThat(router.getTelemetry().getModeTransitionCount())
        .as("Hysteresis band [0.85 M_crit, 1.15 M_crit] must prevent thrashing")
        .isEqualTo(1L);

    // Drop to M_down = 25 -> switches back to FLAT_SIMD (2nd transition)
    List<String> allIds = new ArrayList<>();
    for (int i = 0; i < mUp; i++) {
      allIds.add("sub-" + i);
    }
    for (int i = 1000; i < nextId; i++) {
      allIds.add("osc-" + i);
    }
    for (String id : allIds) {
      if (router.getSubscriptionCount() <= mDown) {
        break;
      }
      router.removeSubscription(id);
    }
    assertThat(router.getSubscriptionCount()).isEqualTo(mDown);
    assertThat(router.getCurrentMode()).isEqualTo(RoutingMode.FLAT_SIMD);
    assertThat(router.getTelemetry().getModeTransitionCount()).isEqualTo(2L);

    // Oscillate inside [M_down + 1, M_up - 1] starting from FLAT_SIMD -> remains FLAT_SIMD
    int flatId = 5000;
    for (int cycle = 0; cycle < 20; cycle++) {
      while (router.getSubscriptionCount() < mUp - 1) {
        router.addSubscription(
            new VectorSubscription("flat-" + (flatId++), randomUnitVector(rng, 64), 0.80f));
        assertThat(router.getCurrentMode()).isEqualTo(RoutingMode.FLAT_SIMD);
      }
      while (router.getSubscriptionCount() > mDown + 1) {
        router.removeSubscription("flat-" + (--flatId));
        assertThat(router.getCurrentMode()).isEqualTo(RoutingMode.FLAT_SIMD);
      }
    }
    assertThat(router.getTelemetry().getModeTransitionCount()).isEqualTo(2L);
  }

  @Test
  public void liveAddRemoveAndReclusterInVoronoiModePreservesZeroFnr() {
    Random rng = new Random(99L);
    RouterConfig config = RouterConfig.builder(32)
        .numClusters(8)
        .expectedSurvivalFraction(0.15d)
        .build();
    ChordalVoronoiRouter router = new ChordalVoronoiRouter(config);

    // Seed 60 subscriptions to enter CHORDAL_VORONOI mode
    List<VectorSubscription> initial = new ArrayList<>();
    for (int i = 0; i < 60; i++) {
      initial.add(new VectorSubscription("init-" + i, randomUnitVector(rng, 32), 0.65f));
    }
    router.addSubscriptions(initial);
    assertThat(router.getCurrentMode()).isEqualTo(RoutingMode.CHORDAL_VORONOI);

    // Live incremental adds (triggers cell radius expansion & periodic recluster)
    for (int i = 0; i < 40; i++) {
      router.addSubscription(new VectorSubscription("live-" + i, randomUnitVector(rng, 32), 0.60f));
    }
    assertThat(router.getSubscriptionCount()).isEqualTo(100);

    // Live removals
    for (int i = 0; i < 20; i++) {
      assertThat(router.removeSubscription("init-" + i)).isTrue();
    }
    assertThat(router.getSubscriptionCount()).isEqualTo(80);
    assertThat(router.removeSubscription("non-existent")).isFalse();

    // Verify FNR == 0 and exact parity with brute force across 500 queries
    for (int i = 0; i < 500; i++) {
      FnrVerificationResult res = router.verifyAgainstBruteForce(randomUnitVector(rng, 32));
      assertThat(res.isExactMatch()).isTrue();
    }

    // Explicit recluster and re-verify
    router.recluster();
    for (int i = 0; i < 200; i++) {
      FnrVerificationResult res = router.verifyAgainstBruteForce(randomUnitVector(rng, 32));
      assertThat(res.isExactMatch()).isTrue();
    }
  }

  @Test
  public void invalidInputsAreRejectedCleanly() {
    ChordalVoronoiRouter router = new ChordalVoronoiRouter(4);

    assertThatThrownBy(() -> new VectorSubscription("", new float[] {1, 0, 0, 0}, 0.5f))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new VectorSubscription("s1", new float[] {0, 0, 0, 0}, 0.5f))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
        () -> new VectorSubscription("s1", new float[] {Float.NaN, 1, 0, 0}, 0.5f))
            .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new VectorSubscription("s1", new float[] {1, 0, 0, 0}, -1.0f))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new VectorSubscription("s1", new float[] {1, 0, 0, 0}, 1.01f))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> router.addSubscription(
        new VectorSubscription("wrong-dim", new float[] {1, 0, 0}, 0.5f)))
            .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> router.route(new float[] {1, 0}))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> router.routeNormalized(new float[] {Float.NaN, 0, 0, 1}))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> router.routeNormalized(new float[] {Float.POSITIVE_INFINITY, 0, 0, 0}))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> router.routeNormalized(new float[] {Float.NEGATIVE_INFINITY, 0, 0, 0}))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void highDimension4096BoundaryVectorsGuaranteeZeroFnrWithScaledSafetyEpsilon() {
    int dim = 4096;
    assertThat(ChordalVoronoiRouter.computeFloatDotSafetyEpsilon(dim))
        .isGreaterThan(ChordalVoronoiRouter.FLOAT_DOT_SAFETY_EPSILON)
        .isGreaterThanOrEqualTo(dim * 1.0e-6d);

    Random rng = new Random(4096L);
    RouterConfig config = RouterConfig.builder(dim)
        .numClusters(8)
        .expectedSurvivalFraction(0.15d)
        .hysteresisDelta(0.15d)
        .randomSeed(4096L)
        .build();
    ChordalVoronoiRouter router = new ChordalVoronoiRouter(config);

    float[][] topics = new float[8][dim];
    for (int t = 0; t < topics.length; t++) {
      topics[t] = randomUnitVector(rng, dim);
    }

    float subNoise = 0.40f / (float) Math.sqrt(dim);
    List<VectorSubscription> subs = new ArrayList<>(64);
    for (int m = 0; m < 64; m++) {
      float[] q = perturbVector(rng, topics[m % topics.length], subNoise);
      float threshold = (m % 8 == 0) ? 0.9999f : 0.70f + (m % 5) * 0.05f;
      subs.add(new VectorSubscription("hd-sub-" + m, q, threshold));
    }
    router.addSubscriptions(subs);
    assertThat(router.getCurrentMode()).isEqualTo(RoutingMode.CHORDAL_VORONOI);
    assertThat(router.getFloatDotSafetyEpsilon()).isGreaterThanOrEqualTo(dim * 1.0e-6d);

    // 100 adversarial boundary vectors (exact copies + vectors placed right at cos == tau_m)
    for (int i = 0; i < 100; i++) {
      VectorSubscription target = subs.get(i % subs.size());
      float[] q = target.getNormalizedQueryVector();
      float[] query = (i % 2 == 0)
          ? q.clone()
          : vectorAtExactCosine(rng, q, target.getSimilarityThreshold());
      FnrVerificationResult res = router.verifyAgainstBruteForce(query);
      assertThat(res.getFalseNegatives())
          .as("High-dim d=4096 boundary FNR must be strictly 0 at iter=%d", i)
          .isZero();
      assertThat(res.getFalsePositives()).isZero();
    }

    // 100 random vectors verifying pruning remains active with FNR=0
    for (int i = 0; i < 100; i++) {
      FnrVerificationResult res = router.verifyAgainstBruteForce(randomUnitVector(rng, dim));
      assertThat(res.isExactMatch()).isTrue();
    }
    assertThat(router.getTelemetry().getPruneRatio()).isGreaterThan(0.20d);
  }

  @Test
  public void sqrtFreeHighamBoundEliminatesSingularityAtTauOneAcrossHighDimensions() {
    int[] dims = new int[] {128, 1536, 4096};
    float[] extremeTaus = new float[] {1.0f, 1.0f - 1.0e-7f, 0.9999f};
    Random rng = new Random(20261121L);

    for (int dim : dims) {
      double gamma = ChordalVoronoiRouter.computeHighamGamma(dim);
      double epsDot = ChordalVoronoiRouter.computeHighamDotEpsilon(dim);
      assertThat(gamma).isGreaterThan(0.0d);
      assertThat(epsDot).isGreaterThan(2.0d * gamma);

      // Force CHORDAL_VORONOI with K == M singleton cells so x == c_k == q_i holds identically,
      // PLUS multi-member cells containing tau_i == 1.0 and 1 - 1e-7.
      RouterConfig singletonCfg = RouterConfig.builder(dim)
          .numClusters(6)
          .boundCheckCost(0.0d)
          .expectedSurvivalFraction(0.0d)
          .hysteresisDelta(0.01d)
          .useCoupledBound(true)
          .randomSeed(dim * 17L)
          .build();
      ChordalVoronoiRouter singletonRouter = new ChordalVoronoiRouter(singletonCfg);
      List<VectorSubscription> extremeSubs = new ArrayList<>();
      for (int i = 0; i < 12; i++) {
        float[] q = randomUnitVector(rng, dim);
        float tau = extremeTaus[i % extremeTaus.length];
        extremeSubs.add(new VectorSubscription("sing-" + dim + "-" + i, q, tau));
      }
      singletonRouter.addSubscriptions(extremeSubs);
      assertThat(singletonRouter.getCurrentMode()).isEqualTo(RoutingMode.CHORDAL_VORONOI);
      assertThat(singletonRouter.getHighamDotEpsilon()).isEqualTo(epsDot);

      // Adversarial check: query x == q_i (and x == c_k == q_i for singleton cells) at tau == 1.0
      for (VectorSubscription sub : extremeSubs) {
        float[] exactCopy = sub.getNormalizedQueryVector();
        FnrVerificationResult res = singletonRouter.verifyAgainstBruteForce(exactCopy);
        assertThat(res.getFalseNegatives())
            .as("Sqrt-free Higham bound must yield FNR=0 at dim=%d, tau=%f",
                dim, sub.getSimilarityThreshold())
            .isZero();
        assertThat(res.getFalsePositives()).isZero();
      }

      // Multi-member cluster check with x == q_i != c_k and tau in {1.0, 1 - 1e-7, 0.9999}
      RouterConfig multiCfg = RouterConfig.builder(dim)
          .numClusters(4)
          .boundCheckCost(0.0d)
          .expectedSurvivalFraction(0.0d)
          .hysteresisDelta(0.01d)
          .useCoupledBound(true)
          .randomSeed(dim * 31L)
          .build();
      ChordalVoronoiRouter multiRouter = new ChordalVoronoiRouter(multiCfg);
      float[][] anchors = new float[4][dim];
      for (int a = 0; a < anchors.length; a++) {
        anchors[a] = randomUnitVector(rng, dim);
      }
      List<VectorSubscription> multiSubs = new ArrayList<>();
      float noise = 0.08f / (float) Math.sqrt(dim);
      for (int i = 0; i < 36; i++) {
        float[] q = (i < 4) ? anchors[i].clone() : perturbVector(rng, anchors[i % 4], noise);
        float tau = extremeTaus[i % extremeTaus.length];
        multiSubs.add(new VectorSubscription("multi-" + dim + "-" + i, q, tau));
      }
      multiRouter.addSubscriptions(multiSubs);
      assertThat(multiRouter.getCurrentMode()).isEqualTo(RoutingMode.CHORDAL_VORONOI);

      for (VectorSubscription sub : multiSubs) {
        FnrVerificationResult res = multiRouter.verifyAgainstBruteForce(
            sub.getNormalizedQueryVector());
        assertThat(res.getFalseNegatives())
            .as("Multi-member sqrt-free bound must yield FNR=0 at dim=%d, tau=%f",
                dim, sub.getSimilarityThreshold())
            .isZero();
        assertThat(res.getFalsePositives()).isZero();
      }
      assertThat(multiRouter.getTelemetry().getPruneRatio()).isGreaterThan(0.50d);
    }
  }

  @Test
  public void coupledCellBoundIsStrictlyAtLeastAsTightAsDecoupledBoundWithZeroFnr() {
    int dim = 128;
    Random rng = new Random(777L);
    float[][] topics = new float[8][dim];
    for (int t = 0; t < topics.length; t++) {
      topics[t] = randomUnitVector(rng, dim);
    }
    // Heterogeneous cells: tight queries near centroid have looser tau (0.65),
    // outer queries far from centroid have strict tau (0.95) -> coupled B_k << R_k + rho_k_max
    List<VectorSubscription> subs = new ArrayList<>(128);
    for (int i = 0; i < 128; i++) {
      boolean isCore = (i % 2 == 0);
      float noise = (isCore ? 0.05f : 0.65f) / (float) Math.sqrt(dim);
      float tau = isCore ? 0.65f : 0.95f;
      subs.add(new VectorSubscription("het-" + i, perturbVector(rng, topics[i % 8], noise), tau));
    }

    RouterConfig coupledCfg = RouterConfig.builder(dim)
        .numClusters(8)
        .expectedSurvivalFraction(0.15d)
        .useCoupledBound(true)
        .randomSeed(777L)
        .build();
    RouterConfig decoupledCfg = RouterConfig.builder(dim)
        .numClusters(8)
        .expectedSurvivalFraction(0.15d)
        .useCoupledBound(false)
        .randomSeed(777L)
        .build();

    ChordalVoronoiRouter coupled = new ChordalVoronoiRouter(coupledCfg);
    ChordalVoronoiRouter decoupled = new ChordalVoronoiRouter(decoupledCfg);
    coupled.addSubscriptions(subs);
    decoupled.addSubscriptions(subs);

    assertThat(coupled.getCurrentMode()).isEqualTo(RoutingMode.CHORDAL_VORONOI);
    assertThat(decoupled.getCurrentMode()).isEqualTo(RoutingMode.CHORDAL_VORONOI);

    for (int i = 0; i < 500; i++) {
      float[] q = (i % 4 == 0)
          ? perturbVector(rng, topics[i % 8], 0.30f / (float) Math.sqrt(dim))
          : randomUnitVector(rng, dim);
      assertThat(coupled.verifyAgainstBruteForce(q).isExactMatch()).isTrue();
      assertThat(decoupled.verifyAgainstBruteForce(q).isExactMatch()).isTrue();
    }
    assertThat(coupled.getTelemetry().getPruneRatio())
        .as("Coupled bound B_k^up must prune at least as many cells as decoupled R_k + rho_k^max")
        .isGreaterThanOrEqualTo(decoupled.getTelemetry().getPruneRatio());
  }

  @Test
  public void incrementalRemoveAvoidsFullKMeansUntilChurnThresholdAndPreservesZeroFnr() {
    int dim = 64;
    Random rng = new Random(9090L);
    RouterConfig cfg = RouterConfig.builder(dim)
        .numClusters(8)
        .expectedSurvivalFraction(0.15d)
        .hysteresisDelta(0.15d)
        .randomSeed(9090L)
        .build();
    ChordalVoronoiRouter router = new ChordalVoronoiRouter(cfg);

    // Seed 100 topic-clustered subscriptions so Voronoi stays active (p_bar << 0.85)
    // and the 20% churn threshold triggers periodic k-means compaction at 20 removals
    float[][] topics = new float[8][dim];
    for (int t = 0; t < topics.length; t++) {
      topics[t] = randomUnitVector(rng, dim);
    }
    float noise = 0.25f / (float) Math.sqrt(dim);
    List<VectorSubscription> subs = new ArrayList<>(100);
    for (int i = 0; i < 100; i++) {
      subs.add(new VectorSubscription("inc-rm-" + i, perturbVector(rng, topics[i % 8], noise),
          0.80f));
    }
    router.addSubscriptions(subs);
    assertThat(router.getCurrentMode()).isEqualTo(RoutingMode.CHORDAL_VORONOI);
    long reclustersAfterInit = router.getTelemetry().getFullReclusterCount();
    assertThat(reclustersAfterInit).isEqualTo(1L);

    // Remove 10 subscriptions (< 20% of 90 = 18): must NOT trigger full spherical k-means
    for (int i = 0; i < 10; i++) {
      VectorSubscription removed = subs.get(i);
      assertThat(router.removeSubscription(removed.getSubscriptionId())).isTrue();
      assertThat(router.getTelemetry().getFullReclusterCount())
          .as("Incremental remove #%d below 20%% churn must not trigger full k-means", i)
          .isEqualTo(reclustersAfterInit);

      // Verify removed subscription is never returned and FNR == 0 holds
      List<ChordalVoronoiRouter.MatchResult> matches =
          router.route(removed.getNormalizedQueryVector());
      assertThat(matches)
          .extracting(ChordalVoronoiRouter.MatchResult::getSubscriptionId)
          .doesNotContain(removed.getSubscriptionId());
      FnrVerificationResult verify =
          router.verifyAgainstBruteForce(removed.getNormalizedQueryVector());
      assertThat(verify.isExactMatch()).isTrue();
    }

    // Verify FNR == 0 across 200 random queries and all 90 remaining exact vectors
    for (int i = 10; i < 100; i++) {
      assertThat(router.verifyAgainstBruteForce(subs.get(i).getNormalizedQueryVector())
          .isExactMatch()).isTrue();
    }
    for (int i = 0; i < 200; i++) {
      assertThat(router.verifyAgainstBruteForce(randomUnitVector(rng, dim)).isExactMatch())
          .isTrue();
    }

    // Remove 10 more subscriptions (cumulative churn hits 20 >= 20% of remaining) -> triggers
    // recluster
    for (int i = 10; i < 20; i++) {
      assertThat(router.removeSubscription(subs.get(i).getSubscriptionId())).isTrue();
    }
    assertThat(router.getSubscriptionCount()).isEqualTo(80);
    assertThat(router.getTelemetry().getFullReclusterCount())
        .as("Reaching 20%% churn threshold must trigger periodic k-means compaction")
        .isGreaterThan(reclustersAfterInit);
    for (int i = 0; i < 100; i++) {
      assertThat(router.verifyAgainstBruteForce(randomUnitVector(rng, dim)).isExactMatch())
          .isTrue();
    }
  }

  @Test
  public void computeMCritReturnsMaxValueAtOrAboveSurvivalCeilingAndStaysFlatSimd() {
    // At p_bar = 1.0 or >= maxVoronoiSurvivalRate (default 0.85), computeMCrit returns MAX_VALUE
    assertThat(ChordalVoronoiRouter.computeMCrit(100, 64, 1.0d, 8, 1.0d, 2.0d))
        .isEqualTo(Integer.MAX_VALUE);
    assertThat(ChordalVoronoiRouter.computeMCrit(100, 64, 0.999d, 8, 1.0d, 2.0d))
        .isEqualTo(Integer.MAX_VALUE);
    assertThat(ChordalVoronoiRouter.computeMCrit(100, 64, 0.85d, 8, 1.0d, 2.0d))
        .isEqualTo(Integer.MAX_VALUE);

    // Below 0.85, matches Proposition 1 closed-form ceil(K * (1 + eta_d) / (1 - p_bar))
    double eta = ChordalVoronoiRouter.computeEta(100, 8, 1.0d, 2.0d);
    int expected84 = (int) Math.ceil((64 * (1.0d + eta)) / (1.0d - 0.84d));
    assertThat(ChordalVoronoiRouter.computeMCrit(100, 64, 0.84d, 8, 1.0d, 2.0d))
        .isEqualTo(expected84);

    // Hysteresis thresholds never overflow when M_crit == Integer.MAX_VALUE
    assertThat(ChordalVoronoiRouter.computeUpperSwitchThreshold(Integer.MAX_VALUE, 0.15d))
        .isEqualTo(Integer.MAX_VALUE);
    assertThat(ChordalVoronoiRouter.computeLowerSwitchThreshold(Integer.MAX_VALUE, 0.15d))
        .isEqualTo(Integer.MAX_VALUE);

    // Router configured with p_bar = 1.0 stays in FLAT_SIMD with zero transitions and FNR == 0
    Random rng = new Random(1001L);
    RouterConfig isoCfg = RouterConfig.builder(64)
        .numClusters(16)
        .expectedSurvivalFraction(1.0d)
        .build();
    ChordalVoronoiRouter router = new ChordalVoronoiRouter(isoCfg);
    assertThat(router.getMCrit()).isEqualTo(Integer.MAX_VALUE);
    List<VectorSubscription> subs = new ArrayList<>(300);
    for (int i = 0; i < 300; i++) {
      subs.add(new VectorSubscription("iso-" + i, randomUnitVector(rng, 64), 0.70f));
    }
    router.addSubscriptions(subs);
    assertThat(router.getCurrentMode()).isEqualTo(RoutingMode.FLAT_SIMD);
    assertThat(router.getTelemetry().getModeTransitionCount()).isZero();
    for (int q = 0; q < 50; q++) {
      assertThat(router.verifyAgainstBruteForce(randomUnitVector(rng, 64)).isExactMatch()).isTrue();
    }
  }

  @Test
  public void onlineSurvivalRateGuardAutomaticallyRevertsIsotropicStreamsToFlatSimd() {
    int dim = 100;
    Random rng = new Random(20261122L);
    RouterConfig cfg = RouterConfig.builder(dim)
        .numClusters(32)
        .expectedSurvivalFraction(0.15d)
        .maxVoronoiSurvivalRate(0.85d)
        .enableSurvivalGuard(true)
        .survivalGuardMinSamples(24)
        .hysteresisDelta(0.15d)
        .randomSeed(20261122L)
        .build();
    ChordalVoronoiRouter router = new ChordalVoronoiRouter(cfg);

    // Register 400 isotropic random unit subscriptions with broad threshold tau=0.30
    // so p_bar_hat -> 1.00 (exceeding the 0.85 survival-rate ceiling)
    List<VectorSubscription> subs = new ArrayList<>(400);
    for (int i = 0; i < 400; i++) {
      subs.add(new VectorSubscription("unif-" + i, randomUnitVector(rng, dim), 0.30f));
    }
    router.addSubscriptions(subs);
    assertThat(router.getCurrentMode())
        .as("400 subs should initially activate CHORDAL_VORONOI before online routing starts")
        .isEqualTo(RoutingMode.CHORDAL_VORONOI);
    assertThat(router.isSurvivalGuardTripped()).isFalse();

    // Route 40 isotropic queries -> online EWMA detects p_bar_hat >= 0.85 and reverts to FLAT_SIMD
    for (int q = 0; q < 40; q++) {
      FnrVerificationResult res = router.verifyAgainstBruteForce(randomUnitVector(rng, dim));
      assertThat(res.isExactMatch())
          .as("FNR must remain strictly 0 before, during, and after survival-guard reversion")
          .isTrue();
    }

    assertThat(router.isSurvivalGuardTripped()).isTrue();
    assertThat(router.getCurrentMode()).isEqualTo(RoutingMode.FLAT_SIMD);
    assertThat(router.getEffectiveMCrit()).isEqualTo(Integer.MAX_VALUE);
    assertThat(router.getObservedSurvivalRate()).isGreaterThanOrEqualTo(0.85d);
    assertThat(router.getTelemetry().getSurvivalGuardReversionCount()).isEqualTo(1L);
    assertThat(router.getTelemetry().getActiveClusterCount()).isZero();

    // Subsequent subscription churn must stay in FLAT_SIMD without thrashing back to Voronoi
    long transitionsAfterTrip = router.getTelemetry().getModeTransitionCount();
    router.addSubscription(new VectorSubscription("unif-extra", randomUnitVector(rng, dim), 0.30f));
    router.removeSubscription("unif-0");
    assertThat(router.getCurrentMode()).isEqualTo(RoutingMode.FLAT_SIMD);
    assertThat(router.getTelemetry().getModeTransitionCount()).isEqualTo(transitionsAfterTrip);

    // Explicit recluster resets the guard cleanly
    router.recluster();
    assertThat(router.isSurvivalGuardTripped()).isFalse();
    assertThat(router.getCurrentMode()).isEqualTo(RoutingMode.CHORDAL_VORONOI);
  }

  @Test
  public void voronoiPerCellCopyOnWriteOmitsFlatArraysAndReplacesOnlyAffectedCell() {
    int dim = 64;
    Random rng = new Random(20261123L);
    RouterConfig cfg = RouterConfig.builder(dim)
        .numClusters(8)
        .expectedSurvivalFraction(0.15d)
        .hysteresisDelta(0.15d)
        .randomSeed(20261123L)
        .build();
    ChordalVoronoiRouter router = new ChordalVoronoiRouter(cfg);

    float[][] topics = new float[8][dim];
    for (int t = 0; t < topics.length; t++) {
      topics[t] = randomUnitVector(rng, dim);
    }
    float noise = 0.20f / (float) Math.sqrt(dim);
    List<VectorSubscription> subs = new ArrayList<>(80);
    for (int i = 0; i < 80; i++) {
      subs.add(new VectorSubscription("cow-" + i, perturbVector(rng, topics[i % 8], noise), 0.80f));
    }
    router.addSubscriptions(subs);

    assertThat(router.getCurrentMode()).isEqualTo(RoutingMode.CHORDAL_VORONOI);
    assertThat(router.hasFlatArraysInCurrentSnapshot())
        .as("CHORDAL_VORONOI snapshots must not maintain O(Md) flat arrays")
        .isFalse();

    // 1. Incremental insert replaces strictly 1 VoronoiCell and reuses all sibling cell references
    ChordalVoronoiRouter.VoronoiCell[] beforeAdd = router.getCurrentCellsForTest();
    assertThat(beforeAdd.length).isEqualTo(8);
    router.addSubscription(
        new VectorSubscription("cow-added", perturbVector(rng, topics[0], noise), 0.80f));
    assertThat(router.hasFlatArraysInCurrentSnapshot()).isFalse();
    ChordalVoronoiRouter.VoronoiCell[] afterAdd = router.getCurrentCellsForTest();
    assertThat(afterAdd.length).isEqualTo(beforeAdd.length);
    int replacedOnAdd = 0;
    for (int k = 0; k < beforeAdd.length; k++) {
      if (beforeAdd[k] != afterAdd[k]) {
        replacedOnAdd++;
      }
    }
    assertThat(replacedOnAdd)
        .as("O((M/K)d) CoW insert must replace strictly 1 VoronoiCell")
        .isEqualTo(1);
    for (int q = 0; q < 50; q++) {
      assertThat(router.verifyAgainstBruteForce(perturbVector(rng, topics[q % 8], noise))
          .isExactMatch()).isTrue();
    }

    // 2. Incremental remove replaces strictly 1 VoronoiCell and preserves FNR == 0
    ChordalVoronoiRouter.VoronoiCell[] beforeRemove = router.getCurrentCellsForTest();
    assertThat(router.removeSubscription("cow-added")).isTrue();
    assertThat(router.hasFlatArraysInCurrentSnapshot()).isFalse();
    ChordalVoronoiRouter.VoronoiCell[] afterRemove = router.getCurrentCellsForTest();
    assertThat(afterRemove.length).isEqualTo(beforeRemove.length);
    int replacedOnRemove = 0;
    for (int k = 0; k < beforeRemove.length; k++) {
      if (beforeRemove[k] != afterRemove[k]) {
        replacedOnRemove++;
      }
    }
    assertThat(replacedOnRemove)
        .as("O((M/K)d) CoW remove must replace strictly 1 VoronoiCell")
        .isEqualTo(1);

    // 3. Verify 1-member cell collapse shifts subscriptionCellIndex cleanly for subsequent removes
    // Pick a cell, remove all its members to collapse it, then remove a member from a later cell
    ChordalVoronoiRouter.VoronoiCell[] cells = router.getCurrentCellsForTest();
    String[] firstCellIds = cells[0].memberIds.clone();
    String laterCellMemberId = cells[cells.length - 1].memberIds[0];
    for (String id : firstCellIds) {
      assertThat(router.removeSubscription(id)).isTrue();
    }
    assertThat(router.removeSubscription(laterCellMemberId)).isTrue();
    assertThat(router.getSubscriptionCount()).isEqualTo(80 - firstCellIds.length - 1);
    for (int q = 0; q < 100; q++) {
      assertThat(router.verifyAgainstBruteForce(randomUnitVector(rng, dim)).isExactMatch())
          .isTrue();
    }
  }

  @Test
  public void theorem1Float64EtaAndOutwardRoundingGuaranteeZeroFnrNearDiameterTwoAndTauOne() {
    assertThat(ChordalVoronoiRouter.FLOAT64_UNIT_ROUNDOFF).isEqualTo(Math.scalb(1.0d, -53));

    int dim = 128;
    double etaF64 = ChordalVoronoiRouter.computeFloat64Eta(dim);
    double normMargin = ChordalVoronoiRouter.computeNormMargin(dim);
    double epsDot = ChordalVoronoiRouter.computeHighamDotEpsilon(dim);
    assertThat(etaF64).isGreaterThan(4.0d * ChordalVoronoiRouter.FLOAT64_UNIT_ROUNDOFF);
    assertThat(normMargin).isGreaterThan(etaF64);

    // Wide cell where B_k^up >= 2.0 must set minCosineThreshold == -Infinity (never prunes)
    float[] unit = new float[dim];
    unit[0] = 1.0f;
    ChordalVoronoiRouter.VoronoiCell wideCell = new ChordalVoronoiRouter.VoronoiCell(unit, 1.5d,
        0.6d, 2.05d, true, epsDot, normMargin, new String[] {"w1"}, unit, new float[] {0.5f});
    assertThat(wideCell.effectiveUpperBound).isEqualTo(2.0d);
    assertThat(wideCell.minCosineThreshold).isEqualTo(Double.NEGATIVE_INFINITY);

    // Non-degenerate cell must apply 8 * u_f64 + Math.nextDown outward rounding strictly below
    // 1 - B^2 / 2 - epsDot
    double b = 0.75d;
    ChordalVoronoiRouter.VoronoiCell tightCell = new ChordalVoronoiRouter.VoronoiCell(unit, 0.25d,
        0.50d, b, true, epsDot, normMargin, new String[] {"t1"}, unit, new float[] {0.9f});
    double unroundedUpper =
        1.0d - 0.5d * tightCell.effectiveUpperBound * tightCell.effectiveUpperBound - epsDot;
    assertThat(tightCell.minCosineThreshold).isLessThan(unroundedUpper);
    assertThat(tightCell.minCosineThreshold).isEqualTo(
        Math.nextDown(unroundedUpper - 8.0d * ChordalVoronoiRouter.FLOAT64_UNIT_ROUNDOFF));

    // Adversarial FNR == 0 test combining antipodal pairs (B_k^up -> 2.0) and tau == 1.0f
    Random rng = new Random(8888L);
    RouterConfig cfg = RouterConfig.builder(dim)
        .numClusters(4)
        .boundCheckCost(0.0d)
        .expectedSurvivalFraction(0.0d)
        .hysteresisDelta(0.01d)
        .enableSurvivalGuard(false)
        .randomSeed(8888L)
        .build();
    ChordalVoronoiRouter router = new ChordalVoronoiRouter(cfg);
    List<VectorSubscription> subs = new ArrayList<>();
    for (int i = 0; i < 16; i++) {
      float[] q = randomUnitVector(rng, dim);
      float[] antipodal = new float[dim];
      for (int d = 0; d < dim; d++) {
        antipodal[d] = -q[d];
      }
      subs.add(new VectorSubscription("pos-" + i, q, (i % 2 == 0) ? 1.0f : 0.01f));
      subs.add(new VectorSubscription("neg-" + i, antipodal, (i % 2 == 0) ? 1.0f : 0.01f));
    }
    router.addSubscriptions(subs);
    for (VectorSubscription sub : subs) {
      assertThat(router.verifyAgainstBruteForce(sub.getNormalizedQueryVector()).isExactMatch())
          .isTrue();
    }
    for (int i = 0; i < 100; i++) {
      assertThat(router.verifyAgainstBruteForce(randomUnitVector(rng, dim)).isExactMatch())
          .isTrue();
    }
  }

  @Test
  public void subnormalAndExtremeMagnitudeVectorsNormalizeWithoutOverflowOrUnderflowAndYieldZeroFnr() {
    float[][] edgeCases = new float[][] {
        new float[] {Float.MIN_VALUE, 0.0f, 0.0f, 0.0f},
        new float[] {Float.MIN_VALUE, -Float.MIN_VALUE, Float.MIN_VALUE, -Float.MIN_VALUE},
        new float[] {1.0e-30f, 2.0e-30f, -1.0e-30f, 0.0f},
        new float[] {1.0e-19f, -1.0e-19f, 1.0e-19f, 1.0e-19f},
        new float[] {Float.MAX_VALUE, Float.MAX_VALUE, 0.0f, 0.0f},
        new float[] {-Float.MAX_VALUE, 0.0f, Float.MAX_VALUE, 0.0f},
        new float[] {Float.MIN_VALUE, 1.0f, 0.0f, 0.0f}
    };
    for (float[] raw : edgeCases) {
      float[] unit = VectorSubscription.normalize(raw);
      double normSq = 0.0d;
      for (float u : unit) {
        assertThat(Float.isFinite(u)).isTrue();
        normSq += (double) u * (double) u;
      }
      assertThat(Math.sqrt(normSq)).isBetween(0.9999d, 1.0001d);
    }

    int dim = 64;
    Random rng = new Random(20261124L);
    RouterConfig cfg = RouterConfig.builder(dim)
        .numClusters(8)
        .expectedSurvivalFraction(0.15d)
        .hysteresisDelta(0.15d)
        .randomSeed(20261124L)
        .build();
    ChordalVoronoiRouter router = new ChordalVoronoiRouter(cfg);

    float[][] topics = new float[8][dim];
    for (int t = 0; t < topics.length; t++) {
      topics[t] = randomUnitVector(rng, dim);
    }
    float[] scales = new float[] {Float.MIN_VALUE, 1.0e-30f, 1.0e-19f, Float.MAX_VALUE, 1.0f};

    List<VectorSubscription> subs = new ArrayList<>(64);
    for (int i = 0; i < 64; i++) {
      float[] base = perturbVector(rng, topics[i % topics.length], 0.05f);
      float scale = scales[i % scales.length];
      float[] scaled = new float[dim];
      for (int d = 0; d < dim; d++) {
        // Ensure non-zero subnormal coordinates even when |base[d]| is ~0.1
        scaled[d] = (scale == Float.MIN_VALUE)
            ? (base[d] >= 0.0f ? Float.MIN_VALUE : -Float.MIN_VALUE)
            : base[d] * scale;
      }
      subs.add(new VectorSubscription("subnorm-" + i, scaled, 0.80f));
    }
    router.addSubscriptions(subs);
    assertThat(router.getCurrentMode()).isEqualTo(RoutingMode.CHORDAL_VORONOI);

    int totalBruteMatches = 0;
    for (int i = 0; i < 200; i++) {
      float[] q = (i % 2 == 0)
          ? perturbVector(rng, topics[i % topics.length], 0.05f)
          : randomUnitVector(rng, dim);
      float scale = scales[i % scales.length];
      float[] scaledQuery = new float[dim];
      for (int d = 0; d < dim; d++) {
        scaledQuery[d] = (scale == Float.MIN_VALUE)
            ? (q[d] >= 0.0f ? Float.MIN_VALUE : -Float.MIN_VALUE)
            : q[d] * scale;
      }
      FnrVerificationResult res = router.verifyAgainstBruteForce(scaledQuery);
      assertThat(res.isExactMatch())
          .as("Subnormal/extreme-scale query iter=%d scale=%g must yield FNR=0 and FP=0", i, scale)
          .isTrue();
      totalBruteMatches += res.getBruteForceMatchCount();
    }
    assertThat(totalBruteMatches).isGreaterThan(0);
    assertThat(router.getTelemetry().getPruneRatio()).isGreaterThan(0.20d);
  }

  @Test
  public void heterogeneousTauStratificationAndBoundAwareInsertIsolateOutliersWithZeroFnr() {
    assertThatThrownBy(() -> RouterConfig.builder(8).thresholdStrata(0))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(RouterConfig.builder(8).build().getThresholdStrata())
        .isEqualTo(ChordalVoronoiRouter.DEFAULT_THRESHOLD_STRATA);

    int dim = 64;
    Random rng = new Random(314159L);
    float[][] topics = new float[8][dim];
    for (int t = 0; t < topics.length; t++) {
      topics[t] = randomUnitVector(rng, dim);
    }

    // 15% low-tau outliers (0.50..0.58) mixed across all 8 topics with 85% high-tau subs
    // (0.93..0.97)
    List<VectorSubscription> subs = new ArrayList<>(200);
    float noise = 0.70f / (float) Math.sqrt(dim);
    for (int i = 0; i < 200; i++) {
      float[] q = perturbVector(rng, topics[i % topics.length], noise);
      float tau = (i % 7 == 0)
          ? 0.50f + (i % 5) * 0.02f
          : 0.93f + (i % 5) * 0.01f;
      subs.add(new VectorSubscription("strata-" + i, q, tau));
    }

    RouterConfig unstratifiedCfg = RouterConfig.builder(dim)
        .numClusters(16)
        .expectedSurvivalFraction(0.15d)
        .thresholdStrata(1)
        .enableSurvivalGuard(false)
        .randomSeed(314159L)
        .build();
    RouterConfig stratifiedCfg = RouterConfig.builder(dim)
        .numClusters(16)
        .expectedSurvivalFraction(0.15d)
        .thresholdStrata(4)
        .enableSurvivalGuard(false)
        .randomSeed(314159L)
        .build();

    ChordalVoronoiRouter unstratified = new ChordalVoronoiRouter(unstratifiedCfg);
    ChordalVoronoiRouter stratified = new ChordalVoronoiRouter(stratifiedCfg);
    unstratified.addSubscriptions(subs);
    stratified.addSubscriptions(subs);

    assertThat(unstratified.getCurrentMode()).isEqualTo(RoutingMode.CHORDAL_VORONOI);
    assertThat(stratified.getCurrentMode()).isEqualTo(RoutingMode.CHORDAL_VORONOI);

    for (int i = 0; i < 500; i++) {
      float[] query = (i % 4 == 0)
          ? perturbVector(rng, topics[i % topics.length], noise)
          : randomUnitVector(rng, dim);
      assertThat(unstratified.verifyAgainstBruteForce(query).isExactMatch()).isTrue();
      assertThat(stratified.verifyAgainstBruteForce(query).isExactMatch()).isTrue();
      unstratified.route(query);
      stratified.route(query);
    }

    // Stratified router isolates low-tau outliers into dedicated bands and prunes far more cells
    assertThat(stratified.getTelemetry().getPruneRatio())
        .isGreaterThan(unstratified.getTelemetry().getPruneRatio() + 0.25d);
    assertThat(stratified.getTelemetry().getObservedSurvivalFraction())
        .isLessThan(unstratified.getTelemetry().getObservedSurvivalFraction());

    // Bound-aware incremental insert: adding new low-tau outliers must NOT poison high-tau cells
    ChordalVoronoiRouter.VoronoiCell[] cellsBefore = stratified.getCurrentCellsForTest();
    int highTauCellsBefore = 0;
    for (ChordalVoronoiRouter.VoronoiCell c : cellsBefore) {
      if (c.minMemberThreshold >= 0.90f) {
        highTauCellsBefore++;
      }
    }
    assertThat(highTauCellsBefore).isGreaterThan(0);

    for (int i = 0; i < 10; i++) {
      stratified.addSubscription(new VectorSubscription("inc-outlier-" + i,
          perturbVector(rng, topics[i % topics.length], noise), 0.51f + i * 0.005f));
    }

    ChordalVoronoiRouter.VoronoiCell[] cellsAfter = stratified.getCurrentCellsForTest();
    int highTauCellsAfter = 0;
    for (ChordalVoronoiRouter.VoronoiCell c : cellsAfter) {
      if (c.minMemberThreshold >= 0.90f) {
        highTauCellsAfter++;
      }
    }
    assertThat(highTauCellsAfter)
        .as("Bound-aware incremental insert must keep high-tau cells unpoisoned")
        .isEqualTo(highTauCellsBefore);

    for (int i = 0; i < 200; i++) {
      assertThat(stratified.verifyAgainstBruteForce(randomUnitVector(rng, dim)).isExactMatch())
          .isTrue();
    }

    // Guard check: K < 4 and narrow tauMax - tauMin < 0.03f cleanly remain single-stratum with
    // FNR=0
    ChordalVoronoiRouter smallKRouter = new ChordalVoronoiRouter(RouterConfig.builder(dim)
        .numClusters(3)
        .boundCheckCost(0.0d)
        .expectedSurvivalFraction(0.0d)
        .hysteresisDelta(0.01d)
        .thresholdStrata(4)
        .build());
    smallKRouter.addSubscriptions(subs);
    assertThat(smallKRouter.getTelemetry().getActiveClusterCount()).isEqualTo(3);
    assertThat(smallKRouter.verifyAgainstBruteForce(randomUnitVector(rng, dim)).isExactMatch())
        .isTrue();

    List<VectorSubscription> narrowTauSubs = new ArrayList<>(64);
    for (int i = 0; i < 64; i++) {
      narrowTauSubs.add(new VectorSubscription("narrow-" + i,
          perturbVector(rng, topics[i % topics.length], noise), 0.90f + (i % 3) * 0.01f));
    }
    ChordalVoronoiRouter narrowRouter = new ChordalVoronoiRouter(RouterConfig.builder(dim)
        .numClusters(8)
        .expectedSurvivalFraction(0.15d)
        .thresholdStrata(4)
        .build());
    narrowRouter.addSubscriptions(narrowTauSubs);
    assertThat(narrowRouter.verifyAgainstBruteForce(randomUnitVector(rng, dim)).isExactMatch())
        .isTrue();
  }

  @Test
  public void periodicSurvivalGuardReprobeRestoresVoronoiAfterRegimeShiftWithZeroFnr() {
    RouterConfig defaults = RouterConfig.builder(8).build();
    assertThat(defaults.getSurvivalGuardReprobeInterval())
        .isEqualTo(ChordalVoronoiRouter.DEFAULT_SURVIVAL_GUARD_REPROBE_INTERVAL);
    assertThat(defaults.getSurvivalGuardRecoveryMargin())
        .isEqualTo(ChordalVoronoiRouter.DEFAULT_SURVIVAL_GUARD_RECOVERY_MARGIN);
    assertThatThrownBy(() -> RouterConfig.builder(8).survivalGuardReprobeInterval(-1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> RouterConfig.builder(8).survivalGuardRecoveryMargin(-0.01d))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> RouterConfig.builder(8).survivalGuardRecoveryMargin(1.0d))
        .isInstanceOf(IllegalArgumentException.class);

    int dim = 64;
    Random rng = new Random(20261125L);
    float[] sharedAnchor = randomUnitVector(rng, dim);

    // Place 8 subscription topics in the positive cap around sharedAnchor (dot ~ 0.80) so queries
    // aligned with sharedAnchor match/survive every Voronoi cell (p_hat -> 1.0), whereas orthogonal
    // queries prune 100% of cells (p_probe -> 0.0).
    float[][] topics = new float[8][dim];
    for (int t = 0; t < topics.length; t++) {
      topics[t] = vectorAtExactCosine(rng, sharedAnchor, 0.82f);
    }
    List<VectorSubscription> subs = new ArrayList<>(128);
    for (int i = 0; i < 128; i++) {
      subs.add(new VectorSubscription("reprobe-" + i,
          perturbVector(rng, topics[i % topics.length], 0.02f), 0.82f));
    }

    RouterConfig cfg = RouterConfig.builder(dim)
        .numClusters(8)
        .expectedSurvivalFraction(0.15d)
        .maxVoronoiSurvivalRate(0.85d)
        .enableSurvivalGuard(true)
        .survivalGuardMinSamples(24)
        .survivalGuardReprobeInterval(64)
        .survivalGuardRecoveryMargin(0.10d)
        .hysteresisDelta(0.15d)
        .randomSeed(20261125L)
        .build();
    ChordalVoronoiRouter router = new ChordalVoronoiRouter(cfg);
    router.addSubscriptions(subs);
    assertThat(router.getCurrentMode()).isEqualTo(RoutingMode.CHORDAL_VORONOI);

    // Phase 1: Anchor-aligned broad queries trip the survival guard -> FLAT_SIMD
    for (int i = 0; i < 40; i++) {
      FnrVerificationResult res =
          router.verifyAgainstBruteForce(perturbVector(rng, sharedAnchor, 0.01f));
      assertThat(res.isExactMatch()).isTrue();
    }
    assertThat(router.isSurvivalGuardTripped()).isTrue();
    assertThat(router.getCurrentMode()).isEqualTo(RoutingMode.FLAT_SIMD);
    assertThat(router.getSurvivalGuardRecoveryCount()).isZero();
    assertThat(router.getTelemetry().getActiveClusterCount()).isZero();

    // Still in Phase 1 across a full reprobe interval + 16-sample burst: must stay FLAT_SIMD
    for (int i = 0; i < 90; i++) {
      FnrVerificationResult res =
          router.verifyAgainstBruteForce(perturbVector(rng, sharedAnchor, 0.01f));
      assertThat(res.isExactMatch()).isTrue();
    }
    assertThat(router.isSurvivalGuardTripped()).isTrue();
    assertThat(router.getCurrentMode()).isEqualTo(RoutingMode.FLAT_SIMD);
    assertThat(router.getSurvivalGuardRecoveryCount()).isZero();

    // Phase 2: Regime shifts to queries orthogonal to sharedAnchor (p_probe = 0.0 <= 0.75)
    for (int i = 0; i < 100; i++) {
      float[] orth = vectorAtExactCosine(rng, sharedAnchor, 0.0f);
      FnrVerificationResult res = router.verifyAgainstBruteForce(orth);
      assertThat(res.isExactMatch()).isTrue();
    }
    assertThat(router.isSurvivalGuardTripped()).isFalse();
    assertThat(router.getCurrentMode()).isEqualTo(RoutingMode.CHORDAL_VORONOI);
    assertThat(router.getEffectiveMCrit()).isEqualTo(router.getMCrit());
    assertThat(router.getSurvivalGuardRecoveryCount()).isEqualTo(1L);
    assertThat(router.getTelemetry().getSurvivalGuardRecoveryCount()).isEqualTo(1L);
    assertThat(router.getTelemetry().getActiveClusterCount()).isEqualTo(8);

    // Verify survivalGuardReprobeInterval(0) disables automatic recovery
    RouterConfig disabledCfg = RouterConfig.builder(dim)
        .numClusters(8)
        .expectedSurvivalFraction(0.15d)
        .maxVoronoiSurvivalRate(0.85d)
        .enableSurvivalGuard(true)
        .survivalGuardMinSamples(24)
        .survivalGuardReprobeInterval(0)
        .hysteresisDelta(0.15d)
        .randomSeed(20261125L)
        .build();
    ChordalVoronoiRouter disabledRouter = new ChordalVoronoiRouter(disabledCfg);
    disabledRouter.addSubscriptions(subs);
    for (int i = 0; i < 40; i++) {
      assertThat(disabledRouter.verifyAgainstBruteForce(perturbVector(rng, sharedAnchor, 0.01f))
          .isExactMatch()).isTrue();
    }
    assertThat(disabledRouter.isSurvivalGuardTripped()).isTrue();
    for (int i = 0; i < 200; i++) {
      assertThat(
          disabledRouter.verifyAgainstBruteForce(vectorAtExactCosine(rng, sharedAnchor, 0.0f))
              .isExactMatch()).isTrue();
    }
    assertThat(disabledRouter.isSurvivalGuardTripped()).isTrue();
    assertThat(disabledRouter.getCurrentMode()).isEqualTo(RoutingMode.FLAT_SIMD);
    assertThat(disabledRouter.getSurvivalGuardRecoveryCount()).isZero();
  }

  private static float[] randomUnitVector(Random rng, int dim) {
    float[] v = new float[dim];
    for (int i = 0; i < dim; i++) {
      v[i] = (float) rng.nextGaussian();
    }
    return VectorSubscription.normalize(v);
  }

  private static float[] perturbVector(Random rng, float[] base, float noiseStd) {
    float[] v = new float[base.length];
    for (int i = 0; i < base.length; i++) {
      v[i] = base[i] + (float) (rng.nextGaussian() * noiseStd);
    }
    return VectorSubscription.normalize(v);
  }

  private static float[] vectorAtExactCosine(Random rng, float[] unitQ, float targetCos) {
    int dim = unitQ.length;
    float[] rand = randomUnitVector(rng, dim);
    // Project out component along unitQ to obtain orthogonal unit vector u
    double dot = 0.0d;
    for (int i = 0; i < dim; i++) {
      dot += (double) rand[i] * (double) unitQ[i];
    }
    float[] orth = new float[dim];
    for (int i = 0; i < dim; i++) {
      orth[i] = (float) (rand[i] - dot * unitQ[i]);
    }
    orth = VectorSubscription.normalize(orth);
    double sin = Math.sqrt(Math.max(0.0d, 1.0d - (double) targetCos * (double) targetCos));
    float[] result = new float[dim];
    for (int i = 0; i < dim; i++) {
      result[i] = (float) (targetCos * unitQ[i] + sin * orth[i]);
    }
    return VectorSubscription.normalize(result);
  }
}
