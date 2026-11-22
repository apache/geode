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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Adaptive continuous vector router implementing:
 * <ul>
 * <li><b>Theorem 1 (Zero-False-Negative Sqrt-Free Inner-Product Voronoi Pruning)</b> on the unit
 * hypersphere {@code S^{d-1}}: partitions {@code M} active subscriptions {@code {(q_i, tau_i)}}
 * into {@code K} Voronoi cells {@code {V_k}} with unit centroids {@code c_k}, precomputed coupled
 * upper bound {@code B_k^up = max_{i in V_k}(||q_i - c_k||_2 + rho_i^up) + delta_norm(d)} in
 * float64, and cell cosine pruning threshold
 * {@code s_k^min = 1 - (B_k^up)^2 / 2 - epsilon_dot(d)} derived directly from Higham's IEEE-754
 * Theorem 3.1 ({@code gamma_d = d * u_f32 / (1 - d * u_f32)}). Pruning cell {@code V_k} iff
 * {@code s_hat_k < s_k^min} (and {@code B_k^up < 2}) eliminates {@code Math.sqrt} from the hot
 * routing path and avoids the {@code 1 / sqrt(1 - s)} singularity near {@code s -> 1}, guaranteeing
 * {@code FNR = 0} unconditionally.</li>
 * <li><b>Proposition 1 (SIMD-to-Voronoi Analytical Crossover {@code M_crit} and Dual-Threshold
 * Hysteresis)</b>: dynamically switches between flat contiguous unrolled SIMD-friendly batch
 * dot-product evaluation ({@link RoutingMode#FLAT_SIMD}) and two-stage Chordal Voronoi routing
 * ({@link RoutingMode#CHORDAL_VORONOI}) across hysteresis band
 * {@code [(1 - delta) * M_crit, (1 + delta) * M_crit]} (default {@code delta = 0.15}).</li>
 * </ul>
 */
public class ChordalVoronoiRouter {

  /** IEEE-754 binary32 (float32) unit roundoff {@code u_f32 = 2^-24}. */
  public static final double FLOAT32_UNIT_ROUNDOFF = Math.scalb(1.0d, -24);

  /** Conservative additive floor for Higham's float32 dot + unit-norm rounding margin. */
  static final double HIGHAM_DOT_SAFETY_FLOOR = 1.0e-5d;

  /** Legacy linear margin constants retained for backward compatibility. */
  static final double FLOAT_DOT_SAFETY_EPSILON = 1.0e-4d;
  static final double FLOAT_DOT_SAFETY_EPSILON_PER_DIM = 1.0e-6d;

  /** Default hardware SIMD lane width (8 floats = 256-bit AVX2). */
  public static final int DEFAULT_SIMD_LANE_WIDTH = 8;
  /** Default per-vectorized-FMA relative cost {@code c_simd}. */
  public static final double DEFAULT_SIMD_FMA_COST = 1.0d;
  /** Default scalar bound-check overhead {@code c_bound}. */
  public static final double DEFAULT_BOUND_CHECK_COST = 4.0d;
  /** Default expected unpruned survival fraction {@code p_bar}. */
  public static final double DEFAULT_EXPECTED_SURVIVAL_FRACTION = 0.15d;
  /**
   * Default online survival-rate ceiling above which Voronoi pruning provides insufficient work
   * reduction and {@link RoutingMode#FLAT_SIMD} is automatically selected.
   */
  public static final double DEFAULT_MAX_VORONOI_SURVIVAL_RATE = 0.85d;
  /** Default EWMA smoothing factor for online survival-rate estimation {@code p_bar_hat}. */
  public static final double DEFAULT_SURVIVAL_EWMA_ALPHA = 0.10d;
  /** Default minimum routed samples before the online survival-rate guard can trip. */
  public static final int DEFAULT_SURVIVAL_GUARD_MIN_SAMPLES = 32;
  /** Default dual-threshold hysteresis half-width {@code delta = 0.15} -> [0.85, 1.15] * M_crit. */
  public static final double DEFAULT_HYSTERESIS_DELTA = 0.15d;
  /** Default cluster count {@code K}. */
  public static final int DEFAULT_NUM_CLUSTERS = 16;
  /** Default maximum spherical k-means Lloyd iterations. */
  public static final int DEFAULT_MAX_KMEANS_ITERATIONS = 15;
  /** Recluster when live incremental churn exceeds 20% of subscriptions. */
  private static final double RECLUSTER_CHURN_RATIO = 0.20d;

  public enum RoutingMode {
    FLAT_SIMD,
    CHORDAL_VORONOI
  }

  /**
   * Configuration for {@link ChordalVoronoiRouter} matching Proposition 1's analytical cost model.
   */
  public static final class RouterConfig {
    private final int dimension;
    private final int numClusters;
    private final int simdLaneWidth;
    private final double simdFmaCost;
    private final double boundCheckCost;
    private final double expectedSurvivalFraction;
    private final double maxVoronoiSurvivalRate;
    private final boolean enableSurvivalGuard;
    private final int survivalGuardMinSamples;
    private final double hysteresisDelta;
    private final int maxKMeansIterations;
    private final long randomSeed;
    private final boolean useCoupledBound;

    private RouterConfig(Builder builder) {
      this.dimension = builder.dimension;
      this.numClusters = builder.numClusters;
      this.simdLaneWidth = builder.simdLaneWidth;
      this.simdFmaCost = builder.simdFmaCost;
      this.boundCheckCost = builder.boundCheckCost;
      this.expectedSurvivalFraction = builder.expectedSurvivalFraction;
      this.maxVoronoiSurvivalRate = builder.maxVoronoiSurvivalRate;
      this.enableSurvivalGuard = builder.enableSurvivalGuard;
      this.survivalGuardMinSamples = builder.survivalGuardMinSamples;
      this.hysteresisDelta = builder.hysteresisDelta;
      this.maxKMeansIterations = builder.maxKMeansIterations;
      this.randomSeed = builder.randomSeed;
      this.useCoupledBound = builder.useCoupledBound;
    }

    public static Builder builder(int dimension) {
      return new Builder(dimension);
    }

    public int getDimension() {
      return dimension;
    }

    public int getNumClusters() {
      return numClusters;
    }

    public int getSimdLaneWidth() {
      return simdLaneWidth;
    }

    public double getSimdFmaCost() {
      return simdFmaCost;
    }

    public double getBoundCheckCost() {
      return boundCheckCost;
    }

    public double getExpectedSurvivalFraction() {
      return expectedSurvivalFraction;
    }

    public double getMaxVoronoiSurvivalRate() {
      return maxVoronoiSurvivalRate;
    }

    public boolean isEnableSurvivalGuard() {
      return enableSurvivalGuard;
    }

    public int getSurvivalGuardMinSamples() {
      return survivalGuardMinSamples;
    }

    public double getHysteresisDelta() {
      return hysteresisDelta;
    }

    public int getMaxKMeansIterations() {
      return maxKMeansIterations;
    }

    public long getRandomSeed() {
      return randomSeed;
    }

    /**
     * Returns whether cells prune with the tighter coupled bound
     * {@code B_k^up = max_i(||q_i - c_k||_2 + rho_i^up)} (default {@code true}) or the looser
     * decoupled bound {@code R_k + rho_k^max} ({@code false}, useful for ablation).
     */
    public boolean isUseCoupledBound() {
      return useCoupledBound;
    }

    public static final class Builder {
      private final int dimension;
      private int numClusters = DEFAULT_NUM_CLUSTERS;
      private int simdLaneWidth = DEFAULT_SIMD_LANE_WIDTH;
      private double simdFmaCost = DEFAULT_SIMD_FMA_COST;
      private double boundCheckCost = DEFAULT_BOUND_CHECK_COST;
      private double expectedSurvivalFraction = DEFAULT_EXPECTED_SURVIVAL_FRACTION;
      private double maxVoronoiSurvivalRate = DEFAULT_MAX_VORONOI_SURVIVAL_RATE;
      private boolean enableSurvivalGuard = true;
      private int survivalGuardMinSamples = DEFAULT_SURVIVAL_GUARD_MIN_SAMPLES;
      private double hysteresisDelta = DEFAULT_HYSTERESIS_DELTA;
      private int maxKMeansIterations = DEFAULT_MAX_KMEANS_ITERATIONS;
      private long randomSeed = 0x5EED1234L;
      private boolean useCoupledBound = true;

      private Builder(int dimension) {
        if (dimension <= 0) {
          throw new IllegalArgumentException("dimension must be > 0, got: " + dimension);
        }
        this.dimension = dimension;
      }

      public Builder numClusters(int numClusters) {
        if (numClusters <= 0) {
          throw new IllegalArgumentException("numClusters must be > 0, got: " + numClusters);
        }
        this.numClusters = numClusters;
        return this;
      }

      public Builder simdLaneWidth(int simdLaneWidth) {
        if (simdLaneWidth <= 0) {
          throw new IllegalArgumentException("simdLaneWidth must be > 0, got: " + simdLaneWidth);
        }
        this.simdLaneWidth = simdLaneWidth;
        return this;
      }

      public Builder simdFmaCost(double simdFmaCost) {
        if (!Double.isFinite(simdFmaCost) || simdFmaCost <= 0.0d) {
          throw new IllegalArgumentException("simdFmaCost must be > 0, got: " + simdFmaCost);
        }
        this.simdFmaCost = simdFmaCost;
        return this;
      }

      public Builder boundCheckCost(double boundCheckCost) {
        if (!Double.isFinite(boundCheckCost) || boundCheckCost < 0.0d) {
          throw new IllegalArgumentException("boundCheckCost must be >= 0, got: " + boundCheckCost);
        }
        this.boundCheckCost = boundCheckCost;
        return this;
      }

      public Builder expectedSurvivalFraction(double expectedSurvivalFraction) {
        if (!Double.isFinite(expectedSurvivalFraction) || expectedSurvivalFraction < 0.0d
            || expectedSurvivalFraction > 1.0d) {
          throw new IllegalArgumentException(
              "expectedSurvivalFraction must be in [0, 1], got: " + expectedSurvivalFraction);
        }
        this.expectedSurvivalFraction = expectedSurvivalFraction;
        return this;
      }

      public Builder maxVoronoiSurvivalRate(double maxVoronoiSurvivalRate) {
        if (!Double.isFinite(maxVoronoiSurvivalRate) || maxVoronoiSurvivalRate <= 0.0d
            || maxVoronoiSurvivalRate > 1.0d) {
          throw new IllegalArgumentException(
              "maxVoronoiSurvivalRate must be in (0, 1], got: " + maxVoronoiSurvivalRate);
        }
        this.maxVoronoiSurvivalRate = maxVoronoiSurvivalRate;
        return this;
      }

      public Builder enableSurvivalGuard(boolean enableSurvivalGuard) {
        this.enableSurvivalGuard = enableSurvivalGuard;
        return this;
      }

      public Builder survivalGuardMinSamples(int survivalGuardMinSamples) {
        if (survivalGuardMinSamples <= 0) {
          throw new IllegalArgumentException(
              "survivalGuardMinSamples must be > 0, got: " + survivalGuardMinSamples);
        }
        this.survivalGuardMinSamples = survivalGuardMinSamples;
        return this;
      }

      public Builder hysteresisDelta(double hysteresisDelta) {
        if (!Double.isFinite(hysteresisDelta) || hysteresisDelta <= 0.0d
            || hysteresisDelta >= 1.0d) {
          throw new IllegalArgumentException(
              "hysteresisDelta must be in (0, 1), got: " + hysteresisDelta);
        }
        this.hysteresisDelta = hysteresisDelta;
        return this;
      }

      public Builder maxKMeansIterations(int maxKMeansIterations) {
        if (maxKMeansIterations <= 0) {
          throw new IllegalArgumentException(
              "maxKMeansIterations must be > 0, got: " + maxKMeansIterations);
        }
        this.maxKMeansIterations = maxKMeansIterations;
        return this;
      }

      public Builder randomSeed(long randomSeed) {
        this.randomSeed = randomSeed;
        return this;
      }

      /**
       * Selects the tighter coupled cell bound {@code B_k^up = max_i(||q_i - c_k||_2 + rho_i^up)}
       * ({@code true}, default) versus the decoupled bound {@code R_k + rho_k^max} ({@code false}).
       */
      public Builder useCoupledBound(boolean useCoupledBound) {
        this.useCoupledBound = useCoupledBound;
        return this;
      }

      public RouterConfig build() {
        return new RouterConfig(this);
      }
    }
  }

  /** Matched subscription ID and cosine similarity score. */
  public static final class MatchResult {
    private final String subscriptionId;
    private final float similarityScore;

    public MatchResult(String subscriptionId, float similarityScore) {
      this.subscriptionId = subscriptionId;
      this.similarityScore = similarityScore;
    }

    public String getSubscriptionId() {
      return subscriptionId;
    }

    public float getSimilarityScore() {
      return similarityScore;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof MatchResult)) {
        return false;
      }
      MatchResult that = (MatchResult) o;
      return Float.compare(that.similarityScore, similarityScore) == 0
          && Objects.equals(subscriptionId, that.subscriptionId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(subscriptionId, similarityScore);
    }

    @Override
    public String toString() {
      return "MatchResult{subscriptionId='" + subscriptionId + "', similarityScore="
          + similarityScore + '}';
    }
  }

  /** Snapshot of router telemetry for observability and testing. */
  public static final class RouterTelemetry {
    private final RoutingMode currentMode;
    private final int subscriptionCount;
    private final int activeClusterCount;
    private final int mCrit;
    private final int upperSwitchThreshold;
    private final int lowerSwitchThreshold;
    private final long modeTransitionCount;
    private final long fullReclusterCount;
    private final long totalRoutedVectors;
    private final long totalCellsEvaluated;
    private final long totalCellsPruned;
    private final long totalSubscriptionsEvaluated;
    private final long totalSubscriptionsPossible;
    private final long fnrVerificationRuns;
    private final long fnrVerificationFalseNegatives;
    private final double observedSurvivalEwma;
    private final boolean survivalGuardTripped;
    private final long survivalGuardReversionCount;

    RouterTelemetry(RoutingMode currentMode, int subscriptionCount, int activeClusterCount,
        int mCrit, int upperSwitchThreshold, int lowerSwitchThreshold, long modeTransitionCount,
        long fullReclusterCount, long totalRoutedVectors, long totalCellsEvaluated,
        long totalCellsPruned, long totalSubscriptionsEvaluated, long totalSubscriptionsPossible,
        long fnrVerificationRuns, long fnrVerificationFalseNegatives, double observedSurvivalEwma,
        boolean survivalGuardTripped, long survivalGuardReversionCount) {
      this.currentMode = currentMode;
      this.subscriptionCount = subscriptionCount;
      this.activeClusterCount = activeClusterCount;
      this.mCrit = mCrit;
      this.upperSwitchThreshold = upperSwitchThreshold;
      this.lowerSwitchThreshold = lowerSwitchThreshold;
      this.modeTransitionCount = modeTransitionCount;
      this.fullReclusterCount = fullReclusterCount;
      this.totalRoutedVectors = totalRoutedVectors;
      this.totalCellsEvaluated = totalCellsEvaluated;
      this.totalCellsPruned = totalCellsPruned;
      this.totalSubscriptionsEvaluated = totalSubscriptionsEvaluated;
      this.totalSubscriptionsPossible = totalSubscriptionsPossible;
      this.fnrVerificationRuns = fnrVerificationRuns;
      this.fnrVerificationFalseNegatives = fnrVerificationFalseNegatives;
      this.observedSurvivalEwma = observedSurvivalEwma;
      this.survivalGuardTripped = survivalGuardTripped;
      this.survivalGuardReversionCount = survivalGuardReversionCount;
    }

    public RoutingMode getCurrentMode() {
      return currentMode;
    }

    public int getSubscriptionCount() {
      return subscriptionCount;
    }

    public int getActiveClusterCount() {
      return activeClusterCount;
    }

    public int getMCrit() {
      return mCrit;
    }

    public int getUpperSwitchThreshold() {
      return upperSwitchThreshold;
    }

    public int getLowerSwitchThreshold() {
      return lowerSwitchThreshold;
    }

    public long getModeTransitionCount() {
      return modeTransitionCount;
    }

    public long getFullReclusterCount() {
      return fullReclusterCount;
    }

    public long getTotalRoutedVectors() {
      return totalRoutedVectors;
    }

    public long getTotalCellsEvaluated() {
      return totalCellsEvaluated;
    }

    public long getTotalCellsPruned() {
      return totalCellsPruned;
    }

    public double getPruneRatio() {
      return totalCellsEvaluated == 0L ? 0.0d
          : (double) totalCellsPruned / (double) totalCellsEvaluated;
    }

    public double getObservedSurvivalFraction() {
      return totalSubscriptionsPossible == 0L ? 0.0d
          : (double) totalSubscriptionsEvaluated / (double) totalSubscriptionsPossible;
    }

    public double getObservedSurvivalEwma() {
      return observedSurvivalEwma;
    }

    public boolean isSurvivalGuardTripped() {
      return survivalGuardTripped;
    }

    public long getSurvivalGuardReversionCount() {
      return survivalGuardReversionCount;
    }

    public long getFnrVerificationRuns() {
      return fnrVerificationRuns;
    }

    public long getFnrVerificationFalseNegatives() {
      return fnrVerificationFalseNegatives;
    }
  }

  /** Result of comparing routed matches against an exact brute-force ground-truth scan. */
  public static final class FnrVerificationResult {
    private final int routedMatchCount;
    private final int bruteForceMatchCount;
    private final int falseNegatives;
    private final int falsePositives;

    FnrVerificationResult(int routedMatchCount, int bruteForceMatchCount, int falseNegatives,
        int falsePositives) {
      this.routedMatchCount = routedMatchCount;
      this.bruteForceMatchCount = bruteForceMatchCount;
      this.falseNegatives = falseNegatives;
      this.falsePositives = falsePositives;
    }

    public int getRoutedMatchCount() {
      return routedMatchCount;
    }

    public int getBruteForceMatchCount() {
      return bruteForceMatchCount;
    }

    public int getFalseNegatives() {
      return falseNegatives;
    }

    public int getFalsePositives() {
      return falsePositives;
    }

    public boolean isZeroFalseNegatives() {
      return falseNegatives == 0;
    }

    public boolean isExactMatch() {
      return falseNegatives == 0 && falsePositives == 0;
    }
  }

  /** Immutable Voronoi cell {@code V_k} with contiguous flat member layout. */
  static final class VoronoiCell {
    final float[] centroid;
    final double coveringRadius; // R_k = max_i ||q_i - c_k||_2
    final double maxChordalTolerance; // rho_k^max = max_i rho_i^up
    final double coupledBound; // max_i (||q_i - c_k||_2 + rho_i^up)
    final double effectiveUpperBound; // B_k^up (clamped to [0, 2])
    final double minCosineThreshold; // s_k^min = 1 - (B_k^up)^2 / 2 - epsilon_dot(d), or -Infinity
    final String[] memberIds;
    final float[] memberVectors; // contiguous: memberCount * dim
    final float[] memberThresholds;

    VoronoiCell(float[] centroid, double coveringRadius, double maxChordalTolerance,
        double coupledBound, boolean useCoupledBound, double epsilonDot, double normMargin,
        String[] memberIds, float[] memberVectors, float[] memberThresholds) {
      this.centroid = centroid;
      this.coveringRadius = coveringRadius;
      this.maxChordalTolerance = maxChordalTolerance;
      this.coupledBound = coupledBound;
      double rawBound =
          (useCoupledBound ? coupledBound : (coveringRadius + maxChordalTolerance)) + normMargin;
      this.effectiveUpperBound = Math.min(2.0d, Math.max(0.0d, rawBound));
      this.minCosineThreshold = (this.effectiveUpperBound >= 2.0d)
          ? Double.NEGATIVE_INFINITY
          : (1.0d - 0.5d * this.effectiveUpperBound * this.effectiveUpperBound - epsilonDot);
      this.memberIds = memberIds;
      this.memberVectors = memberVectors;
      this.memberThresholds = memberThresholds;
    }

    int size() {
      return memberIds.length;
    }
  }

  /** Immutable lock-free snapshot used by the hot routing path. */
  private static final class RouterSnapshot {
    final RoutingMode mode;
    final int dimension;
    final int subscriptionCount;
    final String[] flatIds;
    final float[] flatVectors; // contiguous: subscriptionCount * dim
    final float[] flatThresholds;
    final VoronoiCell[] cells;
    final float[] centroidMatrix; // contiguous: cells.length * dim

    RouterSnapshot(RoutingMode mode, int dimension, String[] flatIds, float[] flatVectors,
        float[] flatThresholds, VoronoiCell[] cells, float[] centroidMatrix) {
      this.mode = mode;
      this.dimension = dimension;
      this.subscriptionCount = flatIds.length;
      this.flatIds = flatIds;
      this.flatVectors = flatVectors;
      this.flatThresholds = flatThresholds;
      this.cells = cells;
      this.centroidMatrix = centroidMatrix;
    }

    static RouterSnapshot empty(int dimension) {
      return new RouterSnapshot(RoutingMode.FLAT_SIMD, dimension, new String[0], new float[0],
          new float[0], new VoronoiCell[0], new float[0]);
    }
  }

  private final RouterConfig config;
  private final int mCrit;
  private final int upperSwitchThreshold;
  private final int lowerSwitchThreshold;
  private final double floatDotSafetyEpsilon;
  private final double highamDotEpsilon;
  private final double normMargin;

  private final ReentrantLock writeLock = new ReentrantLock();
  private final Map<String, VectorSubscription> subscriptions = new LinkedHashMap<>();
  private int churnSinceLastCluster = 0;

  private volatile RouterSnapshot snapshot;

  private final AtomicLong modeTransitionCount = new AtomicLong();
  private final AtomicLong fullReclusterCount = new AtomicLong();
  private final AtomicLong totalRoutedVectors = new AtomicLong();
  private final AtomicLong totalCellsEvaluated = new AtomicLong();
  private final AtomicLong totalCellsPruned = new AtomicLong();
  private final AtomicLong totalSubscriptionsEvaluated = new AtomicLong();
  private final AtomicLong totalSubscriptionsPossible = new AtomicLong();
  private final AtomicLong fnrVerificationRuns = new AtomicLong();
  private final AtomicLong fnrVerificationFalseNegatives = new AtomicLong();
  private final AtomicLong survivalSampleCount = new AtomicLong();
  private final AtomicLong observedSurvivalEwmaBits =
      new AtomicLong(Double.doubleToRawLongBits(Double.NaN));
  private volatile boolean survivalGuardTripped = false;
  private final AtomicLong survivalGuardReversionCount = new AtomicLong();

  public ChordalVoronoiRouter(int dimension) {
    this(RouterConfig.builder(dimension).build());
  }

  public ChordalVoronoiRouter(RouterConfig config) {
    this.config = Objects.requireNonNull(config, "config must not be null");
    this.mCrit = computeMCrit(config.getDimension(), config.getNumClusters(),
        config.getExpectedSurvivalFraction(), config.getSimdLaneWidth(), config.getSimdFmaCost(),
        config.getBoundCheckCost(), config.getMaxVoronoiSurvivalRate());
    this.upperSwitchThreshold =
        computeUpperSwitchThreshold(this.mCrit, config.getHysteresisDelta());
    this.lowerSwitchThreshold =
        computeLowerSwitchThreshold(this.mCrit, config.getHysteresisDelta());
    this.floatDotSafetyEpsilon = computeFloatDotSafetyEpsilon(config.getDimension());
    this.highamDotEpsilon = computeHighamDotEpsilon(config.getDimension());
    this.normMargin = computeNormMargin(config.getDimension());
    this.snapshot = RouterSnapshot.empty(config.getDimension());
  }

  /**
   * Computes Higham's IEEE-754 Theorem 3.1 inner-product relative rounding factor
   * {@code gamma_d = (d * u_f32) / (1 - d * u_f32)} for float32 unit roundoff
   * {@code u_f32 = 2^-24}.
   */
  public static double computeHighamGamma(int dimension) {
    if (dimension <= 0) {
      throw new IllegalArgumentException("dimension must be > 0, got: " + dimension);
    }
    double du = dimension * FLOAT32_UNIT_ROUNDOFF;
    if (du >= 0.5d) {
      throw new IllegalArgumentException(
          "dimension exceeds float32 Higham stability domain (d * u_f32 >= 0.5): " + dimension);
    }
    return du / (1.0d - du);
  }

  /**
   * Computes the sqrt-free cosine pruning error margin {@code epsilon_dot(d)} strictly dominating
   * worst-case Higham float32 dot-product rounding and unit-normalization error on {@code S^{d-1}}.
   */
  public static double computeHighamDotEpsilon(int dimension) {
    double gamma = computeHighamGamma(dimension);
    return 2.0d * gamma + HIGHAM_DOT_SAFETY_FLOOR;
  }

  /**
   * Computes the conservative float64 Euclidean norm margin {@code delta_norm(d)} added to the
   * precomputed cell chordal upper bound {@code B_k^up}.
   */
  public static double computeNormMargin(int dimension) {
    if (dimension <= 0) {
      throw new IllegalArgumentException("dimension must be > 0, got: " + dimension);
    }
    return 4.0d * FLOAT32_UNIT_ROUNDOFF + 1.0e-9d;
  }

  /**
   * Computes a conservative upper bound on {@code ||x - q_i||_2} whenever float32 matching reports
   * {@code dot_hat(x, q_i) >= tau_i}, preventing square-root singularity even at {@code tau_i = 1}.
   */
  static double computeSafeChordalTolerance(float similarityThreshold, double epsilonDot) {
    double radicand = 2.0d * (1.0d - (double) similarityThreshold) + 2.0d * epsilonDot;
    return Math.sqrt(Math.max(0.0d, radicand));
  }

  /**
   * Computes a dimension-scaled IEEE-754 float32 dot-product safety margin covering worst-case
   * {@code O(d * u_f32)} rounding error even at high dimensionality ({@code d >= 4096}).
   */
  public static double computeFloatDotSafetyEpsilon(int dimension) {
    if (dimension <= 0) {
      throw new IllegalArgumentException("dimension must be > 0, got: " + dimension);
    }
    return Math.max(FLOAT_DOT_SAFETY_EPSILON, dimension * FLOAT_DOT_SAFETY_EPSILON_PER_DIM);
  }

  public double getFloatDotSafetyEpsilon() {
    return floatDotSafetyEpsilon;
  }

  public double getHighamDotEpsilon() {
    return highamDotEpsilon;
  }

  /**
   * Computes Proposition 1's normalized scalar-to-SIMD cost ratio
   * {@code eta_d = (c_bound * W) / (c_simd * d)}.
   */
  public static double computeEta(int dimension, int simdLaneWidth, double simdFmaCost,
      double boundCheckCost) {
    if (dimension <= 0 || simdLaneWidth <= 0 || simdFmaCost <= 0.0d || boundCheckCost < 0.0d) {
      throw new IllegalArgumentException("Invalid cost model parameters");
    }
    return (boundCheckCost * simdLaneWidth) / (simdFmaCost * dimension);
  }

  /**
   * Computes Proposition 1's closed-form crossover threshold
   * {@code M_crit(d, K, p_bar) = ceil((K * (1 + eta_d)) / (1 - p_bar))} using the default
   * survival-rate ceiling {@link #DEFAULT_MAX_VORONOI_SURVIVAL_RATE}. Returns
   * {@link Integer#MAX_VALUE} whenever {@code expectedSurvivalFraction >= 1.0} or
   * {@code expectedSurvivalFraction >= DEFAULT_MAX_VORONOI_SURVIVAL_RATE} (signaling that
   * {@link RoutingMode#FLAT_SIMD} dominates unconditionally).
   */
  public static int computeMCrit(int dimension, int numClusters, double expectedSurvivalFraction,
      int simdLaneWidth, double simdFmaCost, double boundCheckCost) {
    return computeMCrit(dimension, numClusters, expectedSurvivalFraction, simdLaneWidth,
        simdFmaCost, boundCheckCost, DEFAULT_MAX_VORONOI_SURVIVAL_RATE);
  }

  /**
   * Computes Proposition 1's closed-form crossover threshold
   * {@code M_crit(d, K, p_bar) = ceil((K * (1 + eta_d)) / (1 - p_bar))}. Returns
   * {@link Integer#MAX_VALUE} whenever {@code expectedSurvivalFraction >= 1.0} or
   * {@code expectedSurvivalFraction >= maxVoronoiSurvivalRate}, avoiding division-by-zero and
   * ensuring isotropic distributions stay in {@link RoutingMode#FLAT_SIMD}.
   */
  public static int computeMCrit(int dimension, int numClusters, double expectedSurvivalFraction,
      int simdLaneWidth, double simdFmaCost, double boundCheckCost,
      double maxVoronoiSurvivalRate) {
    if (numClusters <= 0) {
      throw new IllegalArgumentException("numClusters must be > 0");
    }
    if (!Double.isFinite(expectedSurvivalFraction) || expectedSurvivalFraction < 0.0d) {
      throw new IllegalArgumentException(
          "expectedSurvivalFraction must be finite and >= 0, got: " + expectedSurvivalFraction);
    }
    if (!Double.isFinite(maxVoronoiSurvivalRate) || maxVoronoiSurvivalRate <= 0.0d
        || maxVoronoiSurvivalRate > 1.0d) {
      throw new IllegalArgumentException(
          "maxVoronoiSurvivalRate must be in (0, 1], got: " + maxVoronoiSurvivalRate);
    }
    double eta = computeEta(dimension, simdLaneWidth, simdFmaCost, boundCheckCost);
    if (expectedSurvivalFraction >= 1.0d || expectedSurvivalFraction >= maxVoronoiSurvivalRate) {
      return Integer.MAX_VALUE;
    }
    double raw = (numClusters * (1.0d + eta)) / (1.0d - expectedSurvivalFraction);
    if (!Double.isFinite(raw) || raw >= Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }
    return Math.max(1, (int) Math.ceil(raw));
  }

  /** Computes upper hysteresis threshold {@code M_up = ceil((1 + delta) * M_crit)}. */
  public static int computeUpperSwitchThreshold(int mCrit, double delta) {
    if (mCrit == Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }
    double raw = (1.0d + delta) * mCrit;
    if (!Double.isFinite(raw) || raw >= Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }
    return Math.max(1, (int) Math.ceil(raw));
  }

  /** Computes lower hysteresis threshold {@code M_down = floor((1 - delta) * M_crit)}. */
  public static int computeLowerSwitchThreshold(int mCrit, double delta) {
    if (mCrit == Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }
    return Math.max(0, (int) Math.floor((1.0d - delta) * mCrit));
  }

  public RouterConfig getConfig() {
    return config;
  }

  public int getDimension() {
    return config.getDimension();
  }

  public int getMCrit() {
    return mCrit;
  }

  public int getUpperSwitchThreshold() {
    return upperSwitchThreshold;
  }

  public int getLowerSwitchThreshold() {
    return lowerSwitchThreshold;
  }

  public RoutingMode getCurrentMode() {
    return snapshot.mode;
  }

  public int getSubscriptionCount() {
    return snapshot.subscriptionCount;
  }

  /**
   * Adds or replaces a continuous vector subscription and updates the routing mode under
   * Proposition 1's dual-threshold hysteresis.
   */
  public void addSubscription(VectorSubscription subscription) {
    Objects.requireNonNull(subscription, "subscription must not be null");
    validateDimension(subscription.getDimension());
    writeLock.lock();
    try {
      boolean existed = subscriptions.put(subscription.getSubscriptionId(), subscription) != null;
      churnSinceLastCluster++;
      RoutingMode prevMode = snapshot.mode;
      RoutingMode nextMode = evaluateModeTransition(prevMode, subscriptions.size());
      if (nextMode != prevMode) {
        modeTransitionCount.incrementAndGet();
        rebuildFullSnapshot(nextMode);
      } else if (nextMode == RoutingMode.CHORDAL_VORONOI && !existed
          && snapshot.cells.length > 0 && !shouldRecluster()) {
        incrementallyInsertIntoVoronoi(subscription);
      } else {
        rebuildFullSnapshot(nextMode);
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Registers a batch of subscriptions atomically, rebuilding the index once.
   */
  public void addSubscriptions(Collection<VectorSubscription> batch) {
    Objects.requireNonNull(batch, "batch must not be null");
    if (batch.isEmpty()) {
      return;
    }
    for (VectorSubscription sub : batch) {
      Objects.requireNonNull(sub, "subscription in batch must not be null");
      validateDimension(sub.getDimension());
    }
    writeLock.lock();
    try {
      for (VectorSubscription sub : batch) {
        subscriptions.put(sub.getSubscriptionId(), sub);
      }
      RoutingMode prevMode = snapshot.mode;
      RoutingMode nextMode = evaluateModeTransition(prevMode, subscriptions.size());
      if (nextMode != prevMode) {
        modeTransitionCount.incrementAndGet();
      }
      rebuildFullSnapshot(nextMode);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Removes an active subscription by ID and updates routing mode under hysteresis.
   *
   * @return {@code true} if the subscription existed and was removed.
   */
  public boolean removeSubscription(String subscriptionId) {
    if (subscriptionId == null) {
      return false;
    }
    writeLock.lock();
    try {
      if (subscriptions.remove(subscriptionId) == null) {
        return false;
      }
      churnSinceLastCluster++;
      RoutingMode prevMode = snapshot.mode;
      RoutingMode nextMode = evaluateModeTransition(prevMode, subscriptions.size());
      if (nextMode != prevMode) {
        modeTransitionCount.incrementAndGet();
        rebuildFullSnapshot(nextMode);
      } else if (nextMode == RoutingMode.CHORDAL_VORONOI
          && snapshot.cells.length > 0 && !shouldRecluster()) {
        incrementallyRemoveFromVoronoi(subscriptionId);
      } else {
        rebuildFullSnapshot(nextMode);
      }
      return true;
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Explicitly triggers full spherical k-means reclustering when in
   * {@link RoutingMode#CHORDAL_VORONOI} mode, or re-evaluates mode eligibility if the online
   * survival-rate guard had previously reverted the router to {@link RoutingMode#FLAT_SIMD}.
   */
  public void recluster() {
    writeLock.lock();
    try {
      boolean wasTripped = survivalGuardTripped;
      resetSurvivalGuardLocked();
      RoutingMode target =
          wasTripped ? evaluateModeTransition(snapshot.mode, subscriptions.size()) : snapshot.mode;
      if (target != snapshot.mode) {
        modeTransitionCount.incrementAndGet();
      }
      rebuildFullSnapshot(target);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Resets the online survival-rate EWMA tracker and clears any tripped survival-rate guard so the
   * router may re-evaluate {@link RoutingMode#CHORDAL_VORONOI} eligibility on future changes or
   * {@link #recluster()}.
   */
  public void resetSurvivalGuard() {
    writeLock.lock();
    try {
      resetSurvivalGuardLocked();
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Returns the online EWMA estimate {@code p_bar_hat} of the Voronoi subscription survival rate,
   * or {@code 0.0} if no Voronoi routes have been observed yet.
   */
  public double getObservedSurvivalRate() {
    double val = Double.longBitsToDouble(observedSurvivalEwmaBits.get());
    return Double.isNaN(val) ? 0.0d : val;
  }

  /**
   * Returns {@code true} if the online survival-rate guard observed sustained
   * {@code p_bar_hat >= maxVoronoiSurvivalRate} and automatically reverted routing to
   * {@link RoutingMode#FLAT_SIMD}.
   */
  public boolean isSurvivalGuardTripped() {
    return survivalGuardTripped;
  }

  /**
   * Returns the effective analytical crossover threshold: {@link Integer#MAX_VALUE} when the
   * online survival-rate guard is tripped, otherwise the configured {@link #getMCrit()}.
   */
  public int getEffectiveMCrit() {
    return survivalGuardTripped ? Integer.MAX_VALUE : mCrit;
  }

  /**
   * Routes an incoming embedding (automatically L2-normalized onto {@code S^{d-1}}) against all
   * active subscriptions using the current adaptive mode.
   */
  public List<MatchResult> route(float[] embedding) {
    validateVector(embedding);
    float[] unit = VectorSubscription.normalize(embedding);
    return routeNormalized(unit);
  }

  /**
   * Lock-free hot-path routing for an already L2-normalized embedding on {@code S^{d-1}}.
   */
  public List<MatchResult> routeNormalized(float[] unitEmbedding) {
    validateFiniteVector(unitEmbedding);
    RouterSnapshot snap = this.snapshot;
    totalRoutedVectors.incrementAndGet();
    if (snap.subscriptionCount == 0) {
      return Collections.emptyList();
    }
    totalSubscriptionsPossible.addAndGet(snap.subscriptionCount);
    if (snap.mode == RoutingMode.FLAT_SIMD || snap.cells.length == 0) {
      return routeFlatSimd(unitEmbedding, snap);
    }
    return routeChordalVoronoi(unitEmbedding, snap);
  }

  /**
   * Exact brute-force scan across all active subscriptions for FNR ground-truth verification.
   */
  public List<MatchResult> routeBruteForce(float[] embedding) {
    validateVector(embedding);
    float[] unit = VectorSubscription.normalize(embedding);
    RouterSnapshot snap = this.snapshot;
    if (snap.subscriptionCount == 0) {
      return Collections.emptyList();
    }
    return scanFlat(unit, snap.flatIds, snap.flatVectors, snap.flatThresholds, snap.dimension);
  }

  /**
   * Verification hook comparing adaptive routing output against exact brute-force ground truth for
   * Theorem 1 zero-false-negative validation.
   */
  public FnrVerificationResult verifyAgainstBruteForce(float[] embedding) {
    validateVector(embedding);
    float[] unit = VectorSubscription.normalize(embedding);
    RouterSnapshot snap = this.snapshot;
    List<MatchResult> routed = (snap.mode == RoutingMode.FLAT_SIMD || snap.cells.length == 0)
        ? routeFlatSimd(unit, snap)
        : routeChordalVoronoi(unit, snap);
    List<MatchResult> bruteForce =
        scanFlat(unit, snap.flatIds, snap.flatVectors, snap.flatThresholds, snap.dimension);

    Map<String, Float> routedMap = new HashMap<>(routed.size() * 2);
    for (MatchResult r : routed) {
      routedMap.put(r.getSubscriptionId(), r.getSimilarityScore());
    }
    Map<String, Float> bruteMap = new HashMap<>(bruteForce.size() * 2);
    for (MatchResult b : bruteForce) {
      bruteMap.put(b.getSubscriptionId(), b.getSimilarityScore());
    }

    int falseNegatives = 0;
    for (String id : bruteMap.keySet()) {
      if (!routedMap.containsKey(id)) {
        falseNegatives++;
      }
    }
    int falsePositives = 0;
    for (String id : routedMap.keySet()) {
      if (!bruteMap.containsKey(id)) {
        falsePositives++;
      }
    }
    fnrVerificationRuns.incrementAndGet();
    if (falseNegatives > 0) {
      fnrVerificationFalseNegatives.addAndGet(falseNegatives);
    }
    return new FnrVerificationResult(routed.size(), bruteForce.size(), falseNegatives,
        falsePositives);
  }

  public RouterTelemetry getTelemetry() {
    RouterSnapshot snap = this.snapshot;
    return new RouterTelemetry(snap.mode, snap.subscriptionCount, snap.cells.length,
        getEffectiveMCrit(), upperSwitchThreshold, lowerSwitchThreshold, modeTransitionCount.get(),
        fullReclusterCount.get(), totalRoutedVectors.get(), totalCellsEvaluated.get(),
        totalCellsPruned.get(), totalSubscriptionsEvaluated.get(),
        totalSubscriptionsPossible.get(), fnrVerificationRuns.get(),
        fnrVerificationFalseNegatives.get(), getObservedSurvivalRate(), survivalGuardTripped,
        survivalGuardReversionCount.get());
  }

  private List<MatchResult> routeFlatSimd(float[] unitEmbedding, RouterSnapshot snap) {
    totalSubscriptionsEvaluated.addAndGet(snap.subscriptionCount);
    return scanFlat(unitEmbedding, snap.flatIds, snap.flatVectors, snap.flatThresholds,
        snap.dimension);
  }

  private List<MatchResult> routeChordalVoronoi(float[] unitEmbedding, RouterSnapshot snap) {
    VoronoiCell[] cells = snap.cells;
    float[] centroids = snap.centroidMatrix;
    int dim = snap.dimension;
    int numCells = cells.length;

    List<MatchResult> matches = null;
    long pruned = 0L;
    long subsEvaluated = 0L;

    // Stage 1: Evaluate K Voronoi centroids and apply sqrt-free Theorem 1 inner-product pruning
    for (int k = 0; k < numCells; k++) {
      VoronoiCell cell = cells[k];
      float rawDot =
          VectorSubscription.dotProductContiguous(unitEmbedding, centroids, k * dim, dim);
      // Exact sqrt-free Higham cosine bound: prune iff s_hat_k < s_k^min (no Math.sqrt on hot path)
      if ((double) rawDot < cell.minCosineThreshold) {
        pruned++;
        continue;
      }

      // Stage 2: Exact contiguous scan inside unpruned cell V_k
      String[] ids = cell.memberIds;
      float[] vecs = cell.memberVectors;
      float[] thresholds = cell.memberThresholds;
      int cellSize = ids.length;
      subsEvaluated += cellSize;

      for (int j = 0; j < cellSize; j++) {
        float sim = VectorSubscription.dotProductContiguous(unitEmbedding, vecs, j * dim, dim);
        if (sim >= thresholds[j]) {
          if (matches == null) {
            matches = new ArrayList<>(4);
          }
          matches.add(new MatchResult(ids[j], sim));
        }
      }
    }

    totalCellsEvaluated.addAndGet(numCells);
    if (pruned > 0L) {
      totalCellsPruned.addAndGet(pruned);
    }
    if (subsEvaluated > 0L) {
      totalSubscriptionsEvaluated.addAndGet(subsEvaluated);
    }
    if (snap.subscriptionCount > 0) {
      double sampleSurvival = (double) subsEvaluated / (double) snap.subscriptionCount;
      recordSurvivalObservation(sampleSurvival);
    }
    return matches == null ? Collections.emptyList() : matches;
  }

  private void recordSurvivalObservation(double sampleSurvival) {
    long samples = survivalSampleCount.incrementAndGet();
    double updatedEwma;
    while (true) {
      long prevBits = observedSurvivalEwmaBits.get();
      double prev = Double.longBitsToDouble(prevBits);
      updatedEwma = Double.isNaN(prev)
          ? sampleSurvival
          : (DEFAULT_SURVIVAL_EWMA_ALPHA * sampleSurvival
              + (1.0d - DEFAULT_SURVIVAL_EWMA_ALPHA) * prev);
      if (observedSurvivalEwmaBits.compareAndSet(prevBits,
          Double.doubleToRawLongBits(updatedEwma))) {
        break;
      }
    }
    if (config.isEnableSurvivalGuard() && !survivalGuardTripped
        && samples >= config.getSurvivalGuardMinSamples()
        && updatedEwma >= config.getMaxVoronoiSurvivalRate()) {
      tripSurvivalGuardToFlatSimd();
    }
  }

  private void tripSurvivalGuardToFlatSimd() {
    if (!writeLock.tryLock()) {
      return;
    }
    try {
      RouterSnapshot cur = this.snapshot;
      if (survivalGuardTripped || cur.mode != RoutingMode.CHORDAL_VORONOI) {
        return;
      }
      survivalGuardTripped = true;
      survivalGuardReversionCount.incrementAndGet();
      modeTransitionCount.incrementAndGet();
      // O(1) zero-copy downgrade to FLAT_SIMD reusing existing contiguous flat subscription arrays
      this.snapshot = new RouterSnapshot(RoutingMode.FLAT_SIMD, cur.dimension, cur.flatIds,
          cur.flatVectors, cur.flatThresholds, new VoronoiCell[0], new float[0]);
    } finally {
      writeLock.unlock();
    }
  }

  private void resetSurvivalGuardLocked() {
    survivalGuardTripped = false;
    survivalSampleCount.set(0L);
    observedSurvivalEwmaBits.set(Double.doubleToRawLongBits(Double.NaN));
  }

  private static List<MatchResult> scanFlat(float[] unitEmbedding, String[] ids, float[] vectors,
      float[] thresholds, int dim) {
    List<MatchResult> matches = null;
    for (int m = 0; m < ids.length; m++) {
      float sim = VectorSubscription.dotProductContiguous(unitEmbedding, vectors, m * dim, dim);
      if (sim >= thresholds[m]) {
        if (matches == null) {
          matches = new ArrayList<>(4);
        }
        matches.add(new MatchResult(ids[m], sim));
      }
    }
    return matches == null ? Collections.emptyList() : matches;
  }

  private RoutingMode evaluateModeTransition(RoutingMode currentMode, int subscriptionCount) {
    if (survivalGuardTripped || mCrit == Integer.MAX_VALUE) {
      return RoutingMode.FLAT_SIMD;
    }
    if (currentMode == RoutingMode.FLAT_SIMD) {
      if (subscriptionCount >= upperSwitchThreshold && subscriptionCount >= 2) {
        return RoutingMode.CHORDAL_VORONOI;
      }
      return RoutingMode.FLAT_SIMD;
    } else {
      if (subscriptionCount <= lowerSwitchThreshold || subscriptionCount < 2) {
        return RoutingMode.FLAT_SIMD;
      }
      return RoutingMode.CHORDAL_VORONOI;
    }
  }

  private boolean shouldRecluster() {
    int count = subscriptions.size();
    return count > 0 && churnSinceLastCluster >= Math.max(4, (int) (count * RECLUSTER_CHURN_RATIO));
  }

  private void incrementallyInsertIntoVoronoi(VectorSubscription sub) {
    RouterSnapshot prev = this.snapshot;
    int dim = config.getDimension();
    int newCount = prev.subscriptionCount + 1;

    String[] newFlatIds = Arrays.copyOf(prev.flatIds, newCount);
    newFlatIds[newCount - 1] = sub.getSubscriptionId();

    float[] newFlatVecs = Arrays.copyOf(prev.flatVectors, newCount * dim);
    System.arraycopy(sub.getNormalizedQueryVectorUnsafe(), 0, newFlatVecs, (newCount - 1) * dim,
        dim);

    float[] newFlatThresholds = Arrays.copyOf(prev.flatThresholds, newCount);
    newFlatThresholds[newCount - 1] = sub.getSimilarityThreshold();

    // Assign to nearest existing centroid and expand R_k, rho_k_max to preserve Theorem 1
    VoronoiCell[] prevCells = prev.cells;
    int bestCellIdx = 0;
    float bestDot = Float.NEGATIVE_INFINITY;
    float[] q = sub.getNormalizedQueryVectorUnsafe();
    for (int k = 0; k < prevCells.length; k++) {
      float dot = VectorSubscription.dotProductContiguous(q, prev.centroidMatrix, k * dim, dim);
      if (dot > bestDot) {
        bestDot = dot;
        bestCellIdx = k;
      }
    }

    VoronoiCell target = prevCells[bestCellIdx];
    double dist = VectorSubscription.euclideanChordalDistance(q, target.centroid);
    double rhoUp = computeSafeChordalTolerance(sub.getSimilarityThreshold(), highamDotEpsilon);
    double newRadius = Math.max(target.coveringRadius, dist);
    double newMaxTol = Math.max(target.maxChordalTolerance, rhoUp);
    double newCoupledBound = Math.max(target.coupledBound, dist + rhoUp);

    int newCellSize = target.size() + 1;
    String[] cellIds = Arrays.copyOf(target.memberIds, newCellSize);
    cellIds[newCellSize - 1] = sub.getSubscriptionId();
    float[] cellVecs = Arrays.copyOf(target.memberVectors, newCellSize * dim);
    System.arraycopy(q, 0, cellVecs, (newCellSize - 1) * dim, dim);
    float[] cellThresholds = Arrays.copyOf(target.memberThresholds, newCellSize);
    cellThresholds[newCellSize - 1] = sub.getSimilarityThreshold();

    VoronoiCell[] newCells = Arrays.copyOf(prevCells, prevCells.length);
    newCells[bestCellIdx] = new VoronoiCell(target.centroid, newRadius, newMaxTol, newCoupledBound,
        config.isUseCoupledBound(), highamDotEpsilon, normMargin, cellIds, cellVecs,
        cellThresholds);

    this.snapshot = new RouterSnapshot(RoutingMode.CHORDAL_VORONOI, dim, newFlatIds, newFlatVecs,
        newFlatThresholds, newCells, prev.centroidMatrix);
  }

  /**
   * Incrementally removes {@code subscriptionId} from its Voronoi cell in {@code O(|V_k| * d)}
   * without re-running spherical k-means. Because removing a member only shrinks the cell subset
   * {@code V_k' subset V_k}, retaining the cell's conservative upper bound {@code B_k^up} and
   * cosine threshold {@code s_k^min} strictly preserves Theorem 1 ({@code FNR == 0}) until the
   * periodic 20%-churn recluster compacts cell radii.
   */
  private void incrementallyRemoveFromVoronoi(String subscriptionId) {
    RouterSnapshot prev = this.snapshot;
    int dim = config.getDimension();
    int oldCount = prev.subscriptionCount;
    if (oldCount <= 1) {
      rebuildFullSnapshot(RoutingMode.CHORDAL_VORONOI);
      return;
    }

    int flatIdx = -1;
    for (int i = 0; i < oldCount; i++) {
      if (subscriptionId.equals(prev.flatIds[i])) {
        flatIdx = i;
        break;
      }
    }
    if (flatIdx < 0) {
      rebuildFullSnapshot(RoutingMode.CHORDAL_VORONOI);
      return;
    }

    VoronoiCell[] prevCells = prev.cells;
    int cellIdx = -1;
    int memberIdx = -1;
    outer: for (int k = 0; k < prevCells.length; k++) {
      String[] ids = prevCells[k].memberIds;
      for (int j = 0; j < ids.length; j++) {
        if (subscriptionId.equals(ids[j])) {
          cellIdx = k;
          memberIdx = j;
          break outer;
        }
      }
    }
    if (cellIdx < 0) {
      rebuildFullSnapshot(RoutingMode.CHORDAL_VORONOI);
      return;
    }

    int newCount = oldCount - 1;
    String[] newFlatIds = new String[newCount];
    float[] newFlatVecs = new float[newCount * dim];
    float[] newFlatThresholds = new float[newCount];
    if (flatIdx > 0) {
      System.arraycopy(prev.flatIds, 0, newFlatIds, 0, flatIdx);
      System.arraycopy(prev.flatVectors, 0, newFlatVecs, 0, flatIdx * dim);
      System.arraycopy(prev.flatThresholds, 0, newFlatThresholds, 0, flatIdx);
    }
    int tailFlat = oldCount - flatIdx - 1;
    if (tailFlat > 0) {
      System.arraycopy(prev.flatIds, flatIdx + 1, newFlatIds, flatIdx, tailFlat);
      System.arraycopy(prev.flatVectors, (flatIdx + 1) * dim, newFlatVecs, flatIdx * dim,
          tailFlat * dim);
      System.arraycopy(prev.flatThresholds, flatIdx + 1, newFlatThresholds, flatIdx, tailFlat);
    }

    VoronoiCell target = prevCells[cellIdx];
    VoronoiCell[] newCells;
    float[] newCentroidMatrix;
    if (target.size() == 1) {
      if (prevCells.length <= 1) {
        rebuildFullSnapshot(RoutingMode.CHORDAL_VORONOI);
        return;
      }
      int newNumCells = prevCells.length - 1;
      newCells = new VoronoiCell[newNumCells];
      newCentroidMatrix = new float[newNumCells * dim];
      if (cellIdx > 0) {
        System.arraycopy(prevCells, 0, newCells, 0, cellIdx);
        System.arraycopy(prev.centroidMatrix, 0, newCentroidMatrix, 0, cellIdx * dim);
      }
      int tailCells = prevCells.length - cellIdx - 1;
      if (tailCells > 0) {
        System.arraycopy(prevCells, cellIdx + 1, newCells, cellIdx, tailCells);
        System.arraycopy(prev.centroidMatrix, (cellIdx + 1) * dim, newCentroidMatrix,
            cellIdx * dim, tailCells * dim);
      }
    } else {
      int newCellSize = target.size() - 1;
      String[] cellIds = new String[newCellSize];
      float[] cellVecs = new float[newCellSize * dim];
      float[] cellThresholds = new float[newCellSize];
      if (memberIdx > 0) {
        System.arraycopy(target.memberIds, 0, cellIds, 0, memberIdx);
        System.arraycopy(target.memberVectors, 0, cellVecs, 0, memberIdx * dim);
        System.arraycopy(target.memberThresholds, 0, cellThresholds, 0, memberIdx);
      }
      int tailMembers = target.size() - memberIdx - 1;
      if (tailMembers > 0) {
        System.arraycopy(target.memberIds, memberIdx + 1, cellIds, memberIdx, tailMembers);
        System.arraycopy(target.memberVectors, (memberIdx + 1) * dim, cellVecs, memberIdx * dim,
            tailMembers * dim);
        System.arraycopy(target.memberThresholds, memberIdx + 1, cellThresholds, memberIdx,
            tailMembers);
      }
      newCells = Arrays.copyOf(prevCells, prevCells.length);
      newCells[cellIdx] = new VoronoiCell(target.centroid, target.coveringRadius,
          target.maxChordalTolerance, target.coupledBound, config.isUseCoupledBound(),
          highamDotEpsilon, normMargin, cellIds, cellVecs, cellThresholds);
      newCentroidMatrix = prev.centroidMatrix;
    }

    this.snapshot = new RouterSnapshot(RoutingMode.CHORDAL_VORONOI, dim, newFlatIds, newFlatVecs,
        newFlatThresholds, newCells, newCentroidMatrix);
  }

  private void rebuildFullSnapshot(RoutingMode targetMode) {
    int dim = config.getDimension();
    int count = subscriptions.size();
    if (count == 0) {
      churnSinceLastCluster = 0;
      this.snapshot = RouterSnapshot.empty(dim);
      return;
    }

    List<VectorSubscription> list = new ArrayList<>(subscriptions.values());
    String[] flatIds = new String[count];
    float[] flatVecs = new float[count * dim];
    float[] flatThresholds = new float[count];
    for (int i = 0; i < count; i++) {
      VectorSubscription sub = list.get(i);
      flatIds[i] = sub.getSubscriptionId();
      System.arraycopy(sub.getNormalizedQueryVectorUnsafe(), 0, flatVecs, i * dim, dim);
      flatThresholds[i] = sub.getSimilarityThreshold();
    }

    if (targetMode == RoutingMode.FLAT_SIMD) {
      this.snapshot = new RouterSnapshot(RoutingMode.FLAT_SIMD, dim, flatIds, flatVecs,
          flatThresholds, new VoronoiCell[0], new float[0]);
      return;
    }

    // Build K Voronoi cells using spherical k-means++ + Lloyd refinement on S^{d-1}
    fullReclusterCount.incrementAndGet();
    VoronoiCell[] cells = buildSphericalVoronoiCells(list, dim);
    float[] centroidMatrix = new float[cells.length * dim];
    for (int k = 0; k < cells.length; k++) {
      System.arraycopy(cells[k].centroid, 0, centroidMatrix, k * dim, dim);
    }
    churnSinceLastCluster = 0;
    this.snapshot = new RouterSnapshot(RoutingMode.CHORDAL_VORONOI, dim, flatIds, flatVecs,
        flatThresholds, cells, centroidMatrix);
  }

  /**
   * Partitions subscriptions into {@code K_eff = min(K, M)} spherical Voronoi cells via k-means++
   * seeding and Lloyd iterations on {@code S^{d-1}}.
   */
  private VoronoiCell[] buildSphericalVoronoiCells(List<VectorSubscription> subs, int dim) {
    int m = subs.size();
    int kEff = Math.min(config.getNumClusters(), m);
    Random rng = new Random(config.getRandomSeed() ^ (31L * m));

    float[][] centroids = new float[kEff][dim];
    // Spherical k-means++ initialization using squared chordal distance d_ch^2 = 2(1 - cos)
    int firstIdx = rng.nextInt(m);
    System.arraycopy(subs.get(firstIdx).getNormalizedQueryVectorUnsafe(), 0, centroids[0], 0, dim);

    double[] minDistSq = new double[m];
    Arrays.fill(minDistSq, Double.POSITIVE_INFINITY);

    for (int c = 1; c < kEff; c++) {
      double totalWeight = 0.0d;
      for (int i = 0; i < m; i++) {
        float[] q = subs.get(i).getNormalizedQueryVectorUnsafe();
        float dot = VectorSubscription.dotProductContiguous(q, centroids[c - 1], 0, dim);
        double dSq = Math.max(0.0d, 2.0d * (1.0d - Math.min(1.0d, Math.max(-1.0d, (double) dot))));
        if (dSq < minDistSq[i]) {
          minDistSq[i] = dSq;
        }
        totalWeight += minDistSq[i];
      }

      int chosen = 0;
      if (totalWeight <= 1e-12d) {
        chosen = rng.nextInt(m);
      } else {
        double target = rng.nextDouble() * totalWeight;
        double cumulative = 0.0d;
        chosen = m - 1;
        for (int i = 0; i < m; i++) {
          cumulative += minDistSq[i];
          if (cumulative >= target) {
            chosen = i;
            break;
          }
        }
      }
      System.arraycopy(subs.get(chosen).getNormalizedQueryVectorUnsafe(), 0, centroids[c], 0, dim);
    }

    int[] assignments = new int[m];
    Arrays.fill(assignments, -1);

    // Lloyd spherical k-means refinement
    double[][] sums = new double[kEff][dim];
    int[] counts = new int[kEff];

    for (int iter = 0; iter < config.getMaxKMeansIterations(); iter++) {
      boolean changed = false;
      for (int i = 0; i < m; i++) {
        float[] q = subs.get(i).getNormalizedQueryVectorUnsafe();
        int bestK = 0;
        float bestDot = Float.NEGATIVE_INFINITY;
        for (int k = 0; k < kEff; k++) {
          float dot = VectorSubscription.dotProductContiguous(q, centroids[k], 0, dim);
          if (dot > bestDot) {
            bestDot = dot;
            bestK = k;
          }
        }
        if (assignments[i] != bestK) {
          assignments[i] = bestK;
          changed = true;
        }
      }

      if (!changed && iter > 0) {
        break;
      }

      for (int k = 0; k < kEff; k++) {
        Arrays.fill(sums[k], 0.0d);
        counts[k] = 0;
      }
      for (int i = 0; i < m; i++) {
        int cluster = assignments[i];
        counts[cluster]++;
        float[] q = subs.get(i).getNormalizedQueryVectorUnsafe();
        double[] sum = sums[cluster];
        for (int d = 0; d < dim; d++) {
          sum[d] += q[d];
        }
      }

      for (int k = 0; k < kEff; k++) {
        if (counts[k] == 0) {
          // Re-seed empty cluster from point farthest from its assigned centroid
          int farthestIdx = findFarthestPoint(subs, assignments, centroids, dim);
          System.arraycopy(subs.get(farthestIdx).getNormalizedQueryVectorUnsafe(), 0, centroids[k],
              0, dim);
        } else {
          double normSq = 0.0d;
          double[] sum = sums[k];
          for (int d = 0; d < dim; d++) {
            normSq += sum[d] * sum[d];
          }
          if (normSq > 1e-18d) {
            double invNorm = 1.0d / Math.sqrt(normSq);
            for (int d = 0; d < dim; d++) {
              centroids[k][d] = (float) (sum[d] * invNorm);
            }
          }
        }
      }
    }

    // Final assignment pass against finalized centroids
    List<List<VectorSubscription>> buckets = new ArrayList<>(kEff);
    for (int k = 0; k < kEff; k++) {
      buckets.add(new ArrayList<>());
    }
    for (int i = 0; i < m; i++) {
      float[] q = subs.get(i).getNormalizedQueryVectorUnsafe();
      int bestK = 0;
      float bestDot = Float.NEGATIVE_INFINITY;
      for (int k = 0; k < kEff; k++) {
        float dot = VectorSubscription.dotProductContiguous(q, centroids[k], 0, dim);
        if (dot > bestDot) {
          bestDot = dot;
          bestK = k;
        }
      }
      buckets.get(bestK).add(subs.get(i));
    }

    List<VoronoiCell> nonEmptyCells = new ArrayList<>(kEff);
    for (int k = 0; k < kEff; k++) {
      List<VectorSubscription> members = buckets.get(k);
      if (members.isEmpty()) {
        continue;
      }
      int size = members.size();
      String[] memberIds = new String[size];
      float[] memberVectors = new float[size * dim];
      float[] memberThresholds = new float[size];
      double maxRadius = 0.0d;
      double maxTolerance = 0.0d;
      double maxCoupled = 0.0d;

      for (int j = 0; j < size; j++) {
        VectorSubscription sub = members.get(j);
        float[] q = sub.getNormalizedQueryVectorUnsafe();
        memberIds[j] = sub.getSubscriptionId();
        System.arraycopy(q, 0, memberVectors, j * dim, dim);
        memberThresholds[j] = sub.getSimilarityThreshold();

        double dist = VectorSubscription.euclideanChordalDistance(q, centroids[k]);
        double rhoUp = computeSafeChordalTolerance(sub.getSimilarityThreshold(), highamDotEpsilon);
        if (dist > maxRadius) {
          maxRadius = dist;
        }
        if (rhoUp > maxTolerance) {
          maxTolerance = rhoUp;
        }
        double coupled = dist + rhoUp;
        if (coupled > maxCoupled) {
          maxCoupled = coupled;
        }
      }
      nonEmptyCells.add(new VoronoiCell(centroids[k], maxRadius, maxTolerance, maxCoupled,
          config.isUseCoupledBound(), highamDotEpsilon, normMargin, memberIds, memberVectors,
          memberThresholds));
    }
    return nonEmptyCells.toArray(new VoronoiCell[0]);
  }

  private static int findFarthestPoint(List<VectorSubscription> subs, int[] assignments,
      float[][] centroids, int dim) {
    int farthest = 0;
    float worstDot = Float.POSITIVE_INFINITY;
    for (int i = 0; i < subs.size(); i++) {
      int c = Math.max(0, assignments[i]);
      float dot = VectorSubscription.dotProductContiguous(
          subs.get(i).getNormalizedQueryVectorUnsafe(), centroids[c], 0, dim);
      if (dot < worstDot) {
        worstDot = dot;
        farthest = i;
      }
    }
    return farthest;
  }

  private void validateDimension(int dim) {
    if (dim != config.getDimension()) {
      throw new IllegalArgumentException(
          "Vector dimension mismatch: expected " + config.getDimension() + ", got " + dim);
    }
  }

  private void validateVector(float[] vector) {
    if (vector == null) {
      throw new IllegalArgumentException("Embedding vector must not be null");
    }
    validateDimension(vector.length);
  }

  private void validateFiniteVector(float[] vector) {
    validateVector(vector);
    for (int i = 0; i < vector.length; i++) {
      if (!Float.isFinite(vector[i])) {
        throw new IllegalArgumentException(
            "Embedding vector contains non-finite value at index " + i + ": " + vector[i]);
      }
    }
  }
}
