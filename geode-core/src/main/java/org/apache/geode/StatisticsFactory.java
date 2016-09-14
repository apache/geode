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
package org.apache.geode;

//import org.apache.geode.distributed.DistributedSystem;
//import org.apache.geode.internal.statistics.StatArchiveFormat;
//import java.io.IOException;
//import java.io.Reader;

/**
 * Instances of this interface provide methods that create instances
 * of {@link Statistics}.
 * It can also be used to create instances of {@link StatisticDescriptor}
 * and {@link StatisticsType} because it implements {@link StatisticsTypeFactory}.
 * {@link
 * org.apache.geode.distributed.DistributedSystem} is the only
 * instance of this interface.
 *
 * <P>
 *
 * A <code>StatisticsFactory</code> can create a {@link
 * StatisticDescriptor statistic} of three numeric types:
 * <code>int</code>, <code>long</code>, and <code>double</code>.  A
 * statistic (<code>StatisticDescriptor</code>) can either be a
 * <I>gauge</I> meaning that its value can increase and decrease or a
 * <I>counter</I> meaning that its value is strictly increasing.
 * Marking a statistic as a counter allows statistic display tools
 * to properly display a statistics whose value "wraps around" (that
 * is, exceeds its maximum value).
 * 
 * <P>The following code is an example of how to create a type using the api.
 * In this example the type has two stats whose values always increase:
 * <pre>
    StatisticsFactory f = ...;
    StatisticsType t = f.createType(
        "StatSampler",
        "Stats on the statistic sampler.",
        new StatisticDescriptor[] {
            f.createIntCounter("sampleCount",
                               "Total number of samples taken by this sampler.",
                               "samples"),
            f.createLongCounter("sampleTime",
                                "Total amount of time spent taking samples.",
                                "milliseconds"),
        }
    );
    this.samplerStats = f.createStatistics(t, "statSampler");
    this.sampleCountId = this.samplerStats.nameToId("sampleCount");
    this.sampleTimeId = this.samplerStats.nameToId("sampleTime");
 * </pre>
 * Later on the stat ids can be used to increment the stats:
 * <pre>
    this.samplerStats.incInt(this.sampleCountId, 1);
    this.samplerStats.incLong(this.sampleTimeId, nanosSpentWorking / 1000000);
 * </pre>
 * <P>The following is an example of how to create the same type using XML.
 * The XML data:
 * <pre>
    &lt;?xml version="1.0" encoding="UTF-8"?&gt;
    &lt;!DOCTYPE statistics PUBLIC
      "-//GemStone Systems, Inc.//GemFire Statistics Type//EN"
      "http://www.gemstone.com/dtd/statisticsType.dtd"&gt;
    &lt;statistics&gt;
      &lt;type name="StatSampler"&gt;
        &lt;description&gt;Stats on the statistic sampler.&lt;/description&gt;
        &lt;stat name="sampleCount" storage="int" counter="true"&gt;
          &lt;description&gt;Total number of samples taken by this sampler.&lt;/description&gt;
          &lt;unit&gt;samples&lt;/unit&gt;
        &lt;/stat&gt;
        &lt;stat name="sampleTime" storage="long" counter="true"&gt;
          &lt;description&gt;Total amount of time spent taking samples.&lt;/description&gt;
          &lt;unit&gt;milliseconds&lt;/unit&gt;
        &lt;/stat&gt;
      &lt;/type&gt;
    &lt;/statistics&gt;
 * </pre>
 * The code to create the type:
 * <pre>
      StatisticsFactory f = ...;
      Reader r = new InputStreamReader("fileContainingXmlData"));
      StatisticsType type = f.createTypesFromXml(r)[0];
 * </pre>
 * <P>
 * @see <A href="package-summary.html#statistics">Package introduction</A>
 *
 *
 * @since GemFire 3.0
 */
public interface StatisticsFactory extends StatisticsTypeFactory {
  /**
   * Creates and returns a {@link Statistics} instance of the given {@link StatisticsType type} with default ids.
   * <p>
   * The created instance may not be {@link Statistics#isAtomic atomic}.
   */
  public Statistics createStatistics(StatisticsType type);
  /**
   * Creates and returns a {@link Statistics} instance of the given {@link StatisticsType type}, <code>textId</code>, and with a default numeric id.
   * <p>
   * The created instance may not be {@link Statistics#isAtomic atomic}.
   */
  public Statistics createStatistics(StatisticsType type, String textId);
  /**
   * Creates and returns a {@link Statistics} instance of the given {@link StatisticsType type}, <code>textId</code>, and <code>numericId</code>.
   * <p>
   * The created instance may not be {@link Statistics#isAtomic atomic}.
   */
  public Statistics createStatistics(StatisticsType type, String textId, long numericId);

  /**
   * Creates and returns a {@link Statistics} instance of the given {@link StatisticsType type} with default ids.
   * <p>
   * The created instance will be {@link Statistics#isAtomic atomic}.
   */
  public Statistics createAtomicStatistics(StatisticsType type);
  /**
   * Creates and returns a {@link Statistics} instance of the given {@link StatisticsType type}, <code>textId</code>, and with a default numeric id.
   * <p>
   * The created instance will be {@link Statistics#isAtomic atomic}.
   */
  public Statistics createAtomicStatistics(StatisticsType type, String textId);
  /**
   * Creates and returns a {@link Statistics} instance of the given {@link StatisticsType type}, <code>textId</code>, and <code>numericId</code>.
   * <p>
   * The created instance will be {@link Statistics#isAtomic atomic}.
   */
  public Statistics createAtomicStatistics(StatisticsType type, String textId, long numericId);
  /**
   * Returns an array of all the existing statistics of the given type.
   */
  public Statistics[] findStatisticsByType(StatisticsType type);
  /**
   * Returns an array of all the existing statistics with the given textId.
   */
  public Statistics[] findStatisticsByTextId(String textId);
  /**
   * Returns an array of all the existing statistics with the given numericId.
   */
  public Statistics[] findStatisticsByNumericId(long numericId);
}
