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
package com.gemstone.gemfire;

//import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.StatArchiveFormat;
import java.io.IOException;
import java.io.Reader;

/**
 * Instances of this interface provide methods that create instances
 * of {@link StatisticDescriptor} and {@link StatisticsType}.
 * Every {@link StatisticsFactory} is also a type factory.
 *
 * <P>
 *
 * A <code>StatisticsTypeFactory</code> can create a {@link
 * StatisticDescriptor statistic} of three numeric types:
 * <code>int</code>, <code>long</code>, and <code>double</code>.  A
 * statistic (<code>StatisticDescriptor</code>) can either be a
 * <I>gauge</I> meaning that its value can increase and decrease or a
 * <I>counter</I> meaning that its value is strictly increasing.
 * Marking a statistic as a counter allows statistic display tools
 * to properly display a statistics whose value "wraps around" (that
 * is, exceeds its maximum value).
 * 
 * <P>The following code is an example of how to create a type using XML.
 * In this example the type has two stats whose values always increase:
 * <pre>
    StatisticsTypeFactory f = ...;
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
      StatisticsTypeFactory f = ...;
      Reader r = new InputStreamReader("fileContainingXmlData"));
      StatisticsType type = f.createTypesFromXml(r)[0];
 * </pre>
 * <P>
 * @see <A href="package-summary.html#statistics">Package introduction</A>
 *
 *
 * @since GemFire 3.0
 */
public interface StatisticsTypeFactory {

  /**
   * Creates and returns an int counter {@link StatisticDescriptor} with the given <code>name</code>, <code>description</code>, <code>units</code>,
   * and with larger values indicating better performance.
   */
  public StatisticDescriptor createIntCounter(String name, String description,
                                              String units);
  /**
   * Creates and returns a long counter {@link StatisticDescriptor} with the given <code>name</code>, <code>description</code>, <code>units</code>,
   * and with larger values indicating better performance.
   */
  public StatisticDescriptor createLongCounter(String name, String description,
                                               String units);
  /**
   * Creates and returns a double counter {@link StatisticDescriptor} with the given <code>name</code>, <code>description</code>, <code>units</code>,
   * and with larger values indicating better performance.
   */
  public StatisticDescriptor createDoubleCounter(String name, String description,
                                               String units);
  /**
   * Creates and returns an int gauge {@link StatisticDescriptor} with the given <code>name</code>, <code>description</code>, <code>units</code>,
   * and with smaller values indicating better performance.
   */
  public StatisticDescriptor createIntGauge(String name, String description,
                                              String units);
  /**
   * Creates and returns a long gauge {@link StatisticDescriptor} with the given <code>name</code>, <code>description</code>, <code>units</code>,
   * and with smaller values indicating better performance.
   */
  public StatisticDescriptor createLongGauge(String name, String description,
                                               String units);
  /**
   * Creates and returns a double gauge {@link StatisticDescriptor} with the given <code>name</code>, <code>description</code>, <code>units</code>,
   * and with smaller values indicating better performance.
   */
  public StatisticDescriptor createDoubleGauge(String name, String description,
                                               String units);

  /**
   * Creates and returns an int counter {@link StatisticDescriptor} with the given <code>name</code>, <code>description</code>, <code>largerBetter</code>, and <code>units</code>.
   */
  public StatisticDescriptor createIntCounter(String name, String description,
                                              String units, boolean largerBetter);
  /**
   * Creates and returns a long counter {@link StatisticDescriptor} with the given <code>name</code>, <code>description</code>, <code>largerBetter</code>, and <code>units</code>.
   */
  public StatisticDescriptor createLongCounter(String name, String description,
                                               String units, boolean largerBetter);
  /**
   * Creates and returns a double counter {@link StatisticDescriptor} with the given <code>name</code>, <code>description</code>, <code>largerBetter</code>, and <code>units</code>.
   */
  public StatisticDescriptor createDoubleCounter(String name, String description,
                                               String units, boolean largerBetter);
  /**
   * Creates and returns an int gauge {@link StatisticDescriptor} with the given <code>name</code>, <code>description</code>, <code>largerBetter</code>, and <code>units</code>.
   */
  public StatisticDescriptor createIntGauge(String name, String description,
                                              String units, boolean largerBetter);
  /**
   * Creates and returns a long gauge {@link StatisticDescriptor} with the given <code>name</code>, <code>description</code>, <code>largerBetter</code>, and <code>units</code>.
   */
  public StatisticDescriptor createLongGauge(String name, String description,
                                               String units, boolean largerBetter);
  /**
   * Creates and returns a double gauge {@link StatisticDescriptor} with the given <code>name</code>, <code>description</code>, <code>largerBetter</code>, and <code>units</code>.
   */
  public StatisticDescriptor createDoubleGauge(String name, String description,
                                               String units, boolean largerBetter);


  /**
   * The maximum number of descriptors a single statistics type can have.
   * <P> Current value is: <code>254</code>
   */
  public final int MAX_DESCRIPTORS_PER_TYPE = StatArchiveFormat.ILLEGAL_STAT_OFFSET-1;

  /**
   * Creates or finds and returns a {@link StatisticsType} with the given <code>name</code>, <code>description</code>, and {@link StatisticDescriptor statistic descriptions}.
   * @throws IllegalArgumentException if a type with the given <code>name</code> already exists and it differs from the given parameters.
   */
  public StatisticsType createType(String name, String description,
                                   StatisticDescriptor[] stats);
  /**
   * Finds and returns an already created {@link StatisticsType} with the given <code>name</code>. Returns <code>null</code> if the type does not exist.
   */
  public StatisticsType findType(String name);

  /**
   * Creates one or more {@link StatisticsType} from the contents of the
   * given <code>reader</code>. The created types can be found by calling
   * {@link #findType}.
   *
   * @param reader
   *        The source of the XML data which must comply with the
   *        <code>statisticsType.dtd</code>.
   *
   * @throws IllegalArgumentException if a type defined in the reader
   *  already exists
   * @throws IOException
   *         Something went wrong while reading from
   *         <code>reader</code> 
   */
  public StatisticsType[] createTypesFromXml(Reader reader)
    throws IOException;
}
