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
package org.apache.geode.spark.connector.internal.rdd

import org.apache.spark.Partition

/**
 * This serializable class represents a GeodeRDD partition. Each partition is mapped
 * to one or more buckets of region. The GeodeRDD can materialize the data of the 
 * partition based on all information contained here.
 * @param partitionId partition id, a 0 based number.
 * @param bucketSet region bucket id set for this partition. Set.empty means whole
 *                  region (used for replicated region)
 * @param locations preferred location for this partition                  
 */
case class GeodeRDDPartition (
  partitionId: Int, bucketSet: Set[Int], locations: Seq[String] = Nil)
  extends Partition  {
  
  override def index: Int = partitionId

}
