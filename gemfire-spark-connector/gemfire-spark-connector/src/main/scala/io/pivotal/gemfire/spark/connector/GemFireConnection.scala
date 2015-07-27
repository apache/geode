package io.pivotal.gemfire.spark.connector

import com.gemstone.gemfire.cache.execute.ResultCollector
import com.gemstone.gemfire.cache.query.Query
import com.gemstone.gemfire.cache.Region
import io.pivotal.gemfire.spark.connector.internal.RegionMetadata
import io.pivotal.gemfire.spark.connector.internal.rdd.GemFireRDDPartition


trait GemFireConnection {

  /**
   * Validate region existence and key/value type constraints, throw RuntimeException
   * if region does not exist or key and/or value type do(es) not match.
   * @param regionPath the full path of region
   */
  def validateRegion[K, V](regionPath: String): Unit

  /**
   * Get Region proxy for the given region
   * @param regionPath the full path of region
   */
  def getRegionProxy[K, V](regionPath: String): Region[K, V]

  /**
   * Retrieve region meta data for the given region. 
   * @param regionPath: the full path of the region
   * @return Some[RegionMetadata] if region exists, None otherwise
   */
  def getRegionMetadata[K, V](regionPath: String): Option[RegionMetadata]

  /** 
   * Retrieve region data for the given region and bucket set 
   * @param regionPath: the full path of the region
   * @param whereClause: the set of bucket IDs
   * @param split: GemFire RDD Partition instance
   */
  def getRegionData[K, V](regionPath: String, whereClause: Option[String], split: GemFireRDDPartition): Iterator[(K, V)]

  def executeQuery(regionPath: String, bucketSet: Set[Int], queryString: String): Object
  /** 
   * Create a gemfire OQL query
   * @param queryString GemFire OQL query string
   */
  def getQuery(queryString: String): Query

  /** Close the connection */
  def close(): Unit
}


