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

package org.apache.geode.cache;

/**
 * Indicates that the requested region already exists when a region is
 * being created.
 *
 *
 * 
 * @see Region#createSubregion
 * @see Cache#createRegion
 * @since GemFire 2.0
 */
public class RegionExistsException extends CacheException {
private static final long serialVersionUID = -5643670216230359426L;
  private transient Region region;
  
  /**
   * Constructs an instance of <code>RegionExistsException</code> with the specified Region.
   * @param rgn the Region that exists
   */
  public RegionExistsException(Region rgn) {
    super(rgn.getFullPath());
    this.region = rgn;
  }
  
  /**
   * Constructs an instance of <code>RegionExistsException</code> with the specified detail message
   * and cause.
   * @param rgn the Region that exists
   * @param cause the causal Throwable
   */
  public RegionExistsException(Region rgn, Throwable cause) {
    super(rgn.getFullPath(), cause);
    this.region = rgn;
  }

  /**
   * Return the Region that already exists which prevented region creation.
   * @return the Region that already exists, or null if this exception has
   * been serialized, in which {@link Throwable#getMessage } will return the
   * pathFromRoot for the region that exists.
   */
  public Region getRegion() {
    return this.region;
  }
}
