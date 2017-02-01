#pragma once

#ifndef GEODE_REGIONXMLCREATION_H_
#define GEODE_REGIONXMLCREATION_H_

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

#include <string>
#include <vector>
#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/ExceptionTypes.hpp>
#include <gfcpp/Region.hpp>
#include "RegionXmlCreation.hpp"
#include <gfcpp/RegionAttributes.hpp>

namespace apache {
namespace geode {
namespace client {
class Cache;

/**
 * Represents a {@link Region} that is created declaratively.
 *
 * @since 1.0
 */
class CPPCACHE_EXPORT RegionXmlCreation {
 private:
  /** An <code>AttributesFactory</code> for creating default
    * <code>RegionAttribute</code>s */
  AttributesFactory attrFactory;

  /** The name of this region */
  std::string regionName;

  /** attributeId mentioned in XML file" */
  std::string attrId;

  /** True if region is a root */
  bool isRoot;

  /** The attributes of this region */
  RegionAttributesPtr regAttrs;

  /** This region's subregions */
  std::vector<RegionXmlCreation*> subRegions;

 public:
  /**
   * Fills in the state (that is, creates subregions)
   * of a given <code>Region</code> based on the description provided
   * by this <code>RegionXmlCreation</code>.
   *
   */
  void fillIn(RegionPtr region);

 public:
  ~RegionXmlCreation();
  /**
   * Creates a new <code>RegionCreation</code> with the given name.
   */
  RegionXmlCreation(char* name, bool isRoot = false);

  /**
   * Adds a subregion with the given name to this region
   */
  void addSubregion(RegionXmlCreation* regionPtr);

  /**
   * Sets the attributes of this region
   */
  void setAttributes(RegionAttributesPtr attrsPtr);

  /**
   * Gets the attributes of this region
   */
  RegionAttributesPtr getAttributes();

  /**
   * Creates a root {@link Region} in a given <code>Cache</code>
   * based on the description provided by this
   * <code>RegionCreation</code>
   *
   * @throws OutOfMemoryException if the memory allocation failed
   * @throws NotConnectedException if the cache is not connected
   * @throws InvalidArgumentException if the attributePtr is NULL.
   * or if RegionAttributes is null or if regionName is null,
   * the empty   string, or contains a '/'
   * @throws RegionExistsException
   * @throws CacheClosedException if the cache is closed
   *         at the time of region creation
   * @throws UnknownException otherwise
   */
  void createRoot(Cache* cache);

  /**
   * Creates a {@link Region} with the given parent using the
   * description provided by this <code>RegionCreation</code>.
   *
   * @throws OutOfMemoryException if the memory allocation failed
   * @throws NotConnectedException if the cache is not connected
   * @throws InvalidArgumentException if the attributePtr is NULL.
   * or if RegionAttributes is null or if regionName is null,
   * the empty string,or contains a '/'
   * @throws RegionExistsException
   * @throws CacheClosedException if the cache is closed
   *         at the time of region creation
   * @throws UnknownException otherwise
   *
   */
  void create(RegionPtr parent);

  std::string getAttrId() const;
  void setAttrId(const std::string& attrId);
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_REGIONXMLCREATION_H_
