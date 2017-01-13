#ifndef _GEMFIRE_REGIONXMLCREATION_HPP_
#define _GEMFIRE_REGIONXMLCREATION_HPP_
/*=========================================================================
 * Copyright (c) 2004-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include <string>
#include <vector>
#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/ExceptionTypes.hpp>
#include <gfcpp/Region.hpp>
#include "RegionXmlCreation.hpp"
#include <gfcpp/RegionAttributes.hpp>

namespace gemfire {
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
};
#endif  // #ifndef  _GEMFIRE_REGIONXMLCREATION_HPP_
