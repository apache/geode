/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

/**
 * @file
 */
// CacheConfig.h: interface for the CacheConfig class.
//
//////////////////////////////////////////////////////////////////////

#if !defined(AFX_CacheConfig_H__48B95D79_F676_4F8A_8522_8B172DB33F7E__INCLUDED_)
#define AFX_CacheConfig_H__48B95D79_F676_4F8A_8522_8B172DB33F7E__INCLUDED_

#if _MSC_VER > 1000
#pragma once
#pragma warning(disable : 4786)
#endif  // _MSC_VER > 1000

#include <gfcpp/gfcpp_globals.hpp>
#include <string.h>
#include <map>
#include "RegionConfig.hpp"
#include <gfcpp/ExceptionTypes.hpp>
#include <gfcpp/DistributedSystem.hpp>

//
// Sneaky structure forward decl;
//

struct _xmlNode;
struct _xmlDoc;
typedef struct _xmlDoc xmlDoc;
typedef struct _xmlNode xmlNode;

namespace gemfire {

typedef std::map<std::string, RegionConfigPtr> RegionConfigMapT;

class CPPCACHE_EXPORT CacheConfig {
 public:
  CacheConfig(const char* xmlFileName);

  bool parse();

  bool parseRegion(xmlNode* node);

  bool parseAttributes(const char* name, xmlNode* node);

  RegionConfigMapT& getRegionList();

  virtual ~CacheConfig();

 private:
  CacheConfig();

  xmlDoc* m_doc;
  xmlNode* m_root_element;

  RegionConfigMapT m_regionList;
};
};
#endif  // !defined(AFX_CacheConfig_H__48B95D79_F676_4F8A_8522_8B172DB33F7E__INCLUDED_)
