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

// CacheConfig.cpp: implementation of the CacheConfig class.
//
//////////////////////////////////////////////////////////////////////

#include "CacheConfig.hpp"

#include <libxml/parser.h>
#include <libxml/tree.h>

#include "../DistributedSystem.hpp"

namespace gemfire
{

	//////////////////////////////////////////////////////////////////////
	// Construction/Destruction
	//////////////////////////////////////////////////////////////////////

	CacheConfig::CacheConfig(const char* xmlFileName)
        :         m_doc(NULL),
                  m_root_element(NULL)

	{
		m_doc = xmlParseFile(xmlFileName);
		if (m_doc == NULL)
		{
			 throw IllegalArgumentException("Cacheconfig : xmlParseFile");
		}
		m_root_element = xmlDocGetRootElement(m_doc);
		if (m_root_element == NULL)
		{
			throw IllegalArgumentException("Cacheconfig : xmlDocGetRootElement");
		}
		if (!parse())
			throw IllegalArgumentException("Cacheconfig : parse error");;
	}

	CacheConfig::~CacheConfig()
	{
		if (m_doc)
			xmlFreeDoc(m_doc);
		xmlCleanupParser();

	}


	bool
		CacheConfig::parse( )
	{

		if (strcmp((const char*)m_root_element->name ,"cache")==0)
		{
			xmlNode *cur_node = NULL;

			for (cur_node = m_root_element->children; cur_node; cur_node = cur_node->next)
			{
				if (cur_node->type == XML_ELEMENT_NODE)
	        		{
					if (strcmp((const char*)cur_node->name,"root-region")==0)
					{
						parseRegion(cur_node);
					}
				}
			}
			return true;
		}
		else
		{
			return false;
		}

	}



	bool
		CacheConfig::parseRegion(xmlNode *node)
	{
		xmlChar*	name = xmlGetNoNsProp(node,(const unsigned char*)"name");

		if (name != NULL)
		{
			xmlNode *cur_node = NULL;

			for (cur_node = node->children; cur_node; cur_node = cur_node->next)
			{
				if (cur_node->type == XML_ELEMENT_NODE)
				{
					if (strcmp((const char*)cur_node->name,"region-attributes")==0)
					{
						parseAttributes((const char*)name,cur_node);
					}
				}
			}
			return true;
		}
		return false;
	}

	bool
		CacheConfig::parseAttributes(const char* name,xmlNode *node)
	{
		xmlChar*	scope			 = xmlGetNoNsProp(node,(const unsigned char*)"scope");
		xmlChar*	initialCapacity          = xmlGetNoNsProp(node,(const unsigned char*)"initial-capacity");
                xmlChar*        lruLimit                 = xmlGetNoNsProp(node,(const unsigned char*)"lru-entries-limit");
                xmlChar*        concurrency              = xmlGetNoNsProp(node,(const unsigned char*)"concurrency-level");
                xmlChar*        caching                  = xmlGetNoNsProp(node,(const unsigned char*)"caching-enabled");

		std::string scopeStr = ( scope == NULL ? "invalid" : (const char*) scope );
		std::string initialCapacityStr = ( initialCapacity == NULL ? "1000" : (const char*) initialCapacity );
		std::string limitStr = ( lruLimit == NULL ? "0" : (const char*)lruLimit );
		std::string concStr = ( concurrency == NULL ? "0" : (const char*)concurrency );
		std::string cachingStr = ( caching == NULL ? "true" : (const char*)caching );

		RegionConfigPtr	reg(new RegionConfig(scopeStr,initialCapacityStr));

                reg->setLru( limitStr );
		reg->setConcurrency( concStr );
		reg->setCaching( cachingStr );

		m_regionList.insert( RegionConfigMapT::value_type(name,reg));

		return true;
	}

	RegionConfigMapT&
		CacheConfig::getRegionList()
	{
		return m_regionList;
	}

};
