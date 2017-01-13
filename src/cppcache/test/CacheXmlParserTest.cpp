#include <gtest/gtest.h>

#include <CacheXmlParser.hpp>

using namespace gemfire;

std::string xsd_prefix = R"(<?xml version='1.0' encoding='UTF-8'?>
<client-cache
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xmlns="http://schema.pivotal.io/gemfire/gfcpp-cache"
  xsi:schemaLocation="http://schema.pivotal.io/gemfire/gfcpp-cache"
  version='9.0'
>)";

std::string valid_cache_config_body = R"(<root-region name = 'Root1' >
        <region-attributes scope='local'
                           caching-enabled='true'
                           initial-capacity='25'
                           load-factor='0.32'
                           concurrency-level='10'
                           lru-entries-limit = '35'>
            <region-idle-time>
                <expiration-attributes timeout='20' action='destroy'/> 
            </region-idle-time>
            <entry-idle-time>
                <expiration-attributes timeout='10' action='invalidate'/>
            </entry-idle-time>
            <region-time-to-live>
                <expiration-attributes timeout='0' action='local-destroy'/>
            </region-time-to-live>
            <entry-time-to-live>
                <expiration-attributes timeout='0' action='local-invalidate'/>
            </entry-time-to-live>
        </region-attributes>
        <region name='SubRegion1'>
            <region-attributes scope='local'
                               caching-enabled='true'
                               initial-capacity='23'
                               load-factor='0.89'
                               concurrency-level='52'>
            </region-attributes>
        </region>
    </root-region>
    <root-region name= 'Root2'>
        <region-attributes scope='local'
                           caching-enabled='true'
                           initial-capacity='16'
                           load-factor='0.75'
                           concurrency-level='16'>
            <region-time-to-live>
                <expiration-attributes timeout='0' action='destroy'/>
            </region-time-to-live>
            <region-idle-time>
                <expiration-attributes timeout='0' action='invalidate'/>
            </region-idle-time>
            <entry-time-to-live>
                <expiration-attributes timeout='0' action='destroy'/>
            </entry-time-to-live>
            <entry-idle-time>
                <expiration-attributes timeout='0' action='invalidate'/>
            </entry-idle-time>
        </region-attributes>
        <region name='SubRegion21'>
            <region-attributes scope='local'
                               caching-enabled='true'
                               initial-capacity='16'
                               load-factor='0.75'
                               concurrency-level='16'>
                <region-idle-time>
                    <expiration-attributes timeout='20' action='destroy'/>
                </region-idle-time>
                <entry-idle-time>
                    <expiration-attributes timeout='10' action='invalidate'/>
                </entry-idle-time>
            </region-attributes>
        </region>
        <region name='SubRegion22'>
            <region name='SubSubRegion221'>
            </region>
        </region>
    </root-region>
</client-cache>)";

std::string dtd_prefix = R"(<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE cache PUBLIC
    "-//GemStone Systems, Inc.//GemFire Declarative Caching 3.6//EN"
    "http://www.gemstone.com/dtd/gfcpp-cache3600.dtd">
<client-cache>)";

TEST(CacheXmlParser, CanParseRegionConfigFromAValidXsdCacheConfig) {
  CacheXmlParser parser;
  std::string xml = xsd_prefix + valid_cache_config_body;
  parser.parseMemory(xml.c_str(), static_cast<int>(xml.length()));
}

TEST(CacheXmlParser, CanParseRegionConfigFromAValidDtdCacheConfig) {
  CacheXmlParser parser;
  std::string xml = dtd_prefix + valid_cache_config_body;
  parser.parseMemory(xml.c_str(), static_cast<int>(xml.length()));
}
