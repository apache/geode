/*
 * The Continuous Query QuickStart Example.
 *
 * This example takes the following steps:
 *
 */

// Include the GemFire library.
#include <gfcpp/GemfireCppCache.hpp>

// Include our Query objects, viz. Portfolio and Position.
#include "Portfolio.hpp"
#include "Position.hpp"

// Use the "gemfire" namespace.
using namespace gemfire;

// Use the "testobject" namespace for the query objects.
using namespace testobject;

// The CqQuery QuickStart example.
int main(int argc, char ** argv)
{
  if(argc!=2)
  {
    LOGINFO("usage: %s, <iteration count>", argv[0]);
    return -1;
  }
  int itrCnt=atoi(argv[1]);
  try
  {

    // Create the GemFire cache using the settings from the gfcpp.properties file by default.
    PropertiesPtr prptr = Properties::create();
    prptr->insert("cache-xml-file", "XMLs/clientCqQuery.xml");

    CacheFactoryPtr cacheFactory = CacheFactory::createCacheFactory(prptr);
   
    CachePtr cachePtr = cacheFactory->setSubscriptionEnabled(true)->create();       

    LOGINFO("Created the GemFire Cache");

    // Get the Portfolios Region from the Cache which is declared in the Cache XML file.
    RegionPtr regionPtr = cachePtr->getRegion("Portfolios");

    LOGINFO("Obtained the Region from the Cache");

    // Register our Serializable/Cacheable Query objects, viz. Portfolio and Position.
    Serializable::registerType( Portfolio::createDeserializable);
    Serializable::registerType( Position::createDeserializable);

    LOGINFO("Registered Serializable Query Objects");

    // Populate the Region with some Portfolio objects.
    for(int i =0; i < 150; i++)
    {
      PortfolioPtr portPtr(new Portfolio(i /*ID*/, i*10 /*size*/));
      char buf[128];
      sprintf(buf, "Key%d", i);
      regionPtr->put(buf, portPtr);
    }

    LOGINFO("Populated some Portfolio Objects");

    //make change to generate cq events
    while(itrCnt-- >0) 
    {
      if(itrCnt%10==0)
	LOGINFO("%d iterations left", itrCnt);

      int up = itrCnt%150 + 150;
      for(int i =itrCnt%150; i < up; i++)
      {
        PortfolioPtr portPtr(new Portfolio(up-i /*ID*/, i*10 /*size*/));
        char buf[128];
        sprintf(buf, "Key%d", i);
        regionPtr->put(buf, portPtr);
      }
    }

    LOGINFO("finished updating");

    // Close the GemFire Cache.
    cachePtr->close();

    LOGINFO("Closed the GemFire Cache");

  }
  // An exception should not occur
  catch(const Exception & gemfireExcp)
  {
    LOGERROR("CqQuery GemFire Exception: %s", gemfireExcp.getMessage());
  }
}

