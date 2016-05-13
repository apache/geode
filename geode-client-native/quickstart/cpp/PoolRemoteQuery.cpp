/*
 * The PoolRemoteQuery QuickStart Example.
 * This examples creates pool using locator.
 * This example takes the following steps:
 *
 * 1. Create a GemFire Cache.
 * 2. Get the example Region from the Pool.
 * 3. Populate some query objects on the Region.
 * 4. Get the pool, get the Query Service from Cache. Pool is define in clientRemoteQueryWithPool.xml. Pool has locator to get the server. Apart from that pool is bind to server group "ServerGroup1".
 * 5. Execute a query that returns a Result Set.
 * 6. Execute a query that returns a Struct Set.
 * 7. Execute the region shortcut/convenience query methods.
 * 8. Close the Cache.
 *
 */

// Include the GemFire library.
#include <gfcpp/GemfireCppCache.hpp>

// Include our Query objects, viz. Portfolio and Position.
#include "queryobjects/Portfolio.hpp"
#include "queryobjects/Position.hpp"

// Use the "gemfire" namespace.
using namespace gemfire;

// Use the "testobject" namespace for the query objects.
using namespace testobject;

// The PoolRemoteQuery QuickStart example.
int main(int argc, char ** argv)
{
  try
  {

    CacheFactoryPtr cacheFactory = CacheFactory::createCacheFactory();

    // Create a GemFire Cache with the "clientRemoteQueryWithPool.xml" Cache XML file.
    CachePtr cachePtr = cacheFactory->set("cache-xml-file", "XMLs/clientPoolRemoteQuery.xml")->create();

    LOGINFO("Created the GemFire Cache");

    // Get the example Region from the Cache which is declared in the Cache XML file.
    RegionPtr regionPtr = cachePtr->getRegion("Portfolios");

    LOGINFO("Obtained the Region from the Cache");

    // Register our Serializable/Cacheable Query objects, viz. Portfolio and Position.
    Serializable::registerType( Portfolio::createDeserializable);
    Serializable::registerType( Position::createDeserializable);

    LOGINFO("Registered Serializable Query Objects");

    // Populate the Region with some Portfolio objects.
    PortfolioPtr port1Ptr(new Portfolio(1 /*ID*/, 10 /*size*/));
    PortfolioPtr port2Ptr(new Portfolio(2 /*ID*/, 20 /*size*/));
    PortfolioPtr port3Ptr(new Portfolio(3 /*ID*/, 30 /*size*/));
    regionPtr->put("Key1", port1Ptr);
    regionPtr->put("Key2", port2Ptr);
    regionPtr->put("Key3", port3Ptr);

    LOGINFO("Populated some Portfolio Objects");

    // Get the QueryService from the Cache.
    QueryServicePtr qrySvcPtr = cachePtr->getQueryService("examplePool");

    LOGINFO("Got the QueryService from the Cache");

    // Execute a Query which returns a ResultSet.
    QueryPtr qryPtr = qrySvcPtr->newQuery("SELECT DISTINCT * FROM /Portfolios");
    SelectResultsPtr resultsPtr = qryPtr->execute();

    LOGINFO("ResultSet Query returned %d rows", resultsPtr->size());

    // Execute a Query which returns a StructSet.
    qryPtr = qrySvcPtr->newQuery("SELECT DISTINCT ID, status FROM /Portfolios WHERE ID > 1");
    resultsPtr = qryPtr->execute();

    LOGINFO("StructSet Query returned %d rows", resultsPtr->size());

    // Iterate through the rows of the query result.
    int rowCount = 0;
    SelectResultsIterator iter = resultsPtr->getIterator();
    while (iter.hasNext())
    {
      rowCount++;
      Struct * psi = dynamic_cast<Struct*>( iter.next().ptr() );
      LOGINFO("Row %d Column 1 is named %s, value is %s", rowCount, psi->getFieldName(0), (*psi)[0]->toString()->asChar());
      LOGINFO("Row %d Column 2 is named %s, value is %s", rowCount, psi->getFieldName(1), (*psi)[1]->toString()->asChar());
    }

    // Execute a Region Shortcut Query (convenience method).
    resultsPtr = regionPtr->query("ID = 2");

    LOGINFO("Region Query returned %d rows", resultsPtr->size());

    // Execute the Region selectValue() API.
    SerializablePtr resultPtr = regionPtr->selectValue("ID = 3");
    PortfolioPtr portPtr = dynCast<PortfolioPtr>(resultPtr);

    LOGINFO("Region selectValue() returned an item:\n %s", portPtr->toString()->asChar());

    // Execute the Region existsValue() API.
    bool existsValue = regionPtr->existsValue("ID = 4");

    LOGINFO("Region existsValue() returned %s", existsValue ? "true" : "false");

    // Close the GemFire Cache.
    cachePtr->close();

    LOGINFO("Closed the GemFire Cache");

  }
  // An exception should not occur
  catch(const Exception & gemfireExcp)
  {
    LOGERROR("PoolRemoteQuery GemFire Exception: %s", gemfireExcp.getMessage());
  }
}

