/*
 * The Continuous Query QuickStart Example.
 *
 * This example takes the following steps:
 *
 * 1. Create a GemFire Cache Programmatically.
 * 2. Create the example Region Programmatically.
 * 3. Populate some query objects on the Region.
 * 4. Register a cqQuery listener
 * 5. Execute a cqQuery with initial Results 
 * 6. Close the Cache.
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

class MyCqListener : public CqListener {
  public:
  void onEvent(const CqEvent& cqe)
  {
    char* opStr = (char*)"Default";
    PortfolioPtr portfolio( dynamic_cast<Portfolio*> (cqe.getNewValue().ptr() ));
    CacheableStringPtr key( dynamic_cast<CacheableString*> (cqe.getKey().ptr() ));
    switch (cqe.getQueryOperation())
    {
      case CqOperation::OP_TYPE_CREATE:
      {
        opStr = (char*)"CREATE";
        break;
      }
      case CqOperation::OP_TYPE_UPDATE:
      {
        opStr = (char*)"UPDATE";
        break;
      }
      case CqOperation::OP_TYPE_DESTROY:
      {
        opStr = (char*)"UPDATE";
        break;
      }
        default:
        break;
     } 
     LOGINFO("MyCqListener::OnEvent called with %s, key[%s], value=(%ld,%s)",
	 opStr, key->asChar(), portfolio->getID(), portfolio->getPkid()->asChar()); 
  }

  void onError(const CqEvent& cqe){
    LOGINFO("MyCqListener::OnError called");
  }

  void close(){
    LOGINFO("MyCqListener::close called");
  }
};

// The CqQuery QuickStart example.
int main(int argc, char ** argv)
{
  try
  {
    // Create a GemFire Cache Programmatically.
    CacheFactoryPtr cacheFactory = CacheFactory::createCacheFactory();

    CachePtr cachePtr = cacheFactory->addServer("localhost", 50505)
                                    ->setSubscriptionEnabled(true)
                                    ->create();   
    
    LOGINFO("Created the GemFire Cache");
    
    RegionFactoryPtr regionFactory = cachePtr->createRegionFactory(PROXY);
    
    LOGINFO("Created the RegionFactory");
    
    // Create the example Region programmatically.
    RegionPtr regionPtr = regionFactory->create("Portfolios");

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
    QueryServicePtr qrySvcPtr = cachePtr->getQueryService();

    //Create CqAttributes and Install Listener
    CqAttributesFactory cqFac;
    CqListenerPtr cqLstner (new MyCqListener());
    cqFac.addCqListener(cqLstner);
    CqAttributesPtr cqAttr = cqFac.create();

    //create a new Cq Query
    const char* qryStr = "select * from /Portfolios p where p.ID < 5";
    CqQueryPtr qry = qrySvcPtr->newCq((char*)"MyCq", qryStr, cqAttr);
  
    //execute Cq Query with initial Results
    CqResultsPtr resultsPtr  = qry->executeWithInitialResults();
    
    //make change to generate cq events
    regionPtr->put("Key3", port1Ptr);
    regionPtr->put("Key2", port2Ptr);
    regionPtr->put("Key1", port3Ptr);

    LOGINFO("ResultSet Query returned %d rows", resultsPtr->size());

    // Iterate through the rows of the query result.
    SelectResultsIterator iter = resultsPtr->getIterator();
    while (iter.hasNext())
    {
        SerializablePtr ser = iter.next();
        if (ser != NULLPTR) {
          LOGINFO (" query pulled object %s\n", ser->toString()->asChar());
          
          StructPtr stPtr(dynamic_cast<Struct*>  (ser.ptr() ));                    
          if (stPtr != NULLPTR)
          {
            LOGINFO(" got struct ptr ");
            SerializablePtr serKey = (*(stPtr.ptr()))["key"];           
            if (serKey != NULLPTR)
            {
              LOGINFO("got struct key %s\n", serKey->toString()->asChar());
            }
              
            SerializablePtr serVal = (*(stPtr.ptr()))["value"];
            
            if (serVal != NULLPTR)
            {
              LOGINFO("  got struct value %s\n", serVal->toString()->asChar());
            }
          }
        }
        else {
          printf("query pulled bad object\n");
        }       
    }
    // Stop the GemFire Continuous query.
    qry->stop();
    // Close the GemFire Continuous query.
    qry->close();

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

