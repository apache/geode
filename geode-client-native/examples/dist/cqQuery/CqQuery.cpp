/*
 * The Continuous Query Example.
 *
 * This example takes the following steps:
 *
 */

// Include the GemFire library.
#include <gfcpp/GemfireCppCache.hpp>

// Include the Query headers.
#include <gfcpp/SelectResultsIterator.hpp>
#include <gfcpp/CqQuery.hpp>
#include <gfcpp/CqAttributesFactory.hpp>

// Include our Query objects, viz. Portfolio and Position.
#include "Portfolio.hpp"
#include "Position.hpp"
#include "string.h"

// Use the "gemfire" namespace.
using namespace gemfire;

// Use the "testobject" namespace for the query objects.
using namespace testobject;

#define MAX_LISTNER  8

const char* cqNames[MAX_LISTNER] = {
  (const char*)"MyCq_0",
  (const char*)"MyCq_1",
  (const char*)"MyCq_2",
  (const char*)"MyCq_3",
  (const char*)"MyCq_4",
  (const char*)"MyCq_5",
  (const char*)"MyCq_6",
  (const char*)"MyCq_7"
};

const char* queryStrings[MAX_LISTNER] = {
  (const char*)"select * from /Portfolios p where p.ID < 4",
  (const char*)"select * from /Portfolios p where p.ID < 9",
  (const char*)"select * from /Portfolios p where p.ID < 12",
  (const char*)"select * from /Portfolios p where p.ID < 3",
  (const char*)"select * from /Portfolios p where p.ID < 14",
  (const char*)"select * from /Portfolios p where p.ID < 5",
  (const char*)"select * from /Portfolios p where p.ID < 6",
  (const char*)"select * from /Portfolios p where p.ID < 7"
};

class MyCqListener : public CqListener {
  uint32_t m_updateCnt;
  uint32_t m_createCnt;
  uint32_t m_destroyCnt;
  uint32_t m_errorCnt;
  uint32_t m_eventCnt;
  uint32_t m_id;
  bool m_verbose;
  public:
  MyCqListener(uint32_t id, bool verbose=false):
    m_updateCnt(0),
    m_createCnt(0),
    m_destroyCnt(0),
    m_errorCnt(0),
    m_eventCnt(0),
    m_id(id),
    m_verbose(verbose)
  {
  }
  void onEvent(const CqEvent& cqe){
    m_eventCnt++;
    CqQueryPtr cq = cqe.getCq();
    char* opStr = (char*)"Default";
    PortfolioPtr portfolio( dynamic_cast<Portfolio*> (cqe.getNewValue().ptr() ));
    CacheableStringPtr key( dynamic_cast<CacheableString*> (cqe.getKey().ptr() ));
    switch (cqe.getQueryOperation())
      {
        case CqOperation::OP_TYPE_CREATE:
	  {
            opStr = (char*)"CREATE";
	    m_createCnt++;
            break;
	  }
        case CqOperation::OP_TYPE_UPDATE:
	  {
            opStr = (char*)"UPDATE";
	    m_updateCnt++;
            break;
	  }
        case CqOperation::OP_TYPE_DESTROY:
	  {
            opStr = (char*)"DESTROY";
	    m_destroyCnt++;
            break;
	  }
        default:
          break;
       }
       if(m_eventCnt%5000==0)
       {
          if(m_verbose)
          {
             LOGINFO("MyCqListener[%d]::OnEvent called with %s, key[%s], value=(%ld,%s)",
	             m_id, opStr, key->asChar(), portfolio->getID(), portfolio->getPkid()->asChar()); 
             LOGINFO("MyCqListener[%d]::OnEvent portfolio %s", m_id, portfolio->toString()->asChar()); 
          }else 
          {
             LOGINFO("cq[%s], listener[%d]::OnEvent update Count=%d,create Count=%d, destroy Count=%d, total count=%d",
                     cq->getName(), m_updateCnt, m_createCnt, m_destroyCnt, m_eventCnt);
	  }
          LOGINFO("************Type \'q\' to quit!!!!*****************");
       }
  }

  void onError(const CqEvent& cqe){
       m_eventCnt++;
       m_errorCnt++;
       if(m_verbose)
          LOGINFO("MyCqListener::OnError called");
  }

  void close(){
    m_eventCnt++;
    if(m_verbose)
       LOGINFO("MyCqListener::close called");
  }
};

//Cache Listener
class MyCacheListener : public CacheListener
{
  uint32_t m_eventCount;
  bool m_verbose;

  void check(const EntryEvent& event, const char* opStr)
  {
    m_eventCount++;
    if(m_eventCount%5000==0)
    {
       if(m_verbose)
       {
          PortfolioPtr portfolio( dynamic_cast<Portfolio*> (event.getNewValue().ptr() ));
          CacheableStringPtr key( dynamic_cast<CacheableString*> (event.getKey().ptr() ));
          if(key!=NULLPTR && portfolio!=NULLPTR)
          {
            LOGINFO("CacheListener called with %s, key[%s], value=(%ld,%s)",
   opStr, key->asChar(), portfolio->getID(), portfolio->getPkid()->asChar()); 
          }
       } else 
       {
          LOGINFO("cache listener event count=%d", m_eventCount);
       }
       LOGINFO("************Type \'q\' to quit!!!!*****************");
    }
  }

  public:
  MyCacheListener(bool verbose=false):
    m_eventCount(0),
    m_verbose(verbose)
  {
  }

  virtual void afterCreate( const EntryEvent& event )
  {
    check(event, (const char*)"Create");
  }

  virtual void afterUpdate( const EntryEvent& event )
  {
    check(event, (const char*)"Update");
  }
  virtual void afterRegionInvalidate( const RegionEvent& event ) 
  {
    LOGINFO("afterRegionInvalidate called.");
  }
  virtual void afterRegionDestroy( const RegionEvent& event )
  {
    LOGINFO("afterRegionDestroy called.");
  }
  virtual void afterRegionLive( const RegionEvent& event )
  {
    LOGINFO("afterRegionLive called.");
  }

};

// The CqQuery example.
int main(int argc, char ** argv)
{
  if(argc>2)
  {
    LOGINFO("usage: %s, [-v] \n -v indicate verbose ", argv[0]);
    return -1;
  }
  bool verbose = false;

  if(argc==2 && !strcmp(argv[1], "-v"))
      verbose = true;
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
    AttributesMutatorPtr attrMutator = regionPtr->getAttributesMutator();
    CacheListenerPtr ptr(new MyCacheListener(verbose));
    attrMutator->setCacheListener(ptr);

    LOGINFO("Obtained the Region from the Cache");

    // Register our Serializable/Cacheable Query objects, viz. Portfolio and Position.
    Serializable::registerType( Portfolio::createDeserializable);
    Serializable::registerType( Position::createDeserializable);

    LOGINFO("Registered Serializable Query Objects");

    regionPtr->registerAllKeys(false, NULLPTR, true);

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
    CqListenerPtr cqLstner (new MyCqListener(0, verbose));
    cqFac.addCqListener(cqLstner);
    CqAttributesPtr cqAttr = cqFac.create();

    //create a new Cq Query
    CqQueryPtr qry = qrySvcPtr->newCq(cqNames[0], queryStrings[0], cqAttr); 
  
    //execute Cq Query with initial Results
    CqResultsPtr resultsPtr  = qry->executeWithInitialResults();
    
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
    //restart Cq Query with initial Results
    qry->execute();

    for(int i=1; i < MAX_LISTNER; i++)
    {
      CqListenerPtr cqLstner1(new MyCqListener(i, verbose));
      cqFac.addCqListener(cqLstner1);
      cqAttr = cqFac.create();
      qry = qrySvcPtr->newCq(cqNames[i], queryStrings[i], cqAttr);
    }

    qry = qrySvcPtr->getCq(cqNames[6]);
    cqAttr = qry->getCqAttributes();
    VectorOfCqListener vl;
    cqAttr->getCqListeners(vl);
    LOGINFO("number of listeners for cq[%s] is %d", cqNames[6], vl.size());

    qry = qrySvcPtr->getCq(cqNames[0]);
    CqAttributesMutatorPtr cqAttrMtor = qry->getCqAttributesMutator();
    for(int32_t i=0; i < vl.size(); i++)
    {
      CqListenerPtr ptr = vl[i];
      cqAttrMtor->addCqListener(ptr);
    }

    // Stop the GemFire Continuous query.
    qry->stop();

    //start all Cq Query
    qrySvcPtr->executeCqs();

    for(int i=0; i < MAX_LISTNER; i++)
    {
       LOGINFO("get info for cq[%s]:", cqNames[i]);
       CqQueryPtr cqy = qrySvcPtr->getCq(cqNames[i]);
       CqStatisticsPtr cqStats = cqy->getStatistics();
       LOGINFO("Cq[%s]: CqStatistics: numInserts[%d], numDeletes[%d], numUpdates[%d], numEvents[%d]", 
                     cqNames[i], cqStats->numInserts(), cqStats->numDeletes(), cqStats->numUpdates(), cqStats->numEvents());
     }

     CqServiceStatisticsPtr serviceStats = qrySvcPtr->getCqServiceStatistics();
     LOGINFO("numCqsActive=%d, numCqsCreated=%d, numCqsClosed=%d,numCqsStopped=%d, numCqsOnClient=%d", 
                     serviceStats->numCqsActive(), serviceStats->numCqsCreated(),
     serviceStats->numCqsClosed(), serviceStats->numCqsStopped(),
     serviceStats->numCqsOnClient());

    LOGINFO("***************************************************");
    LOGINFO("***************************************************");
    LOGINFO("***************************************************");
    LOGINFO("************Type \'q\' to quit!!!!*****************");
    LOGINFO("***************************************************");
    LOGINFO("***************************************************");
    LOGINFO("***************************************************");
    while(1)
    {
      char in[2];
      fscanf(stdin, "%s",in);
      if(in[0]=='q')
	break;
    }

    // Stop all the GemFire Continuous query.
    qrySvcPtr->stopCqs();

    for(int i=0; i < MAX_LISTNER; i++)
    {
       LOGINFO("get info for cq[%s]:", cqNames[i]);
       CqQueryPtr cqy = qrySvcPtr->getCq(cqNames[i]);
       cqAttr = cqy->getCqAttributes();
       cqAttr->getCqListeners(vl);
       LOGINFO("number of listeners for cq[%s] is %d", cqNames[i], vl.size());

       CqStatisticsPtr cqStats = cqy->getStatistics();
       LOGINFO("Cq[%s]: CqStatistics: numInserts[%d], numDeletes[%d], numUpdates[%d], numEvents[%d]", 
                   cqNames[i], cqStats->numInserts(), cqStats->numDeletes(), cqStats->numUpdates(), cqStats->numEvents());
     }

    // Close all the GemFire Continuous query.
    qrySvcPtr->closeCqs();
    LOGINFO("numCqsActive=%d, numCqsCreated=%d, numCqsClosed=%d,numCqsStopped=%d, numCqsOnClient=%d", 
                   serviceStats->numCqsActive(), serviceStats->numCqsCreated(),
      serviceStats->numCqsClosed(), serviceStats->numCqsStopped(),
      serviceStats->numCqsOnClient());

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

