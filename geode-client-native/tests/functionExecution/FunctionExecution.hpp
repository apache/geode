/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/**
  * @file    FunctionExecution.hpp
  * @since   1.0
  * @version 1.0
  * @see
  *
  */

// ----------------------------------------------------------------------------

#ifndef __FUNCTION_EXECUTION_TEST_HPP__
#define __FUNCTION_EXECUTION_TEST_HPP__

// ----------------------------------------------------------------------------

#include "GemfireCppCache.hpp"
#include "Assert.hpp"
#include "fwklib/FrameworkTest.hpp"
#include "fwklib/FwkObjects.hpp"
#include "fwklib/TestClient.hpp"
#include "fwklib/FrameworkTest.hpp"
#include <ace/Signal.h>
#include <stdlib.h>

#ifdef WIN32
  #ifdef llabs
    #undef llabs
  #endif
  #define llabs(x) ( ((x) < 0) ? ((x) * -1LL) : (x) )
#endif


namespace gemfire {
 namespace testframework {
   namespace functionexe {

std::string FUNCTION_EXECUTIONBB( "FUNCTION_EXECUTIONBB" );
std::string REGIONSBB( "Regions" );

//--------------------DoOpsTask-----------------------

static const int INVALIDATE             = 1;
static const int DESTROY                = 2;
static const int UPDATE_EXISTING_KEY    = 3;
static const int GET                    = 4;
static const int ADD_NEW_KEY            = 5;
static const int PUTALL_NEW_KEY         = 6;
static const int LOCAL_INVALIDATE       = 7;
static const int LOCAL_DESTROY          = 8;
static const int NUM_EXTRA_KEYS = 100;
static const int operations[] = {INVALIDATE, DESTROY, UPDATE_EXISTING_KEY, GET, ADD_NEW_KEY, PUTALL_NEW_KEY};
// LOCAL_INVALIDATE and  LOCAL_DESTROY is not supported for server. It is only for client in java
class DoFETask: public ClientTask {
  protected:
    RegionPtr regionPtr;
    bool m_istransaction;
    FrameworkTest * m_test;
    AtomicInc m_Cntr;
    std::string m_funcname;
    ACE_TSS<perf::Counter> m_count;
    ACE_TSS<perf::Counter> m_MyOffset;
    uint32_t m_iters;
    ACE_Recursive_Thread_Mutex m_lock;
    bool addNewKey(RegionPtr regionPtr);
    bool putAllNewKey(RegionPtr regionPtr);
    bool updateExistingKey(RegionPtr regionPtr);
    bool invalidate(RegionPtr regionPtr);
    bool destroy(RegionPtr regionPtr);
    bool localInvalidate(RegionPtr regionPtr);
    bool localDestroy(RegionPtr regionPtr);
    bool get(RegionPtr regionPtr);
    CacheablePtr GetValue(int32_t value,char* update = NULL);
    //std::vector<int> availableOps;
  public:

    DoFETask(RegionPtr reg, bool istransaction,FrameworkTest * test) :
    	regionPtr(reg), m_istransaction(istransaction),m_test(test),m_MyOffset(), m_iters(100) {
    	//availableOps = new vector<int>(operations, operations + sizeof(operations) / sizeof(operations[0]) );
    	m_funcname = m_test->getStringValue( "funcName" );
   }

    virtual bool doSetup(int32_t id) {
         return true;
    }

    virtual void doCleanup(int32_t id) {
    }
    virtual ~DoFETask() {
    }
    virtual uint32_t doTask(int32_t id) {
      char logmsg[2048];
      int opcode;
      std::vector<int> availableOps (operations, operations + sizeof(operations) / sizeof(operations[0]) );

      while(m_Run && (availableOps.size() != 0)){
    	ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_lock);
        bool doneWithOps = false;
		//bool rolledback = false;
        uint32_t i = GsRandom::random((uint32_t)0,( uint32_t)availableOps.size());
         try {
         opcode = availableOps[i];//getStringValue( "entryOps" );
         if (m_test->gettxManager() != NULLPTR) {
        	 m_test->gettxManager()->begin();
         }
         switch (opcode) {
         	 	 case ADD_NEW_KEY:
					 doneWithOps = addNewKeyFunction();
					 break;
				 /*case QUERY:
					 doneWithOps = queryFunction();
					 break;*/
				 case PUTALL_NEW_KEY:
					 doneWithOps = putAllNewKeyFunction();
					 break;
				 case INVALIDATE:
					 doneWithOps = invalidateFunction();
					 break;
				 case DESTROY:
					 doneWithOps = destroyFunction();
					 break;
				 case UPDATE_EXISTING_KEY:
					 doneWithOps = updateExistingKeyFunction();
					 break;
				 case GET:
					 doneWithOps = getFunction();
					 break;
				 case LOCAL_INVALIDATE:
					 doneWithOps = localInvalidateFunction();
					 break;
				 case LOCAL_DESTROY:
					 doneWithOps = localDestroyFunction();
					 break;
                  default: {
                	  sprintf( logmsg, "Invalid operation specified: %d.\n", opcode );
                	  throw new FwkException( logmsg );
                  }
               }
           if ((m_test->gettxManager() != NULLPTR) && (!doneWithOps)) {
             try {
            	 m_test->gettxManager()->commit();
           } catch (CommitConflictException e) {
                   // currently not expecting any conflicts ...
        	   throw Exception("Unexpected CommitConflictException : %s",  e.getMessage());
           }
		 }
	   }
	   catch ( TimeoutException &e ) {
    	   sprintf( logmsg, "Caught unexpected timeout exception during entry operation: %d  %s\n",opcode,e.getMessage() );
    	   throw TimeoutException(logmsg);
       } catch (IllegalStateException &e) {
    	   sprintf( logmsg, "Caught IllegalStateException during entry operation: %d  %s\n",opcode,e.getMessage() );
    	   throw IllegalStateException(logmsg);
    	} catch ( Exception &e ) {
    	   sprintf( logmsg, "Caught exception during entry operation: %d %s exiting task.\n" , opcode, e.getMessage());
    	   throw Exception( logmsg);
       }
       if (doneWithOps) {
         FWKINFO("Done with operation "<< opcode);
         if ((m_test->gettxManager() != NULLPTR))
         {
        	 try {
            m_test->gettxManager()->rollback();
        	 } catch (IllegalStateException &e) {
                	   sprintf( logmsg, "Caught IllegalStateException during rollback: %s\n",e.getMessage() );
                	   throw IllegalStateException(logmsg);
             }catch ( Exception &e ) {
          	   sprintf( logmsg, "Caught exception during entry operation: %s exiting task.\n" , e.getMessage());

          	   throw Exception( logmsg);
             }
         }
         availableOps.erase(availableOps.begin() + i);
      }
       //count++;
     }
       return 0;
   }
  private:
    void checkContainsValueForKey(CacheablePtr key, bool expected, std::string logStr);
    void verifyFEResult(CacheableVectorPtr exefuncResult, std::string funcName);
    bool addNewKeyFunction();
    bool putAllNewKeyFunction();
    bool invalidateFunction();
    bool destroyFunction();
    bool updateExistingKeyFunction();
    bool getFunction();
    bool localInvalidateFunction();
    bool localDestroyFunction();
    bool queryFunction();

 };
//--------------------DoOpsTask-----------------------
class SilenceListener : public CacheListener
{
public:

  SilenceListener( FrameworkTest * test ):
	  m_test( test )
  {
  }

  virtual void afterCreate( const EntryEvent& event ) {
  }

  virtual void afterUpdate( const EntryEvent& event ) {

  }

  virtual void afterInvalidate( const EntryEvent& event ) {

  }

  virtual void afterDestroy( const EntryEvent& event ) {
  }

  virtual void afterRegionInvalidate( const RegionEvent& event ){

  }

  virtual void afterRegionDestroy( const RegionEvent& event ) {

  }

  virtual void close( const RegionPtr& region ){}

  virtual ~SilenceListener() {

  }

private:

  FrameworkTest * m_test;
};

class MyResultCollector : public ResultCollector
{
  public:
   MyResultCollector():
     m_resultList(CacheableVector::create()),
     m_isResultReady(false),
     m_endResultCount(0),
     m_addResultCount(0),
     m_getResultCount(0)
   {
   }
   ~MyResultCollector()
   {
   }
   CacheableVectorPtr getResult(uint32_t timeout )
   {
     m_getResultCount++;
     if(m_isResultReady == true)
       return m_resultList;
     else
     {
       for(uint32_t i=0; i < timeout; i++)
       {
         if(m_isResultReady == true)
           return m_resultList;
       }
       throw FunctionExecutionException(
          "Result is not ready, endResults callback is called before invoking getResult() method");
       }
   }

   void addResult(CacheablePtr& resultItem)
   {
     m_addResultCount++;
     if(resultItem == NULLPTR)
       return;
     CacheableArrayListPtr result = dynCast<CacheableArrayListPtr>(resultItem);
     for(int32_t i=0; i < result->size(); i++)
     {
       m_resultList->push_back(result->operator[](i));
     }
   }
   void endResults()
   {
     m_isResultReady = true;
     m_endResultCount++;
   }
   uint32_t getEndResultCount()
   {
     return m_endResultCount;
   }
   uint32_t getAddResultCount()
   {
      return m_addResultCount;
   }
   uint32_t getGetResultCount()
   {
      return m_getResultCount;
   }

  private:
    CacheableVectorPtr m_resultList;
    volatile bool m_isResultReady;
    uint32_t m_endResultCount;
    uint32_t m_addResultCount;
    uint32_t m_getResultCount;
};

class MyResultCollectorHA : public ResultCollector
{
  public:
   MyResultCollectorHA():
     m_resultList(CacheableVector::create()),
     m_isResultReady(false),
     m_endResultCount(0),
     m_addResultCount(0),
     m_getResultCount(0)//,
     //m_gotLastBool(false)
   {
   }
   ~MyResultCollectorHA()
   {
   }
   CacheableVectorPtr getResult(uint32_t timeout )
   {
     m_getResultCount++;
     if(m_isResultReady == true)
       return m_resultList;
     else
     {
       for(uint32_t i=0; i < timeout; i++)
       {
         if(m_isResultReady == true)
           return m_resultList;
       }
       throw FunctionExecutionException(
          "Result is not ready, endResults callback is called before invoking getResult() method");
       }
   }

   void addResult(CacheablePtr& resultItem)
   {
      m_addResultCount++;
      if(resultItem == NULLPTR)
           return;
      CacheableStringPtr result;
      result = dynCast<CacheableStringPtr>(resultItem);
        m_resultList->push_back(result);
  }
   void endResults() {
		m_isResultReady = true;
		m_endResultCount++;
	}

	void clearResults() {
		m_resultList->clear();
	}

	uint32_t getEndResultCount() {
		return m_endResultCount;
	}
	uint32_t getAddResultCount() {
		return m_addResultCount;
	}
	uint32_t getGetResultCount() {
		return m_getResultCount;
	}
	/*bool gotLastBool()
	 {
	 return m_gotLastBool;
	 }*/

private:
	CacheableVectorPtr m_resultList;
	volatile bool m_isResultReady;
	uint32_t m_endResultCount;
	uint32_t m_addResultCount;
	uint32_t m_getResultCount;
	//bool m_gotLastBool;
};

//typedef SharedPtr<MyResultCollector> MyResultCollectorPtr;
class FunctionExecution : public FrameworkTest
{
public:
  FunctionExecution( const char * initArgs ) :FrameworkTest( initArgs ){}

  virtual ~FunctionExecution( void) {}

  void checkTest( const char * taskId );
  int32_t createRegion();
  int32_t createPools();
  int32_t loadRegion();
  int32_t addNewKeyFunction();
  int32_t doExecuteFunctions();
  int32_t doExecuteExceptionHandling();
  int32_t doExecuteFunctionsHA();
  int32_t ClearRegion();
  int32_t doOps();
  int32_t GetServerKeys();
  int32_t UpdateServerKeys();

private:
  RegionPtr getRegionPtr( const char * reg = NULL );
  void executeFunctionOnRegion(const char* ops);
  void verifyAddNewResult(CacheableVectorPtr exefuncResult, CacheableVectorPtr filterObj);
  void verifyResult(CacheableVectorPtr exefuncResult, CacheableVectorPtr filterObj,const char* ops);
  void reCreateEntry(CacheableVectorPtr filterObj,const char* ops,bool getresult);
  void executeFunction(CacheableVectorPtr filterObj,const char* ops);
  void doFireAndForgetFunctionExecution();
  void doParitionedRegionFunctionExecution();
  void doOnServersFunctionExcecution();
  void doReplicatedRegionFunctionExecution();

};

   } //   namespace functionexe
 } // namespace testframework
} // namespace gemfire
// ----------------------------------------------------------------------------

#endif // __FUNCTION_EXECUTION_TEST_HPP__
