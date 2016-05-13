/*
* The PdxSerializer QuickStart Example.
* This example takes the following steps:
*
* This example shows PdxSerializer usage. 
*
* 1. Create a GemFire Cache.
* 2. Get the Person from the Cache.
* 3. Populate some query Person objects on the Region.
* 4. Get the pool, get the Query Service from Pool. Pool is define in clientPdxSerializer.xml. 
* 5. Execute a query that returns a Result Set.
* 6. Execute a query that returns a Struct Set.
* 7. Execute the region shortcut/convenience query methods.
* 8. Close the Cache.
*
*/


// Include the GemFire library.
#include <gfcpp/GemfireCppCache.hpp>

// Use the "gemfire" namespace.
using namespace gemfire;

static const char * CLASSNAME = "com.example.Person";

//This demonstrates Person domain class
class Person
{
private:
  char* m_name;    
  int m_id;
  int m_age;

public:
  Person() { }

  Person(char* name, int id, int age)
  {
    m_name = name;
    m_id = id;
    m_age = age;
  }

  char* getName() const
  {
    return m_name;
  }
  int getID()
  {
    return m_id;
  }
  int getAge()
  {
    return m_age;
  }
  void setName(char* name)
  {
    m_name = name;
  }
  void setID(int id)
  {
    m_id = id;
  }
  void setAge(int age)
  {
    m_age = age;
  }
};

//This demonstrates, how to extend PdxSerializer without modifying Person domain object, necessary for serialization/desrialization to PDX format.
class PersonPdxSerializer : public PdxSerializer
{
public:

  static void deallocate(void * testObject, const char * className)
  {    
    LOGINFO("PersonPdxSerializer::deallocate called");
    if (strcmp(className, CLASSNAME) == 0) {
      Person* per = reinterpret_cast<Person*>(testObject);
      delete per;
    }
  }

  static uint32_t objectSize(void * testObject, const char * className)
  {    
    LOGINFO("PersonPdxSerializer::objectSize called");
    if (strcmp(className, CLASSNAME) == 0) {
      Person* per = reinterpret_cast<Person*>(testObject);
      uint32_t size = 0;
      size += sizeof(Person);
      if (per->getName() != NULL) {
        size += strlen(per->getName());
      }
      return size;
    }
    return 0;
  }

  UserDeallocator getDeallocator(const char * className)
  {    
    if (strcmp(className, CLASSNAME) == 0) {
      return deallocate;
    }
    return NULL;
  }

  UserObjectSizer getObjectSizer(const char * className)
  {   
    if (strcmp(className, CLASSNAME) == 0) {
      return objectSize;
    }
    return NULL;
  }

  void * fromData(const char * className, PdxReaderPtr pr)
  { 
    if (strcmp(className, CLASSNAME) == 0) {
      Person* per = new Person();

      try
      {
        per->setID(pr->readInt("m_id"));
        per->setAge(pr->readInt("m_age"));
        per->setName(pr->readString("m_name"));
      }
      catch (...)
      {
        return NULL;
      }
      return (void*) per;
    }
    return NULL;    
  }  

  bool toData(void * testObject, const char * className, PdxWriterPtr pw)
  {
    if (strcmp(className, CLASSNAME) == 0) {

      Person* per = reinterpret_cast<Person*>(testObject);

      try
      {
        pw->writeInt("m_id" ,per->getID());
        pw->writeInt("m_age" ,per->getAge());
        pw->writeString("m_name" ,per->getName());
      }
      catch (...)
      {
        return false;
      }
      return true;
    }    
    return false;
  }
};

// This PdxSerializer QuickStart example demonstrartes query on .NET objects without having corresponding java classes at server.
int main(int argc, char ** argv)
{
  try
  {
    // Create a GemFire Cache.
    CacheFactoryPtr cacheFactory = CacheFactory::createCacheFactory();

    // Create a GemFire Cache with the "clientPdxSerializer.xml" Cache XML file.
    CachePtr cachePtr = cacheFactory->set("cache-xml-file", "XMLs/clientPdxSerializer.xml")->create();          

    LOGINFO("Created the GemFire Cache");

    // Get the example Region from the Cache which is declared in the Cache XML file.
    RegionPtr regionPtr = cachePtr->getRegion("Person");       

    LOGINFO("Obtained the Region from the Cache.");

    // Register PersonPdxSerializer to serialize the domain types(Person class) as pdx format
    Serializable::registerPdxSerializer(PdxSerializerPtr(new PersonPdxSerializer));    
    LOGINFO("Registered Person Query Objects");

    // Populate the Region with some Person objects.
    Person* p1 = new Person("John", 1 /*ID*/, 23 /*age*/);
    PdxWrapperPtr pdxobj1(new PdxWrapper(p1, CLASSNAME));
    regionPtr->put("Key1", pdxobj1);

    Person* p2 = new Person("Jack", 2 /*ID*/, 20 /*age*/);
    PdxWrapperPtr pdxobj2(new PdxWrapper(p2, CLASSNAME));
    regionPtr->put("Key2", pdxobj2);

    Person* p3 = new Person("Tony", 3 /*ID*/, 35 /*age*/);
    PdxWrapperPtr pdxobj3(new PdxWrapper(p3, CLASSNAME));
    regionPtr->put("Key3", pdxobj3);

    LOGINFO("Populated some Person Objects through PdxWrapper");

    //find the pool
    PoolPtr poolPtr = PoolManager::find("examplePool");

    // Get the QueryService from the Pool.
    QueryServicePtr qrySvcPtr = poolPtr->getQueryService();

    LOGINFO("Got the QueryService from the Pool");

    // Execute a Query which returns a ResultSet.    
    QueryPtr qryPtr = qrySvcPtr->newQuery("SELECT DISTINCT * FROM /Person");
    SelectResultsPtr resultsPtr = qryPtr->execute();

    LOGINFO("ResultSet Query returned %d rows", resultsPtr->size());

    // Execute a Query which returns a StructSet.
    qryPtr = qrySvcPtr->newQuery("SELECT m_name, m_age FROM /Person WHERE m_id = 1");
    resultsPtr = qryPtr->execute();

    LOGINFO("StructSet Query returned %d rows", resultsPtr->size());

    // Iterate through the rows of the query result.
    int rowCount = 0;
    SelectResultsIterator iter = resultsPtr->getIterator();
    while (iter.hasNext())
    {
      rowCount++;
      Struct * psi = dynamic_cast<Struct*>( iter.next().ptr() );
      LOGINFO("Row %d Column 1 is named %s, value is %S", rowCount, psi->getFieldName(0), (*psi)[0]->toString()->asWChar());
      LOGINFO("Row %d Column 2 is named %s, value is %s", rowCount, psi->getFieldName(1), (*psi)[1]->toString()->asChar());
    }

    // Execute a Region Shortcut Query (convenience method).
    resultsPtr = regionPtr->query("m_id = 2");

    LOGINFO("Region Query returned %d rows", resultsPtr->size()); 

    // Execute the Region selectValue() API.
    PdxWrapperPtr pdxWrapperPtr = regionPtr->selectValue("m_id = 3");
    Person* per = reinterpret_cast<Person*>(pdxWrapperPtr->getObject());

    LOGINFO("Region selectValue() returned an item:\n Person Name = %s and Person Age = %d", per->getName(), per->getAge());

    // Execute the Region existsValue() API.
    bool existsValue = regionPtr->existsValue("m_id = 4");

    LOGINFO("Region existsValue() returned %s", existsValue ? "true" : "false");

    // Close the GemFire Cache.
    cachePtr->close();

    LOGINFO("Closed the GemFire Cache");

  }
  // An exception should not occur
  catch(const Exception & gemfireExcp)
  {    
    LOGERROR("PdxSerializer GemFire Exception: %s", gemfireExcp.getMessage());
  }
}

