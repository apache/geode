/*
* The PdxInstance QuickStart Example.
* This example takes the following steps:
*
* This example shows PdxInstanceFactory and PdxInstance usage.
*
* 1. Create a GemFire Cache.
* 2. Creates the PdxInstanceFactory for Person class.
* 3. Then creates instance of PdxInstance
* 4. It does put.
* 5. Then it does get and access it fields.
* 6. Close the Cache.
*
*/

// Include the GemFire library.
#include <gfcpp/GemfireCppCache.hpp>

// Use the "gemfire" namespace.
using namespace gemfire;

class Person {
 private:
  char* m_name;
  int m_id;
  int m_age;

 public:
  Person() {}

  Person(char* name, int id, int age) {
    m_name = name;
    m_id = id;
    m_age = age;
  }

  char* getName() const { return m_name; }
  int getID() { return m_id; }
  int getAge() { return m_age; }
};

// The PdxInstance QuickStart example.
int main(int argc, char** argv) {
  try {
    // Create a GemFire Cache.
    CacheFactoryPtr cacheFactory = CacheFactory::createCacheFactory();

    CachePtr cachePtr =
        cacheFactory->set("cache-xml-file", "XMLs/clientPdxInstance.xml")
            ->create();

    LOGINFO("Created the GemFire Cache");

    // Get the example Region from the Cache which is declared in the Cache XML
    // file.
    RegionPtr regionPtr = cachePtr->getRegion("Person");

    LOGINFO("Obtained the Region from the Cache.");

    Person* p = new Person("Jack", 7, 21);

    // PdxInstanceFactory for Person class
    PdxInstanceFactoryPtr pif = cachePtr->createPdxInstanceFactory("Person");
    LOGINFO("Created PdxInstanceFactory for Person class");

    pif->writeString("m_name", p->getName());
    pif->writeInt("m_id", p->getID());
    pif->markIdentityField("m_id");
    pif->writeInt("m_age", p->getAge());

    PdxInstancePtr pdxInstance = pif->create();

    LOGINFO("Created PdxInstance for Person class");

    regionPtr->put("Key1", pdxInstance);

    LOGINFO("Populated PdxInstance Object");

    PdxInstancePtr retPdxInstance = regionPtr->get("Key1");

    LOGINFO("Got PdxInstance Object");

    int id = 0;
    retPdxInstance->getField("m_id", id);

    int age = 0;
    retPdxInstance->getField("m_age", age);

    char* name = NULL;
    retPdxInstance->getField("m_name", &name);

    if (id == p->getID() && age == p->getAge() &&
        strcmp(name, p->getName()) == 0 &&
        retPdxInstance->isIdentityField("m_id")) {
      LOGINFO("PdxInstance returns all fields value expected");
    } else {
      LOGINFO("PdxInstance doesn't returns all fields value expected");
    }

    delete p;

    // Close the GemFire Cache.
    cachePtr->close();

    LOGINFO("Closed the GemFire Cache");

    return 0;
  }
  // An exception should not occur
  catch (const Exception& gemfireExcp) {
    LOGERROR("PdxInstance GemFire Exception: %s", gemfireExcp.getMessage());

    return 1;
  }
}
