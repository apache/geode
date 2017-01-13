/*
 * The PdxInstance QuickStart Example.
 * This example takes the following steps:
 *
 * This example shows IPdxInstanceFactory and IPdxInstance usage. 
 *
 * 1. Create a GemFire Cache.
 * 2. Creates the PdxInstanceFactory for Person class.
 * 3. Then creates instance of PdxInstance
 * 4. It does put.
 * 5. Then it does get and access it fields.
 * 6. Close the Cache.
 *
 */
// Use standard namespaces
using System;
using System.Reflection;
// Use the GemFire namespace
using GemStone.GemFire.Cache.Generic;


namespace GemStone.GemFire.Cache.Generic.QuickStart
{
  public class Person
  {
    private string name;
    //this is the only field used on server to create hashcode and use in equals method
    [PdxIdentityField]
    private int id;
    private int age;
    
    public Person() { }

    public Person(string name, int id, int age)
    {
      this.name = name;
      this.id = id;
      this.age = age;
    }
    
    #region Public Properties
    public string Name
    {
      get { return name; }
    }
    public int ID
    {
      get { return id; }
    }
    public int Age
    {
      get { return age; }
    }
    #endregion
  }   
  // The PdxInstance QuickStart example.
  class PdxInstance
  {
    static void Main(string[] args)
    {
      try
      {

        CacheFactory cacheFactory = CacheFactory.CreateCacheFactory();

        Console.WriteLine("Connected to the GemFire Distributed System");

        // Create a GemFire Cache with the "clientPdxRemoteQuery.xml" Cache XML file.
        // Set SetPdxReadSerialized to true to access PdxInstance
        Cache cache = cacheFactory.Set("cache-xml-file", "XMLs/clientPdxInstance.xml").Create();

        Console.WriteLine("Created the GemFire Cache");

        // Get the example Region from the Cache which is declared in the Cache XML file.
        IRegion<string, IPdxInstance> region = cache.GetRegion<string, IPdxInstance>("Person");

        Console.WriteLine("Obtained the Region from the Cache");

        Person p = new Person("Jack", 7, 21);
        
        //PdxInstanceFactory for Person class
        IPdxInstanceFactory pif = cache.CreatePdxInstanceFactory("Person");
        
        pif.WriteString("name", p.Name);
        pif.WriteInt("id", p.ID);
        pif.MarkIdentityField("id");
        pif.WriteInt("age", p.Age);
        
        IPdxInstance pdxInstance = pif.Create();
        
        Console.WriteLine("Created PdxInstance for Person class");
        
        region["Key1"] = pdxInstance;

        Console.WriteLine("Populated PdxInstance Object");

        IPdxInstance retPdxInstance = region["Key1"];

        if((int)retPdxInstance.GetField("id") == p.ID
             && (int)retPdxInstance.GetField("age") == p.Age
               && (string)retPdxInstance.GetField("name") == p.Name 
                 && retPdxInstance.IsIdentityField("id") == true)
           Console.WriteLine("PdxInstance returns all fields value expected");
        else
           Console.WriteLine("PdxInstance doesn't returns all fields value expected");
        
        // Close the GemFire Cache.
        cache.Close();

        Console.WriteLine("Closed the GemFire Cache");

      }
      // An exception should not occur
      catch (GemFireException gfex)
      {
        Console.WriteLine("PdxInstance GemFire Exception: {0}", gfex.Message);
      }
    }
  }
}
