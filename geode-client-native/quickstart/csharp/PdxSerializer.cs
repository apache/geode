/*
 * The PdxSerializer QuickStart Example.
 * This example takes the following steps:
 *
 * This example shows IPdxSerializer usage. 
 * We have used inbuilt ReflectionBasedAutoSerializer as IPdxSerializer. ReflectionBasedAutoSerializer class implements the IPdxSerializer interface
 * This auto serializer uses reflection to serialize and de-serialize class members.
 * User can use PdxIdentityField attribute on member fields to define field as identity field.
 * ReflectionBasedAutoSerializer will use this attribute to set field as identity field for Pdx data.
 *
 * AutoSerializerEx class extends the ReflectionBasedAutoSerializer class and override WriteTransform and ReadTransform methods.
 * In this method it handles serialization of .NET decimal type and Guid type.
 * 
 * PdxTypeMapper class demonstrates that how app can map .NET types to pdx types(java types)
 * 
 * Domain class should have default constructor(zero-arg).
 *
 * After that test demonstrartes query on .NET objects without having corresponding java classes at server.
 *
 * 1. Create a GemFire Cache.
 * 2. Get the Person from the Cache.
 * 3. Populate some query Person objects on the Region.
 * 4. Get the pool, get the Query Service from Pool. Pool is define in clientPdxRemoteQuery.xml. 
 * 5. Execute a query that returns a Result Set.
 * 6. Execute a query that returns a Struct Set.
 * 7. Execute the region shortcut/convenience query methods.
 * 8. Close the Cache.
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
    //private decimal salary;
    private Guid guid = Guid.NewGuid();

    public Person() { }

    public Person(string name, int id, int age)
    {
      this.name = name;
      this.id = id;
      this.age = age;
      //this.salary = 217773.12321M;
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
  
  //This demonstrates, how to extend ReflectionBasedAutoSerializer
  public class AutoSerializerEx : ReflectionBasedAutoSerializer
  {
    public override object WriteTransform(FieldInfo fi, Type type, object originalValue)
    {
      if (fi.FieldType.Equals(Type.GetType("System.Guid")))
      {
        return originalValue.ToString();
      }
      else if (fi.FieldType.Equals(Type.GetType("System.Decimal")))
      {
        return originalValue.ToString();
      }
      else
        return base.WriteTransform(fi, type, originalValue);
    }
    
    public override object ReadTransform(FieldInfo fi, Type type, object serializeValue)
    {
      if (fi.FieldType.Equals(Type.GetType("System.Guid")))
      {
        Guid g = new Guid((string)serializeValue);
        return g;
      }
      else if (fi.FieldType.Equals(Type.GetType("System.Decimal")))
      {
        return Convert.ToDecimal((string)serializeValue);
      }
      else
        return base.ReadTransform(fi, type, serializeValue);
    }
    
    public override FieldType GetFieldType(FieldInfo fi, Type type)
    {
      if (fi.FieldType.Equals(Type.GetType("System.Guid")) || fi.FieldType.Equals(Type.GetType("System.Decimal")))
        return FieldType.STRING;
      return base.GetFieldType(fi, type);
    }
    
    public override bool IsIdentityField(FieldInfo fi, Type type)
    {
      if (fi.Name == "_identityField")
        return true;
      return base.IsIdentityField(fi, type);
    }
    public override string GetFieldName(FieldInfo fi, Type type)
    {
      if (fi.Name == "_nameChange")
        return fi.Name + "NewName";

      return fi.Name ;
    }

    public override bool IsFieldIncluded(FieldInfo fi, Type type)
    {
      if (fi.Name == "_notInclude")
        return false;
      return base.IsFieldIncluded(fi, type);
    }
  }
  
  //This demonstrates, how to map .NET type to pdx type or java type
  public class PdxTypeMapper : IPdxTypeMapper
  { 
    public string ToPdxTypeName(string localTypeName)
    {
      return "pdx_" + localTypeName;
    }

    public string FromPdxTypeName(string pdxTypeName)
    {
      return pdxTypeName.Substring(4);//need to extract "pdx_"
    }
  }
  
  // The PdxRemoteQuery QuickStart example.
  class PdxSerializer
  {
    static void Main(string[] args)
    {
      try
      {

        CacheFactory cacheFactory = CacheFactory.CreateCacheFactory();

        Console.WriteLine("Connected to the GemFire Distributed System");

        // Create a GemFire Cache with the "clientPdxRemoteQuery.xml" Cache XML file.
        Cache cache = cacheFactory.Set("cache-xml-file", "XMLs/clientPdxSerializer.xml").Create();

        Console.WriteLine("Created the GemFire Cache");

        // Get the example Region from the Cache which is declared in the Cache XML file.
        IRegion<string, Person> region = cache.GetRegion<string, Person>("Person");

        Console.WriteLine("Obtained the Region from the Cache");

        //to map .net type tp pdx type or java type
        Serializable.SetPdxTypeMapper(new PdxTypeMapper());
        
        // Register inbuilt reflection based autoserializer to serialize the domain types(Person class) as pdx format
        Serializable.RegisterPdxSerializer(new AutoSerializerEx());
        Console.WriteLine("Registered Person Query Objects");

        // Populate the Region with some PortfolioPdx objects.
        Person p1 = new Person("John", 1 /*ID*/, 23 /*age*/);
        Person p2 = new Person("Jack", 2 /*ID*/, 20 /*age*/);
        Person p3 = new Person("Tony", 3 /*ID*/, 35 /*age*/);
        
        region["Key1"] = p1;
        region["Key2"] = p2;
        region["Key3"] = p3;

        Console.WriteLine("Populated some Person Objects");

        //find the pool
        Pool pool = PoolManager.Find("examplePool");

        // Get the QueryService from the pool
        QueryService<string, Person> qrySvc = pool.GetQueryService<string, Person>();

        Console.WriteLine("Got the QueryService from the Pool");

        // Execute a Query which returns a ResultSet.    
        Query<Person> qry = qrySvc.NewQuery("SELECT DISTINCT * FROM /Person");
        ISelectResults<Person> results = qry.Execute();

        Console.WriteLine("ResultSet Query returned {0} rows", results.Size);

        // Execute a Query which returns a StructSet.
        QueryService<string, Struct> qrySvc1 = pool.GetQueryService<string, Struct>();
        Query<Struct> qry1 = qrySvc1.NewQuery("SELECT name, age FROM /Person WHERE id = 1");
        ISelectResults<Struct> results1 = qry1.Execute();

        Console.WriteLine("StructSet Query returned {0} rows", results1.Size);

        // Iterate through the rows of the query result.
        int rowCount = 0;
        foreach (Struct si in results1)
        {
          rowCount++;
          Console.WriteLine("Row {0} Column 1 is named {1}, value is {2}", rowCount, si.Set.GetFieldName(0), si[0].ToString());
          Console.WriteLine("Row {0} Column 2 is named {1}, value is {2}", rowCount, si.Set.GetFieldName(1), si[1].ToString());
        }

        
        // Close the GemFire Cache.
        cache.Close();

        Console.WriteLine("Closed the GemFire Cache");

      }
      // An exception should not occur
      catch (GemFireException gfex)
      {
        Console.WriteLine("PdxSerializer GemFire Exception: {0}", gfex.Message);
      }
    }
  }
}
