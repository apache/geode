/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 * This example measures the time it takes for a client caching operation to 
 * return in a hierarchical cache configuration. 
 *
 * While this example uses a console application, it is not a requirement.
 *
 * Please note that this example requires that the BenchmarkHierarchicalServer
 * be running prior to execution.  To start the BenchmarkHierarchicalServer
 * QuickStart example, please refer to the GemFire Quickstart documentation.
//////////////////////////////////////////////////////////////////////////////*/
using System;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

using GemStone.GemFire.Cache;

public class BenchmarkHierarchicalClient
{
  [STAThread()]
  public static void Main()
  {
    /* Total number of benchmark samples to benchmark and the number of puts
     * to make for each sample.
    */
    const int cnBenchmarkedSamples = 60,
              cnOperationsPerSample = 5000;

    DistributedSystem MyDistributedSystem = null;
    Cache MyCache = null;

    try
    {
      DateTime[] BenchmarkedItemTimes = new DateTime[cnBenchmarkedSamples];

      // Determine what the serialized overhead is.
      MemoryStream SerializedStream = new MemoryStream();

      new BinaryFormatter().Serialize(SerializedStream, new byte[0]);

      /* The payload size is done in this manner because we want a 1KB size,
       * and, therefore, the overhead must be backed out of the overall length.
      */
      byte[] Payload = new byte[1024 - SerializedStream.Length];

      SerializedStream.Close();

      DateTime StartingTime;

      Console.WriteLine("* Connecting to the distributed system and creating the cache.");

      /* Properties can be passed to GemFire through two different mechanisms: the 
       * Properties object as is done below or the gfcpp.properties file. The 
       * settings passed in a Properties object take precedence over any settings
       * in a file. This way the end-user cannot bypass any required settings.
       * 
       * Using a gfcpp.properties file can be useful when you want to change the 
       * behavior of certain settings without doing a new build, test, and deploy cycle.
       * 
       * See gfcpp.properties for details on some of the other settings used in this 
       * project.
      */
      Properties DistributedSystemProperties = new Properties();

      DistributedSystemProperties.Insert("log-file", "C:/temp/benchmarkClient.log");
      DistributedSystemProperties.Insert("log-level", "debug");

      // Set the name used to identify the member of the distributed system.
      DistributedSystemProperties.Insert("name", "BenchmarkHierarchicalClient");

      /* Specify the file whose contents are used to initialize the cache when it is created.
       * 
       * An XML file isn't needed at all because everything can be specified in  code--much
       * as the "license-file" property is. However, it provides a convenient way
       * to isolate common settings that can be updated without a build/test/deploy cycle.
      */
      DistributedSystemProperties.Insert("cache-xml-file", "BenchmarkHierarchicalClient.xml");

      /* Define where the license file is located. It is very useful to do this in
       * code vs. the gemfire.properties file, because it allows you to access the
       * license used by the GemFire installation (as pointed to by the GEMFIRE
       * environment variable).
      */
      DistributedSystemProperties.Insert("license-file", "../../gfCppLicense.zip");

      // Connect to the GemFire distributed system.
      MyDistributedSystem = DistributedSystem.Connect("BenchmarkClient", DistributedSystemProperties);

      // Create the cache. This causes the cache-xml-file to be parsed.
      MyCache = CacheFactory.Create("BenchmarkClient", MyDistributedSystem);


      // Get the example region which is a subregion of /root
      Region MyExampleRegion = MyCache.GetRegion("/root/exampleRegion");

      Console.WriteLine("{0}* Region, {1}, was opened in the cache.{2}",
        Environment.NewLine, MyExampleRegion.FullPath, Environment.NewLine);

      Console.WriteLine("Please wait while the benchmark tests are performed.");

      StartingTime = System.DateTime.Now;

      // Perform benchmark until cnBenchmarkedSamples are executed
      for (int nCurrentBenchmarkedItem = 0; nCurrentBenchmarkedItem < cnBenchmarkedSamples; nCurrentBenchmarkedItem++)
      {
        for (int nOperations = 0; nOperations < cnOperationsPerSample; nOperations++)
        {
          /* Perform the serialization every time to more accurately 
           * represent the normal behavior of an application.
           * 
           * Optimize performance by allocating memory for 1KB.
          */
          MyExampleRegion.Put("Key3", CacheableBytes.Create(Payload));

        }

        BenchmarkedItemTimes[nCurrentBenchmarkedItem] = System.DateTime.Now;
      }

      Console.WriteLine("{0}Finished benchmarking. Analyzing the results.",
        Environment.NewLine);

      long nTotalOperations = cnBenchmarkedSamples * cnOperationsPerSample;

      // Calculate the total time for the benchmark.
      TimeSpan BenchmarkTimeSpan = BenchmarkedItemTimes[cnBenchmarkedSamples - 1] - StartingTime;

      // Find the best sample.
      TimeSpan BestSampleTime = BenchmarkedItemTimes[0] - StartingTime;

      for (int nCurrentSample = 1; nCurrentSample < BenchmarkedItemTimes.Length; nCurrentSample++)
      {
        // Evaluation the sample's time with the sample preceding it.
        TimeSpan CurrentSampleTime = BenchmarkedItemTimes[nCurrentSample] - BenchmarkedItemTimes[nCurrentSample - 1];

        if (CurrentSampleTime < BestSampleTime)
        {
          BestSampleTime = CurrentSampleTime;
        }
      }

      Console.WriteLine("{0}Benchmark Statistics:", Environment.NewLine);
      Console.WriteLine("\tNumber of Samples: {0}", cnBenchmarkedSamples);
      Console.WriteLine("\t1KB Operations/Sample: {0}", cnOperationsPerSample);
      Console.WriteLine("\tTotal 1KB Operations: {0}", nTotalOperations);
      Console.WriteLine("\tTotal Time: {0:N2} seconds",
        BenchmarkTimeSpan.TotalSeconds);

      Console.WriteLine("{0}Benchmark Averages (Mean):", Environment.NewLine);
      Console.WriteLine("\tKB/Second: {0:N2}",
        nTotalOperations / BenchmarkTimeSpan.TotalSeconds);
      Console.WriteLine("\tBytes/Second: {0:N2}",
        (1024 * nTotalOperations) / BenchmarkTimeSpan.TotalSeconds);
      Console.WriteLine("\tMilliseconds/KB: {0:N2}",
        BenchmarkTimeSpan.TotalMilliseconds / nTotalOperations);
      Console.WriteLine("\tNanoseconds/KB: {0}",
        BenchmarkTimeSpan.Ticks / nTotalOperations);
      Console.WriteLine("\tNanoseconds/Byte: {0:N2}",
        BenchmarkTimeSpan.Ticks / (1024D * nTotalOperations));

      Console.WriteLine("{0}Best Benchmark Results:", Environment.NewLine);
      Console.WriteLine("\tKB/Second = {0:N2}",
        cnOperationsPerSample / BestSampleTime.TotalSeconds);
      Console.WriteLine("\tBytes/Second = {0:N2}",
        (1024 * cnOperationsPerSample) / BestSampleTime.TotalSeconds);
      Console.WriteLine("\tMilliseconds/KB = {0:N2}",
        BestSampleTime.TotalMilliseconds / cnOperationsPerSample);
      Console.WriteLine("\tNanoseconds/KB: {0}",
        BestSampleTime.Ticks / cnOperationsPerSample);
      Console.WriteLine("\tNanoseconds/Byte: {0:N2}",
        BestSampleTime.Ticks / (1024D * cnOperationsPerSample));

      // Keep the console active until <Enter> is pressed.
      Console.WriteLine("{0}---[ Press <Enter> to End the Application ]---",
        Environment.NewLine);
      Console.ReadLine();
    }
    catch (Exception ThrownException)
    {
      Console.Error.WriteLine(ThrownException.Message);
      Console.Error.WriteLine("---[ Press <Enter> to End the Application ]---");
      Console.Error.WriteLine(ThrownException.StackTrace);
      Console.ReadLine();
    }
    finally
    {
      /* While there are not any ramifications of terminating without closing the cache
       * and disconnecting from the distributed system, it is considered a best practice
       * to do so.
      */
      try
      {
        Console.WriteLine("Closing the cache and disconnecting.{0}",
          Environment.NewLine);
      }
      catch {/* Ignore any exceptions */}

      try
      {
        /* Close the cache. This terminates the cache and releases all the resources. 
         * Generally speaking, after a cache is closed, any further method calls on 
         * it or region object will throw an exception.
        */
        MyCache.Close();
      }
      catch {/* Ignore any exceptions */}
    }
  }
}
