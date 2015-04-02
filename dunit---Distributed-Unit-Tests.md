The dunit framework is an extension of junit. You should be familiar with writing junit tests. GemFire currently is using junit 4.  dunit tests may be run via gradle (distributedTest task) or in your IDE (run as a junit test).

The base class for all dunit tests is DistributedTestCase. However the most common way to write a dunit test is to extend CacheTestCase, which provides helper methods to create a cache.

Here's a sample test with some comments describing what each part of the tests does. This is simplified version of PdxSerializableDUnitTest.

    /**
    * By extending cache test case, we pick up the setUp and tearDown methods from that base class, which
    * take care of cleaning up the cache on all vms after the test is done.
    * CacheTestCase also provides a helper method to get a cache: getCache().
    *
    * getCache() will get or create a cache, connecting it up with the other dunit vms.
    */
    public class PdxSerializableDUnitTest extends CacheTestCase {
    
    public PdxSerializableDUnitTest(String name) {
        super(name);
    }

    public void testSimplePut() {
        
        // This is how we access the other vms. The framework has already launched these vms
        // Now, we fetch VM objects from the host singleton. The VM object is an RMI stub
        // which lets us execute code in that vm.
        Host host = Host.getHost(0);
        VM vm1 = host.getVM(0);
        VM vm2 = host.getVM(1);
        VM vm3 = host.getVM(2);
        
        // Here, we're creating a callable to execute in another VM.
        // SerializableRunnable and SerializableCallable are helpful base classes to create
        // a runnable that we can send to another VM.
        SerializableCallable createRegion = new SerializableCallable() {
        public Object call() throws Exception {
            AttributesFactory af = new AttributesFactory();
            af.setScope(Scope.DISTRIBUTED_ACK);
            af.setDataPolicy(DataPolicy.REPLICATE);
            // this particular code uses a helper method from CacheTestCase to create a region in the cache.
            createRootRegion("testSimplePdx", af.create());
            return null;
        } };
        
        // By calling invoke on the RM stub, the framework ships the callable to the remote VM
        // and executes the code
        // By executing this code in three VMs, we are creating the cache and region in each of the VMs
        // These caches will be part of the same distributed system because the framework has taken care
        // of setting up the connection to the distributed system.
        vm1.invoke(createRegion);
        vm2.invoke(createRegion);
        vm3.invoke(createRegion);
        
        // Now, we can perform a put and make an assertion in the remote VM.
        vm1.invoke(new SerializableCallable() {
            public Object call() throws Exception {
                // Check to make sure the type region is not yet created
                Region r = getRootRegion("testSimplePdx");
                r.put(1, new SimpleClass(57, (byte) 3));
                // Ok, now the type registry should exist
                assertNotNull(getRootRegion(PeerTypeRegistration.REGION_NAME));
                return null;
            } });
        }
    }

