## Connecting to Geode

There are two ways to connect Spark to Geode:
 - Specify Geode connection properties via `SparkConf`.
 - Specify Geode connection properties via `GemFireConnectionConf`.

### Specify Geode connection properties via `SparkConf`
The only required Geode connection property is `spark.gemfire.locators`. 
This can be specified in `<spark dir>/conf/spark-defaults.conf` or in Spark 
application code. In the following examples, we assume you want to provide
3 extra properties: `security-client-auth-init`, `security-username`, and 
`security-password`, note that they are prefixed with `spark.gemfire.`.
 
In `<spark dir>/conf/spark-defaults.com`
```
spark.gemfire.locators=192.168.1.47[10334]
spark.gemfire.security-client-auth-init=templates.security.UserPasswordAuthInit.create
spark.gemfire.security-username=scott
spark.gemfire.security-password=tiger
```
 
Or in the Spark application code:
```
import io.pivotal.gemfire.spark.connector._
val sparkConf = new SparkConf()
  .set(GemFireLocatorPropKey, "192.168.1.47[10334]")
  .set("spark.gemfire.security-client-auth-init", "templates.security.UserPasswordAuthInit.create")
  .set("spark.gemfire.security-username", "scott")
  .set("spark.gemfire.security-password", "tiger")
```

After this, you can use all connector APIs without providing `GemfireConnectionConf`.
 
### Specify Geode connection properties via `GemFireConnectionConf`
Here's the code that creates `GemFireConnectionConf` with the same set of 
properties as the examples above:
```
val props = Map("security-client-auth-init" -> "templates.security.UserPasswordAuthInit.create",
                "security-username" -> "scott",
                "security-password" -> "tiger")
val connConf = GemFireConnectionConf("192.168.1.47[10334]", props)
``` 

Please note that those properties are **not** prefixed with `spark.gemfire.`.

After this, you can use all connector APIs that require `GemFireConnectionConf`.

### Notes about locators
 - You can specify locator in two formats: `host[port]` or `host:port`. For
   example `192.168.1.47[10334]` or `192.168.1.47:10334`
 - If your Geode cluster has multiple locators, list them all and separated
   by `,`. For example: `host1:10334,host2:10334`.


Next: [Loading Data from Geode](4_loading.md)
