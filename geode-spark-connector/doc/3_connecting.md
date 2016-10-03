## Connecting to Geode

There are two ways to connect Spark to Geode:
 - Specify Geode connection properties via `SparkConf`.
 - Specify Geode connection properties via `GeodeConnectionConf`.

### Specify Geode connection properties via `SparkConf`
The only required Geode connection property is `spark.geode.locators`. 
This can be specified in `<spark dir>/conf/spark-defaults.conf` or in Spark 
application code. In the following examples, we assume you want to provide
3 extra properties: `security-client-auth-init`, `security-username`, and 
`security-password`, note that they are prefixed with `spark.geode.`.
 
In `<spark dir>/conf/spark-defaults.com`
```
spark.geode.locators=192.168.1.47[10334]
spark.geode.security-client-auth-init=org.apache.geode.security.templates.UserPasswordAuthInit.create
spark.geode.security-username=scott
spark.geode.security-password=tiger
```
 
Or in the Spark application code:
```
import org.apache.geode.spark.connector._
val sparkConf = new SparkConf()
  .set(GeodeLocatorPropKey, "192.168.1.47[10334]")
  .set("spark.geode.security-client-auth-init", "org.apache.geode.security.templates.UserPasswordAuthInit.create")
  .set("spark.geode.security-username", "scott")
  .set("spark.geode.security-password", "tiger")
```

After this, you can use all connector APIs without providing `GeodeConnectionConf`.
 
### Specify Geode connection properties via `GeodeConnectionConf`
Here's the code that creates `GeodeConnectionConf` with the same set of 
properties as the examples above:
```
val props = Map("security-client-auth-init" -> "org.apache.geode.security.templates.UserPasswordAuthInit.create",
                "security-username" -> "scott",
                "security-password" -> "tiger")
val connConf = GeodeConnectionConf("192.168.1.47[10334]", props)
``` 

Please note that those properties are **not** prefixed with `spark.geode.`.

After this, you can use all connector APIs that require `GeodeConnectionConf`.

### Notes about locators
 - You can specify locator in two formats: `host[port]` or `host:port`. For
   example `192.168.1.47[10334]` or `192.168.1.47:10334`
 - If your Geode cluster has multiple locators, list them all and separated
   by `,`. For example: `host1:10334,host2:10334`.


Next: [Loading Data from Geode](4_loading.md)
