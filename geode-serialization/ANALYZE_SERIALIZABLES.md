# Analyze Serializables

## Adding AnalyzeSerializables test to a new geode module

If you've created a new geode module, then add an integration test to detect any additions or changes to serializable classes in the module.

Change the module's `build.gradle` to add a dependency on geode-serialization:
```
integrationTestImplementation(project(':geode-serialization'))
```

Create `AnalyzeModuleSerializablesIntegrationTest` that extends `AnalyzeSerializablesJUnitTestBase`. It needs to be in package `org.apache.geode.codeAnalysis`.

## Implementing SanctionedSerializablesService

If you've changed or added any serializable classes to a geode module, the previously added integration test should fail. You'll need to implement `SanctionedSerializablesService` for the geode module.

Change the module's `build.gradle` to add a dependency on geode-serialization:
```
implementation(project(':geode-serialization'))
```

Create a new `ModuleSanctionedSerializablesService` that implements `SanctionedSerializablesService`:
```
geode-module/src/main/java/org/apache/geode/module/internal/ModuleSanctionedSerializablesService.java
```

Add a service file for `SanctionedSerializablesService`:
```
geode-module/src/main/resources/META-INF/services/org.apache.geode.internal.serialization.SanctionedSerializablesService
```

Add a line to the service file specifying the fully qualified name of the service implementation:
```
org.apache.geode.module.internal.ModuleSanctionedSerializablesService
```

Update `AnalyzeModuleSerializablesIntegrationTest` to return the new `SanctionedSerializablesService` implementation from `getModuleClass`: 
```java
@Override
protected Optional<Class<?>> getModuleClass() {
  return Optional.of(ModuleSanctionedSerializablesService.class);
}
```

## Fixing failures in AnalyzeModuleSerializablesIntegrationTest

`AnalyzeModuleSerializablesIntegrationTest` analyzes serializable classes in a module. It fails if either:
- a new serializable class is added to the main src
- an existing serializable class in main src is modified

The content of the failure message depends on whether the module implements `SanctionedSerializablesService`.

If it implements `SanctionedSerializablesService`, the failure message looks like:
```
New or moved classes----------------------------------------
org/apache/geode/CancelException,true,3215578659523282642

If the class is not persisted or sent over the wire, add it to the file
    /path/to/geode/geode-core/src/integrationTest/resources/org/apache/geode/codeAnalysis/excludedClasses.txt
Otherwise, if this doesn't break backward compatibility, copy the file
    /path/to/geode/geode-core/build/integrationTest/test-worker-000009/actualSerializables.dat
    to 
    /path/to/geode/geode-core/src/main/resources/org/apache/geode/internal/sanctioned-geode-core-serializables.txt
If this potentially breaks backward compatibility, follow the instructions in
    geode-serialization/ANALYZE_SERIALIZABLES.md
```

If the module does not implement `SanctionedSerializablesService`, the failure message looks like:
```
New or moved classes----------------------------------------
org/apache/geode/tools/pulse/internal/security/GemFireAuthentication,true,550: new class

If the class is not persisted or sent over the wire, add it to the file
    /path/to/geode/geode-pulse/src/integrationTest/resources/org/apache/geode/codeAnalysis/excludedClasses.txt
Otherwise, follow the instructions in
    geode-serialization/ANALYZE_SERIALIZABLES.md
```

### If the module does not implement SanctionedSerializablesService

Fixing will require determining if the class will actually go on the wire or not.

If the class is neither persisted nor sent over the wire, add it to `excludedClasses.txt` in integrationTest resources:
```
org/apache/geode/tools/pulse/internal/security/GemFireAuthentication
```

If `excludedClasses.txt` does not yet exist, create an empty file in integrationTest resources. It needs to be in package `org.apache.geode.codeAnalysis`:
```
geode-module/src/integrationTest/resources/org/apache/geode/codeAnalysis/excludedClasses.txt
```

If the class will go on the wire, then follow the instructions for implementing `SanctionedSerializablesService`. Rerun the test and follow the instructions in the following section.

### If the module implements SanctionedSerializablesService

If the change does not break backward compatibility, copy the file `actualSerializables.dat` to `sanctioned-geode-module-serializables.txt` using the paths indicated in the failure message.

If the class is neither persisted nor sent over the wire, add a line to `excludedClasses.txt` identifying the class to exclude. 

If `excludedClasses.txt` does not yet exist, create an empty file in integrationTest resources. It needs to be in package `org.apache.geode.codeAnalysis`:
```
geode-module/src/integrationTest/resources/org/apache/geode/codeAnalysis/excludedClasses.txt
```

The failure message identifies the class. For example, if the message contains a line such as:
```
New or moved classes----------------------------------------
org/apache/geode/CancelException,true,3215578659523282642
```

Then add this line to `excludedClasses.txt` if you need to exclude that class:
```
org/apache/geode/CancelException
```
