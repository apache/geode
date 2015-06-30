To run and compile these tests, you need a gemfire checkout 6.5+
In that gemfire checkout, cd to the tests directory, and create a symbolic link to the "hibe" directory in here. eg: ln -s /Users/gregp/plugins/gemfire-plugins/src/test/hydra/hibe

Then apply the following patch to build.xml:
Index: build.xml
===================================================================
--- build.xml	(revision 34744)
+++ build.xml	(working copy)
@@ -866,6 +866,7 @@
         <pathelement location="${jetty.dir}/jsp-2.1.jar"/>
         <pathelement location="${bcel.dir}/bcel.jar"/>
       	<pathelement location="${osgi.core.jar}"/>
+	<pathelement
location="/home/sbawaska/.m2/repository/org/hibernate/hibernate-core/3.5.0-Final/hibernate-core-3.5.0-Final.jar"/>
       </classpath>
     </javac>
 
@@ -936,6 +937,7 @@
         <pathelement location="${ant.home}/lib/ant.jar"/>
         <pathelement location="${jetty.dir}/core-3.1.1.jar"/>
         <pathelement location="${jetty.dir}/jsp-2.1.jar"/>
+        <pathelement
location="/home/sbawaska/.m2/repository/org/hibernate/hibernate-core/3.5.0-Final/hibernate-core-3.5.0-Final.jar"/>
       </classpath>
     </javac>
 
@@ -996,6 +998,7 @@
       	<include name="hyperictest/lib/*.jar"/>
       	<include name="hyperictest/config/*.properties"/>
         <include name="jta/*.xml"/>
+        <include name="hibe/*.xml"/>
         <include name="junit/runner/excluded.properties"/>
         <include name="**/*.bt"/>
         <include name="**/*.conf"/>
@@ -2901,6 +2904,7 @@
           <pathelement location="${jetty.dir}/core-3.1.1.jar"/>
           <pathelement location="${jetty.dir}/jsp-2.1.jar"/>
 	        <pathelement location="cobertura.jar"/>
+          <pathelement
location="/home/sbawaska/.m2/repository/org/hibernate/hibernate-core/3.5.0-Final/hibernate-core-3.5.0-Final.jar"/>
         </classpath>
 
         <env key="GEMFIRE" value="${product.dir}"/>



In hibe/hibe.inc , there are references to the modules jar in /Users/gregp that need to be changed, also, there are also references to /export/monaco1 , so those need to be reachable. 
In gemfire checkout main dir, run ./build.sh compile-tests execute-battery -Dbt.file=`pwd`/tests/hibe/hibe.bt
