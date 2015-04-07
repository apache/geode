#Solving the problem of poorly performing Web Applications
---------need to put images in here from original document [here](https://docs.google.com/a/pivotal.io/document/d/1vR1f40lD6qYJ-3DrNOhJOkMpKtxij36Pz9Bjax7OT1s/edit#heading=h.fa8ah6dp3ymu)

#####Joshua Davis Senior Solutions Architect, Pivotal

##Introduction

There have been many times since the “Web” era started that I have been asked to provide a solution for a non-performing website or web application.  The first thing that I create is a set of questions to understand the application or environment that is having problems.  There are some basic questions that I would ask, especially if I was coming into the situation fresh without any background or context.

######<i>Sample Use Case:</i>

A customer was having trouble accessing their authorization policies in a timely manner from a custom built authorization system that they had created.  It used a complex policy RDBMS that could return the data under normal load.  When faced with load that was higher than normal it did not handle the number of queries and changes to the policies that had to happen at the same time.

Geode was used (embedded) to create a local application NoSQL layer on top of the main relational database.  The performance increased 200% or more based upon the complexity of the policy.  The embedded mode for Geode was used to enable the data to be as close as possible to the application.  It also enabled a NoSQL layer that backed a set of REST services for managing policies that were much more responsive than hitting the RDBMS directly.

##Questions that should be asked
<i>Is the problem a user’s perception of the performance of the website not being responsive?</i>

Perception of a website’s performance can be affected by many factors including network, server performance, and choice of front end tool (JavaScript vs. others).  Most of these issues can be overcome with good application architecture focused on a responsive web experience that has an asynchronous backend.  Typically, I have found that this question can uncover many of the typical user experience issues that are incorrectly implemented by the application.  One example would be a shopping cart that saves in a synchronous manner to a back-end database at the conclusion of each step.

<i>Does the performance problem center around search or retrieval of data from the back-end of the website?</i>

It has been proven in many surveys over the years that most problems with application performance can be driven down to the queries that are run in the database.  Usually, there are two sources of data retrieval that cause problems.  The first is a heavily indexed relational database that is used by every application and reporting environment in the organization.  The second is the retrieval of data through REST or SOAP messages across many different sources.  Either of these or both can cause issues with performance in the long-term and constitute a form of technical debt.

<i>Is the performance issue only when saving in-process data in a long running business transaction?</i>

Many websites and web based applications in general have issues holding and preserving in-process state.  This data is the voluminous information that is garnered by the application before an actual physical transaction is recorded.  In many applications this data is either saved directly to the back end database, or held in the session state of the web application.  Either one of these solutions is likely to create problems when the application needs to scale and deal with failover.

<i>Does the back-end database have issues when there is a peak usage of the database?</i>

Most databases are not used just for the web application(s) that create the transactions.  They have other responsibilities such as reporting and batch processing.  Peak usage of this kind of database come at unusual times and creates a poor user experience.

<i>Does your application have “pausing” issues waiting for the data sources to be available?</i>

In certain cases if the application relies on databases, application services or an Enterprise Service Bus that is externally hosted network reliability can be an issue that affects application performance.  Having the data contained within the application can create a serious performance improvement as well as improving reliability.  Since the data is published to the backend asynchronously the data can be queued and wait for network connectivity to “reappear.”

##Solution Architecture

###Why use the Geode Embedded Topology as the in-memory NoSQL database for your application?

Using an embedded in-memory database such as Geode is a great way of mitigating issues around Application performance.



<ul>
<li>It is a simple solution that can be placed into your application “in-between” your existing front end user interface and your back end database.</li>

<li>It is a NoSQL solution that can represent the domain of the web-site instead of forcing your web application to have the same domain model as your back end database.</li>

<li>Creates a high performance layer that can be asynchronously synchronized with your back end database.</li>

<li>Introduces transactional layer that will only communicate with your database when a transaction needs to placed into the back end database as the source of record.</li>

<li>Enables the future use of a distributed data cache when you need it instead of dealing with the up-front costs of implementation. </li>
</ul>
###Why use the embedded Geode Topology over the Client/Server Topology?

Geode is a product that contains many options for deployment, only one of them is embedding the server within your application.  The Client/Server approach is an In Memory Data Grid (IMDG) that employs a horizontal scaling and failover approach. The additional Geode component when compared to the embedded topology is the Geode Locator.  Locators keep track of the nodes within the IMDG and coordinate queries.  This enables Geode clients in applications to access an abstraction for the “location” of where the Geode nodes are on the network and their exact topology implementation.

In the embedded mode you have the ability to share and exchange data between the nodes of embedded Geode, but you must either know of the other nodes, or use the Multicast protocol to discover the other embedded nodes.  There are other differences when it comes to security options, WAN replication, load balancing and complexity that should be considered when choosing a topology.  

The basic rules of thumb for using the Geode embedded topology are:
<ul>
<li>Is the Geode database used to support specific application(s) and the data  would not need to be shared with applications that do not have a Geode embedded database?</li>

<li>Since this topology does not support replication of data across a WAN (Wan replication) is their some other type of replication implemented in the application stack or is it not needed? </li>

<li>Is a simple solution needed for degrading application performance?</li>
</ul>

###How to use Geode in your application
There are a few resources where there are some great examples of using Geode within your Java application.
  
-Accessing Data in GemFire Guides
-Spring GemFire Examples

Specifically, I recommend the use of the writethrough example which provides a template for enabling back-end database synchronous and asynchronous replication from the front-end Geode cache.  Another recommended example that provides an end-to-end example for a real world application is a version of the Spring Pet Clinic application adapted for Geode.  
