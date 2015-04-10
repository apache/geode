The traditional Publish-Subscribe pattern for data dissemination involves producers of data who publish events and consumers who subscribe to and receive the events. Communication channels are set up, with each channel allowing a uni-directional flow of events from publishers to subscribers. This pattern allows multiple applications to be integrated across the enterprise based on subscriptions to one or more channels. Subscribers may choose to receive all events published on a channel or may request only for a subset based on some filtering criteria. 
Publishers and subscribers are loosely coupled to each other. Publishers are abstracted from availability or the speed of individual subscribers. Event delivery to subscribers is usually done asynchronously. Latency- sensitive applications may choose to designate certain subscribers to receive events synchronously, but expanding that set to include more than a few subscribers usually results in poor performance and message backlogs.
Reliability is a key attribute of pub/sub systems and ensuring high availability across the system becomes one of the key challenges. 

## Key features for Client Subscriptions

The following are key features provided by Geode in delivering event notification to Geode Clients. They address the needs of large numbers of clients that must receive updates reliably. 
It should be remembered that clients may also be publishers, but since updates, 'out-of-the-box' are always processed synchronously to the server, there are fewer needs to be considered.

### Subscription Options

Geode clients can choose to register interest in all data elements for a given data region on the server. All interaction with the server is done through key-value pairs. Both the key and the value may be any Serializable object (for Java) or DataSerializable objects.
Clients may request an initial snapshot of data from the server in order to populate their Level 1 cache. They may register interest in insertions, updates and deletions for subsets of region data by specifying individual keys, lists of keys, regular expressions (that are evaluated against the keyset on the server) against the region on the server.

Once interest is registered, the server takes on the responsibility of delivering events to the subscriber. 

### Asynchronous delivery of events
The publisher and the subscriber are decoupled by a queuing mechanism that allows the subscriber to send qualifying events directly to the subscriber, isolating other subscribers and the publisher from the ill effects of a slow subscriber. Each subscriber has its own queue on the server. Queues are memory based, but may be backed up and/or overflow to disk (preserving memory space). Maximum queue sizes for subscriptions are configurable on a server wide basis. 

### Event Ordering
Geode provides ordering of events at the publisher thread level. Two events published from the same publisher thread will be delivered to all subscribers in that exact order. Event ordering allows a consistent view of the system to all subscribers. 

### Conflation
Conflation is the act of deleting unset events for a given key when a new event is received. For example, if a subscriber has not consumed an update for a stock update with key "COKE" and value "$54.33" by the time another update with a value of "$54.45" is put on the queue, the first update is deleted from the queue and never sent. Each subscriber has the ability to configure whether the events in its subscription channel should be conflated or not. Different subscribers may have different conflation policies for the events. Conflation allows subscribers to be notified of the latest values to a data element and also reduces the memory footprint on the server that may be queuing events to send to individual subscribers. 

### High availability for subscriptions
Geode manages subscriptions in-memory, reducing disk latency. Redundancy for queues is also configurable on a per subscriber basis. Redundancy is managed by keeping the subscription queue replicated in the memory of a peer server. This ensures that no messages are lost if a server holding messages for a subscriber goes down or is taken down for maintenance. Let's take a closer look at how Geode provides high availability for its subscriptions.

[[images/PubSub.png]]

As illustrated in the diagram and elaborated in the sequence diagram, a Geode client can be configured to register subscriptions. When this attribute is turned on, there is a dedicated TCP connection made from the client to the server, with a socket reader on the client side. On the server side, a queue is created which will contain qualifying messages for the client. The client is configured with a redundancy level and connects up to the minimum number of servers needed to satisfy its redundancy. The client knows the identities of the servers holding subscription queues for it and actively tries to maintain the redundancy contract. Only one of these servers, commonly referred to as the primary, actively dispatches events to the client. If the primary server becomes inaccessible, the client immediately designates a new server as the primary which then starts to dispatch messages to the client. Each server that has a subscription queue needs to be configured to receive updates which is how updates make it into the queue (A client can register interest in all keys for Region R1. The server that receives and maintains that subscriptions needs to have region R1 defined in order to receive the updates which it then adds to the client's queue)

The message is not removed from the queue until the client has acknowledged receipt of the message. As long as there is at least one server that the client is connected to for its subscriptions, the queues are not lost. If the client is configured as a durable client (client id remains the same across client invocations), then the queue is maintained on the server for a configurable amount of time, even if the client process goes away.

### Durable subscriptions

Subscribers can choose to have some or all of their subscriptions be **'durable'**. In this case, the subscription would be kept alive and events queued up for the subscriber, even if the subscriber process is disconnected from the server. Upon return, the subscriber receives all of the messages for the period that they were gone, is told that the event stream is "live" at that point, and then continues to receive events normally after that. Durable subscriptions are established by 'name', so essentially, a second process could take the place of the failing process, claim the queued subscription by name, and continue processing events where the first process left off.

## JMS vs Geode messaging

We recommend the use of Geode for reliable event notifications instead of JMS when the following are true:
Publishers and subscribers share a common data model (say a common database). You could use Geode as the common distributed data cache with event semantics
If very low latency is required, and peer-2-peer messaging is opted for, then, the design is such that each subscriber is always available (see note on "reliable roles" to see how multiple physical processes can play a single subscription role for HA)
when async event notifications is required, the reliability offered through memory based replication will suffice
