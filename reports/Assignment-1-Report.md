

## Part 1 - Design for streaming analytics

1. *As a tenant, select a dataset suitable for streaming data analytics as a running scenario. Explain the dataset and why thedataset is suitable for streaming data analytics in your scenario. As a tenant, present at least two different analytics in the interest of the tenant    : (i) a streaming analytics (tenantstreamapp) which analyzes streaming data from the tenant and (ii) a batch analytics which analyzes historical results outputted by the streaming analytics. The explanation should be at a high level to allow us to understand the data and possible analytics so that, later on, you can implement and use them in answering other questions.* 
    

    In order to run Streaming Analytics our data should be suitable for streaming. Such data should be continuous near real time thus data from IOT devices are perfect  set for our purposes. They can contain many measurement results continuously in real time and also can contain many different measurements which could give more opportunities to perform analytics on.

    Because of these reasons BTS (Base Transceiver Stations) data suits our purposes because this data is a datastream from different base stations and each base station has different sensors and alarms. Data contains following columns:

    * station_id: the id of the stations
    * datapoint_id: the id of the sensor (data point)
    * alarm_id: the id of the alarm
    * event_time: the time at which the event occurs
    * value: the value of the measurement of the datapoint
    * valueThreshold: the threshold set for the alarm. Note that some threshold values are set to a default value of 999999.
    * isActive: the alarm is active (true ) or not (false)

    With such data there are many ways in which we could do analytics and I have chosen following analytics:

    i) Streaming Analytics:  there is a column in our data called isActive where it is true if the sensor measurement is not normal and it causes some alarms. My Streaming Analytics will check if any alarm is active then it would appropriately notify the tenant about the alarm. This is very close to real scenario because if some alarm is raised in base station you would need to check it in order to fix the problems as soon as possible. So anayltics need to be provided in real time. 

    II) Batch Analytics: We could analyze which station had most alarms raised for some defined time. It could help because if some station is raising many alarms in brief time then it could have serious problems and tenant would need to go and fix it urgently.




2. *The tenant will send data through a messaging system, which provides stream data sources. Discuss and explain the
following aspects for the streaming analytics: (i) should the analytics handle keyed or non-keyed data streams for the tenant data, and (ii) which types of delivery guarantees should be suitable for the stream analytics and why.* 

    I will use Flink and RabbitMQ so I discuss this question related to these implementations.

    i) Flink supports Keyed and non-keyed data streams. It affects the parallelization of analytics because Flink partitions stream into substreams based on keys and if we process analytics parallelly then each processing node will have access only to its substream with keys. It will not have access to all stream. In our analytics situation it's perfect to use keyed data streams since we can choose to process data partitioned to substream using station_id as a key. This way if we decide to paralellyse processing then each processing node will have access to all data from same station and we can perform our batch analytics appropriately. And as an additional plus we will be able to parallelyse processing. Consequently Keyed data streams is best for our problem.

    ii) Flink and RabbitMQ together supports END-to-END  exactly once delivery guarantee. It suits us best because we don't want same alarm to be processed more than once. Otherwise  This could lead to duplicate alarms be recorded many times and first of all maybe problem has already been fixed and we still get alarm but also it would affect our batch analytics since we would have more alarms for same station. END-to-END exactly once guarantees that same event (data) will be delivered and processed exactly once.




3. *Given streaming data from the tenant (selected before). Explain the following issues: (i) which types of time should be
associated with stream data sources for the analytics and be considered in stream processing (if the data sources have no
timestamps associated with events, then what would be your solution), (ii) which types of windows should be developed for
the analytics (if no window, then why), (iii) what could cause out-of-order data/records with your selected data in your
running example, and (iv) will watermarks be needed or not, explain why. Explain these aspects and give examples.* 


    i) In BTS data  we already have event_time  so we can associate this time with stream data when doing the analytics. In case that we don't have this event_time we could then choose receiving time to FLink or queue as our timestamp. which in real situation wouldn't differ much from event_time unless some of components go down for big amount of time.

    ii)  For our purpose we can use tumbling windows of Flink with specified window size. for example we could specify window size of 10 mins and thus we could perform oour batch analysis on that window. 

    iii) Our bts data is not ordered by time thus the data is already out of order. This out of order can also be increased because of some in process irregularities (some components going down for any reason ).


4. *List performance metrics which would be important for the streaming analytics for your tenant cases. For each metric,
explain its definition, how to measure it in your analytics/platform and why it is important for the analytics of the tenant.*
 

* The processing time to process all the events would be crucial for my analytics because if it gets slow then there can be problems to react to the alarms.
* Delayed data  - This could also be crucial because we should see if we can improve it in some way so that we have as less delayed data as possible .
* Overall reach time - this would be the time between the alarm creation and its reach and notification by our streaming analytics. It is also crucial because we want to see if alarms are being delayed for a long time.
   

5. *Provide a design of your architecture for the streaming analytics service in which you clarify: tenant data sources,
mysimbdp messaging system, mysimbdp streaming computing service, tenant streaming analytics app, mysimbdpcoredms, and other components, if needed. Explain your choices of technologies for implementing your design and
reusability of existing assignment works. Note that the result from tenantstreamapp will be sent back to the tenant in near
real-time and to be ingested into mysimbdp-coredms.* 


    ![  ]( design.png "Design")

    Data is being streamed from Data source to the RabbitMQ Message queue. From the queue Streaming Analytics app (Flink) accesses the events and  then in case Alarm is on then it notifies and ingests data to MYSIMBDP. Additionally, Flink also puts notification to another RabbitMQ queue where tenant is receiving the notifications.

## Part 2 - Implementation of streaming analytics   
1. *As a tenant, implement a tenantstreamapp. Explain (i) the structures of the input streaming data and the analytics output
result in your implementation, and (ii) the data serialization/deserialization, for the streaming analytics application
(tenantstreamapp).*


2. *Explain the key logic of functions for processing events/records in tenantstreamapp in your implementation. Explain under
which conditions/configurations and how the results are sent back to the tenant in a near real time manner and/or are stored
into mysimbdp-coredms as the final sink.* 

3. *Explain a test environment for testing tenantstreamapp, including how you emulate streaming data, configuration of
mysimbdp and other relevant parameters. Run tenantstreamapp and show the operation of the tenantstreamapp with
your test environments. Discuss the analytics and its performance observations when you increase/vary the speed of
streaming data.*


4. *Present your tests and explain them for the situation in which wrong data is sent from or is within the data sources. Explain
how you emulate wrong data for your tests. Report how your implementation deals with that (e.g., exceptions, failures, and
decreasing performance). You should test with different error rates.* 

5. *Explain parallelism settings in your implementation and test with different (higher) degrees of parallelism for at least two
instances of tenantstreamapp (e.g., using different subsets of the same dataset). Report the performance and issues you
have observed in your testing environments.*


## Part 3 - Extension


1. *Assume that you have an external RESTful (micro) service which accepts a batch of data and performs a type of analytics
and returns the result (e.g., in case of a dynamic machine learning inference) in a near real time manner (like the stream
analytics app), how would you connect such a service into your current platform? Explain what the tenant must do in order to
use such a service.*



1. *Given the output of streaming analytics stored in mysimbdp-coredms for a long time. Explain a batch analytics (see also
Part 1, question 1) that could be used to analyze such historical data. How would you implement it?*

     We can analyze which station had most alarms raised for some defined time. It can help because if some station is raising many alarms in brief time then it can have serious problems and tenant would need to go and fix it urgently.

     For implementation I can implement a separate application inside mysimbdp which runs for predefined time and analyzes all the notifications received for that time.
     We can also implement it inside the Flink by setting appropriate key schema and window sizes.



1. *Assume that the streaming analytics detects a critical condition (e.g., a very high rate of alerts) that should trigger the
execution of a batch analytics to analyze historical data. The result of the batch analytics will be shared into a cloud storage
and a user within the tenant will receive the information about the result. Explain how you will use workflow technologies to
coordinate these interactions and tasks (use a figure to explain your work).*




1. *If you want to scale your streaming analytics service for many tenants, many streaming apps, and data, which components
would you focus and which techniques you want to use?*

    First of all we can add more queues in the messaging service and create some kind of hierarchy to be able to identify some properties from  the queue. Then we should assign every tenant to specific queue and provide a customerstreamapp for each of tenants. Then by design Flink can run them parallely according to the traffic.



5. *Is it possible to achieve end-to-end exactly once delivery in your current tenantstreamapp design and implementation? If
yes, explain why. If not, what could be conditions and changes to achieve end-to-end exactly once? If it is impossible to have
end-to-end exactly once delivery in your view, explain why.*

    It would be possible if I would add another message queue between mysimbdp and Streaming analytics. Then RabbitMQ and FLink together can have end to  ned exactly once delivery.
