 Kafka Mostly used Commends are:
  All these commends should be run in cmd
  in separate tabs (mean this is the required commends)

        1) First we have to start the zookeeper:
            zookeeper-server-start.bat config\zookeeper-properties

        2) Then we have to start the kafka server
            kafka-server-start.bat  config\server.properties



   Now Open New Tab and do the following Actions:
    1) to create topics we use:
        kafka-topics --zookeeper localhost:2181  --topic topic_Name --create --partitions numberOfPartitions --replication-factor NoOfFactor(mostly 1)
    Example:
        kafka-topics --zookeeper 127:0.0.1:2181 --topic first_topic --create --partitions 4 --replication-factor 1

    2) Now to send data to the topic we use the producer like below
        kafka-console-producer --bootstrap-server localhost:9092 --topic topic_name
    Example:
        kafka-console-producer --bootstrap-server localhost:9092 --topic tweets


    3) Now to see what data we have send we use the consumer like bellow:
        a) kafka-console-consumer --bootstrap-server localhost:9092 --topic topicNameToReadFrom --group groupName
        // to start from beginning add the --from-beginning commend
        b) kafka-console-consumer --bootstrap-server localhost:9092 --topic topicNameToReadFrom --group groupName --from-beginning
    Example:
        a) kafka-console-consumer --bootstrap-server localhost:9092 --topic tweets --group first-app
        a) kafka-console-consumer --bootstrap-server localhost:9092 --topic tweets --group first-app --from-beginning




Other commends:

      To Reset the Offset or to clear the cluster we use the below Commend:
        Commend:
            kafka-consumer-groups --bootstrap-server localhost:9092 --group first-app --reset-offsets --execute --to-earliest --topic topicName
        Example:
            kafka-consumer-groups --bootstrap-server localhost:9092 --group first-app --reset-offsets --execute --to-earliest --topic tweets



Start Elastic Search:
    First:
        Download the elastic search from it's own website
            (download the zip file)
        Place and extract the zip file in the main directory like so:
            C:\elasticsearch
        The set the environment variable to :
            C:\elasticsearch\elasticsearch-7.10.1\bin
    Second:
        To start Elastic search i have set the Environment variable
          just open the CMD and write elasticsearch
          this will start the service in the posrt of 9200
        Open in the browser:
          http://localhost:9200

        Use this address in Postman API
          Create topic:
            http://localhost:9200/Topic_Name
          Create type in the topic:
            http://localhost:9200/Topic_Name/Topic_Type/id




Work With MongoDB:
    First:
        Download the MongoDB from website:
            (download the msi file and install as default)
        Create the like below directory because the MongoDB will automatically
        detect this directory:
            C:\data\db\
                (mean create data and then db folder)

    Second:
        To start the MongoDB
            Move to the following directory from CMD:
                C:\Program Files\MongoDB\Server\4.4\bin

            Then write mongod.exe and press enter
                This will start the MongoDB.
