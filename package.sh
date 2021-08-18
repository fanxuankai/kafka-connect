cd kafka-connect-dependencies
mvn clean install
cd ../kafka-connect-parent
mvn clean install
cd ../kafka-connect-sink-redis
mvn clean install package
cd ../kafka-connect-transforms
mvn clean install package