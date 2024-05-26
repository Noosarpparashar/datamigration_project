FROM spark:scala
COPY target/scala-2.12/ecart-migration-assembly-0.1.0-SNAPSHOT.jar /opt/spark/jars/

#sbt assembly
#docker build -t my-spark-job:latest .
#docker tag my-spark-job:latest 8084500696/my-spark-job:latest
#docker push 8084500696/my-spark-job:latest
#This is for old job docker run --rm my-spark-job:latest  spark-submit  --class com.its.esales.framework.jobs.controllers.Try  --master local[*]  /opt/spark/jars/ecart-migration-assembly-0.1.0-SNAPSHOT.jar
#docker run -it my-spark-job:latest  /opt/spark/bin/spark-submit  --class com.its.esales.framework.jobs.controllers.Try  --master local[*]  /opt/spark/jars/ecart-migration-assembly-0.1.0-SNAPSHOT.jar
#This is required just for driver not for executor COPY target/scala-2.12/ecart-migration-assembly-0.1.0-SNAPSHOT.jar /opt/spark/jars/