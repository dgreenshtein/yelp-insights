FROM p7hb/docker-spark:2.1.1
ARG APP_PATH=/opt/yelp-insights

# Prepare the app path
RUN mkdir -p $APP_PATH/lib
RUN mkdir -p $APP_PATH/scripts
RUN mkdir -p $APP_PATH/test-data
WORKDIR $APP_PATH

# Copy jar
COPY target/yelp-insights-1.0-SNAPSHOT.jar $APP_PATH/lib
COPY src/scripts/convertJsonToTsv.py $APP_PATH/scripts

COPY src/scripts/start-job.sh $APP_PATH/scripts
RUN chown root.root $APP_PATH/scripts/start-job.sh
RUN chmod 700 $APP_PATH/scripts/start-job.sh

COPY src/test/resources/ $APP_PATH/test-data/

COPY bootstrap.sh /etc/bootstrap.sh
RUN chown root.root /etc/bootstrap.sh
RUN chmod 700 /etc/bootstrap.sh
