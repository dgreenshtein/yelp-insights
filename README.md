# Yelp insights challenge

Application finding the list of businesses reviewed by Yelp users having higher influence in Yelp social network

Spark job accepting [Yelp dataset](https://www.yelp.de/dataset_challenge) in TSV format as an input

* running [Page Rank](https://en.wikipedia.org/wiki/PageRank) algorithm to find top 20 high influencers
* running query to find the businesses reviewed
* writing results to the disk

Initial Yelp dataset has JSON format, to transform it to TSV (tab delimited) run script

    $ python scripts/convertJsonToTsv.py yelp_academic_dataset.json # Creates yelp_academic_dataset.tsv


## How to build and run the application

How to build application with Maven


    mvn clean verify


How to build a Docker image

    docker build -t dgreenshtein/yelp-insights ${PROJECT_HOME}


How to run application Docker container and start Spark cluster

    docker run -it -p 4040:4040 -p 8080:8080 -p 8081:8081 -h insights --name=insights dgreenshtein/yelp-insights /bin/bash


How to run Spark job

    # start Spark Master and Worker
    root@insights$ /etc/bootstrap.sh

    root@insights$ cd /opt/yelp-insights/

    # to run application with test data set
    root@insights$ scripts/start-job.sh test-data/business.tsv test-data/reviews.tsv test-data/users.tsv /opt/yelp-insights/results/


Spark Master Web UI http://localhost:8080

## Tools and versions

1. Spark SQL, GraphX 2.1.1
2. [Graphframes](https://github.com/graphframes/graphframes) 0.5.0
3. [Pandas Dataframe](https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.to_csv.html)
4. Docker 17.05.0-ce

## References

* [Json to CSV converter](https://gist.github.com/paulgb/5265767)
* [Graphframes example](https://stackoverflow.com/questions/39158954/how-to-create-a-simple-spark-graphframe-using-java)