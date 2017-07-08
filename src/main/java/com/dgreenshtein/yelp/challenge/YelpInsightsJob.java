package com.dgreenshtein.yelp.challenge;


import com.dgreenshtein.yelp.challenge.entity.Edge;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;
import org.graphframes.lib.PageRank;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.desc;

/**
 * Created by davidgreenshtein on 04.07.17.
 */
public class YelpInsightsJob implements Closeable {

    private SparkSession spark;
    private final YelpInsightsJobParameters jobParameters;
    private static final Logger LOG = LoggerFactory.getLogger(YelpInsightsJob.class);

    public YelpInsightsJob(final String[] args) {
        this.jobParameters = new YelpInsightsJobParameters(args);
    }

    public void init(){
         spark = jobParameters.isRunLocal() ?
                 SparkSession
                         .builder()
                         .master("local")
                         .appName("YelpInsightsJob")
                         .getOrCreate() :
                 SparkSession
                         .builder()
                         .appName("YelpInsightsJob")
                         .getOrCreate();

        LOG.info("Initialized YelpInsightsJob");
    }

    public void run(){

        Map<String, String> tsvOptions = buildOptions();

        Dataset<Row> business = spark.read().options(tsvOptions).csv(jobParameters.getBusinessInputPath());
        business.createOrReplaceTempView("business");

        Dataset<Row> users = spark.read().options(tsvOptions).csv(jobParameters.getUsersInputPath());

        Dataset<Row> reviews = spark.read().options(tsvOptions).csv(jobParameters.getReviewsInputPath());
        reviews.createOrReplaceTempView("reviews");

        JavaRDD<Edge> rdd = users.toJavaRDD().flatMap(row-> getEdge(row));

        // Converting RDD to Dataset of Row
        Dataset<Row> relation = spark.createDataFrame(rdd, Edge.class);

        // Initializing GraphFrame
        GraphFrame gFrame = GraphFrame.fromEdges(relation);

        // Calculate top higher influencer in the network using PageRank algorithm
        PageRank pRank = gFrame.pageRank().resetProbability(0.01).maxIter(1);
        Dataset<Row> topUsers = pRank.run().vertices().select("id", "pagerank").orderBy(desc("pagerank")).limit(20);

        topUsers.createOrReplaceTempView("topUsers");

        // Selecting business names rated by higher influencers
        spark.sql("SELECT t1.user_id as user_id," +
                                                    " t1.pagerank as pagerank," +
                                                    " b.name as business_name " +
                                                    "FROM business b " +
                                                    "JOIN " +
                                                        "(SELECT r.business_id as business_id," +
                                                        " t.id as user_id," +
                                                        " t.pagerank as pagerank " +
                                                        "FROM topUsers t " +
                                                        "JOIN " +
                                                        "reviews r ON t.id == r.user_id) t1" +
                                                    " ON b.business_id == t1.business_id " +
                                                    "GROUP BY t1.user_id, t1.pagerank, b.name")
        .write().option("header", "true").csv(jobParameters.getOutputPath());
    }

    private static Iterator<Edge> getEdge(Row row) {
        String user_id = row.getAs("user_id");
        String friends = row.getAs("friends");
        List<String> friendsList = Arrays.asList(friends.split(","));
        List<Edge> result = new ArrayList<>();
        friendsList.forEach(f->{
            result.add(new Edge(user_id, f));
        });
        return result.iterator();
    }

    private static Map<String, String> buildOptions(){
        Map<String, String> tsvOptions = new HashMap();
        tsvOptions.put("header", "true");
        tsvOptions.put("delimiter", "\t");
        return tsvOptions;
    }

    public static void main(String[] args) throws Exception {
        try (YelpInsightsJob job = new YelpInsightsJob(args)) {
            job.init();
            job.run();
        }
    }

    public SparkSession getSparkSession(){
        return this.spark;
    }

    @Override
    public void close() {
        // close spark context
        if (spark != null) {
            spark.close();
        }
    }
}
