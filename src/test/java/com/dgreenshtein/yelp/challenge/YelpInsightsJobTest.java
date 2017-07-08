package com.dgreenshtein.yelp.challenge;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static junit.framework.TestCase.*;

/**
 * Created by davidgreenshtein on 06.07.17.
 */
public class YelpInsightsJobTest {

    private String businessInputPath;
    private String reviewsInputPath;
    private String usersInputPath;
    private String outputPath;

    @Before
    public void setUp(){
        businessInputPath = "src/test/resources/business.tsv";
        reviewsInputPath = "src/test/resources/reviews.tsv";
        usersInputPath = "src/test/resources/users.tsv";
        outputPath = "target/out";
    }


    @Test
    public void testSuccess(){
        String[] args = new String[]{"--businessInputPath=" + businessInputPath,
                "--runLocal=true",
                "--reviewsInputPath=" + reviewsInputPath,
                "--usersInputPath=" + usersInputPath,
                "--outputPath=" + outputPath};

        YelpInsightsJob job = new YelpInsightsJob(args);
        job.init();
        job.run();

        Dataset<Row> results = job.getSparkSession().read().option("header", "true").csv(outputPath);

        assertEquals(1, results.count());

        results.createOrReplaceTempView("results");
        results.show();

        job.getSparkSession().sql("SELECT business_name FROM results").toJavaRDD()
           .map(row -> {
            assertEquals("Innovative Vapors", row.getAs("business_name"));
            return row;
        });
    }

    @Test (expected = SparkException.class)
    public void testWrongInputFormat(){
        usersInputPath = "src/test/resources/users-wrong-format.tsv";
        outputPath = "target/out/wrong";

        String[] args = new String[]{"--businessInputPath=" + businessInputPath,
                "--runLocal=true",
                "--reviewsInputPath=" + reviewsInputPath,
                "--usersInputPath=" + usersInputPath,
                "--outputPath=" + outputPath};

        YelpInsightsJob job = new YelpInsightsJob(args);
        job.init();
        job.run();

        Dataset<Row> results = job.getSparkSession().read().option("header", "true").csv(outputPath);
        assertEquals(1, results.count());
    }

    @After
    public void cleanup() throws IOException {
        FileUtils.deleteDirectory(new File(outputPath));
    }
}
