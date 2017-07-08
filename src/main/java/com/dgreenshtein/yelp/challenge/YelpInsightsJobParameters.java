package com.dgreenshtein.yelp.challenge;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * Created by davidgreenshtein on 04.07.17.
 */
public class YelpInsightsJobParameters {

    private static final String OPTION_RUN_LOCAL = "runLocal";
    private static final String OPTION_USERS_INPUT_PATH = "usersInputPath";
    private static final String OPTION_REVIEWS_INPUT_PATH = "reviewsInputPath";
    private static final String OPTION_BUSINESS_INPUT_PATH = "businessInputPath";
    private static final String OPTION_OUTPUT_PATH = "outputPath";

    private static final OptionParser PARSER = new OptionParser();

    static {
        PARSER
                .accepts(OPTION_RUN_LOCAL, "Run in local mode");

        PARSER
                .accepts(OPTION_USERS_INPUT_PATH)
                .withRequiredArg()
                .required()
                .describedAs("Base input path for Yelp users dataset");

        PARSER
                .accepts(OPTION_REVIEWS_INPUT_PATH)
                .withRequiredArg()
                .required()
                .describedAs("Base input path for Yelp reviews dataset");

        PARSER
                .accepts(OPTION_BUSINESS_INPUT_PATH)
                .withRequiredArg()
                .required()
                .describedAs("Base input path for Yelp business dataset");

        PARSER
                .accepts(OPTION_OUTPUT_PATH)
                .withRequiredArg()
                .required()
                .describedAs("Output result path");

    }

    private final boolean runLocal;
    private final String usersInputPath;
    private final String reviewsInputPath;
    private final String businessInputPath;
    private final String outputPath;

    /**
     * @param args the program arguments
     */
    public YelpInsightsJobParameters(String[] args) {
        OptionSet options = PARSER.parse(args);

        runLocal = options.has(OPTION_RUN_LOCAL);
        usersInputPath = (String) options.valueOf(OPTION_USERS_INPUT_PATH);
        reviewsInputPath = (String) options.valueOf(OPTION_REVIEWS_INPUT_PATH);
        businessInputPath = (String) options.valueOf(OPTION_BUSINESS_INPUT_PATH);
        outputPath = (String) options.valueOf(OPTION_OUTPUT_PATH);
    }

    public boolean isRunLocal() {
        return runLocal;
    }

    public String getUsersInputPath() {
        return usersInputPath;
    }

    public String getReviewsInputPath() {
        return reviewsInputPath;
    }

    public String getBusinessInputPath() {
        return businessInputPath;
    }

    public String getOutputPath() {
        return outputPath;
    }

    @Override
    public String toString() {
        return String.format("YelpInsightsJobParameters{runLocal=%s, usersInputPath='%s', reviewsInputPath='%s', businessInputPath='%s', " +
                                     "outputPath='%s'}", runLocal, usersInputPath, reviewsInputPath, businessInputPath, outputPath);
    }

}
