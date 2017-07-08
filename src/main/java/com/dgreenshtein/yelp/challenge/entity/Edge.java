package com.dgreenshtein.yelp.challenge.entity;

/**
 * Created by davidgreenshtein on 04.07.17.
 */
public class Edge {
    private String src;
    private String dst;

    public Edge(String src, String dst) {
        super();
        this.src = src;
        this.dst = dst;
    }

    public String getSrc() {
        return src;
    }

    public String getDst() {
        return dst;
    }
}