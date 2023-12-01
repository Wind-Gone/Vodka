package metrics;

import lombok.Getter;
import lombok.Setter;

import java.util.Arrays;

/*
 * Copyright (C) 2022, Zirui Hu, Rong Yu, Jinkai Xu, Yao Luo, Qingshuai Wang
 */
@Getter
@Setter
public class HTAPScore implements Metrics {
    private final double[] queryDelay;
    private final double averageAdHocDelay;
    private final double averageBatchDelay;
    private final OLAPThroughput olapThroughput;
    private final Freshness freshness;
    private double result;


    public HTAPScore(double[] queryDelay, double averageAdHocDelay, double averageBatchDelay, OLAPThroughput olapThroughput, Freshness freshness) {
        this.queryDelay = queryDelay;
        this.averageAdHocDelay = averageAdHocDelay;
        this.averageBatchDelay = averageBatchDelay;
        this.olapThroughput = olapThroughput;
        this.freshness = freshness;
    }


    @Override
    public double getResult() {
        double sumQueryDelay = Arrays.stream(queryDelay).sum();
        double sumDelay = sumQueryDelay + averageBatchDelay + averageAdHocDelay;
        double averageQphH = 60 * 60 / (0.00001 + sumDelay) * (queryDelay.length + 2);
        result = averageQphH * olapThroughput.getResult() / (1 + freshness.getResult());
        return result;
    }


    @Override
    public String toString() {
        return "HTAPScore{" +
                "result=" + result +
                '@' + olapThroughput.getSpecifiedTPS() +
                '}';
    }
}
