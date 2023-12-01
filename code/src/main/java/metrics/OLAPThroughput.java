package metrics;

import lombok.Getter;
import lombok.Setter;

/*
 * Copyright (C) 2022, Zirui Hu, Rong Yu, Jinkai Xu, Yao Luo, Qingshuai Wang
 */
@Getter
@Setter
public class OLAPThroughput implements Metrics {
    private double QphH;
    private double OLAPThreadsCount;
    private double specifiedTPS;
    private double result;

    public OLAPThroughput(double QphH, double OLAPThreadsCount, double specifiedTPS) {
        this.QphH = QphH;
        this.OLAPThreadsCount = OLAPThreadsCount;
        this.specifiedTPS = specifiedTPS;
        this.result = QphH / OLAPThreadsCount;
    }

    @Override
    public double getResult() {
        return result;
    }

    @Override
    public String toString() {
        return "OLAPThroughput{"
                + result +
                '@' + specifiedTPS + '}';
    }
}
