package metrics;

/*
 * Copyright (C) 2022, Zirui Hu, Rong Yu, Jinkai Xu, Yao Luo, Qingshuai Wang
 */
public class ResourceUsage implements Metrics {
    public final double alpha = 1.1;        // magic number, wait for revision, just for demo now
    public final double belta = 0.6;
    public final double gamma = 0.2;

    public double CPUUsage;
    public double MemoryUsage;
    public double DiskUsage;

    public ResourceUsage(double CPUUsage, double MemoryUsage, double DiskUsage) {
        this.CPUUsage = CPUUsage;
        this.MemoryUsage = MemoryUsage;
        this.DiskUsage = DiskUsage;
    }

    @Override
    public double getResult() {
        return alpha * CPUUsage + belta * MemoryUsage + gamma * DiskUsage;
    }

}
