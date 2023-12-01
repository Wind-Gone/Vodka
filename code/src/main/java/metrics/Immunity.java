package metrics;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Immunity implements Metrics {
    private final double specifiedTpmC;           //  指定的TpmC
    private final double DegredationBound;        //  指定的性能降级边界
    private final Integer MaximumAPThreadCount;   //  最大支持AP线程数量

    public Immunity(double specifiedTpmC, double degredationBound, Integer maximumAPThreadCount) {
        this.specifiedTpmC = specifiedTpmC;
        DegredationBound = degredationBound;
        MaximumAPThreadCount = maximumAPThreadCount;
    }


    @Override
    public double getResult() {
        return MaximumAPThreadCount;
    }

    @Override
    public String toString() {
        return "Immunity{" +
                "MaximumAPThreadCount=" + MaximumAPThreadCount +
                '@' + specifiedTpmC + '&' + DegredationBound + '}';
    }
}
