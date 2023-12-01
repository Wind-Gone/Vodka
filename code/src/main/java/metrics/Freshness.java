package metrics;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/*
 * Copyright (C) 2022, Zirui Hu, Rong Yu, Jinkai Xu, Yao Luo, Qingshuai Wang
 */
@Getter
@Setter
@ToString
public class Freshness implements Metrics {
    private final double ValidQueryNumber;
    private final double QueryNumber;

    public Freshness(double ValidQueryNumber, double QueryNumber) {
        this.QueryNumber = QueryNumber;
        this.ValidQueryNumber = ValidQueryNumber;
    }

    @Override
    public double getResult() {
        return ValidQueryNumber / QueryNumber;
    }

    static class AdHocQuery {
        private final double T_AdHocStartTime;                  // Ad-hoc Query的启动时间
        private final double T_LastRevisedTrxCompleteTime;      // 上一个使TP与AP端数据不一致的事务完成时间
        private final double T_preAdHocExecutionTime;           // Ad-hoc Query预先执行时的执行时间

        AdHocQuery(double t_adHocStartTime, double t_lastRevisedTrxCompleteTime, double t_preAdHocExecutionTime) {
            T_AdHocStartTime = t_adHocStartTime;
            T_LastRevisedTrxCompleteTime = t_lastRevisedTrxCompleteTime;
            T_preAdHocExecutionTime = t_preAdHocExecutionTime;
        }

        public double getResult() {
            return T_AdHocStartTime - T_LastRevisedTrxCompleteTime + T_preAdHocExecutionTime;   // 该Query最终统一代价的时间
        }
    }

    static class BatchQuery {
        private final double T_RetryTime;                  // Retry到满足新鲜度需求的重试时间
        private final double T_PostBatchExecutionTime;      // Batch Query后置执行时的实际执行时间

        BatchQuery(double t_retryTime, double t_postBatchExecutionTime) {
            T_RetryTime = t_retryTime;
            T_PostBatchExecutionTime = t_postBatchExecutionTime;
        }

        public double getResult() {
            return T_RetryTime + T_PostBatchExecutionTime;  // 该Query最终统一代价的时间
        }
    }

}
