package benchmark.oltp.entity;

public class StockLevelData extends CommonEntityData {
    /* terminal input data */
    public int w_id;
    public int d_id;
    public int threshold;

    /* terminal output data */
    public int low_stock;

    @Override
    public String toString() {
        return "StockLevelData{" +
                "w_id=" + w_id +
                ", d_id=" + d_id +
                ", threshold=" + threshold +
                ", low_stock=" + low_stock +
                '}';
    }
}
