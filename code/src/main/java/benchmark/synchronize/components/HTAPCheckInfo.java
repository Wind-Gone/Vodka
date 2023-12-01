package benchmark.synchronize.components;

public class HTAPCheckInfo {
    public boolean isHtapCheck;
    public int htapCheckType;
	public int htapCheckCrossQuantity; // 交叉量
	public int htapCheckCrossFrequency; // 交叉频率
	public int htapCheckApNum;
	public String htapCheckApConn;
    public String htapCheckTpConn;
    public int htapCheckFreshnessDataBound;
    public int warehouseNum;
    public String resultDir;
    public int gapTime;
    public int dbType;
    public String dbTypeStr;
    public int lFreshLagBound;
    public int rFreshLagBound;
    public int htapCheckQueryNumber;
    public boolean isWeakRead;
    public int weakReadTime;
}
