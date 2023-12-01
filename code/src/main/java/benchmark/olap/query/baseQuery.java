package benchmark.olap.query;

import benchmark.olap.OLAPTerminal;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.zip.ZipOutputStream;

public abstract class baseQuery {
    public String q;
    public String name;
    public double filterRate;
    //    public long tpmc;
    public String dynamicParam;
    //    public int warehouseNumber;
    public static int orderOriginSize = 4500000;       //oorder表的原始大小           已调整在initFilterRatio函数里一并更新
    public static int olOriginSize =  44997903;         //orderline表的原始大小
    public static int olNotnullSize =  31498050;        //orderline表非空的数量
//    public int orderTSize;                          //oorder表的实时大小
//    public int orderlineTSize;                      //orderline表的实时大小

    baseQuery() throws ParseException {
        // this.tpmc = tpmc;
        // this.q = getQuery();
        this.name = this.getClass().getName();
        // this.warehouseNumber = warehouseNumber;
        // this.orderTSize = OLAPTerminal.oorderTableSize;
        // this.orderlineTSize = OLAPTerminal.orderLineTableSize;
    }

    public abstract String getQuery() throws ParseException;

    public abstract String updateQuery() throws ParseException;

    // public abstract String getCountQuery();
    public abstract String getExplainQuery();

    public abstract String getFilterCheckQuery();

    public abstract String getDetailedExecutionPlan();

    public Date getDateAfter(Date d, int s) {
        Calendar now = Calendar.getInstance();
        now.setTime(d);
//        System.out.println("原先的时间是：" + now.getTime() + ",需要增加" + s + "秒");
        now.add(Calendar.SECOND, s);
        // now.set(Calendar.DATE, now.get(Calendar.DATE) + day);
//        System.out.println("现在的时间是：" + now.getTime());
        return now.getTime();
    }

//    public static void setTableSize(int orderOriginSize, int olOriginSize, int olNotnullSize) {
//        this.orderOriginSize = orderOriginSize;
//
//    }
}
