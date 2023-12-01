package bean;

import lombok.Getter;

import java.sql.Timestamp;

@Getter
public class OrderLine {
    public Timestamp ol_delivery_d;
    public Timestamp ol_receipdate;
    public Timestamp ol_commitdate;

    public OrderLine(Timestamp ol_delivery_d, Timestamp ol_commitdate, Timestamp ol_receiptdate) {
        this.ol_delivery_d = ol_delivery_d;
        this.ol_receipdate = ol_receiptdate;
        this.ol_commitdate = ol_commitdate;
    }

    public int compareToByReceiptDate(OrderLine other) {
        return ol_receipdate.compareTo(other.ol_receipdate);
    }

    public int compareToByDeliveryDate(OrderLine other) {
        return ol_delivery_d.compareTo(other.ol_delivery_d);
    }

    public int compareToByCommitDate(OrderLine other) {
        return ol_commitdate.compareTo(other.ol_commitdate);
    }
}