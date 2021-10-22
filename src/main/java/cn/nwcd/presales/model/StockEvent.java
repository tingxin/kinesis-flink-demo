package cn.nwcd.presales.model;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


public class StockEvent extends Event {

    public final String name;
    public final String event_time;
    public final double price;
    public final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-ddTHH:mm:ss.SSS");

    public StockEvent() {
        price = 0;
        name = "";
        event_time = "";
    }


    @Override
    public String toString() {
        return "StockEvent{" +
                "name=" + name +
                ", event_time=" + event_time +
                ", price=" + price +
                '}';
    }

    @Override
    public long getTimestamp() {
        Date dateTime;
        try {
            dateTime = formatter.parse(this.event_time);
            return dateTime.getTime();

        } catch (ParseException e) {
            return 0;

        }
    }
}
