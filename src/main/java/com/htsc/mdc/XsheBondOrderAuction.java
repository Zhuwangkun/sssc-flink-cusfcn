package com.htsc.mdc;


import com.htsc.mdc.model.EMDRecordTypeProtos;
import com.htsc.mdc.model.MDOrderRecordProtos;
import com.htsc.mdc.model.MDSecurityRecordProtos;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@FunctionHint(output = @DataTypeHint("ROW<" +
        "MDDate             String,\n" +
        "MDTime             String,\n" +
        "SecurityType       String,\n" +
        "SecuritySubType    String,\n" +
        "SecurityID         String,\n" +
        "SecurityIDSource   String,\n" +
        "Symbol             String,\n" +
        "HTSCSecurityID     String,\n" +
        "ReceiveDateTime    String,\n" +
        "OrderIndex         String,\n" +
        "OrderType          String,\n" +
        "OrderPrice         String,\n" +
        "OrderQty           String,\n" +
        "OrderBSFlag        String,\n" +
        "ChannelNo          String,\n" +
        "ApplSeqNum         String>"))
public class XsheBondOrderAuction extends TableFunction<Row> {


    Logger logger = LoggerFactory.getLogger(XsheBondOrderAuction.class);

    public void eval(byte[] rawData)  {
        try {

            MDSecurityRecordProtos.MDSecurityRecord mdSecurityRecord = MDSecurityRecordProtos.MDSecurityRecord.parseFrom(rawData);
            //判断MDRecordType（MarketType-行情、TransactionType-逐笔委托、OrderType-逐笔成交）
            EMDRecordTypeProtos.EMDRecordType mdRecordType = mdSecurityRecord.getMDRecordType();
            String name = mdRecordType.getValueDescriptor().getName();

            if (name.equals("OrderType")){

                MDOrderRecordProtos.MDOrderRecord mdOrder = mdSecurityRecord.getMDOrder();
                //MDStockRecordProtos.MDStockRecord mdStock = mdSecurityRecord.getMDStock();
                //MDTransactionRecordProtos.MDTransactionRecord mdTransaction = mdSecurityRecord.getMDTransaction();

                Row row = new Row(16);
                row.setField(0,mdOrder.getMDDate()+"");
                row.setField(1,mdOrder.getMDTime()+"");
                row.setField(2,mdOrder.getSecurityType()+"");
                row.setField(3,mdOrder.getSecuritySubType()+"");
                row.setField(4,mdOrder.getSecurityID()+"");
                row.setField(5,mdOrder.getSecurityIDSource()+"");
                row.setField(6,mdOrder.getSymbol()+"");
                row.setField(7,mdOrder.getHTSCSecurityID()+"");
                row.setField(8,mdOrder.getReceiveDateTime()+"");
                row.setField(9,mdOrder.getOrderIndex()+"");
                row.setField(10,mdOrder.getOrderType()+"");
                row.setField(11,mdOrder.getOrderPrice()+"");
                row.setField(12,mdOrder.getOrderQty()+"");
                row.setField(13,mdOrder.getOrderBSFlag()+"");
                row.setField(14,mdOrder.getChannelNo()+"");
                row.setField(15,mdOrder.getApplSeqNum()+"");
                collect(row);
            }
        } catch (Throwable t) {
            logger.error("eval rawData={} failed", rawData, t);
        }
    }

}