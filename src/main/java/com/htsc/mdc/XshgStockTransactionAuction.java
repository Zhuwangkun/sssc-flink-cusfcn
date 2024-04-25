package com.htsc.mdc;


import com.htsc.mdc.model.*;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@FunctionHint(output = @DataTypeHint("ROW<" +
        "MDDate                   String,\n" +
        "MDTime                   String,\n" +
        "SecurityType             String,\n" +
        "SecuritySubType          String,\n" +
        "SecurityID               String,\n" +
        "SecurityIDSource         String,\n" +
        "Symbol                   String,\n" +
        "TradeIndex               String,\n" +
        "TradeBuyNo               String,\n" +
        "TradeSellNo              String,\n" +
        "TradeType                String,\n" +
        "TradeBSFlag              String,\n" +
        "TradePrice               String,\n" +
        "TradeQty                 String,\n" +
        "TradeMoney               String,\n" +
        "HTSCSecurityID           String,\n" +
        "ReceiveDateTime          String,\n" +
        "ChannelNo                String,\n" +
        "ApplSeqNum               String>"))
public class XshgStockTransactionAuction extends TableFunction<Row> {


    Logger logger = LoggerFactory.getLogger(XshgStockTransactionAuction.class);

    public void eval(byte[] rawData)  {
        try {

            MDSecurityRecordProtos.MDSecurityRecord mdSecurityRecord = MDSecurityRecordProtos.MDSecurityRecord.parseFrom(rawData);

            //判断MDRecordType（MarketType-行情、TransactionType-逐笔委托、OrderType-逐笔成交）
            EMDRecordTypeProtos.EMDRecordType mdRecordType = mdSecurityRecord.getMDRecordType();
            String name = mdRecordType.getValueDescriptor().getName();

            if (name.equals("TransactionType")){

                //MDOrderRecordProtos.MDOrderRecord mdOrder = mdSecurityRecord.getMDOrder();
                //MDStockRecordProtos.MDStockRecord mdStock = mdSecurityRecord.getMDStock();
                MDTransactionRecordProtos.MDTransactionRecord mdTransaction = mdSecurityRecord.getMDTransaction();

                Row row = new Row(19);
                row.setField(0,mdTransaction.getMDDate()+"");
                row.setField(1,mdTransaction.getMDTime()+"");
                row.setField(2,mdTransaction.getSecurityType()+"");
                row.setField(3,mdTransaction.getSecuritySubType()+"");
                row.setField(4,mdTransaction.getSecurityID()+"");
                row.setField(5,mdTransaction.getSecurityIDSource()+"");
                row.setField(6,mdTransaction.getSymbol()+"");
                row.setField(7,mdTransaction.getTradeIndex()+"");
                row.setField(8,mdTransaction.getTradeBuyNo()+"");
                row.setField(9,mdTransaction.getTradeSellNo()+"");
                row.setField(10,mdTransaction.getTradeType()+"");
                row.setField(11,mdTransaction.getTradeBSFlag()+"");
                row.setField(12,mdTransaction.getTradePrice()+"");
                row.setField(13,mdTransaction.getTradeQty()+"");
                row.setField(14,mdTransaction.getTradeMoney()+"");
                row.setField(15,mdTransaction.getHTSCSecurityID()+"");
                row.setField(16,mdTransaction.getReceiveDateTime()+"");
                row.setField(17,mdTransaction.getChannelNo()+"");
                row.setField(18,mdTransaction.getApplSeqNum()+"");
                collect(row);
            }
        } catch (Throwable t) {
            logger.error("eval rawData={} failed", rawData, t);
        }
    }

}