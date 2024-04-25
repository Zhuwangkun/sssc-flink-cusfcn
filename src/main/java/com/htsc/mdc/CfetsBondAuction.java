package com.htsc.mdc;


import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.htsc.mdc.model.*;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.text.DecimalFormat;

@FunctionHint(output = @DataTypeHint("ROW<" +
        "HTSCSecurityID                       string,\n" +
        "MDTime                               string,\n" +
        "TradeIndex                           string,\n" +
        "FITradingMethod                      string,\n" +
        "SecurityType                         int,\n" +
        "Symbol                               string,\n" +
        "TradeQty                             double,\n" +
        "MDDate                               string,\n" +
        "TradePrice                           double,\n" +
        "SecurityID                           string,\n" +
        "SecurityIDSource                     int,\n" +
        "ReceiveDateTime                      bigint,\n" +
        "TradeType                            int,\n" +
        "MaturityYield                        double,\n" +
        "SecuritySubType                      string,\n" +
        "MDRecordType                         int,\n" +
        "MDValidType                          int,\n" +
        "HKTradeType                          int,\n" +
        "SettlType                            int,\n" +
        "Duration                             double,\n" +
        "TradeMoney                           double,\n" +
        "ChannelNo                            int,\n" +
        "MDChannel                            int>")
)
public class CfetsBondAuction extends TableFunction<Row> {

    Logger  logger = LoggerFactory.getLogger(CfetsBondAuction.class);
    public void eval(byte[] rawData)  {
        try {

            MDSecurityRecordProtos.MDSecurityRecord mdSecurityRecord = MDSecurityRecordProtos.MDSecurityRecord.parseFrom(rawData);
            EMDRecordTypeProtos.EMDRecordType mdRecordType = mdSecurityRecord.getMDRecordType();
            String name = mdRecordType.getValueDescriptor().getName();
            if (name.equals("TransactionType")) {
                MDTransactionRecordProtos.MDTransactionRecord mdTransaction = mdSecurityRecord.getMDTransaction();

                Row row = new Row(23);
                row.setField(0,       mdTransaction.getHTSCSecurityID()+"" );
                row.setField(1,       mdTransaction.getMDTime()+"" );
                row.setField(2,       mdTransaction.getTradeIndex() +"");
                row.setField(3,       mdTransaction.getFITradingMethod()+""  );
                row.setField(4,       mdTransaction.getSecurityType().getNumber() );
                row.setField(5,       mdTransaction.getSymbol()+"" );
                row.setField(6,       mdTransaction.getTradeQty() );
                row.setField(7,       mdTransaction.getMDDate() +"" );
                row.setField(8,       mdTransaction.getTradePrice()  );
                row.setField(9,       mdTransaction.getSecurityID()+"" );
                row.setField(10,      mdTransaction.getSecurityIDSource().getNumber()  );
                row.setField(11,      mdTransaction.getReceiveDateTime()  );
                row.setField(12,      mdTransaction.getTradeType() );
                row.setField(13,      mdTransaction.getMaturityYield()  );
                row.setField(14,      mdTransaction.getSecuritySubType()+"" );
                row.setField(15,      mdTransaction.getMDRecordType().getNumber());
                row.setField(16,      mdTransaction.getMDValidType().getNumber() );
                row.setField(17,      mdTransaction.getHKTradeType() );
                row.setField(18,      mdTransaction.getSettlType()  );
                row.setField(19,      mdTransaction.getDuration()  );
                row.setField(20,      mdTransaction.getTradeMoney() );
                row.setField(21,      mdTransaction.getChannelNo() );
                row.setField(22,      mdTransaction.getMDChannel().getNumber()  );

                this.collect(row);

            }
        }
        catch (Throwable t){
            logger.error("错误===========2================");
            logger.error("eval rawData={} failed",rawData,t);

        }
    }

}

