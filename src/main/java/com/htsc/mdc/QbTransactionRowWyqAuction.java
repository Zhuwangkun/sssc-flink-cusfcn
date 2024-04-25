package com.htsc.mdc;


import com.htsc.mdc.model.EMDRecordTypeProtos;
import com.htsc.mdc.model.MDSecurityRecordProtos;
import com.htsc.mdc.model.QBTransactionRecordProtos;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@FunctionHint(output = @DataTypeHint("ROW<" +
//        "MDValidType              string,\n" +
//        "Yield                    string\n" +
//        "SecuritySubType          string,\n" +
//        "FullPrice                string,\n" +
//        "MDLevel                  string,\n" +
//        "Side                     string,\n" +
//        "Symbol                   string,\n" +
//        "OrderQty                 string,\n" +
//        "SecurityID               string,\n" +
//        "HTSCSecurityID           string,\n" +
//        "MDDate                   string,\n" +
//        "MDRecordType             string,\n" +
//        "DealStatus               string,\n" +
//        "SecurityType             string,\n" +
//        "StrikePrice              string,\n" +
//        "MDTime                   string,\n" +
//        "BrokerDataType           string,\n" +
//        "ReceiveDateTime          string,\n" +
//        "InstrmtAssignmentMethod  string,\n" +
//        "OriginalPrice            string,\n" +
//        "PriceType                string,\n" +
//        "SecurityIDSource         string,\n" +
//        "MessageNumber            string,\n" +
//        "QuoteType                string,\n" +
//        "QuoteID                  string,\n" +
//        "QuoteReqID               string,\n" +
//        "TransactTime             string,\n" +
//        "TradingPhaseCode         string,\n" +
//        "MDChannel                string,\n" +
//        "ClearSpeed               string" +
          "RowDataLog          string>")
)
public class QbTransactionRowWyqAuction extends TableFunction<Row> {

    Logger  logger = LoggerFactory.getLogger(QbTransactionRowWyqAuction.class);
    public void eval(byte[] rawData)  {
        try {

            MDSecurityRecordProtos.MDSecurityRecord mdSecurityRecord = MDSecurityRecordProtos.MDSecurityRecord.parseFrom(rawData);
            EMDRecordTypeProtos.EMDRecordType mdRecordType = mdSecurityRecord.getMDRecordType();
            String name = mdRecordType.getValueDescriptor().getName();
            QBTransactionRecordProtos.QBTransactionRecord qbTransaction = mdSecurityRecord.getQBTransaction();
            Row row = new Row(1);
            row.setField(0,qbTransaction+"");
            this.collect(row);
//            if (name.equals("QBTransactionType")) {
//                QBTransactionRecordProtos.QBTransactionRecord qbTransaction = mdSecurityRecord.getQBTransaction();
//                Row row = new Row(30);
//                row.setField(0,  qbTransaction.getMDValidType().getNumber()+ "");
//                row.setField(1,  qbTransaction.getYield() + "" );
//                row.setField(2,  qbTransaction.getSecuritySubType() + "");
//                row.setField(3,  qbTransaction.getFullPrice() + "" );
////                row.setField(4,  qbTransaction.getMDLevel()+ "");
//                row.setField(4,  qbTransaction.getMDLevel().getNumber()+ "");
//                row.setField(5,  qbTransaction.getSide() + "" );
//                row.setField(6,  qbTransaction.getSymbol() + "");
//                row.setField(7,  qbTransaction.getOrderQty()+ "" );
//                row.setField(8,  qbTransaction.getSecurityID() + "");
//                row.setField(9,  qbTransaction.getHTSCSecurityID() + "");
//                row.setField(10, qbTransaction.getMDDate() + "");
////                row.setField(11, qbTransaction.getMDRecordType() +"" );
//                row.setField(11, qbTransaction.getMDRecordType().getNumber() +"" );
//                row.setField(12, qbTransaction.getDealStatus() + "" );
////                row.setField(13, qbTransaction.getSecurityType()+ "");
//                row.setField(13, qbTransaction.getSecurityType().getNumber()+ "");
//                row.setField(14, qbTransaction.getStrikePrice()+ ""  );
//                row.setField(15, qbTransaction.getMDTime() + "");
//                row.setField(16, qbTransaction.getBrokerDataType()+ "" );
//                row.setField(17, qbTransaction.getReceiveDateTime() + "" );
//                row.setField(18, qbTransaction.getInstrmtAssignmentMethod()+ ""  );
//                row.setField(19, qbTransaction.getOriginalPrice() + "");
//                row.setField(20, qbTransaction.getPriceType() + "" );
////                row.setField(21, qbTransaction.getSecurityIDSource()+ "");
//                row.setField(21, qbTransaction.getSecurityIDSource().getNumber()+ "");
//                row.setField(22, qbTransaction.getMessageNumber() + "" );
//                row.setField(23, qbTransaction.getQuoteType() + "" );
//                row.setField(24, qbTransaction.getQuoteID() + "");
//                row.setField(25, qbTransaction.getQuoteReqID() + "");
//                row.setField(26, qbTransaction.getTransactTime() + "");
//                row.setField(27, qbTransaction.getTradingPhaseCode() + "");
//                row.setField(28, qbTransaction.getMDChannel().getNumber()+ "");
//                row.setField(29, qbTransaction.getClearSpeed()+ "" );
//                row.setField(29, qbTransaction.get()+ "" );

//                this.collect(row);

//            }
        }
        catch (Throwable t){
            logger.error("__________________transaction_____________________");
            logger.error("eval rawData={} failed",rawData,t);

        }
    }

}
