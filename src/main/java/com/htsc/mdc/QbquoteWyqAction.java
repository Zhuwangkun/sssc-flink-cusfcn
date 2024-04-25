package com.htsc.mdc;


import com.htsc.mdc.model.*;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

@FunctionHint(output = @DataTypeHint("ROW<" +
        "BidNetPrice                                   String,\n" +
        "BidYield                                      String,\n" +
        "BidSsDetect                                   String,\n" +
        "OfrNetPrice                                   String,\n" +
        "SecurityType                                  String,\n" +
        "SecurityIDSource                              String,\n" +
        "Symbol                                        String,\n" +
        "MDLevel                                       String,\n" +
        "OfferPx                                       String,\n" +
        "BrokerDataType                                String,\n" +
        "OfrRelationFlag                               String,\n" +
        "OfrExerciseFlag                               String,\n" +
        "BidComment                                    String,\n" +
        "MessageNumber                                 String,\n" +
        "TradingPhaseCode                              String,\n" +
        "OfrYield                                      String,\n" +
        "MDRecordType                                  String,\n" +
        "OfferSize                                     String,\n" +
        "OfrBargainFlag                                String,\n" +
        "MDValidType                                   String,\n" +
        "BidExerciseFlag                               String,\n" +
        "OfrSsDetect                                   String,\n" +
        "BidID                                         String,\n" +
        "SecurityID                                    String,\n" +
        "OfrID                                         String,\n" +
        "SCQuoteTime                                   String,\n" +
        "MDChannel                                     String,\n" +
        "OfrComment                                    String,\n" +
        "BidSize                                       String,\n" +
        "ReceiveDateTime                               String,\n" +
        "HTSCSecurityID                                String,\n" +
        "MDTime                                        String,\n" +
        "BidBargainFlag                                String,\n" +
        "MDDate                                        String,\n" +
        "tSecuritySubType                              String,\n" +
        "tBidRelationFlag                              String,\n" +
        "BidPx                                         String,\n" +
        "SinkTime                                      String>")
)
public class QbquoteWyqAction extends TableFunction<Row> {

    Logger  logger = LoggerFactory.getLogger(QbTransactionWyqAuction.class);
    public void eval(byte[] rawData)  {
        try {

            MDSecurityRecordProtos.MDSecurityRecord mdSecurityRecord = MDSecurityRecordProtos.MDSecurityRecord.parseFrom(rawData);
            EMDRecordTypeProtos.EMDRecordType mdRecordType = mdSecurityRecord.getMDRecordType();
            String name = mdRecordType.getValueDescriptor().getName();
            if (name.equals("QBQuoteType")) {
                QBQuoteRecordProtos.QBQuoteRecord qbQuote = mdSecurityRecord.getQBQuote();
                Row row = new Row(38);
                row.setField(0,  qbQuote.getBidNetPrice()          +""        );
                row.setField(1,  qbQuote.getBidYield()             +""     );
                row.setField(2,  qbQuote.getBidSsDetect()          +""        );
                row.setField(3,  qbQuote.getOfrNetPrice()          +""       );
                row.setField(4,  qbQuote.getSecurityType().getNumber()                  + "");
                row.setField(5,  qbQuote.getSecurityIDSource().getNumber()                  + "");
                row.setField(6,  qbQuote.getSymbol()                  + "");
                row.setField(7,  qbQuote.getMDLevel().getNumber()                  + "");
                row.setField(8,  qbQuote.getOfferPx()             +""   );
                row.setField(9,  qbQuote.getBrokerDataType()      +""            );
                row.setField(10, qbQuote.getOfrRelationFlag()     +""              );
                row.setField(11, qbQuote.getOfrExerciseFlag()     +""             );
                row.setField(12, qbQuote.getBidComment()          +""         );
                row.setField(13, qbQuote.getMessageNumber()       +""           );
                row.setField(14, qbQuote.getTradingPhaseCode()                  + "");
                row.setField(15, qbQuote.getOfrYield()           +""    );
                row.setField(16, qbQuote.getMDRecordType().getNumber()                  + "");
                row.setField(17, qbQuote.getOfferSize()          +""      );
                row.setField(18, qbQuote.getOfrBargainFlag()     +""            );
                row.setField(19, qbQuote.getMDValidType().getNumber()                  + "");
                row.setField(20, qbQuote.getBidExerciseFlag()     +""         );
                row.setField(21, qbQuote.getOfrSsDetect()         +""       );
                row.setField(22, qbQuote.getBidID()                  + "");
                row.setField(23, qbQuote.getSecurityID()                  + "");
                row.setField(24, qbQuote.getOfrID()                  + "");
                row.setField(25, qbQuote.getSCQuoteTime()                  + "");
                row.setField(26, qbQuote.getMDChannel().getNumber()                  + "");
                row.setField(27, qbQuote.getOfrComment()                  + "");
                row.setField(28, qbQuote.getBidSize()               +""  );
                row.setField(29, qbQuote.getReceiveDateTime()       +""         );
                row.setField(30, qbQuote.getHTSCSecurityID()                  + "");
                row.setField(31, qbQuote.getMDTime()                  + "");
                row.setField(32, qbQuote.getBidBargainFlag()      +""       );
                row.setField(33, qbQuote.getMDDate()                  + "");
                row.setField(34, qbQuote.getSecuritySubType()                  + "");
                row.setField(35, qbQuote.getBidRelationFlag()  +""                );
                row.setField(36, qbQuote.getBidPx()             +"");
                row.setField(37, System.currentTimeMillis()             +"");

                this.collect(row);

            }
        }
        catch (Throwable t){
            logger.error("eval rawData={} failed",rawData,t);

        }
    }

}
