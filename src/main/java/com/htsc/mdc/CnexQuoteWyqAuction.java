package com.htsc.mdc;


import com.htsc.mdc.model.*;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

@FunctionHint(output = @DataTypeHint("ROW<" +
        "QuoteID                                  string,\n" +
        "QuoteType                                  string,\n" +
        "CnexDataType                               string,\n" +
        "CnexSecurityType                           string,\n" +
        "MDDate                           string,\n" +
        "SecurityIDSource                           string,\n" +
        "Symbol                                     string,\n" +
        "CreditRating                               string,\n" +
        "HTSCSecurityID                             string,\n" +
        "MDTime                             string,\n" +
        "ReceiveDateTime                             string,\n" +
        "MatchId                             string,\n" +
        "IssueDataTime                             string,\n" +
        "MDChannel                             string,\n" +
        "MaturityMonthYear                             string,\n" +
        "MDValidType                                string>")
)
public class CnexQuoteWyqAuction extends TableFunction<Row> {

    Logger  logger = LoggerFactory.getLogger(QbTransactionWyqAuction.class);
    public void eval(byte[] rawData)  {
        try {
//            CnexQuoteRecordProtos.CnexQuoteRecord cnexQuoteRecord = CnexQuoteRecordProtos.CnexQuoteRecord.parseFrom(rawData);
            MDSecurityRecordProtos.MDSecurityRecord mdSecurityRecord = MDSecurityRecordProtos.MDSecurityRecord.parseFrom(rawData);
            EMDRecordTypeProtos.EMDRecordType mdRecordType = mdSecurityRecord.getMDRecordType();
            String name = mdRecordType.getValueDescriptor().getName();
            if (name.equals("CnexQuoteType")) {
                CnexQuoteRecordProtos.CnexQuoteRecord cnexQuote = mdSecurityRecord.getCnexQuote();
                Row row = new Row(16);
                row.setField(0,  cnexQuote.getQuoteID()             +""      );
                row.setField(1,  cnexQuote.getQuoteType()           +""        );
                row.setField(2,  cnexQuote.getCnexDataType()        +""           );
                row.setField(3,  cnexQuote.getCnexSecurityType()    +""              );
                row.setField(4,  cnexQuote.getMDDate()    +""               );
                row.setField(5,  cnexQuote.getSecurityIDSource()    +""               );
                row.setField(6,  cnexQuote.getSymbol()              +""     );
                row.setField(7,  cnexQuote.getCreditRating()        +""           );
                row.setField(8,  cnexQuote.getHTSCSecurityID()      +""          );
                row.setField(9,  cnexQuote.getMDTime()                +""  );
                row.setField(10, cnexQuote.getReceiveDateTime()       +""            );
                row.setField(11, cnexQuote.getMatchId()               +""   );
                row.setField(12, cnexQuote.getIssueDataTime()         +""          );
                row.setField(13, cnexQuote.getMDChannel()             +""     );
                row.setField(14, cnexQuote.getMaturityMonthYear()     +""              );
                row.setField(15, cnexQuote.getMDValidType()           +""    );
//                row.setField(16, cnexQuote.getMDRecordType()        +"           );
//                row.setField(17, cnexQuote.getOfferSize()                 );
//                row.setField(18, cnexQuote.getOfrBargainFlag()                  );
//                row.setField(19, cnexQuote.getMDValidType()                   );
//                row.setField(20, cnexQuote.getBidExerciseFlag()              );
//                row.setField(21, cnexQuote.getOfrSsDetect()                );
//                row.setField(22, cnexQuote.getBidID()                   );
//                row.setField(23, cnexQuote.getSecurityID()                   );
//                row.setField(24, cnexQuote.getOfrID()                   );
//                row.setField(25, cnexQuote.getSCQuoteTime()                   );
//                row.setField(26, cnexQuote.getMDChannel()                   );
//                row.setField(27, cnexQuote.getOfrComment()                   );
//                row.setField(28, cnexQuote.getBidSize()                  );
//                row.setField(29, cnexQuote.getReceiveDateTime()                 );
//                row.setField(30, cnexQuote.getHTSCSecurityID()                   );
//                row.setField(31, cnexQuote.getMDTime()                   );
//                row.setField(32, cnexQuote.getBidBargainFlag()             );
//                row.setField(33, cnexQuote.getMDDate()                   );
//                row.setField(34 , cnexQuote.getSecuritySubType()                   );
//                row.setField(35 , cnexQuote.getBidRelationFlag()                  );
//                row.setField(36, cnexQuote.getBidPx()             );

                this.collect(row);

            }
        }
        catch (Throwable t){

            logger.error("eval rawData={} failed",rawData,t);
            logger.error("____________________1111_____________" );

        }
    }

}

