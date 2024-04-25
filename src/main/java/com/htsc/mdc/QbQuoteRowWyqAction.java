package com.htsc.mdc;


import com.htsc.mdc.model.EMDRecordTypeProtos;
import com.htsc.mdc.model.MDSecurityRecordProtos;
import com.htsc.mdc.model.QBQuoteRecordProtos;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@FunctionHint(output = @DataTypeHint("ROW<" +
//         "RowData                String,\n" +
         "RecordName                   String >")
)
public class QbQuoteRowWyqAction extends TableFunction<Row> {

    Logger  logger = LoggerFactory.getLogger(QbTransactionWyqAuction.class);
    public void eval(byte[] rawData)  {
        try {

            MDSecurityRecordProtos.MDSecurityRecord mdSecurityRecord = MDSecurityRecordProtos.MDSecurityRecord.parseFrom(rawData);
            EMDRecordTypeProtos.EMDRecordType mdRecordType = mdSecurityRecord.getMDRecordType();
            String name = mdRecordType.getValueDescriptor().getName();
            Row row = new Row(0);
//            row.setField(0,mdSecurityRecord.getQBQuote()+"");
//            row.setField(1,mdRecordType.getValueDescriptor().getName()+"");
            row.setField(0,mdRecordType.getValueDescriptor().getName()+"");
            this.collect(row);
//            if (name.equals("QBQuoteType")) {
//                QBQuoteRecordProtos.QBQuoteRecord qbQuote = mdSecurityRecord.getQBQuote();
//                Row row = new Row(37);
//                row.setField(0,  qbQuote.getBidNetPrice()                   );
//                row.setField(1,  qbQuote.getBidYield()                   );
//                row.setField(2,  qbQuote.getBidSsDetect()                   );
//                row.setField(3,  qbQuote.getOfrNetPrice()                  );
//                row.setField(4,  qbQuote.getSecurityType()                  + "");
//                row.setField(5,  qbQuote.getSecurityIDSource()                  + "");
//                row.setField(6,  qbQuote.getSymbol()                  + "");
//                row.setField(7,  qbQuote.getMDLevel()                  + "");
//                row.setField(8,  qbQuote.getOfferPx()                );
//                row.setField(9,  qbQuote.getBrokerDataType()                  );
//                row.setField(10, qbQuote.getOfrRelationFlag()                   );
//                row.setField(11, qbQuote.getOfrExerciseFlag()                  );
//                row.setField(12, qbQuote.getBidComment()                  + "");
//                row.setField(13, qbQuote.getMessageNumber()                  );
//                row.setField(14, qbQuote.getTradingPhaseCode()                  + "");
//                row.setField(15, qbQuote.getOfrYield()               );
//                row.setField(16, qbQuote.getMDRecordType()                  + "");
//                row.setField(17, qbQuote.getOfferSize()                 );
//                row.setField(18, qbQuote.getOfrBargainFlag()                  );
//                row.setField(19, qbQuote.getMDValidType()                  + "");
//                row.setField(20, qbQuote.getBidExerciseFlag()              );
//                row.setField(21, qbQuote.getOfrSsDetect()                );
//                row.setField(22, qbQuote.getBidID()                  + "");
//                row.setField(23, qbQuote.getSecurityID()                  + "");
//                row.setField(24, qbQuote.getOfrID()                  + "");
//                row.setField(25, qbQuote.getSCQuoteTime()                  + "");
//                row.setField(26, qbQuote.getMDChannel()                  + "");
//                row.setField(27, qbQuote.getOfrComment()                  + "");
//                row.setField(28, qbQuote.getBidSize()                  );
//                row.setField(29, qbQuote.getReceiveDateTime()                 );
//                row.setField(30, qbQuote.getHTSCSecurityID()                  + "");
//                row.setField(31, qbQuote.getMDTime()                  + "");
//                row.setField(32, qbQuote.getBidBargainFlag()             );
//                row.setField(33, qbQuote.getMDDate()                  + "");
//                row.setField(34 , qbQuote.getSecuritySubType()                  + "");
//                row.setField(35 , qbQuote.getBidRelationFlag()                  );
//                row.setField(36, qbQuote.getBidPx()             );
//
//                this.collect(row);
//
//            }
        }
        catch (Throwable t){
            logger.error("eval rawData={} failed",rawData,t);

        }
    }

}
