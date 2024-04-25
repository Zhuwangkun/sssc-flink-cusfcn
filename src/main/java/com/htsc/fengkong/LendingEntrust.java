package com.htsc.fengkong;


import com.google.protobuf.ByteString;
import com.htsc.morphling.protocol.MorphlingEntry;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.List;


@FunctionHint(output = @DataTypeHint("ROW<" +
        "id                   String  , \n" +
        "user_id              String  , \n" +
        "entrust_no           String  , \n" +
        "orig_entrust_no      String  , \n" +
        "entrust_type         String  , \n" +
        "user_type            String  , \n" +
        "source               String  , \n" +
        "client_id            String  , \n" +
        "fund_account         String  , \n" +
        "stock_account        String  , \n" +
        "seat_no              String  , \n" +
        "stock_code           String  , \n" +
        "stock_type           String  , \n" +
        "exchange_type        String  , \n" +
        "shdc_circulate_type  String  , \n" +
        "stock_property       String  , \n" +
        "stock_source         String  , \n" +
        "stock_name           String  , \n" +
        "stock_price          String  , \n" +
        "term                 String  , \n" +
        "agreement_no         String  , \n" +
        "sf_agreement_no      String  , \n" +
        "entrust_amount       String  , \n" +
        "deal_amount          String  , \n" +
        "intention_amount     String  , \n" +
        "apply_rate           String  , \n" +
        "fee                  String  , \n" +
        "exp_time             String  , \n" +
        "exp_date             String  , \n" +
        "cancel_time          String  , \n" +
        "matching_flag        String  , \n" +
        "sysnode_id           String  , \n" +
        "branch_no            String  , \n" +
        "stock_src_group_id   String  , \n" +
        "out_entrust_no       String  , \n" +
        "status               String  , \n" +

//        "cancel_flag          String  , \n" +
//        "op_station           String  , \n" +
//        "cre_time             String  , \n" +
//        "upd_time             String  , \n" +

        "schema_name String,\n" +
        "table_name String,\n" +
        "optype_name String,\n" +
        "opts_time String,\n" +
        "udtf_process_time Timestamp>"))
public class LendingEntrust extends TableFunction<Row> {

    Logger LOG = LoggerFactory.getLogger(LendingEntrust.class);

    long errorCount = 0L;
    String schemaName = null;
    String tableName = null;
    String opTypeName = null;
    String columeName = null;
    ByteString columeValue = null;
    String columeType = null;
    String optsTime = null;

    public LendingEntrust() {
    }

    public void eval(byte[] rawData) {
        try {
            MorphlingEntry.Entry entry = MorphlingEntry.Entry.parseFrom(rawData);
            MorphlingEntry.Header header = entry.getHeader();
            this.schemaName = header.getSchemaName().toUpperCase();
            this.tableName = header.getTableName().toUpperCase();
            this.optsTime = header.getPropsList().get(0).getValue();

            MorphlingEntry.RowChange rowChange = MorphlingEntry.RowChange.parseFrom(entry.getStoreValue());
            List<MorphlingEntry.RowData> rowDataModelList = rowChange.getRowDatasList();

            if (this.schemaName.equals("SLS") && this.tableName.equals("SECURITY_LENDING_ENTRUST")) {
                MorphlingEntry.EventType opType = header.getEventType();
                for (int rowDataIndex = 0; rowDataIndex < rowDataModelList.size(); ++rowDataIndex) {
                      MorphlingEntry.RowData morphlingEntryRowData;
                    List beforeColumns;
                    if (MorphlingEntry.EventType.INSERT.equals(opType)) {
                        this.opTypeName = "insert";
                        morphlingEntryRowData = (MorphlingEntry.RowData) rowDataModelList.get(rowDataIndex);
                        beforeColumns = morphlingEntryRowData.getAfterColumnsList();
                        this.collectRows(beforeColumns);
                    } else if (MorphlingEntry.EventType.DELETE.equals(opType)) {
                        this.opTypeName = "delete";
                        morphlingEntryRowData = (MorphlingEntry.RowData) rowDataModelList.get(rowDataIndex);
                        beforeColumns = morphlingEntryRowData.getBeforeColumnsList();
                        this.collectRows(beforeColumns);
                    } else if (MorphlingEntry.EventType.UPDATE.equals(opType)) {
                        morphlingEntryRowData = (MorphlingEntry.RowData) rowDataModelList.get(rowDataIndex);
                        this.opTypeName = "update";
                        List<MorphlingEntry.Column> afterColumns = morphlingEntryRowData.getAfterColumnsList();
                        this.collectRows(afterColumns);
                    }
                }
            }
        } catch (Throwable var11) {
            if (this.errorCount % 10000L == 0L) {
                this.LOG.info(var11.toString(), var11);
                var11.printStackTrace();
                ++this.errorCount;
                this.LOG.info("table function error count: " + this.errorCount);
            }
        }
    }

    private void collectRows(List<MorphlingEntry.Column> columns) {
        int cls = 36;
        Row row = new Row(5 + cls);
        for (int i = 0; columns != null && i < cls; ++i) {
            this.columeName = ((MorphlingEntry.Column) columns.get(i)).getName();
            this.columeValue = ((MorphlingEntry.Column) columns.get(i)).getValue();
            this.columeType = ((MorphlingEntry.Column) columns.get(i)).getMysqlType();
            if (this.columeValue.toStringUtf8() == null || "".equals(this.columeValue.toStringUtf8()) ){
                row.setField(i, null);
            }else{
                row.setField(i, this.columeValue.toStringUtf8());
            }
            if (this.columeName.equalsIgnoreCase("status")){
                break;
            }
        }
        row.setField(cls, this.schemaName);
        row.setField(cls + 1, this.tableName);
        row.setField(cls + 2, this.opTypeName);
        row.setField(cls + 3, this.optsTime);
        //获取当前时间作为udtf处理时间
        LocalDateTime udtfProcessTime = LocalDateTime.now();
        row.setField(cls + 4, udtfProcessTime);
        //this.LOG.info("row:::" + row.toString());
        this.collect(row);
    }
}