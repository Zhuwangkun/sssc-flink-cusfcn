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
        "init_date String,\n" +
        "curr_date String,\n" +
        "curr_time String,\n" +
        "op_branch_no String,\n" +
        "operator_no String,\n" +
        "op_entrust_way String,\n" +
        "op_station String,\n" +
        "entrust_no String,\n" +
        "orig_entrust_no String,\n" +
        "branch_no String,\n" +
        "fund_account String,\n" +
        "client_id String,\n" +
        "exchange_type String,\n" +
        "stock_account String,\n" +
        "stock_type String,\n" +
        "stock_code String,\n" +
        "refbusi_code String,\n" +
        "orig_refbusi_code String,\n" +
        "refentrust_source String,\n" +
        "cbpconfer_id String,\n" +
        "oppo_seatno String,\n" +
        "oppo_stkaccount String,\n" +
        "ref_term String,\n" +
        "refbase_rate String,\n" +
        "preend_date String,\n" +
        "entrust_amount String,\n" +
        "money_type String,\n" +
        "entrust_balance String,\n" +
        "busi_report_amount String,\n" +
        "intent_report_amount String,\n" +
        "withdraw_amount String,\n" +
        "intent_cancel_amount String,\n" +
        "business_amount String,\n" +
        "business_balance String,\n" +
        "refentrust_status String,\n" +
        "compact_id String,\n" +
        "position_str String,\n" +
        "company_no String,\n" +
        "fsmpsrcgroup_id String,\n" +
        "batch_id String,\n" +
        "schema_name String,\n" +
        "table_name String,\n" +
        "optype_name String,\n" +
        "opts_time String,\n" +
        "udtf_process_time Timestamp>"))
public class HsRefRefbentrustFormat extends TableFunction<Row> {

    Logger LOG = LoggerFactory.getLogger(HsRefRefbentrustFormat.class);

    long errorCount = 0L;
    String schemaName = null;
    String tableName = null;
    String opTypeName = null;
    String columeName = null;
    ByteString columeValue = null;
    String columeType = null;
    String optsTime = null;

    public HsRefRefbentrustFormat() {
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

            if (this.schemaName.equals("HS_REF") && this.tableName.equals("REFBENTRUST")) {
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
        int cls = columns.size();
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