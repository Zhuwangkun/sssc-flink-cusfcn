package com.htsc.fengkong;


import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
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

        "business_date                   String    ,\n" +
        "company_id                      String    ,\n" +
        "fund_id                         String    ,\n" +
        "asset_id                        String    ,\n" +
        "combi_id                        String    ,\n" +
        "invest_type                     String    ,\n" +
        "position_type                   String    ,\n" +
        "market_no                       String    ,\n" +
        "report_code                     String    ,\n" +
        "stock_name                      String    ,\n" +
        "bind_seat                       String    ,\n" +
        "stockholder_id                  String    ,\n" +
        "inter_code                      String    ,\n" +
        "begin_amount                    String    ,\n" +
        "current_amount                  String    ,\n" +
        "buy_amount                      String    ,\n" +
        "sale_amount                     String    ,\n" +
        "buy_balance                     String    ,\n" +
        "sale_balance                    String    ,\n" +
        "buy_fee                         String    ,\n" +
        "sale_fee                        String    ,\n" +
        "real_buy_fee                    String    ,\n" +
        "real_sale_fee                   String    ,\n" +
        "begin_original_cost             String    ,\n" +
        "begin_carryover_cost            String    ,\n" +
        "begin_original_interest_cost    String    ,\n" +
        "begin_carryover_interest_cost   String    ,\n" +
        "begin_carryover_real_cost       String    ,\n" +
        "begin_original_real_cost        String    ,\n" +
        "begin_original_profit           String    ,\n" +
        "begin_carryover_profit          String    ,\n" +
        "begin_original_real_profit      String    ,\n" +
        "begin_carryover_real_profit     String    ,\n" +
        "begin_original_int_profit       String    ,\n" +
        "begin_carryover_int_profit      String    ,\n" +
        "vote_rights_amount              String    ,\n" +
        "frozen_amount                   String    ,\n" +
        "unfrozen_amount                 String    ,\n" +
        "lastday_left_amount             String    ,\n" +
        "current_cost                    String    ,\n" +
        "today_close_profit              String    ,\n" +
        "total_close_profit              String    ,\n" +
        "org_id                          String    ,\n" +
        "fund_trade_account              String    ,\n" +
        "ontheway_dividend               String    ,\n" +
        "uncarryover_dividend            String    ,\n" +
        "dividend                        String    ,\n" +
        "stock_type                      String    ,\n" +
        "asset_type                      String    ,\n" +
        "fa_bond_value_type              String    ,\n" +
        "fa_price                        String    ,\n" +
        "accumulate_profit               String    ,\n" +
        "floating_profit                 String    ,\n" +
        "position_market_value           String    ,\n" +
        "position_stock_type             String    ,\n" +

        "restricted_amount               string    ,\n" +
        "check_data                      string    ,\n" +
        "current_real_cost               string    ,\n" +
        "full_price_today_real_profit    string    ,\n" +
        "full_price_floating_profit      string    ,\n" +
        "shareout_right_amount           string    ,\n" +
        "today_shareout                  string    ,\n" +
        "open_cost                       string    ,\n" +

        "schema_name String,\n" +
        "table_name String,\n" +
        "optype_name String,\n" +
        "opts_time String,\n" +
        "udtf_process_time Timestamp>"))
public class BbTunitstock extends TableFunction<Row> {

    Logger LOG = LoggerFactory.getLogger(BbTunitstock.class);

    long errorCount = 0L;
    String schemaName = null;
    String tableName = null;
    String opTypeName = null;
    String columeName = null;
    ByteString columeValue = null;
    String columeType = null;
    String optsTime = null;

    public BbTunitstock() {
    }

    public void eval(byte[] rawData) throws InvalidProtocolBufferException {
//        try {
            MorphlingEntry.Entry entry = MorphlingEntry.Entry.parseFrom(rawData);
            MorphlingEntry.Header header = entry.getHeader();
            this.schemaName = header.getSchemaName().toUpperCase();
            this.tableName = header.getTableName().toUpperCase();
            this.optsTime = header.getPropsList().get(0).getValue();

            MorphlingEntry.RowChange rowChange = MorphlingEntry.RowChange.parseFrom(entry.getStoreValue());
            List<MorphlingEntry.RowData> rowDataModelList = rowChange.getRowDatasList();

            if (this.schemaName.equalsIgnoreCase("dbreport") && this.tableName.equalsIgnoreCase("bb_tunitstock")) {
                System.out.println(this.optsTime+"       "+this.schemaName+"     "+this.tableName);
                MorphlingEntry.EventType opType = header.getEventType();
                for (int rowDataIndex = 0; rowDataIndex < rowDataModelList.size(); ++rowDataIndex) {
                      MorphlingEntry.RowData morphlingEntryRowData;
                    List beforeColumns;
                    if (MorphlingEntry.EventType.INSERT.equals(opType)) {
                        this.opTypeName = "insert";
                        morphlingEntryRowData = (MorphlingEntry.RowData) rowDataModelList.get(rowDataIndex);
                        beforeColumns = morphlingEntryRowData.getAfterColumnsList();
                        this.collectRows(beforeColumns);
                        this.LOG.info("insert_flag::: " + beforeColumns.size());
                    } else if (MorphlingEntry.EventType.DELETE.equals(opType)) {
                        this.opTypeName = "delete";
                        morphlingEntryRowData = (MorphlingEntry.RowData) rowDataModelList.get(rowDataIndex);
                        beforeColumns = morphlingEntryRowData.getBeforeColumnsList();
                        this.collectRows(beforeColumns);
                        this.LOG.info("delete_flag::: " + beforeColumns.size());
                    } else if (MorphlingEntry.EventType.UPDATE.equals(opType)) {
                        morphlingEntryRowData = (MorphlingEntry.RowData) rowDataModelList.get(rowDataIndex);
                        this.opTypeName = "update";
                        List<MorphlingEntry.Column> afterColumns = morphlingEntryRowData.getAfterColumnsList();
                        this.collectRows(afterColumns);
                        this.LOG.info("update_flag::: " + afterColumns.size());
                    }
                }
            }
//        } catch (Throwable var11) {
//            if (this.errorCount % 10000L == 0L) {
//                this.LOG.info(var11.toString(), var11);
//                var11.printStackTrace();
//                ++this.errorCount;
//                this.LOG.info("table function error count: " + this.errorCount);
//            }
//        }
    }

    private void collectRows(List<MorphlingEntry.Column> columns) {
        int cls = columns.size();
        Row row = new Row(5 + cls);
        for (int i = 0; columns != null && i < cls; ++i) {
            this.columeName = ((MorphlingEntry.Column) columns.get(i)).getName();
            this.columeValue = ((MorphlingEntry.Column) columns.get(i)).getValue();
            this.columeType = ((MorphlingEntry.Column) columns.get(i)).getMysqlType();
            if (this.columeValue.toStringUtf8() == null || "".equalsIgnoreCase(this.columeValue.toStringUtf8()) ){
                row.setField(i, "0");
            }else{
                row.setField(i, this.columeValue.toStringUtf8());
            }
//            if (this.columeName.equalsIgnoreCase("position_stock_type")){
//                break;
//            }
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