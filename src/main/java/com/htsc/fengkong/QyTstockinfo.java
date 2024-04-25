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

        "business_date                        String    ,\n" +
        "market_no                            String    ,\n" +
        "stock_type                           String    ,\n" +
        "asset_type                           String    ,\n" +
        "report_code                          String    ,\n" +
        "inter_code                           String    ,\n" +
        "stock_name                           String    ,\n" +
        "stock_fullname                       String    ,\n" +
        "stock_spell                          String    ,\n" +
        "stock_status                         String    ,\n" +
        "underwriter_id_list                  String    ,\n" +
        "wx_listing_date                      String    ,\n" +
        "listing_date                         String    ,\n" +
        "frozen_code                          String    ,\n" +
        "asset_relative_code                  String    ,\n" +
        "match_relative_code                  String    ,\n" +
        "issuer_id                            String    ,\n" +
        "trade_currency_no                    String    ,\n" +
        "settle_currency_no                   String    ,\n" +
        "uplimited_amount                     String    ,\n" +
        "downlimited_amount                   String    ,\n" +
        "buy_unit                             String    ,\n" +
        "sale_unit                            String    ,\n" +
        "amount_per_hand                      String    ,\n" +
        "uplimited_ratio                      String    ,\n" +
        "downlimited_ratio                    String    ,\n" +
        "transfer_mode                        String    ,\n" +
        "stb_trans_status                     String    ,\n" +
        "total_share                          String    ,\n" +
        "outstanding_share                    String    ,\n" +
        "non_turnover_share                   String    ,\n" +
        "reference_price                      String    ,\n" +
        "mmbz                                 String    ,\n" +
        "board_type                           String    ,\n" +
        "long_stop_flag                       String    ,\n" +
        "marketprice_order_max                String    ,\n" +
        "marketprice_order_min                String    ,\n" +
        "issue_total_share                    String    ,\n" +
        "publish_date                         String    ,\n" +
        "online_estimated_ratio               String    ,\n" +
        "offline_estimate_ratio               String    ,\n" +
        "gzstock_layer                        String    ,\n" +

        "mktp_buy_amount_unit                 string    ,\n" +
        "mktp_sale_amount_unit                string    ,\n" +
        "no_profit_flag                       string    ,\n" +
        "voting_right_diff_flag               string    ,\n" +
        "regist_system_flag                   string    ,\n" +
        "vie_flag                             string    ,\n" +
        "stock_stop_flag                      string    ,\n" +

        "schema_name String,\n" +
        "table_name String,\n" +
        "optype_name String,\n" +
        "opts_time String,\n" +
        "udtf_process_time Timestamp>"))
public class QyTstockinfo extends TableFunction<Row> {

    Logger LOG = LoggerFactory.getLogger(QyTstockinfo.class);

    long errorCount = 0L;
    String schemaName = null;
    String tableName = null;
    String opTypeName = null;
    String columeName = null;
    ByteString columeValue = null;
    String columeType = null;
    String optsTime = null;

    public QyTstockinfo() {
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

            if (this.schemaName.equalsIgnoreCase("dbtrade") && this.tableName.equalsIgnoreCase("qy_tstockinfo")) {
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
                row.setField(i, "0");
            }else{
                row.setField(i, this.columeValue.toStringUtf8());
            }
//            if (this.columeName.equalsIgnoreCase("gzstock_layer")){
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