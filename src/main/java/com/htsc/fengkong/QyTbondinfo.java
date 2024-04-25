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

        "business_date                          String    ,\n" +
        "inter_code                             String    ,\n" +
        "market_no                              String    ,\n" +
        "stock_type                             String    ,\n" +
        "asset_type                             String    ,\n" +
        "report_code                            String    ,\n" +
        "stock_name                             String    ,\n" +
        "stock_fullname                         String    ,\n" +
        "stock_spell                            String    ,\n" +
        "stock_status                           String    ,\n" +
        "trustee                                String    ,\n" +
        "net_price_type                         String    ,\n" +
        "curr_face_price                        String    ,\n" +
        "publish_price                          String    ,\n" +
        "issuer_id                              String    ,\n" +
        "face_value                             String    ,\n" +
        "publish_date                           String    ,\n" +
        "listing_date                           String    ,\n" +
        "wx_listing_date                        String    ,\n" +
        "underwriter_id_list                    String    ,\n" +
        "trade_deadline                         String    ,\n" +
        "expire_date                            String    ,\n" +
        "guarantor_id                           String    ,\n" +
        "frozen_code                            String    ,\n" +
        "asset_relative_code                    String    ,\n" +
        "match_relative_code                    String    ,\n" +
        "mother_company_ids                     String    ,\n" +
        "bond_count                             String    ,\n" +
        "investor_operation_type                String    ,\n" +
        "investor_operation_date                String    ,\n" +
        "change_price                           String    ,\n" +
        "change_begin_date                      String    ,\n" +
        "change_end_date                        String    ,\n" +
        "yesterday_change_price                 String    ,\n" +
        "zqhs_price                             String    ,\n" +
        "zqhs_begindate                         String    ,\n" +
        "zqhs_enddate                           String    ,\n" +
        "publisher_operation_type               String    ,\n" +
        "publisher_operation_date               String    ,\n" +
        "publisher_operation_price              String    ,\n" +
        "pay_interest_type                      String    ,\n" +
        "interest_pay_freq                      String    ,\n" +
        "value_date                             String    ,\n" +
        "nextpayment_date                       String    ,\n" +
        "interest_daytype                       String    ,\n" +
        "interest_rate_type                     String    ,\n" +
        "nominal_interest_rate                  String    ,\n" +
        "bond_interest                          String    ,\n" +
        "today_interest                         String    ,\n" +
        "next_tradedate_interest                String    ,\n" +
        "basic_rate_type                        String    ,\n" +
        "basic_rate                             String    ,\n" +
        "least_rate                             String    ,\n" +
        "yield_rate_type                        String    ,\n" +
        "chinabond_fair_yield_ratio             String    ,\n" +
        "chinasecuriti_fair_yield_ratio         String    ,\n" +
        "ms_maturity                            String    ,\n" +
        "valuate_duration                       String    ,\n" +
        "valuate_point_value                    String    ,\n" +
        "valuate_spread_duration                String    ,\n" +
        "valuate_rate_duration                  String    ,\n" +
        "valuate_convexity                      String    ,\n" +
        "valuate_waiting_period                 String    ,\n" +
        "pay_capital_type                       String    ,\n" +
        "bond_payment_type                      String    ,\n" +
        "amount_per_hand                        String    ,\n" +
        "uplimited_amount                       String    ,\n" +
        "downlimited_amount                     String    ,\n" +
        "buy_unit                               String    ,\n" +
        "sale_unit                              String    ,\n" +
        "uplimited_ratio                        String    ,\n" +
        "downlimited_ratio                      String    ,\n" +
        "reference_price                        String    ,\n" +
        "trade_platform_list                    String    ,\n" +
        "investor_operation_price               String    ,\n" +
        "duration_year                          String    ,\n" +
        "special_clause                         String    ,\n" +
        "bond_issue_remain_balance              String    ,\n" +
        "chinabond_fair_price                   String    ,\n" +
        "chinasecurities_fair_price             String    ,\n" +
        "issue_amount                           String    ,\n" +
        "city_build_bond_flag                   String    ,\n" +
        "perpetual_bond_flag                    String    ,\n" +
        "bond_guarantor_flag                    String    ,\n" +
        "bond_secondary_type_id                 String    ,\n" +
        "daytype                                String    ,\n" +
        "small_micro_bond_flag                  String    ,\n" +
        "interest_pay_times                     String    ,\n" +
        "public_issue_flag                      String    ,\n" +
        "online_estimated_ratio                 String    ,\n" +
        "offline_estimate_ratio                 String    ,\n" +
        "startcal_date                          String    ,\n" +

        "pass_sppi_test_flag                    string    ,\n" +
        "sppi_test_fail_reason                  string    ,\n" +
        "stock_stop_flag                        string    ,\n" +
        "impawn_unit                            string    ,\n" +
        "impawn_amount_upper                    string    ,\n" +
        "impawn_amount_lower                    string    ,\n" +
        "convert_unit                           string    ,\n" +
        "convert_amount_upper                   string    ,\n" +
        "convert_amount_lower                   string    ,\n" +
        "resale_unit                            string    ,\n" +
        "resale_amount_lower                    string    ,\n" +
        "resale_amount_upper                    string    ,\n" +

        "schema_name String,\n" +
        "table_name String,\n" +
        "optype_name String,\n" +
        "opts_time String,\n" +
        "udtf_process_time Timestamp>"))
public class QyTbondinfo extends TableFunction<Row> {

    Logger LOG = LoggerFactory.getLogger(QyTbondinfo.class);

    long errorCount = 0L;
    String schemaName = null;
    String tableName = null;
    String opTypeName = null;
    String columeName = null;
    ByteString columeValue = null;
    String columeType = null;
    String optsTime = null;


    public QyTbondinfo() {
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

            if (this.schemaName.equalsIgnoreCase("dbtrade") && this.tableName.equalsIgnoreCase("qy_tbondinfo")) {
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
            if (this.columeValue.toStringUtf8() == null || "".equalsIgnoreCase(this.columeValue.toStringUtf8()) ){
                row.setField(i, "0");
            }else{
                row.setField(i, this.columeValue.toStringUtf8());
            }
//            if (this.columeName.equalsIgnoreCase("startcal_date")){
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