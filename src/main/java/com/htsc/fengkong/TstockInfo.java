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

        "l_date                                  String        ,\n" +
        "vc_inter_code                           String        ,\n" +
        "c_market_no                             String        ,\n" +
        "vc_report_code                          String        ,\n" +
        "vc_stock_name                           String        ,\n" +
        "vc_stock_spell                          String        ,\n" +
        "c_stock_type                            String        ,\n" +
        "c_asset_class                           String        ,\n" +
        "en_uplimited_ratio                      String        ,\n" +
        "en_downlimited_ratio                    String        ,\n" +
        "en_uplimited_price                      String        ,\n" +
        "en_downlimited_price                    String        ,\n" +
        "l_uplimited_amount                      String        ,\n" +
        "l_downlimited_amount                    String        ,\n" +
        "c_default_price                         String        ,\n" +
        "vc_frozen_code                          String        ,\n" +
        "vc_relative_frozen_code                 String        ,\n" +
        "vc_assign_code                          String        ,\n" +
        "vc_asset_relative_code                  String        ,\n" +
        "vc_match_relative1_code                 String        ,\n" +
        "vc_match_relative2_code                 String        ,\n" +
        "l_total_amount                          String        ,\n" +
        "l_turnover_amount                       String        ,\n" +
        "en_repay_year                           String        ,\n" +
        "en_yesterday_close_price                String        ,\n" +
        "en_open_price                           String        ,\n" +
        "l_company_id                            String        ,\n" +
        "en_yield_ratio                          String        ,\n" +
        "vc_mixed_type                           String        ,\n" +
        "l_near_amount                           String        ,\n" +
        "l_publish_date                          String        ,\n" +
        "l_turnover_date                         String        ,\n" +
        "l_total_lock_days                       String        ,\n" +
        "l_left_lock_days                        String        ,\n" +
        "vc_stock_fullname                       String        ,\n" +
        "c_stop_flag                             String        ,\n" +
        "en_last_price                           String        ,\n" +
        "en_avg_price                            String        ,\n" +
        "en_max_price                            String        ,\n" +
        "en_min_price                            String        ,\n" +
        "l_market_deal_amount                    String        ,\n" +
        "en_market_deal_balance                  String        ,\n" +
        "en_reference_price                      String        ,\n" +
        "vc_international_code                   String        ,\n" +
        "vc_price_relative_code                  String        ,\n" +
        "vc_ric_code                             String        ,\n" +
        "vc_sedol_code                           String        ,\n" +
        "vc_cusip_code                           String        ,\n" +
        "en_buy_price1                           String        ,\n" +
        "l_buy_amount1                           String        ,\n" +
        "en_buy_price2                           String        ,\n" +
        "l_buy_amount2                           String        ,\n" +
        "en_buy_price3                           String        ,\n" +
        "l_buy_amount3                           String        ,\n" +
        "en_buy_price4                           String        ,\n" +
        "l_buy_amount4                           String        ,\n" +
        "en_buy_price5                           String        ,\n" +
        "l_buy_amount5                           String        ,\n" +
        "en_sale_price1                          String        ,\n" +
        "l_sale_amount1                          String        ,\n" +
        "en_sale_price2                          String        ,\n" +
        "l_sale_amount2                          String        ,\n" +
        "en_sale_price3                          String        ,\n" +
        "l_sale_amount3                          String        ,\n" +
        "en_sale_price4                          String        ,\n" +
        "l_sale_amount4                          String        ,\n" +
        "en_sale_price5                          String        ,\n" +
        "l_sale_amount5                          String        ,\n" +
        "vc_currency_no                          String        ,\n" +
        "en_face_values                          String        ,\n" +
        "l_listing_date                          String        ,\n" +
        "c_long_stop_flag                        String        ,\n" +
        "en_fair_price2                          String        ,\n" +
        "vc_actual_code                          String        ,\n" +
        "l_buy_unit                              String        ,\n" +
        "l_sale_unit                             String        ,\n" +
        "vc_issue_country                        String        ,\n" +
        "vc_risk_country                         String        ,\n" +
        "vc_bloomberg_code                       String        ,\n" +
        "l_wx_listing_date                       String        ,\n" +
        "l_add_date                              String        ,\n" +
        "vc_busin_classes                        String        ,\n" +
        "en_estimate_prob                        String        ,\n" +
        "vc_trade_currency_no                    String        ,\n" +
        "vc_accountant_stock_code                String        ,\n" +
        "c_interest_mode                         String        ,\n" +
        "l_underwriter_id                        String        ,\n" +
        "vc_fund_shr_class                       String        ,\n" +
        "en_right_price                          String        ,\n" +
        "en_initestimate_prob                    String        ,\n" +
        "en_venueestimate_prob                   String        ,\n" +
        "c_ipo_marketcap                         String        ,\n" +
        "vc_reference_fa_code                    String        ,\n" +
        "vc_contractid_id                        String        ,\n" +
        "l_virtual_key                           String        ,\n" +
        "vc_timestamp                            String        ,\n" +
        "c_attorn_type                           String        ,\n" +
        "l_amount_per_hand                       String        ,\n" +
        "c_zrzt                                  String        ,\n" +
        "c_cqcx                                  String        ,\n" +
        "l_zsssl                                 String        ,\n" +
        "en_zzd_price                            String        ,\n" +
        "en_zzd_price2                           String        ,\n" +
        "en_zz_price                             String        ,\n" +
        "en_zz_price2                            String        ,\n" +
        "c_trade_active                          String        ,\n" +
        "c_hzjy_flag                             String        ,\n" +
        "c_bs_flag                               String        ,\n" +
        "vc_custom_class                         String        ,\n" +
        "c_trading_phase                         String        ,\n" +
        "c_stock_fee_type                        String        ,\n" +
        "vc_layering                             String        ,\n" +
        "c_noprofit                              String        ,\n" +
        "c_voterights                            String        ,\n" +
        "c_stock_property                        String        ,\n" +
        "c_cdr_market_no                         String        ,\n" +
        "l_issue_total_amount                    String        ,\n" +
        "vc_mother_company_ids                   String        ,\n" +
        "en_lowest_inquery_value                 String        ,\n" +
        "en_zz_full_price                        String        ,\n" +
        "en_zzd_price_else                       String        ,\n" +
        "l_uplimited_amount_sj                   String        ,\n" +
        "l_downlimited_amount_sj                 String        ,\n" +
        "en_pledge_ratio                         String        ,\n" +
        "c_registration                          String        ,\n" +
        "c_vie                                   String        ,\n" +
        "c_marketizationflag                     String        ,\n" +
        "c_restrictedflag                        String        ,\n" +
        "c_auctionlimit_type                     String        ,\n" +
        "en_auctionlimit_rate                    String        ,\n" +
        "en_auctionlimit_price                   String        ,\n" +
        "c_auctionlimit_pricetype                String        ,\n" +
        "vc_stock_sub_type                       String        ,\n" +
        "vc_sz_stockstatus                       String        ,\n" +
        "en_daystart_iopv                        String        ,\n" +
        "vc_timestamp3                           String        ,\n" +
        "en_zzd_full_price                       String        ,\n" +
        "en_custom_price                         String        ,\n" +
        "vc_timestamp2                           String        ,\n" +
        "l_end_date                              String        ,\n" +
        "en_buy_protective_price                 String        ,\n" +
        "en_sale_protective_price                String        ,\n" +
        "l_customize_issuer_id                   String        ,\n" +
        "c_open_mar_optimize                     String        ,\n" +
        "en_avgweighted_price                    String        ,\n" +
        "en_issuer_price                         String        ,\n" +
        "c_is_hisdata                            String        ,\n" +
        "c_issue_type                            String        ,\n" +


        "schema_name String,\n" +
        "table_name String,\n" +
        "optype_name String,\n" +
        "opts_time String,\n" +
        "udtf_process_time Timestamp>"))
public class TstockInfo extends TableFunction<Row> {

    Logger LOG = LoggerFactory.getLogger(TstockInfo.class);

    long errorCount = 0L;
    String schemaName = null;
    String tableName = null;
    String opTypeName = null;
    String columeName = null;
    ByteString columeValue = null;
    String columeType = null;
    String optsTime = null;

    public TstockInfo() {
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

            if (this.schemaName.equalsIgnoreCase("trade") && this.tableName.equalsIgnoreCase("tstockinfo")) {
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
//            if (this.columeName.equalsIgnoreCase("c_issue_type")){
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