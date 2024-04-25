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

        "l_date                             String     ,\n" +
        "l_unit_id                          String     ,\n" +
        "l_basecombi_id                     String     ,\n" +
        "l_fund_id                          String     ,\n" +
        "c_invest_type                      String     ,\n" +
        "vc_bind_seat                       String     ,\n" +
        "vc_stockholder_id                  String     ,\n" +
        "vc_inter_code                      String     ,\n" +
        "c_market_no                        String     ,\n" +
        "l_begin_amount                     String     ,\n" +
        "l_current_amount                   String     ,\n" +
        "l_frozen_amount                    String     ,\n" +
        "l_unfrozen_amount                  String     ,\n" +
        "l_buy_amount                       String     ,\n" +
        "l_sale_amount                      String     ,\n" +
        "l_prebuy_amount                    String     ,\n" +
        "l_presale_amount                   String     ,\n" +
        "l_temp_frozen_amount               String     ,\n" +
        "l_temp_unfrozen_amount             String     ,\n" +
        "l_t1_temp_frozen_amount            String     ,\n" +
        "l_t1_frozen_amount                 String     ,\n" +
        "en_current_impawn_amount           String     ,\n" +
        "en_pre_impawn_amount               String     ,\n" +
        "en_impawn_amount                   String     ,\n" +
        "en_pre_return_amount               String     ,\n" +
        "en_return_amount                   String     ,\n" +
        "en_buy_balance                     String     ,\n" +
        "en_sale_balance                    String     ,\n" +
        "en_buy_fee                         String     ,\n" +
        "en_sale_fee                        String     ,\n" +
        "en_untransfered_invest             String     ,\n" +
        "en_interest_invest                 String     ,\n" +
        "en_accumulate_profit               String     ,\n" +
        "en_turn_invest                     String     ,\n" +
        "en_turn_interest_invest            String     ,\n" +
        "en_turn_profit                     String     ,\n" +
        "l_asset_id                         String     ,\n" +
        "l_buy_unsettle_amount              String     ,\n" +
        "l_sale_unsettle_amount             String     ,\n" +
        "en_buy_unsettle_balance            String     ,\n" +
        "en_sale_unsettle_balance           String     ,\n" +
        "en_today_profit                    String     ,\n" +
        "en_current_cost                    String     ,\n" +
        "c_position_flag                    String     ,\n" +
        "l_etf_apply_amount                 String     ,\n" +
        "l_etf_redeem_amount                String     ,\n" +
        "en_trade_balance                   String     ,\n" +
        "en_real_buy_fee                    String     ,\n" +
        "en_real_sale_fee                   String     ,\n" +
        "en_real_cost                       String     ,\n" +
        "en_uncarryover_real_cost           String     ,\n" +
        "en_net_gain                        String     ,\n" +
        "en_uncarryover_net_gain            String     ,\n" +
        "en_uncarryover_dividend            String     ,\n" +
        "en_dividend                        String     ,\n" +
        "en_trading_fee                     String     ,\n" +
        "en_uncarryover_trading_fee         String     ,\n" +
        "en_interest_profit                 String     ,\n" +
        "en_turn_interest_profit            String     ,\n" +
        "en_beginyear_turn_profit           String     ,\n" +
        "en_beginyear_unturn_profit         String     ,\n" +
        "en_current_quote_impawn_amount     String     ,\n" +
        "en_pre_quote_impawn_amount         String     ,\n" +
        "en_pre_quote_return_amount         String     ,\n" +
        "en_interest_gain                   String     ,\n" +
        "en_turn_interest_gain              String     ,\n" +
        "l_lastday_left_amount              String     ,\n" +
        "l_buy_deduct_amount                String     ,\n" +
        "l_sale_deduct_amount               String     ,\n" +
        "l_lastday_frzn_amount              String     ,\n" +
        "l_t1_prebuy_amount                 String     ,\n" +
        "l_t1_presale_amount                String     ,\n" +
        "l_buy_unstt_amount                 String     ,\n" +
        "l_sale_unstt_amount                String     ,\n" +
        "l_today_entr_stt_amount            String     ,\n" +
        "l_total_entr_stt_amount            String     ,\n" +
        "l_today_deal_stt_amount            String     ,\n" +
        "l_total_deal_stt_amount            String     ,\n" +
        "en_current_lock_amount             String     ,\n" +
        "en_pre_lock_amount                 String     ,\n" +
        "en_lock_amount                     String     ,\n" +
        "en_pre_unlock_amount               String     ,\n" +
        "en_unlock_amount                   String     ,\n" +
        "en_current_covered_amount          String     ,\n" +
        "en_pre_covered_amount              String     ,\n" +
        "en_tmppre_covered_amount           String     ,\n" +
        "en_option_prefrz_amount            String     ,\n" +
        "l_buy_lock_amount                  String     ,\n" +
        "l_redeem_lock_amount               String     ,\n" +
        "l_apply_lock_amount                String     ,\n" +
        "l_buy_unasset_amount               String     ,\n" +
        "l_sale_unasset_amount              String     ,\n" +
        "en_turn_invest_new                 String     ,\n" +
        "en_untransfered_invest_new         String     ,\n" +
        "en_accumulate_profit_new           String     ,\n" +
        "en_turn_profit_new                 String     ,\n" +
        "l_etf_apply_unsettle               String     ,\n" +
        "en_hedging_amount                  String     ,\n" +
        "l_otc_frzn_amount                  String     ,\n" +
        "l_otc_unfrzn_amount                String     ,\n" +
        "l_t1otc_frzn_amount                String     ,\n" +
        "l_partmerge_amount                 String     ,\n" +
        "l_partmerge_deduct_amount          String     ,\n" +
        "en_bailcomb_amount                 String     ,\n" +
        "l_today_left_amount                String     ,\n" +
        "en_agrtmnt_impawn_amount           String     ,\n" +
        "en_bailcomb_cost                   String     ,\n" +
        "en_pre_bailcomb_amount             String     ,\n" +
        "en_fut_prebuy_cost                 String     ,\n" +
        "en_broker_cost                     String     ,\n" +
        "l_begin_enable_deduct_amount       String     ,\n" +
        "l_t0_enable_deduct_amount          String     ,\n" +
        "l_t1_sale_amount                   String     ,\n" +
        "l_etf_occupy_amount                String     ,\n" +
        "l_restricted_amount                String     ,\n" +
        "en_discount_ratio                  String     ,\n" +
        "l_gold_apply_amount                String     ,\n" +
        "l_stgplc_amount              String ,\n" +
        "l_stgplc_frozen_amount       String ,\n" +
        "l_stgplc_temp_frozen_amount  String ,\n" +
        "en_sale_real_cost            String ,\n" +
        "en_begin_bailcomb_amount     String ,\n" +
        "c_businspec_property         String ,\n" +
        "en_impawn_balance            String ,\n" +
        "l_put_back_amount            String ,\n" +
        "l_put_back_cancel_amount     String ,\n" +
        "l_gg_buy_unsettle_amount     String ,\n" +
        "l_gg_sale_unsettle_amount    String ,\n" +
        "l_xyhg_netting_amount        String ,\n" +
        "l_pre_resold_amount          String ,\n" +
        "l_pre_repurchased_amount     String ,\n" +
        "l_impawn_deduct_amount       String ,\n" +
        "l_temp_impawn_deduct_amount  String ,\n" +
        "en_pay_interest              String ,\n" +
        "en_turn_pay_interest         String ,\n" +
        "en_bankhg_interest           String ,\n" +
        "en_turn_bankhg_interest      String ,\n" +
        "l_restricted_frozen_amount   String ,\n" +
        "l_tbdc_bond_amount           String ,\n" +
        "l_begin_enable_amount        String ,\n" +
        "schema_name String,\n" +
        "table_name String,\n" +
        "optype_name String,\n" +
        "opts_time String,\n" +
        "udtf_process_time Timestamp>"))
public class TunitStock extends TableFunction<Row> {

    Logger LOG = LoggerFactory.getLogger(TunitStock.class);

    long errorCount = 0L;
    String schemaName = null;
    String tableName = null;
    String opTypeName = null;
    String columeName = null;
    ByteString columeValue = null;
    String columeType = null;
    String optsTime = null;

    public TunitStock() {
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

            if (this.schemaName.equalsIgnoreCase("trade") && this.tableName.equalsIgnoreCase("tunitstock")) {
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
//            if (this.columeName.equalsIgnoreCase("l_gold_apply_amount")){
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