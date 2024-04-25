package com.htsc.mdc;


import com.htsc.mdc.model.EMDRecordTypeProtos;
import com.htsc.mdc.model.MDBondRecordProtos;
import com.htsc.mdc.model.MDSecurityRecordProtos;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@FunctionHint(output = @DataTypeHint("ROW<" +
        "MDDate                       String,\n" +
        "MDTime                       String,\n" +
        "SecurityType                 String,\n" +
        "SecuritySubType              String,\n" +
        "SecurityID                   String,\n" +
        "SecurityIDSource             String,\n" +
        "Symbol                       String,\n" +
        "TradingPhaseCode             String,\n" +
        "PreClosePx                   String,\n" +
        "NumTrades                    String,\n" +
        "TotalVolumeTrade             String,\n" +
        "TotalValueTrade              String,\n" +
        "LastPx                       String,\n" +
        "OpenPx                       String,\n" +
        "ClosePx                      String,\n" +
        "HighPx                       String,\n" +
        "LowPx                        String,\n" +
        "DiffPx1                      String,\n" +
        "DiffPx2                      String,\n" +
        "MaxPx                        String,\n" +
        "MinPx                        String,\n" +
        "TotalBidQty                  String,\n" +
        "TotalOfferQty                String,\n" +
        "WeightedAvgBidPx             String,\n" +
        "WeightedAvgOfferPx           String,\n" +
        "YieldToMaturity              String,\n" +
        "WeightedAvgPx                String,\n" +
        "WeightedAvgPxBP              String,\n" +
        "PreCloseWeightedAvgPx        String,\n" +
        "PreCloseYield                String,\n" +
        "PreWeightedAvgYield          String,\n" +
        "OpenYield                    String,\n" +
        "HighYield                    String,\n" +
        "LowYield                     String,\n" +
        "LastYield                    String,\n" +
        "WeightedAvgYield             String,\n" +
        "Buy1Price                    String,\n" +
        "Buy1OrderQty                 String,\n" +
        "Buy1NumOrders                String,\n" +
        "Buy1NoOrders                 String,\n" +
        "Buy1OrderDetail              String,\n" +
        "Sell1Price                   String,\n" +
        "Sell1OrderQty                String,\n" +
        "Sell1NumOrders               String,\n" +
        "Sell1NoOrders                String,\n" +
        "Sell1OrderDetail             String,\n" +
        "Buy2Price                    String,\n" +
        "Buy2OrderQty                 String,\n" +
        "Buy2NumOrders                String,\n" +
        "Sell2Price                   String,\n" +
        "Sell2OrderQty                String,\n" +
        "Sell2NumOrders               String,\n" +
        "Buy3Price                    String,\n" +
        "Buy3OrderQty                 String,\n" +
        "Buy3NumOrders                String,\n" +
        "Sell3Price                   String,\n" +
        "Sell3OrderQty                String,\n" +
        "Sell3NumOrders               String,\n" +
        "Buy4Price                    String,\n" +
        "Buy4OrderQty                 String,\n" +
        "Buy4NumOrders                String,\n" +
        "Sell4Price                   String,\n" +
        "Sell4OrderQty                String,\n" +
        "Sell4NumOrders               String,\n" +
        "Buy5Price                    String,\n" +
        "Buy5OrderQty                 String,\n" +
        "Buy5NumOrders                String,\n" +
        "Sell5Price                   String,\n" +
        "Sell5OrderQty                String,\n" +
        "Sell5NumOrders               String,\n" +
        "Buy6Price                    String,\n" +
        "Buy6OrderQty                 String,\n" +
        "Buy6NumOrders                String,\n" +
        "Sell6Price                   String,\n" +
        "Sell6OrderQty                String,\n" +
        "Sell6NumOrders               String,\n" +
        "Buy7Price                    String,\n" +
        "Buy7OrderQty                 String,\n" +
        "Buy7NumOrders                String,\n" +
        "Sell7Price                   String,\n" +
        "Sell7OrderQty                String,\n" +
        "Sell7NumOrders               String,\n" +
        "Buy8Price                    String,\n" +
        "Buy8OrderQty                 String,\n" +
        "Buy8NumOrders                String,\n" +
        "Sell8Price                   String,\n" +
        "Sell8OrderQty                String,\n" +
        "Sell8NumOrders               String,\n" +
        "Buy9Price                    String,\n" +
        "Buy9OrderQty                 String,\n" +
        "Buy9NumOrders                String,\n" +
        "Sell9Price                   String,\n" +
        "Sell9OrderQty                String,\n" +
        "Sell9NumOrders               String,\n" +
        "Buy10Price                   String,\n" +
        "Buy10OrderQty                String,\n" +
        "Buy10NumOrders               String,\n" +
        "Sell10Price                  String,\n" +
        "Sell10OrderQty               String,\n" +
        "Sell10NumOrders              String,\n" +
        "HTSCSecurityID               String,\n" +
        "ReceiveDateTime              String,\n" +
        "ChannelNo                    String>"))
public class XsheBondSnapshotLevel2 extends TableFunction<Row> {


    Logger logger = LoggerFactory.getLogger(XsheBondSnapshotLevel2.class);

    public void eval(byte[] rawData)  {
        try {

            MDSecurityRecordProtos.MDSecurityRecord mdSecurityRecord = MDSecurityRecordProtos.MDSecurityRecord.parseFrom(rawData);

            //判断MDRecordType（MarketType-行情、TransactionType-逐笔委托、OrderType-逐笔成交）
            EMDRecordTypeProtos.EMDRecordType mdRecordType = mdSecurityRecord.getMDRecordType();
            String name = mdRecordType.getValueDescriptor().getName();

            //MarketType-行情
            if (name.equals("MarketType")){

                //MDOrderRecordProtos.MDOrderRecord mdOrder = mdSecurityRecord.getMDOrder();
                //MDTransactionRecordProtos.MDTransactionRecord mdTransaction = mdSecurityRecord.getMDTransaction();
                MDBondRecordProtos.MDBondRecord mdBond = mdSecurityRecord.getMDBond();

                Row row = new Row(103);
                row.setField(0,mdBond.getMDDate()+"");
                row.setField(1,mdBond.getMDTime()+"");
                row.setField(2,mdBond.getSecurityType()+"");
                row.setField(3,mdBond.getSecuritySubType()+"");
                row.setField(4,mdBond.getSecurityID()+"");
                row.setField(5,mdBond.getSecurityIDSource()+"");
                row.setField(6,mdBond.getSymbol()+"");
                row.setField(7,mdBond.getTradingPhaseCode()+"");
                row.setField(8,mdBond.getPreClosePx()+"");
                row.setField(9,mdBond.getNumTrades()+"");
                row.setField(10,mdBond.getTotalVolumeTrade()+"");
                row.setField(11,mdBond.getTotalValueTrade()+"");
                row.setField(12,mdBond.getLastPx()+"");
                row.setField(13,mdBond.getOpenPx()+"");
                row.setField(14,mdBond.getClosePx()+"");
                row.setField(15,mdBond.getHighPx()+"");
                row.setField(16,mdBond.getLowPx()+"");
                row.setField(17,mdBond.getDiffPx1()+"");
                row.setField(18,mdBond.getDiffPx2()+"");
                row.setField(19,mdBond.getMaxPx()+"");
                row.setField(20,mdBond.getMinPx()+"");
                row.setField(21,mdBond.getTotalBidQty()+"");
                row.setField(22,mdBond.getTotalOfferQty()+"");
                row.setField(23,mdBond.getWeightedAvgBidPx()+"");
                row.setField(24,mdBond.getWeightedAvgOfferPx()+"");
                row.setField(25,mdBond.getYieldToMaturity()+"");
                row.setField(26,mdBond.getWeightedAvgPx()+"");
                row.setField(27,mdBond.getWeightedAvgPxBP()+"");
                row.setField(28,mdBond.getPreCloseWeightedAvgPx()+"");
                row.setField(29,mdBond.getPreCloseYield()+"");
                row.setField(30,mdBond.getPreWeightedAvgYield()+"");
                row.setField(31,mdBond.getOpenYield()+"");
                row.setField(32,mdBond.getHighYield()+"");
                row.setField(33,mdBond.getLowYield()+"");
                row.setField(34,mdBond.getLastYield()+"");
                row.setField(35,mdBond.getWeightedAvgYield()+"");
                row.setField(36,mdBond.getBuy1Price()+"");
                row.setField(37,mdBond.getBuy1OrderQty()+"");
                row.setField(38,mdBond.getBuy1NumOrders()+"");
                row.setField(39,mdBond.getBuy1NoOrders()+"");
                row.setField(40,mdBond.getBuy1OrderDetailList().toString()+"");
                row.setField(41,mdBond.getSell1Price()+"");
                row.setField(42,mdBond.getSell1OrderQty()+"");
                row.setField(43,mdBond.getSell1NumOrders()+"");
                row.setField(44,mdBond.getSell1NoOrders()+"");
                row.setField(45,mdBond.getSell1OrderDetailList().toString()+"");
                row.setField(46,mdBond.getBuy2Price()+"");
                row.setField(47,mdBond.getBuy2OrderQty()+"");
                row.setField(48,mdBond.getBuy2NumOrders()+"");
                row.setField(49,mdBond.getSell2Price()+"");
                row.setField(50,mdBond.getSell2OrderQty()+"");
                row.setField(51,mdBond.getSell2NumOrders()+"");
                row.setField(52,mdBond.getBuy3Price()+"");
                row.setField(53,mdBond.getBuy3OrderQty()+"");
                row.setField(54,mdBond.getBuy3NumOrders()+"");
                row.setField(55,mdBond.getSell3Price()+"");
                row.setField(56,mdBond.getSell3OrderQty()+"");
                row.setField(57,mdBond.getSell3NumOrders()+"");
                row.setField(58,mdBond.getBuy4Price()+"");
                row.setField(59,mdBond.getBuy4OrderQty()+"");
                row.setField(60,mdBond.getBuy4NumOrders()+"");
                row.setField(61,mdBond.getSell4Price()+"");
                row.setField(62,mdBond.getSell4OrderQty()+"");
                row.setField(63,mdBond.getSell4NumOrders()+"");
                row.setField(64,mdBond.getBuy5Price()+"");
                row.setField(65,mdBond.getBuy5OrderQty()+"");
                row.setField(66,mdBond.getBuy5NumOrders()+"");
                row.setField(67,mdBond.getSell5Price()+"");
                row.setField(68,mdBond.getSell5OrderQty()+"");
                row.setField(69,mdBond.getSell5NumOrders()+"");
                row.setField(70,mdBond.getBuy6Price()+"");
                row.setField(71,mdBond.getBuy6OrderQty()+"");
                row.setField(72,mdBond.getBuy6NumOrders()+"");
                row.setField(73,mdBond.getSell6Price()+"");
                row.setField(74,mdBond.getSell6OrderQty()+"");
                row.setField(75,mdBond.getSell6NumOrders()+"");
                row.setField(76,mdBond.getBuy7Price()+"");
                row.setField(77,mdBond.getBuy7OrderQty()+"");
                row.setField(78,mdBond.getBuy7NumOrders()+"");
                row.setField(79,mdBond.getSell7Price()+"");
                row.setField(80,mdBond.getSell7OrderQty()+"");
                row.setField(81,mdBond.getSell7NumOrders()+"");
                row.setField(82,mdBond.getBuy8Price()+"");
                row.setField(83,mdBond.getBuy8OrderQty()+"");
                row.setField(84,mdBond.getBuy8NumOrders()+"");
                row.setField(85,mdBond.getSell8Price()+"");
                row.setField(86,mdBond.getSell8OrderQty()+"");
                row.setField(87,mdBond.getSell8NumOrders()+"");
                row.setField(88,mdBond.getBuy9Price()+"");
                row.setField(89,mdBond.getBuy9OrderQty()+"");
                row.setField(90,mdBond.getBuy9NumOrders()+"");
                row.setField(91,mdBond.getSell9Price()+"");
                row.setField(92,mdBond.getSell9OrderQty()+"");
                row.setField(93,mdBond.getSell9NumOrders()+"");
                row.setField(94,mdBond.getBuy10Price()+"");
                row.setField(95,mdBond.getBuy10OrderQty()+"");
                row.setField(96,mdBond.getBuy10NumOrders()+"");
                row.setField(97,mdBond.getSell10Price()+"");
                row.setField(98,mdBond.getSell10OrderQty()+"");
                row.setField(99,mdBond.getSell10NumOrders()+"");
                row.setField(100,mdBond.getHTSCSecurityID()+"");
                row.setField(101,mdBond.getReceiveDateTime()+"");
                row.setField(102,mdBond.getChannelNo()+"");
                collect(row);
            }
        } catch (Throwable t) {
            logger.error("eval rawData={} failed", rawData, t);
        }
    }

}