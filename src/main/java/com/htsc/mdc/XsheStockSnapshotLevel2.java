package com.htsc.mdc;


import com.htsc.mdc.model.EMDRecordTypeProtos;
import com.htsc.mdc.model.MDSecurityRecordProtos;
import com.htsc.mdc.model.MDStockRecordProtos;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@FunctionHint(output = @DataTypeHint("ROW<" +
        "MDDate                              String, \n" +
        "MDTime                              String, \n" +
        "SecurityType                        String, \n" +
        "SecuritySubType                     String, \n" +
        "SecurityID                          String, \n" +
        "SecurityIDSource                    String, \n" +
        "Symbol                              String, \n" +
        "TradingPhaseCode                    String, \n" +
        "PreClosePx                          String, \n" +
        "NumTrades                           String, \n" +
        "TotalVolumeTrade                    String, \n" +
        "TotalValueTrade                     String, \n" +
        "LastPx                              String, \n" +
        "OpenPx                              String, \n" +
        "ClosePx                             String, \n" +
        "HighPx                              String, \n" +
        "LowPx                               String, \n" +
        "DiffPx1                             String, \n" +
        "DiffPx2                             String, \n" +
        "MaxPx                               String, \n" +
        "MinPx                               String, \n" +
        "TotalBidQty                         String, \n" +
        "TotalOfferQty                       String, \n" +
        "WeightedAvgBidPx                    String, \n" +
        "WeightedAvgOfferPx                  String, \n" +
        "AfterHoursNumTrades                 String, \n" +
        "AfterHoursTotalVolumeTrade          String, \n" +
        "AfterHoursTotalValueTrade           String, \n" +
        "Buy1Price                           String, \n" +
        "Buy1OrderQty                        String, \n" +
        "Buy1NumOrders                       String, \n" +
        "Buy1NoOrders                        String, \n" +
        "Buy1OrderDetail                     String, \n" +
        "Sell1Price                          String, \n" +
        "Sell1OrderQty                       String, \n" +
        "Sell1NumOrders                      String, \n" +
        "Sell1NoOrders                       String, \n" +
        "Sell1OrderDetail                    String, \n" +
        "Buy2Price                           String, \n" +
        "Buy2OrderQty                        String, \n" +
        "Buy2NumOrders                       String, \n" +
        "Sell2Price                          String, \n" +
        "Sell2OrderQty                       String, \n" +
        "Sell2NumOrders                      String, \n" +
        "Buy3Price                           String, \n" +
        "Buy3OrderQty                        String, \n" +
        "Buy3NumOrders                       String, \n" +
        "Sell3Price                          String, \n" +
        "Sell3OrderQty                       String, \n" +
        "Sell3NumOrders                      String, \n" +
        "Buy4Price                           String, \n" +
        "Buy4OrderQty                        String, \n" +
        "Buy4NumOrders                       String, \n" +
        "Sell4Price                          String, \n" +
        "Sell4OrderQty                       String, \n" +
        "Sell4NumOrders                      String, \n" +
        "Buy5Price                           String, \n" +
        "Buy5OrderQty                        String, \n" +
        "Buy5NumOrders                       String, \n" +
        "Sell5Price                          String, \n" +
        "Sell5OrderQty                       String, \n" +
        "Sell5NumOrders                      String, \n" +
        "Buy6Price                           String, \n" +
        "Buy6OrderQty                        String, \n" +
        "Buy6NumOrders                       String, \n" +
        "Sell6Price                          String, \n" +
        "Sell6OrderQty                       String, \n" +
        "Sell6NumOrders                      String, \n" +
        "Buy7Price                           String, \n" +
        "Buy7OrderQty                        String, \n" +
        "Buy7NumOrders                       String, \n" +
        "Sell7Price                          String, \n" +
        "Sell7OrderQty                       String, \n" +
        "Sell7NumOrders                      String, \n" +
        "Buy8Price                           String, \n" +
        "Buy8OrderQty                        String, \n" +
        "Buy8NumOrders                       String, \n" +
        "Sell8Price                          String, \n" +
        "Sell8OrderQty                       String, \n" +
        "Sell8NumOrders                      String, \n" +
        "Buy9Price                           String, \n" +
        "Buy9OrderQty                        String, \n" +
        "Buy9NumOrders                       String, \n" +
        "Sell9Price                          String, \n" +
        "Sell9OrderQty                       String, \n" +
        "Sell9NumOrders                      String, \n" +
        "Buy10Price                          String, \n" +
        "Buy10OrderQty                       String, \n" +
        "Buy10NumOrders                      String, \n" +
        "Sell10Price                         String, \n" +
        "Sell10OrderQty                      String, \n" +
        "Sell10NumOrders                     String, \n" +
        "HTSCSecurityID                      String, \n" +
        "ReceiveDateTime                     String, \n" +
        "ChannelNo                           String  >"))
public class XsheStockSnapshotLevel2 extends TableFunction<Row> {


    Logger logger = LoggerFactory.getLogger(XsheStockSnapshotLevel2.class);

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
                MDStockRecordProtos.MDStockRecord mdStock = mdSecurityRecord.getMDStock();


                Row row = new Row(95);
                row.setField(0,mdStock.getMDDate()+"");
                row.setField(1,mdStock.getMDTime()+"");
                row.setField(2,mdStock.getSecurityType()+"");
                row.setField(3,mdStock.getSecuritySubType()+"");
                row.setField(4,mdStock.getSecurityID()+"");
                row.setField(5,mdStock.getSecurityIDSource()+"");
                row.setField(6,mdStock.getSymbol()+"");
                row.setField(7,mdStock.getTradingPhaseCode()+"");
                row.setField(8,mdStock.getPreClosePx()+"");
                row.setField(9,mdStock.getNumTrades()+"");
                row.setField(10,mdStock.getTotalVolumeTrade()+"");
                row.setField(11,mdStock.getTotalValueTrade()+"");
                row.setField(12,mdStock.getLastPx()+"");
                row.setField(13,mdStock.getOpenPx()+"");
                row.setField(14,mdStock.getClosePx()+"");
                row.setField(15,mdStock.getHighPx()+"");
                row.setField(16,mdStock.getLowPx()+"");
                row.setField(17,mdStock.getDiffPx1()+"");
                row.setField(18,mdStock.getDiffPx2()+"");
                row.setField(19,mdStock.getMaxPx()+"");
                row.setField(20,mdStock.getMinPx()+"");
                row.setField(21,mdStock.getTotalBidQty()+"");
                row.setField(22,mdStock.getTotalOfferQty()+"");
                row.setField(23,mdStock.getWeightedAvgBidPx()+"");
                row.setField(24,mdStock.getWeightedAvgOfferPx()+"");
                row.setField(25,mdStock.getAfterHoursNumTrades()+"");
                row.setField(26,mdStock.getAfterHoursTotalVolumeTrade()+"");
                row.setField(27,mdStock.getAfterHoursTotalValueTrade()+"");
                row.setField(28,mdStock.getBuy1Price()+"");
                row.setField(29,mdStock.getBuy1OrderQty()+"");
                row.setField(30,mdStock.getBuy1NumOrders()+"");
                row.setField(31,mdStock.getBuy1NoOrders()+"");
                row.setField(32,mdStock.getBuy1OrderDetailList().toString()+"");
                row.setField(33,mdStock.getSell1Price()+"");
                row.setField(34,mdStock.getSell1OrderQty()+"");
                row.setField(35,mdStock.getSell1NumOrders()+"");
                row.setField(36,mdStock.getSell1NoOrders()+"");
                row.setField(37,mdStock.getSell1OrderDetailList().toString()+"");
                row.setField(38,mdStock.getBuy2Price()+"");
                row.setField(39,mdStock.getBuy2OrderQty()+"");
                row.setField(40,mdStock.getBuy2NumOrders()+"");
                row.setField(41,mdStock.getSell2Price()+"");
                row.setField(42,mdStock.getSell2OrderQty()+"");
                row.setField(43,mdStock.getSell2NumOrders()+"");
                row.setField(44,mdStock.getBuy3Price()+"");
                row.setField(45,mdStock.getBuy3OrderQty()+"");
                row.setField(46,mdStock.getBuy3NumOrders()+"");
                row.setField(47,mdStock.getSell3Price()+"");
                row.setField(48,mdStock.getSell3OrderQty()+"");
                row.setField(49,mdStock.getSell3NumOrders()+"");
                row.setField(50,mdStock.getBuy4Price()+"");
                row.setField(51,mdStock.getBuy4OrderQty()+"");
                row.setField(52,mdStock.getBuy4NumOrders()+"");
                row.setField(53,mdStock.getSell4Price()+"");
                row.setField(54,mdStock.getSell4OrderQty()+"");
                row.setField(55,mdStock.getSell4NumOrders()+"");
                row.setField(56,mdStock.getBuy5Price()+"");
                row.setField(57,mdStock.getBuy5OrderQty()+"");
                row.setField(58,mdStock.getBuy5NumOrders()+"");
                row.setField(59,mdStock.getSell5Price()+"");
                row.setField(60,mdStock.getSell5OrderQty()+"");
                row.setField(61,mdStock.getSell5NumOrders()+"");
                row.setField(62,mdStock.getBuy6Price()+"");
                row.setField(63,mdStock.getBuy6OrderQty()+"");
                row.setField(64,mdStock.getBuy6NumOrders()+"");
                row.setField(65,mdStock.getSell6Price()+"");
                row.setField(66,mdStock.getSell6OrderQty()+"");
                row.setField(67,mdStock.getSell6NumOrders()+"");
                row.setField(68,mdStock.getBuy7Price()+"");
                row.setField(69,mdStock.getBuy7OrderQty()+"");
                row.setField(70,mdStock.getBuy7NumOrders()+"");
                row.setField(71,mdStock.getSell7Price()+"");
                row.setField(72,mdStock.getSell7OrderQty()+"");
                row.setField(73,mdStock.getSell7NumOrders()+"");
                row.setField(74,mdStock.getBuy8Price()+"");
                row.setField(75,mdStock.getBuy8OrderQty()+"");
                row.setField(76,mdStock.getBuy8NumOrders()+"");
                row.setField(77,mdStock.getSell8Price()+"");
                row.setField(78,mdStock.getSell8OrderQty()+"");
                row.setField(79,mdStock.getSell8NumOrders()+"");
                row.setField(80,mdStock.getBuy9Price()+"");
                row.setField(81,mdStock.getBuy9OrderQty()+"");
                row.setField(82,mdStock.getBuy9NumOrders()+"");
                row.setField(83,mdStock.getSell9Price()+"");
                row.setField(84,mdStock.getSell9OrderQty()+"");
                row.setField(85,mdStock.getSell9NumOrders()+"");
                row.setField(86,mdStock.getBuy10Price()+"");
                row.setField(87,mdStock.getBuy10OrderQty()+"");
                row.setField(88,mdStock.getBuy10NumOrders()+"");
                row.setField(89,mdStock.getSell10Price()+"");
                row.setField(90,mdStock.getSell10OrderQty()+"");
                row.setField(91,mdStock.getSell10NumOrders()+"");
                row.setField(92,mdStock.getHTSCSecurityID()+"");
                row.setField(93,mdStock.getReceiveDateTime()+"");
                row.setField(94,mdStock.getChannelNo()+"");
                collect(row);
            }
        } catch (Throwable t) {
            logger.error("eval rawData={} failed", rawData, t);
        }
    }

}