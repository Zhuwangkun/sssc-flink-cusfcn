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
        "MDDate String ,\n" +
        "MDTime String ,\n" +
        "SecurityType String ,\n" +
        "SecuritySubType String ,\n" +
        "SecurityID String ,\n" +
        "SecurityIDSource String ,\n" +
        "Symbol String ,\n" +
        "TradingPhaseCode String ,\n" +
        "PreClosePx String ,\n" +
        "NumTrades String ,\n" +
        "TotalVolumeTrade String ,\n" +
        "TotalValueTrade String ,\n" +
        "LastPx String ,\n" +
        "OpenPx String ,\n" +
        "ClosePx String ,\n" +
        "HighPx String ,\n" +
        "LowPx String ,\n" +
        "MaxPx String ,\n" +
        "MinPx String ,\n" +
        "TotalBidQty String ,\n" +
        "TotalOfferQty String ,\n" +
        "WeightedAvgBidPx String ,\n" +
        "WeightedAvgOfferPx String ,\n" +
        "WithdrawBuyNumber String ,\n" +
        "WithdrawBuyAmount String ,\n" +
        "WithdrawBuyMoney String ,\n" +
        "WithdrawSellNumber String ,\n" +
        "WithdrawSellAmount String ,\n" +
        "WithdrawSellMoney String ,\n" +
        "TotalBidNumber String ,\n" +
        "TotalOfferNumber String ,\n" +
        "BidTradeMaxDuration String ,\n" +
        "OfferTradeMaxDuration String ,\n" +
        "NumBidOrders String ,\n" +
        "NumOfferOrders String ,\n" +
        "AfterHoursNumTrades String ,\n" +
        "AfterHoursTotalVolumeTrade String ,\n" +
        "AfterHoursTotalValueTrade String ,\n" +
        "Buy1Price String ,\n" +
        "Buy1OrderQty String ,\n" +
        "Buy1NumOrders String ,\n" +
        "Buy1NoOrders String ,\n" +
        "Buy1OrderDetail String ,\n" +
        "Sell1Price String ,\n" +
        "Sell1OrderQty String ,\n" +
        "Sell1NumOrders String ,\n" +
        "Sell1NoOrders String ,\n" +
        "Sell1OrderDetail String ,\n" +
        "Buy2Price String ,\n" +
        "Buy2OrderQty String ,\n" +
        "Buy2NumOrders String ,\n" +
        "Sell2Price String ,\n" +
        "Sell2OrderQty String ,\n" +
        "Sell2NumOrders String ,\n" +
        "Buy3Price String ,\n" +
        "Buy3OrderQty String ,\n" +
        "Buy3NumOrders String ,\n" +
        "Sell3Price String ,\n" +
        "Sell3OrderQty String ,\n" +
        "Sell3NumOrders String ,\n" +
        "Buy4Price String ,\n" +
        "Buy4OrderQty String ,\n" +
        "Buy4NumOrders String ,\n" +
        "Sell4Price String ,\n" +
        "Sell4OrderQty String ,\n" +
        "Sell4NumOrders String ,\n" +
        "Buy5Price String ,\n" +
        "Buy5OrderQty String ,\n" +
        "Buy5NumOrders String ,\n" +
        "Sell5Price String ,\n" +
        "Sell5OrderQty String ,\n" +
        "Sell5NumOrders String ,\n" +
        "Buy6Price String ,\n" +
        "Buy6OrderQty String ,\n" +
        "Buy6NumOrders String ,\n" +
        "Sell6Price String ,\n" +
        "Sell6OrderQty String ,\n" +
        "Sell6NumOrders String ,\n" +
        "Buy7Price String ,\n" +
        "Buy7OrderQty String ,\n" +
        "Buy7NumOrders String ,\n" +
        "Sell7Price String ,\n" +
        "Sell7OrderQty String ,\n" +
        "Sell7NumOrders String ,\n" +
        "Buy8Price String ,\n" +
        "Buy8OrderQty String ,\n" +
        "Buy8NumOrders String ,\n" +
        "Sell8Price String ,\n" +
        "Sell8OrderQty String ,\n" +
        "Sell8NumOrders String ,\n" +
        "Buy9Price String ,\n" +
        "Buy9OrderQty String ,\n" +
        "Buy9NumOrders String ,\n" +
        "Sell9Price String ,\n" +
        "Sell9OrderQty String ,\n" +
        "Sell9NumOrders String ,\n" +
        "Buy10Price String ,\n" +
        "Buy10OrderQty String ,\n" +
        "Buy10NumOrders String ,\n" +
        "Sell10Price String ,\n" +
        "Sell10OrderQty String ,\n" +
        "Sell10NumOrders String ,\n" +
        "HTSCSecurityID String ,\n" +
        "ReceiveDateTime String ,\n" +
        "ChannelNo String>"))
public class XshgStockSnapshotLevel2 extends TableFunction<Row> {


    Logger logger = LoggerFactory.getLogger(XshgStockSnapshotLevel2.class);

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


                Row row = new Row(105);
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
                row.setField(17,mdStock.getMaxPx()+"");
                row.setField(18,mdStock.getMinPx()+"");
                row.setField(19,mdStock.getTotalBidQty()+"");
                row.setField(20,mdStock.getTotalOfferQty()+"");
                row.setField(21,mdStock.getWeightedAvgBidPx()+"");
                row.setField(22,mdStock.getWeightedAvgOfferPx()+"");
                row.setField(23,mdStock.getWithdrawBuyNumber()+"");
                row.setField(24,mdStock.getWithdrawBuyAmount()+"");
                row.setField(25,mdStock.getWithdrawBuyMoney()+"");
                row.setField(26,mdStock.getWithdrawSellNumber()+"");
                row.setField(27,mdStock.getWithdrawSellAmount()+"");
                row.setField(28,mdStock.getWithdrawSellMoney()+"");
                row.setField(29,mdStock.getTotalBidNumber()+"");
                row.setField(30,mdStock.getTotalOfferNumber()+"");
                row.setField(31,mdStock.getBidTradeMaxDuration()+"");
                row.setField(32,mdStock.getOfferTradeMaxDuration()+"");
                row.setField(33,mdStock.getNumBidOrders()+"");
                row.setField(34,mdStock.getNumOfferOrders()+"");
                row.setField(35,mdStock.getAfterHoursNumTrades()+"");
                row.setField(36,mdStock.getAfterHoursTotalVolumeTrade()+"");
                row.setField(37,mdStock.getAfterHoursTotalValueTrade()+"");
                row.setField(38,mdStock.getBuy1Price()+"");
                row.setField(39,mdStock.getBuy1OrderQty()+"");
                row.setField(40,mdStock.getBuy1NumOrders()+"");
                row.setField(41,mdStock.getBuy1NoOrders()+"");
                row.setField(42,mdStock.getBuy1OrderDetailList().toString()+"");
                row.setField(43,mdStock.getSell1Price()+"");
                row.setField(44,mdStock.getSell1OrderQty()+"");
                row.setField(45,mdStock.getSell1NumOrders()+"");
                row.setField(46,mdStock.getSell1NoOrders()+"");
                row.setField(47,mdStock.getSell1OrderDetailList().toString()+"");
                row.setField(48,mdStock.getBuy2Price()+"");
                row.setField(49,mdStock.getBuy2OrderQty()+"");
                row.setField(50,mdStock.getBuy2NumOrders()+"");
                row.setField(51,mdStock.getSell2Price()+"");
                row.setField(52,mdStock.getSell2OrderQty()+"");
                row.setField(53,mdStock.getSell2NumOrders()+"");
                row.setField(54,mdStock.getBuy3Price()+"");
                row.setField(55,mdStock.getBuy3OrderQty()+"");
                row.setField(56,mdStock.getBuy3NumOrders()+"");
                row.setField(57,mdStock.getSell3Price()+"");
                row.setField(58,mdStock.getSell3OrderQty()+"");
                row.setField(59,mdStock.getSell3NumOrders()+"");
                row.setField(60,mdStock.getBuy4Price()+"");
                row.setField(61,mdStock.getBuy4OrderQty()+"");
                row.setField(62,mdStock.getBuy4NumOrders()+"");
                row.setField(63,mdStock.getSell4Price()+"");
                row.setField(64,mdStock.getSell4OrderQty()+"");
                row.setField(65,mdStock.getSell4NumOrders()+"");
                row.setField(66,mdStock.getBuy5Price()+"");
                row.setField(67,mdStock.getBuy5OrderQty()+"");
                row.setField(68,mdStock.getBuy5NumOrders()+"");
                row.setField(69,mdStock.getSell5Price()+"");
                row.setField(70,mdStock.getSell5OrderQty()+"");
                row.setField(71,mdStock.getSell5NumOrders()+"");
                row.setField(72,mdStock.getBuy6Price()+"");
                row.setField(73,mdStock.getBuy6OrderQty()+"");
                row.setField(74,mdStock.getBuy6NumOrders()+"");
                row.setField(75,mdStock.getSell6Price()+"");
                row.setField(76,mdStock.getSell6OrderQty()+"");
                row.setField(77,mdStock.getSell6NumOrders()+"");
                row.setField(78,mdStock.getBuy7Price()+"");
                row.setField(79,mdStock.getBuy7OrderQty()+"");
                row.setField(80,mdStock.getBuy7NumOrders()+"");
                row.setField(81,mdStock.getSell7Price()+"");
                row.setField(82,mdStock.getSell7OrderQty()+"");
                row.setField(83,mdStock.getSell7NumOrders()+"");
                row.setField(84,mdStock.getBuy8Price()+"");
                row.setField(85,mdStock.getBuy8OrderQty()+"");
                row.setField(86,mdStock.getBuy8NumOrders()+"");
                row.setField(87,mdStock.getSell8Price()+"");
                row.setField(88,mdStock.getSell8OrderQty()+"");
                row.setField(89,mdStock.getSell8NumOrders()+"");
                row.setField(90,mdStock.getBuy9Price()+"");
                row.setField(91,mdStock.getBuy9OrderQty()+"");
                row.setField(92,mdStock.getBuy9NumOrders()+"");
                row.setField(93,mdStock.getSell9Price()+"");
                row.setField(94,mdStock.getSell9OrderQty()+"");
                row.setField(95,mdStock.getSell9NumOrders()+"");
                row.setField(96,mdStock.getBuy10Price()+"");
                row.setField(97,mdStock.getBuy10OrderQty()+"");
                row.setField(98,mdStock.getBuy10NumOrders()+"");
                row.setField(99,mdStock.getSell10Price()+"");
                row.setField(100,mdStock.getSell10OrderQty()+"");
                row.setField(101,mdStock.getSell10NumOrders()+"");
                row.setField(102,mdStock.getHTSCSecurityID()+"");
                row.setField(103,mdStock.getReceiveDateTime()+"");
                row.setField(104,mdStock.getChannelNo()+"");
                collect(row);
            }
        } catch (Throwable t) {
            logger.error("eval rawData={} failed", rawData, t);
        }
    }

}