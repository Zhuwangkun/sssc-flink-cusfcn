package com.htsc.mdc;

import com.google.protobuf.InvalidProtocolBufferException;
import com.htsc.mdc.model.MDSecurityRecordProtos;
import com.htsc.mdc.model.MDStockRecordProtos;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;


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
        "ChannelNo String\n"
))
public class XshgStockType extends TableFunction<Row> {


    Logger logger = LoggerFactory.getLogger(XshgStockType.class);

    public void eval(String rawData) {
        try {
            byte[] bytes = DatatypeConverter.parseHexBinary(rawData);
            MDSecurityRecordProtos.MDSecurityRecord mdSecurityRecord = MDSecurityRecordProtos.MDSecurityRecord.parseFrom(bytes);
            MDStockRecordProtos.MDStockRecord mdStockRecord = mdSecurityRecord.getMDStock();

            Row row = new Row(105);
            row.setField(0,mdStockRecord.getMDDate()+"");
            row.setField(1,mdStockRecord.getMDTime()+"");
            row.setField(2,mdStockRecord.getSecurityType()+"");
            row.setField(3,mdStockRecord.getSecuritySubType()+"");
            row.setField(4,mdStockRecord.getSecurityID()+"");
            row.setField(5,mdStockRecord.getSecurityIDSource()+"");
            row.setField(6,mdStockRecord.getSymbol()+"");
            row.setField(7,mdStockRecord.getTradingPhaseCode()+"");
            row.setField(8,mdStockRecord.getPreClosePx()+"");
            row.setField(9,mdStockRecord.getNumTrades()+"");
            row.setField(10,mdStockRecord.getTotalVolumeTrade()+"");
            row.setField(11,mdStockRecord.getTotalValueTrade()+"");
            row.setField(12,mdStockRecord.getLastPx()+"");
            row.setField(13,mdStockRecord.getOpenPx()+"");
            row.setField(14,mdStockRecord.getClosePx()+"");
            row.setField(15,mdStockRecord.getHighPx()+"");
            row.setField(16,mdStockRecord.getLowPx()+"");
            row.setField(17,mdStockRecord.getMaxPx()+"");
            row.setField(18,mdStockRecord.getMinPx()+"");
            row.setField(19,mdStockRecord.getTotalBidQty()+"");
            row.setField(20,mdStockRecord.getTotalOfferQty()+"");
            row.setField(21,mdStockRecord.getWeightedAvgBidPx()+"");
            row.setField(22,mdStockRecord.getWeightedAvgOfferPx()+"");
            row.setField(23,mdStockRecord.getWithdrawBuyNumber()+"");
            row.setField(24,mdStockRecord.getWithdrawBuyAmount()+"");
            row.setField(25,mdStockRecord.getWithdrawBuyMoney()+"");
            row.setField(26,mdStockRecord.getWithdrawSellNumber()+"");
            row.setField(27,mdStockRecord.getWithdrawSellAmount()+"");
            row.setField(28,mdStockRecord.getWithdrawSellMoney()+"");
            row.setField(29,mdStockRecord.getTotalBidNumber()+"");
            row.setField(30,mdStockRecord.getTotalOfferNumber()+"");
            row.setField(31,mdStockRecord.getBidTradeMaxDuration()+"");
            row.setField(32,mdStockRecord.getOfferTradeMaxDuration()+"");
            row.setField(33,mdStockRecord.getNumBidOrders()+"");
            row.setField(34,mdStockRecord.getNumOfferOrders()+"");
            row.setField(35,mdStockRecord.getAfterHoursNumTrades()+"");
            row.setField(36,mdStockRecord.getAfterHoursTotalVolumeTrade()+"");
            row.setField(37,mdStockRecord.getAfterHoursTotalValueTrade()+"");
            row.setField(38,mdStockRecord.getBuy1Price()+"");
            row.setField(39,mdStockRecord.getBuy1OrderQty()+"");
            row.setField(40,mdStockRecord.getBuy1NumOrders()+"");
            row.setField(41,mdStockRecord.getBuy1NoOrders()+"");
            row.setField(42,mdStockRecord.getBuy1OrderDetailList().toString()+"");
            row.setField(43,mdStockRecord.getSell1Price()+"");
            row.setField(44,mdStockRecord.getSell1OrderQty()+"");
            row.setField(45,mdStockRecord.getSell1NumOrders()+"");
            row.setField(46,mdStockRecord.getSell1NoOrders()+"");
            row.setField(47,mdStockRecord.getSell1OrderDetailList().toString()+"");
            row.setField(48,mdStockRecord.getBuy2Price()+"");
            row.setField(49,mdStockRecord.getBuy2OrderQty()+"");
            row.setField(50,mdStockRecord.getBuy2NumOrders()+"");
            row.setField(51,mdStockRecord.getSell2Price()+"");
            row.setField(52,mdStockRecord.getSell2OrderQty()+"");
            row.setField(53,mdStockRecord.getSell2NumOrders()+"");
            row.setField(54,mdStockRecord.getBuy3Price()+"");
            row.setField(55,mdStockRecord.getBuy3OrderQty()+"");
            row.setField(56,mdStockRecord.getBuy3NumOrders()+"");
            row.setField(57,mdStockRecord.getSell3Price()+"");
            row.setField(58,mdStockRecord.getSell3OrderQty()+"");
            row.setField(59,mdStockRecord.getSell3NumOrders()+"");
            row.setField(60,mdStockRecord.getBuy4Price()+"");
            row.setField(61,mdStockRecord.getBuy4OrderQty()+"");
            row.setField(62,mdStockRecord.getBuy4NumOrders()+"");
            row.setField(63,mdStockRecord.getSell4Price()+"");
            row.setField(64,mdStockRecord.getSell4OrderQty()+"");
            row.setField(65,mdStockRecord.getSell4NumOrders()+"");
            row.setField(66,mdStockRecord.getBuy5Price()+"");
            row.setField(67,mdStockRecord.getBuy5OrderQty()+"");
            row.setField(68,mdStockRecord.getBuy5NumOrders()+"");
            row.setField(69,mdStockRecord.getSell5Price()+"");
            row.setField(70,mdStockRecord.getSell5OrderQty()+"");
            row.setField(71,mdStockRecord.getSell5NumOrders()+"");
            row.setField(72,mdStockRecord.getBuy6Price()+"");
            row.setField(73,mdStockRecord.getBuy6OrderQty()+"");
            row.setField(74,mdStockRecord.getBuy6NumOrders()+"");
            row.setField(75,mdStockRecord.getSell6Price()+"");
            row.setField(76,mdStockRecord.getSell6OrderQty()+"");
            row.setField(77,mdStockRecord.getSell6NumOrders()+"");
            row.setField(78,mdStockRecord.getBuy7Price()+"");
            row.setField(79,mdStockRecord.getBuy7OrderQty()+"");
            row.setField(80,mdStockRecord.getBuy7NumOrders()+"");
            row.setField(81,mdStockRecord.getSell7Price()+"");
            row.setField(82,mdStockRecord.getSell7OrderQty()+"");
            row.setField(83,mdStockRecord.getSell7NumOrders()+"");
            row.setField(84,mdStockRecord.getBuy8Price()+"");
            row.setField(85,mdStockRecord.getBuy8OrderQty()+"");
            row.setField(86,mdStockRecord.getBuy8NumOrders()+"");
            row.setField(87,mdStockRecord.getSell8Price()+"");
            row.setField(88,mdStockRecord.getSell8OrderQty()+"");
            row.setField(89,mdStockRecord.getSell8NumOrders()+"");
            row.setField(90,mdStockRecord.getBuy9Price()+"");
            row.setField(91,mdStockRecord.getBuy9OrderQty()+"");
            row.setField(92,mdStockRecord.getBuy9NumOrders()+"");
            row.setField(93,mdStockRecord.getSell9Price()+"");
            row.setField(94,mdStockRecord.getSell9OrderQty()+"");
            row.setField(95,mdStockRecord.getSell9NumOrders()+"");
            row.setField(96,mdStockRecord.getBuy10Price()+"");
            row.setField(97,mdStockRecord.getBuy10OrderQty()+"");
            row.setField(98,mdStockRecord.getBuy10NumOrders()+"");
            row.setField(99,mdStockRecord.getSell10Price()+"");
            row.setField(100,mdStockRecord.getSell10OrderQty()+"");
            row.setField(101,mdStockRecord.getSell10NumOrders()+"");
            row.setField(102,mdStockRecord.getHTSCSecurityID()+"");
            row.setField(103,mdStockRecord.getReceiveDateTime()+"");
            row.setField(104,mdStockRecord.getChannelNo()+"");

            collect(row);
        } catch (Throwable t) {
            logger.error("eval rawData={} failed", rawData, t);
        }


    }

}