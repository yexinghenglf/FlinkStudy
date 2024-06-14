package com.lfl.bigwork;

/**
 * @author 叶星痕
 * @data 2024/6/14 上午8:56
 * 文件名 : Stock
 * 描述 :
 */
public class Stock {
    private String stockCode;
    private String tradeDate;
    private String tradeTime;
    private double price;
    private int volume;

    public Stock() {
    }

    public Stock(String stockCode, String tradeDate, String tradeTime, double price, int volume) {
        this.stockCode = stockCode;
        this.tradeDate = tradeDate;
        this.tradeTime = tradeTime;
        this.price = price;
        this.volume = volume;
    }

    public String getStockCode() {
        return stockCode;
    }

    public void setStockCode(String stockCode) {
        this.stockCode = stockCode;
    }

    public String getTradeDate() {
        return tradeDate;
    }

    public void setTradeDate(String tradeDate) {
        this.tradeDate = tradeDate;
    }

    public String getTradeTime() {
        return tradeTime;
    }

    public void setTradeTime(String tradeTime) {
        this.tradeTime = tradeTime;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public int getVolume() {
        return volume;
    }

    public void setVolume(int volume) {
        this.volume = volume;
    }

    @Override
    public String toString() {
        return "Stock{" +
                "stockCode='" + stockCode + '\'' +
                ", tradeDate='" + tradeDate + '\'' +
                ", tradeTime='" + tradeTime + '\'' +
                ", price=" + price +
                ", volume=" + volume +
                '}';
    }
}
