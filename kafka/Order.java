package com.i2i.kafka;

public class Order {
    private String orderId;
    private String product;
    private int quantity;
    private double totalPrice;

    public Order() {}

    public Order(String orderId, String product, int quantity, double totalPrice) {
        this.orderId = orderId;
        this.product = product;
        this.quantity = quantity;
        this.totalPrice = totalPrice;
    }

    public String getOrderId()       { return orderId; }
    public String getProduct()       { return product; }
    public int    getQuantity()      { return quantity; }
    public double getTotalPrice()    { return totalPrice; }

    public void setOrderId(String orderId)       { this.orderId = orderId; }
    public void setProduct(String product)       { this.product = product; }
    public void setQuantity(int quantity)        { this.quantity = quantity; }
    public void setTotalPrice(double totalPrice) { this.totalPrice = totalPrice; }

    @Override
    public String toString() {
        return String.format("Order{orderId='%s', product='%s', quantity=%d, totalPrice=%.2f}",
                orderId, product, quantity, totalPrice);
    }
}
