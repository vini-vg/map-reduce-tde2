package com.company.analysis;

public class TransactionParser {
    private String country;
    private int year;
    private String commodityCode;
    private String commodity;
    private String flow;
    private double price;
    private double weight;
    private String unit;
    private double amount;
    private String category;

    public boolean parse(String line) {
        if (line.startsWith("Country;Year;")) return false; // Ignora cabeçalho

        String[] parts = line.split(";");
        if (parts.length != 10) return false;

        try {
            country = parts[0].trim();
            year = Integer.parseInt(parts[1].trim());
            commodityCode = parts[2].trim();
            commodity = parts[3].trim();
            flow = parts[4].trim();
            price = Double.parseDouble(parts[5].trim());
            weight = parts[6].isEmpty() ? 0 : Double.parseDouble(parts[6].trim());
            unit = parts[7].trim();
            amount = parts[8].isEmpty() ? 0 : Double.parseDouble(parts[8].trim());
            category = parts[9].trim();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    // Getters
    public String getCountry() { return country; }
    public int getYear() { return year; }
    public String getFlow() { return flow; }
    public double getPrice() { return price; }
    public String getCategory() { return category; }
    public double getAmount() { return amount; }
    public String getCommodity() { return commodity; }
}
