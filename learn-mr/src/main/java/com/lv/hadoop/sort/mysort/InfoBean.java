package com.lv.hadoop.sort.mysort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class InfoBean implements WritableComparable<InfoBean> {

    private String account; // 账号
    private double income;  // 收入
    private double expense; // 支出
    private double surplus; // 结余

    public void set(String account, double income, double expense) {
        this.account = account;
        this.income = income;
        this.expense = expense;
        this.surplus = this.income - this.expense;
    }

    // 比较器
    @Override
    public int compareTo(InfoBean obj) {
        // 按照收入降序, 支出升序
        if (this.income == obj.getIncome()) {
            return this.expense > obj.getExpense() ? 1 : -1;
        } else {
            return this.income > obj.getIncome() ? -1 : 1;
        }
    }

    // 序列化
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(account);
        out.writeDouble(income);
        out.writeDouble(expense);
        out.writeDouble(surplus);
    }

    // 反序列化
    @Override
    public void readFields(DataInput in) throws IOException {
        this.account = in.readUTF();
        this.income = in.readDouble();
        this.expense = in.readDouble();
        this.surplus = in.readDouble();
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public double getIncome() {
        return income;
    }

    public void setIncome(double income) {
        this.income = income;
    }

    public double getExpense() {
        return expense;
    }

    public void setExpense(double expense) {
        this.expense = expense;
    }

    public double getSurplus() {
        return surplus;
    }

    public void setSurplus(double surplus) {
        this.surplus = surplus;
    }

    @Override
    public String toString() {
        return account + "\t" + income + "\t" + expense + "\t" + surplus + "\t";
    }
}
