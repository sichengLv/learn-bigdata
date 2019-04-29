package com.lv.learn.hbase;

import com.lv.learn.util.HBaseManager;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

public class HBaseTest {

    public static void main(String[] args) {
        String table = "stu";
        String rowKey = "luffy";

        byte[][] prefixes = new byte[][]{Bytes.toBytes("a"), Bytes.toBytes("c")};
        MultipleColumnPrefixFilter multiPrefixFilter = new MultipleColumnPrefixFilter(prefixes);
        FamilyFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("info")));

        Result result = HBaseManager.get(table, rowKey, multiPrefixFilter);
        if (result != null) {
            List<Cell> cells = result.listCells();
            if (null == cells || cells.isEmpty()) {
                System.out.println(">>>>> Empty result <<<<<");
                return;
            }
            for (Cell cell : cells) {
                String cf = Bytes.toString(CellUtil.cloneFamily(cell));
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                String row = Bytes.toString(CellUtil.cloneRow(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println("rowkey=>" + row + " cf=>" + cf + " col=>" + qualifier + " value=>" + value);
            }
        }

    }

}
