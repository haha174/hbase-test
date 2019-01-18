package com.wen.hbase.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HBaseOperation {

    Configuration conf = null;
    Connection conn = null;
    TableName tableName=TableName.valueOf("test");
    @Before
    public void getConfigAndConnection() {
        conf = HBaseConfiguration.create();
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void dropTable() throws IOException {
        Admin admin = conn.getAdmin();
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        //关闭链接
    }
    @Test
    public void createTable() throws IOException {
        Admin admin = conn.getAdmin();
        if (!admin.tableExists(tableName)) {
            //表描述器构造器
            TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tableName);
            //列族描述起构造器
            ColumnFamilyDescriptorBuilder cdb = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("user"));
            //获得列描述起
            ColumnFamilyDescriptor cfd = cdb.build();
            //添加列族
            tdb.setColumnFamily(cfd);
            //获得表描述器
            TableDescriptor td = tdb.build();
            //创建表
            //admin.addColumnFamily(tableName, cfd); //给标添加列族
            admin.createTable(td);
        }else {
            System.out.println("table is Exists");
        }
        //关闭链接
    }

    //单条插入
    @Test
    public void insertOneData() throws IOException {
        //new 一个列  ，haha_000为row key
        Put put = new Put(Bytes.toBytes("100001"));
        //下面三个分别为，列族，列名，列值
        put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("name"), Bytes.toBytes("hgs"));
        //得到 table
        Table table = conn.getTable(tableName);
        //执行插入
        table.put(put);
    }

    //插入多个列
    @Test
    public void insertManyData() throws IOException {
        Table table = conn.getTable(tableName);
        List<Put> puts = new ArrayList<Put>();
        Put put1 = new Put(Bytes.toBytes("haha_001"));
        put1.addColumn(Bytes.toBytes("user"), Bytes.toBytes("name"), Bytes.toBytes("wd"));

        Put put2 = new Put(Bytes.toBytes("haha_001"));
        put2.addColumn(Bytes.toBytes("user"), Bytes.toBytes("age"), Bytes.toBytes("25"));

        Put put3 = new Put(Bytes.toBytes("haha_001"));
        put3.addColumn(Bytes.toBytes("user"), Bytes.toBytes("weight"), Bytes.toBytes("60kg"));

        Put put4 = new Put(Bytes.toBytes("haha_001"));
        put4.addColumn(Bytes.toBytes("user"), Bytes.toBytes("sex"), Bytes.toBytes("男"));
        puts.add(put1);
        puts.add(put2);
        puts.add(put3);
        puts.add(put4);
        table.put(puts);
        table.close();
    }

    //同一条数据的插入
    @Test
    public void singleRowInsert() throws IOException {
        Table table = conn.getTable(tableName);

        Put put1 = new Put(Bytes.toBytes("haha_005"));

        put1.addColumn(Bytes.toBytes("user"), Bytes.toBytes("name"), Bytes.toBytes("cm"));
        put1.addColumn(Bytes.toBytes("user"), Bytes.toBytes("age"), Bytes.toBytes("22"));
        put1.addColumn(Bytes.toBytes("user"), Bytes.toBytes("weight"), Bytes.toBytes("88kg"));
        put1.addColumn(Bytes.toBytes("user"), Bytes.toBytes("sex"), Bytes.toBytes("男"));

        table.put(put1);
        table.close();
    }

    //数据的更新,hbase对数据只有追加，没有更新，但是查询的时候会把最新的数据返回给哦我们
    @Test
    public void updateData() throws IOException {
        Table table = conn.getTable(tableName);
        Put put1 = new Put(Bytes.toBytes("haha_005"));
        put1.addColumn(Bytes.toBytes("user"), Bytes.toBytes("weight"), Bytes.toBytes("66kg"));
        table.put(put1);
        table.close();
    }

    //删除数据
    @Test
    public void deleteData() throws IOException {
        Table table = conn.getTable(tableName);
        //参数为 row key
        //删除一列
        Delete delete1 = new Delete(Bytes.toBytes("haha_000"));
        delete1.addColumn(Bytes.toBytes("user"), Bytes.toBytes("weight"));
        //删除多列
        Delete delete2 = new Delete(Bytes.toBytes("haha_001"));
        delete2.addColumns(Bytes.toBytes("user"), Bytes.toBytes("age"));
        delete2.addColumns(Bytes.toBytes("user"), Bytes.toBytes("sex"));
        //删除某一行的列族内容
        Delete delete3 = new Delete(Bytes.toBytes("haha_002"));
        delete3.addFamily(Bytes.toBytes("user"));

        //删除一整行
        Delete delete4 = new Delete(Bytes.toBytes("haha_003"));
        table.delete(delete1);
        table.delete(delete2);
        table.delete(delete3);
        table.delete(delete4);
        table.close();
    }

    //查询
    //
    @Test
    public void querySingleRow() throws IOException {
        Table table = conn.getTable(tableName);
        //获得一行
        Get get = new Get(Bytes.toBytes("haha_005"));
        Result set = table.get(get);
        Cell[] cells = set.rawCells();
        for (Cell cell : cells) {
            System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + "::" +
                    Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
        }
        table.close();
        //Bytes.toInt(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("password")))

    }

    //全表扫描
    @Test
    public void scanTable() throws IOException {
        Table table = conn.getTable(tableName);
        Scan scan = new Scan();
        //scan.addFamily(Bytes.toBytes("info"));
        //scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("password"));
        //scan.setStartRow(Bytes.toBytes("wangsf_0"));
        //scan.setStopRow(Bytes.toBytes("wangwu"));
        ResultScanner rsacn = table.getScanner(scan);
        for (Result rs : rsacn) {
            String rowkey = Bytes.toString(rs.getRow());
            System.out.println("row key :" + rowkey);
            Cell[] cells = rs.rawCells();
            for (Cell cell : cells) {
                System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + "::" +
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("-----------------------------------------");
        }
    }
    //过滤器

    @Test
    //列值过滤器
    public void singColumnFilter() throws IOException {
        Table table = conn.getTable(tableName);
        Scan scan = new Scan();
        //下列参数分别为，列族，列名，比较符号，值
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("user"), Bytes.toBytes("name"),
                CompareOperator.EQUAL, Bytes.toBytes("wd"));
      //  filter.setFilterIfMissing(true);
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result rs : scanner) {
            String rowkey = Bytes.toString(rs.getRow());
            System.out.println("row key :" + rowkey);
            Cell[] cells = rs.rawCells();
            for (Cell cell : cells) {
                System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + "::" +
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("-----------------------------------------");
        }
    }

    //row key过滤器
    @Test
    public void rowkeyFilter() throws IOException {
        Table table = conn.getTable(tableName);
        Scan scan = new Scan();
        RowFilter filter = new RowFilter(CompareOperator.EQUAL, new RegexStringComparator("^haha_00*"));
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result rs : scanner) {
            String rowkey = Bytes.toString(rs.getRow());
            System.out.println("row key :" + rowkey);
            Cell[] cells = rs.rawCells();
            for (Cell cell : cells) {
                System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + "::" +
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("-----------------------------------------");
        }
    }

    //列名前缀过滤器
    @Test
    public void columnPrefixFilter() throws IOException {
        Table table = conn.getTable(tableName);
        Scan scan = new Scan();
        ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes("name"));
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result rs : scanner) {
            String rowkey = Bytes.toString(rs.getRow());
            System.out.println("row key :" + rowkey);
            Cell[] cells = rs.rawCells();
            for (Cell cell : cells) {
                System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + "::" +
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("-----------------------------------------");
        }
    }

    //过滤器集合
    @Test
    public void FilterSet() throws IOException {
        Table table = conn.getTable(tableName);
        Scan scan = new Scan();
        FilterList list = new FilterList(Operator.MUST_PASS_ALL);
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes("user"), Bytes.toBytes("age"),
                CompareOperator.GREATER, Bytes.toBytes("23"));
        ColumnPrefixFilter filter2 = new ColumnPrefixFilter(Bytes.toBytes("weig"));
        list.addFilter(filter1);
        list.addFilter(filter2);

        scan.setFilter(list);
        ResultScanner scanner = table.getScanner(scan);
        for (Result rs : scanner) {
            String rowkey = Bytes.toString(rs.getRow());
            System.out.println("row key :" + rowkey);
            Cell[] cells = rs.rawCells();
            for (Cell cell : cells) {
                System.out.println(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + "::" +
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("-----------------------------------------");
        }

    }

    @After
    public void closeConn() throws IOException {
        conn.close();
    }
}
