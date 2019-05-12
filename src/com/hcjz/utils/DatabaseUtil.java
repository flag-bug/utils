package com.hcjz.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DatabaseUtil {
    private final static Logger LOGGER = LoggerFactory.getLogger(DatabaseUtil.class);

    private static final String DRIVER = "com.mysql.jdbc.Driver";
    private static final String URL = "jdbc:mysql://localhost:3306/hjmall?useUnicode=true&characterEncoding=utf8";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "root";

    private static final String SQL = "SELECT * FROM ";// 数据库操作

    static {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            LOGGER.error("can not load jdbc driver", e);
        }
    }

    /**
     * 获取数据库连接
     *
     * @return
     */
    public static Connection getConnection() {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(URL, USERNAME, PASSWORD);
        } catch (SQLException e) {
            LOGGER.error("get connection failure", e);
        }
        return conn;
    }

    /**
     * 关闭数据库连接
     * @param conn
     */
    public static void closeConnection(Connection conn) {
        if(conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                LOGGER.error("close connection failure", e);
            }
        }
    }

    /**
     * 获取数据库下的所有表名
     */
    public static List<String> getTableNames() {
        List<String> tableNames = new ArrayList<>();
        Connection conn = getConnection();
        ResultSet rs = null;
        try {
            //获取数据库的元数据
            DatabaseMetaData db = conn.getMetaData();
            //从元数据中获取到所有的表名
            rs = db.getTables(null, null, null, new String[] { "TABLE" });
            while(rs.next()) {
                tableNames.add(rs.getString(3));
            }
        } catch (SQLException e) {
            LOGGER.error("getTableNames failure", e);
        } finally {
            try {
                rs.close();
                closeConnection(conn);
            } catch (SQLException e) {
                LOGGER.error("close ResultSet failure", e);
            }
        }
        return tableNames;
    }

    /**
     * 获取表中所有字段名称
     * @param tableName 表名
     * @return
     */
    public static List<String> getColumnNames(String tableName) {
        List<String> columnNames = new ArrayList<>();
        //与数据库的连接
        Connection conn = getConnection();
        PreparedStatement pStemt = null;
        String tableSql = SQL + tableName;
        try {
            pStemt = conn.prepareStatement(tableSql);
            //结果集元数据
            ResultSetMetaData rsmd = pStemt.getMetaData();
            //表列数
            int size = rsmd.getColumnCount();
            for (int i = 0; i < size; i++) {
                columnNames.add(rsmd.getColumnName(i + 1));
            }
        } catch (SQLException e) {
            LOGGER.error("getColumnNames failure", e);
        } finally {
            if (pStemt != null) {
                try {
                    pStemt.close();
                    closeConnection(conn);
                } catch (SQLException e) {
                    LOGGER.error("getColumnNames close pstem and connection failure", e);
                }
            }
        }
        return columnNames;
    }

    /**
     * 获取表中所有字段类型
     * @param tableName
     * @return
     */
    public static List<String> getColumnTypes(String tableName) {
        List<String> columnTypes = new ArrayList<>();
        //与数据库的连接
        Connection conn = getConnection();
        PreparedStatement pStemt = null;
        String tableSql = SQL + tableName;
        try {
            pStemt = conn.prepareStatement(tableSql);
            //结果集元数据
            ResultSetMetaData rsmd = pStemt.getMetaData();
            //表列数
            int size = rsmd.getColumnCount();
            for (int i = 0; i < size; i++) {
                columnTypes.add(rsmd.getColumnTypeName(i + 1));
            }
        } catch (SQLException e) {
            LOGGER.error("getColumnTypes failure", e);
        } finally {
            if (pStemt != null) {
                try {
                    pStemt.close();
                    closeConnection(conn);
                } catch (SQLException e) {
                    LOGGER.error("getColumnTypes close pstem and connection failure", e);
                }
            }
        }
        return columnTypes;
    }

    /**
     * 获取表中字段的所有注释
     * @param tableName
     * @return
     */
    public static List<String> getColumnComments(String tableName) {
        List<String> columnTypes = new ArrayList<>();
        //与数据库的连接
        Connection conn = getConnection();
        PreparedStatement pStemt = null;
        String tableSql = SQL + tableName;
        List<String> columnComments = new ArrayList<>();//列名注释集合
        ResultSet rs = null;
        try {
            pStemt = conn.prepareStatement(tableSql);
            rs = pStemt.executeQuery("show full columns from " + tableName);
            while (rs.next()) {
                columnComments.add(rs.getString("Comment"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                    closeConnection(conn);
                } catch (SQLException e) {
                    LOGGER.error("getColumnComments close ResultSet and connection failure", e);
                }
            }
        }
        return columnComments;
    }

    public static void main(String[] args) {
        List<String> tableNames = getTableNames();
        Connection conn = getConnection();
        ResultSet rs = null;    //表
        ResultSet rs2 = null;   //列
//        ResultSet rs3 = null;   //主键
        DatabaseMetaData metaData = null;
        FileWriter fw = null;
        try {
            //获取数据库的元数据
            metaData = conn.getMetaData();
            //从元数据中获取到所有的表名
            rs = metaData.getTables(null, null, null, new String[]{"TABLE"});
            while (rs.next()) {
                tableNames.add(rs.getString(3));
            }
            /*int a = 0;
            while (columns.next()) {
                System.out.println(columns.getString(1));//数据库名
                System.out.println(columns.getString(2));
                System.out.println(columns.getString(3));//表名
                System.out.println(columns.getString(4));//字段名
                System.out.println(columns.getString(5));
                System.out.println(columns.getString(6));//字段类型
                System.out.println(columns.getString(7));
                System.out.println(columns.getString(8));
                System.out.println(columns.getString(9));
                System.out.println(columns.getString(10));
                System.out.println(++a);
            }*/
            File f = new File("E:\\hjmalltest\\database-operation\\src\\com\\hcjz\\utils\\gen.xml");
            fw = new FileWriter(f, true);
            for (String tableName : tableNames) {
                rs2 = metaData.getColumns(null, null, tableName, null);
                fw.write("<table tableName=\"" + tableName + "\">");
                while (rs2.next()) {
                    String columnName = rs2.getString(4);
                    String columnType = rs2.getString(6);
                    if (columnType.equalsIgnoreCase("LONGTEXT")) {
                        fw.write("\n\t");
                        fw.write("<columnOverride column=\"" + columnName + "\" jdbcType=\"${jdbcType.longtext}\" />");
                    } else if (columnType.equalsIgnoreCase("TEXT")) {
                        fw.write("\n\t");
                        fw.write("<columnOverride column=\"" + columnName + "\" jdbcType=\"${jdbcType.text}\" />");
                    } else if (columnType.equalsIgnoreCase("TINYINT")) {
                        fw.write("\n\t");
                        fw.write("<columnOverride column=\"" + columnName + "\" jdbcType=\"${jdbcType.tinyint}\" javaType=\"Integer\" />");
                    } else if (columnType.equalsIgnoreCase("SMALLINT")) {
                        fw.write("\n\t");
                        fw.write("<columnOverride column=\"" + columnName + "\" jdbcType=\"${jdbcType.smallint}\" javaType=\"Integer\" />");
                    }
                }
                fw.write("\n</table>\n");
            }
            LOGGER.info("complete!");
        } catch (SQLException e) {
            LOGGER.error("getTableNames failure", e);
        } catch (IOException e) {
            ;
        } finally {
            try {
                fw.close();
                rs.close();
                closeConnection(conn);
            } catch (SQLException e) {
                LOGGER.error("close ResultSet failure", e);
            } catch (IOException e) {
                ;
            }
        }


    }
}
