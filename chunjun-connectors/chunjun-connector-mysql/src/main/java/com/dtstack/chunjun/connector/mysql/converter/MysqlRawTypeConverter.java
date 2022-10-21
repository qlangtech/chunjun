//package com.dtstack.chunjun.connector.mysql.converter;
//
//
//import org.apache.flink.table.api.DataTypes;
//import org.apache.flink.table.types.DataType;
//
//import com.qlangtech.tis.plugin.ds.ColMeta;
//import com.qlangtech.tis.plugin.ds.DataType.TypeVisitor;
//
///**
// * @program: ChunJun
// * @author: wuren
// * @create: 2021/04/14
// */
//public class MysqlRawTypeConverter {
//
//    /**
//     * 将MySQL数据库中的类型，转换成flink的DataType类型。 转换关系参考 com.mysql.jdbc.MysqlDefs 类里面的信息。
//     * com.mysql.jdbc.ResultSetImpl.getObject(int)
//     */
//    public static DataType apply(ColMeta type) {
//        return type.type.accept(new TypeVisitor<DataType>() {
//            @Override
//            public DataType bigInt(com.qlangtech.tis.plugin.ds.DataType type) {
//                return DataTypes.BIGINT();
//            }
//
//            @Override
//            public DataType doubleType(com.qlangtech.tis.plugin.ds.DataType type) {
//                return DataTypes.DOUBLE();
//            }
//
//            @Override
//            public DataType dateType(com.qlangtech.tis.plugin.ds.DataType type) {
//                return DataTypes.DATE();
//            }
//
//            @Override
//            public DataType timestampType(com.qlangtech.tis.plugin.ds.DataType type) {
//                return DataTypes.TIMESTAMP(0);
//            }
//
//            @Override
//            public DataType bitType(com.qlangtech.tis.plugin.ds.DataType type) {
//                return DataTypes.BOOLEAN();
//            }
//
//            @Override
//            public DataType blobType(com.qlangtech.tis.plugin.ds.DataType type) {
//                return DataTypes.BYTES();
//            }
//
//            @Override
//            public DataType varcharType(com.qlangtech.tis.plugin.ds.DataType type) {
//                return DataTypes.STRING();
//            }
//
//            @Override
//            public DataType intType(com.qlangtech.tis.plugin.ds.DataType type) {
//                return DataTypes.INT();
//            }
//
//            @Override
//            public DataType floatType(com.qlangtech.tis.plugin.ds.DataType type) {
//                return DataTypes.FLOAT();
//            }
//
//            @Override
//            public DataType decimalType(com.qlangtech.tis.plugin.ds.DataType type) {
//                return DataTypes.DECIMAL(38, 18);
//            }
//
//            @Override
//            public DataType timeType(com.qlangtech.tis.plugin.ds.DataType type) {
//                return DataTypes.TIME();
//            }
//
//            @Override
//            public DataType tinyIntType(com.qlangtech.tis.plugin.ds.DataType dataType) {
//                return DataTypes.TINYINT();
//            }
//
//            @Override
//            public DataType smallIntType(com.qlangtech.tis.plugin.ds.DataType dataType) {
//                return DataTypes.SMALLINT();
//            }
//        });
////        switch (type.toUpperCase(Locale.ENGLISH)) {
////            case "BOOLEAN":
////            case "BIT":
////                return DataTypes.BOOLEAN();
////            case "TINYINT":
////                return DataTypes.TINYINT();
////            case "TINYINT UNSIGNED":
////            case "SMALLINT":
////                return DataTypes.SMALLINT();
////            case "SMALLINT UNSIGNED":
////            case "MEDIUMINT":
////            case "MEDIUMINT UNSIGNED":
////            case "INT":
////            case "INTEGER":
////            case "INT24":
////                return DataTypes.INT();
////            case "INT UNSIGNED":
////            case "BIGINT":
////                return DataTypes.BIGINT();
////            case "REAL":
////            case "FLOAT":
////            case "FLOAT UNSIGNED":
////                return DataTypes.FLOAT();
////            case "DECIMAL":
////            case "DECIMAL128":
////            case "DECIMAL UNSIGNED":
////            case "NUMERIC":
////                return DataTypes.DECIMAL(38, 18);
////            case "DOUBLE":
////            case "DOUBLE UNSIGNED":
////                return DataTypes.DOUBLE();
////            case "CHAR":
////            case "VARCHAR":
////            case "STRING":
////                return DataTypes.STRING();
////            case "DATE":
////                return DataTypes.DATE();
////            case "TIME":
////                return DataTypes.TIME();
////            case "YEAR":
////                return DataTypes.INTERVAL(DataTypes.YEAR());
////            case "TIMESTAMP":
////            case "DATETIME":
////                return DataTypes.TIMESTAMP(0);
////            case "TINYBLOB":
////            case "BLOB":
////            case "MEDIUMBLOB":
////            case "LONGBLOB":
////                return DataTypes.BYTES();
////            case "TINYTEXT":
////            case "TEXT":
////            case "MEDIUMTEXT":
////            case "LONGTEXT":
////                return DataTypes.STRING();
////            case "BINARY":
////            case "VARBINARY":
////                // BYTES 底层调用的是VARBINARY最大长度
////                return DataTypes.BYTES();
////            case "JSON":
////                return DataTypes.STRING();
////            case "ENUM":
////            case "SET":
////                return DataTypes.STRING();
////            case "GEOMETRY":
////                return DataTypes.BYTES();
////
////            default:
////                throw new UnsupportedTypeException(type);
////        }
//    }
//}
