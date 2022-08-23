package com.dtstack.chunjun.connector.doris.rest;

import com.dtstack.chunjun.converter.AbstractRowConverter.ColVal;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.IntStream;

/**
 * Company：www.dtstack.com.
 *
 * @author shitou
 * @date 2022/1/17
 */
public class Carrier implements Serializable {
    private static final long serialVersionUID = 1L;
    private final List<Map<String, Object>> insertContent;
    private final StringJoiner deleteContent;
    private int batch = 0;
    private String database;
    private String table;
    private List<String> columns;
    private final Set<Integer> rowDataIndexes = new HashSet<>();

    public Carrier() {
        insertContent = new ArrayList<>();
        deleteContent = new StringJoiner(" OR ");
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public List<Map<String, Object>> getInsertContent() {
        return insertContent;
    }

    public String getDeleteContent() {
        return deleteContent.toString();
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public Set<Integer> getRowDataIndexes() {
        return rowDataIndexes;
    }

    public void addRowDataIndex(int index) {
        rowDataIndexes.add(index);
    }


    public void addUpdateData(Pair<List<String>, List<ColVal>> update) {
        List<ColVal> pkVals = null;
        List<String> insertV = null;
        // Pair<List<String>, List<ColVal>> p = processGenericRowData(value, converter);
        if (update.getLeft() != null) {
            insertV = update.getLeft();
        }
        if (update.getRight() != null) {
            pkVals = update.getRight();
        }

//        List<String> joiner = Lists.newArrayList();
//        if (RowKind.INSERT.equals(value.getRowKind())) {
//            converter.toExternal(value, joiner);
//            //String[] split = String.valueOf(toExternal).split(",");
//            insertV.addAll(joiner);
//        }

        this.addInsertContent(insertV);
        this.addDeleteContent(pkVals);
    }


    private void addInsertContent(List<String> insertV) {
        if (CollectionUtils.isNotEmpty(insertV)) {
            if (insertV.size() > columns.size()) {
                // It is certain that in this case, the size
                // of insertV is twice the size of column
                List<String> forward = insertV.subList(0, columns.size());
                final Map<String, Object> forwardV = new HashMap<>(columns.size());
                IntStream.range(0, columns.size())
                        .forEach(i -> forwardV.put(columns.get(i), forward.get(i)));
                insertContent.add(forwardV);
                List<String> behind = insertV.subList(columns.size(), insertV.size());
                final Map<String, Object> behindV = new HashMap<>(columns.size());
                IntStream.range(0, columns.size())
                        .forEach(i -> behindV.put(columns.get(i), behind.get(i)));
                insertContent.add(behindV);
            } else {
                final Map<String, Object> values = new HashMap<>(columns.size());
                IntStream.range(0, columns.size())
                        .forEach(i -> values.put(columns.get(i), insertV.get(i)));
                insertContent.add(values);
            }
        }
    }

    private void addDeleteContent(List<ColVal> deletePks) {

//        if (!deleteV.isEmpty()) {
//            String s = buildMergeOnConditions(columns, deleteV);
//            deleteContent.add(s);
//        }

        if (CollectionUtils.isEmpty(deletePks)) {
            return;
        }


        //  String s =;
        deleteContent.add(buildMergeOnConditions(deletePks));

    }

    public void updateBatch() {
        batch++;
    }

    /**
     * Construct the Doris delete on condition, which only takes effect in the merge http request.
     *
     * @return delete on condition
     */
    private String buildMergeOnConditions(
            List<ColVal> deletePks
            //        List<String> columns, List<String> values
    ) {
        List<String> deleteOnStr = new ArrayList<>();
//        for (int i = 0, size = columns.size(); i < size; i++) {
//            String s =
//                    "`"
//                            + columns.get(i)
//                            + "`"
//                            + "<=>"
//                            + "'"
//                            + ((values.get(i)) == null ? "" : values.get(i))
//                            + "'";
//            deleteOnStr.add(s);
//        }

        for (ColVal cv : deletePks) {
            deleteOnStr.add("`"
                    + cv.key
                    + "`"
                    + "<=>"
                    + "'"
                    + String.valueOf(cv.val)
                    + "'");
        }
        return "(" + StringUtils.join(deleteOnStr, " AND ") + ")";
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{database:");
        sb.append(database);
        sb.append(", table:");
        sb.append(table);
        sb.append(", columns:[");
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(",");
            }
            sb.append(columns.get(i));
        }
        sb.append("], insert_value:");
        sb.append(insertContent);
        sb.append(", delete_value:");
        sb.append(deleteContent);
        sb.append(", batch:");
        sb.append(batch);
        sb.append("}");
        return sb.toString();
    }
}
