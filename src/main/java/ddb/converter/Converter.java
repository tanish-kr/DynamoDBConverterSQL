/**
 * 
 */
package ddb.converter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * DynamoDB Export File Converter
 * @author tatsunori_nishikori
 */
public class Converter {

    protected final static Integer THREAD_COUNT = 5;

    private static StringBuilder convertSqlSb = new StringBuilder();

    public static StringBuilder getConvertSqlSb() {
        return convertSqlSb;
    }

    public void setConvertSqlSb(StringBuilder sqlString) {
        Converter.convertSqlSb = sqlString;
    }

    public static void setConvertSqlSb(String sqlString) {
        convertSqlSb.append(sqlString);
    }
    /**
     * 
     */
    public Converter() {
        // TODO 自動生成されたコンストラクター・スタブ
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        String tableName = args[0];
        String inputFile = args[1];
        String outputFile = args[2];
        System.out.println(tableName);
        try {
            JSONArray ddbData = ddbJSONFileReader(inputFile);
            jsonToSql(tableName, ddbData);
            saveSqlFile(outputFile);
        } catch (IOException e) {
            // TODO 自動生成された catch ブロック
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * dynamodb JSON File Reader
     * 
     * @param filePath
     * @return
     * @throws IOException
     */
    protected static JSONArray ddbJSONFileReader(String filePath)
            throws IOException {
        StringBuilder ddbData = new StringBuilder();
        File file = new File(filePath);
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line;
        while ((line = br.readLine()) != null) {
            ddbData.append(line);
        }
        br.close();
        return new JSONArray(ddbData.toString());
    }

    /**
     * save sql
     * @param filePath
     * @throws IOException
     */
    protected static void saveSqlFile(String filePath) throws IOException {
        File file = new File(filePath);
        FileWriter writer = new FileWriter(file);
        writer.write(getConvertSqlSb().toString());
        writer.close();
    }

    /**
     * json convert to SQL
     * 
     * @param ddbData
     * @return
     */
    protected static String jsonToSql(final String tableName, JSONArray ddbData) {
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < ddbData.length(); i++) {
            final JSONObject row = ddbData.getJSONObject(i);
            // keyset = INSERT key
            executor.execute(new Runnable() {

                public void run() {
                    String sql = createInsertQuery(tableName, row);
                    // file save
                    setConvertSqlSb(sql);
                }
            });
        }
        try {
            executor.shutdown();
            if (!executor.awaitTermination(30, TimeUnit.MINUTES)) {
                executor.shutdownNow();
            }
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }
        return getConvertSqlSb().toString();
    }

    /**
     * create insert Query
     * 
     * @param tableName
     * @param ddbRow
     * @return
     */
    protected static String createInsertQuery(String tableName,
            JSONObject ddbRow) {
        StringBuilder sql = new StringBuilder();
        Set<String> columnSet = ddbRow.keySet();
        sql.append("INSERT IGNORE INTO ").append(tableName).append(" (");
        sql.append(String.join(",", columnSet)).append(") VALUES (");
        for (String column : columnSet) {
            JSONObject row = ddbRow.getJSONObject(column);
            if (row.has("s")) {
                // format type 's' String
                sql.append("'").append(row.getString("s")).append("'")
                        .append(",");
            } else if (row.has("n")) {
                // format type 'n' Number
                sql.append(Integer.valueOf(row.getString("n"))).append(",");
            } else if (row.has("b")) {
                // TODO binary type
                // format type 'b'
                sql.append("").append(",");
            }
        }
        sql.deleteCharAt(sql.lastIndexOf(","));
        sql.append(");\n");
        return sql.toString();
    }

    /**
     * create mulit insert TODO
     * 
     * @param tableName
     * @param columns
     * @param ddbRow
     * @return
     */
    protected String createMultiInsertQuery(String tableName, String[] columns,
            JSONObject ddbRow) {
        return null;
    }

    /**
     * 
     * @param dbType
     * @param sql
     */
    protected void insertDB(String dbType, String sql) {

    }
}
