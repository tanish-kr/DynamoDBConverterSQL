/**
 * 
 */
package ddb.converter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

/**
 * @author tatsunori_nishikori
 * 
 */
public class DBDriver {

    private Connection conn = null;

    private String dbType;
    private String dbUrl;

    /**
     * Contractor
     * @param dbType
     * @param dbHost
     * @param dbPort
     * @param dbName
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public DBDriver(String dbType, String dbHost, String dbPort, String dbName,
            String user, String password, Boolean ssl)
            throws ClassNotFoundException, SQLException {
        this.dbUrl = "jdbc:" + dbHost + "://" + dbHost + ":" + dbPort + "/"
                + dbName;
        connection(user, password, ssl);
    }

    /**
     * Contractor
     * @param dbType
     * @param dbHost
     * @param dbPort
     * @param dbName
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public DBDriver(String dbType, String dbHost, String dbPort, String dbName,
            String user, String password) throws ClassNotFoundException,
            SQLException {
        this.dbUrl = "jdbc:" + dbHost + "://" + dbHost + ":" + dbPort + "/"
                + dbName;
        connection(user, password, false);
    }

    /**
     * DB connection
     * @param driver
     * @param user
     * @param password
     * @param ssl
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    protected void connection(final String user,
            final String password, Boolean ssl) throws ClassNotFoundException,
            SQLException {
        if (this.dbType.equalsIgnoreCase("mysql")) {
            Class.forName("com.mysql.jdbc.Driver");
        } else if (this.dbType.equalsIgnoreCase("postgresql")) {
            Class.forName("org.postgresql.Driver");
        } else {
            throw new IllegalArgumentException(
                    "DB type is mysql or postgresql!");
        }

        // Open a connection and define properties.
        Properties props = new Properties();

        // Uncomment the following line if using a keystore.
        props.setProperty("user", user);
        props.setProperty("password", password);
        if (ssl) {
            props.setProperty("ssl", "true");
            props.setProperty("sslfactory",
                    "org.postgresql.ssl.NonValidatingFactory");
        }

        this.conn = DriverManager.getConnection(this.dbUrl, props);
    }

    /**
     * DB close
     */
    protected void close() {
        this.conn = null;
    }

    /**
     * execute sql
     * @param sql
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    protected void execute(String sql) throws ClassNotFoundException,
            SQLException {
        Statement stmt = null;
        stmt = conn.createStatement();
        stmt.execute(sql);
        // stmt.executeQuery(sql);
        stmt.close();
    }

    /**
     * sql select
     * 
     * @param sql
     * @return
     */
    private List<HashMap<String, String>> where(String sql) {
        return null;
    }
}
