package datasets.utility;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ResourceBundle;

/**
 * Wraps the SQL connection so the user can easily open connections without entering credentials everytime.
 */
public class ConnectionWrapper {

    private static final String USERNAME;
    private static final String PASSWORD;
    private static final String URL;

    static {
        ResourceBundle rb = ResourceBundle.getBundle("resources");
        USERNAME = rb.getString("DB-user");
        PASSWORD = rb.getString("DB-password");
        URL =  rb.getString("DB-url");
    }

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(URL, USERNAME, PASSWORD);
    }
}
