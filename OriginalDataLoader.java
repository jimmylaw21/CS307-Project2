/**
 * 导入不同表时，注意修改stmt预编译用的String，修改LoadData方法和main方法里的String，set集合，part[]等等
 */
import com.sun.org.apache.xpath.internal.objects.XString;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.Properties;
import java.sql.*;
import java.net.URL;
import java.util.stream.Collectors;

public class OriginalDataLoader {
    private static final int  BATCH_SIZE = 500;
    private static URL        propertyURL = OriginalDataLoader.class
            .getResource("/loader.cnf");

    private static Connection         con = null;
    private static PreparedStatement  stmt1 = null;
    private static PreparedStatement  stmt2 = null;
    private static PreparedStatement  stmt3 = null;
    private static PreparedStatement  stmt4 = null;
    private static boolean            verbose = false;

    private static String staffLoader =
            "insert into staff(id,name,age,gender,number,supply_center,mobile_phone,type)"
                    +" values(?,?,?,?,?,?,?,?)";
    private static String modelLoader =
            "insert into model(id,number,model,name,unit_price)"
                    +" values(?,?,?,?,?)";
    private static String enterpriseLoader =
            "insert into enterprise(id,name,country,city,supply_center,industry)"+"values(?,?,?,?,?,?)";

    private static String centerLoader =
            "insert into center(id,name)"+"values(?,?)";

    private static void openDB(String host, String dbname,
                               String user, String pwd) {
        try {
            Class.forName("org.postgresql.Driver");
        } catch(Exception e) {
            System.err.println("Cannot find the Postgres driver. Check CLASSPATH.");
            System.exit(1);
        }
        String url = "jdbc:postgresql://" + host + "/" + dbname;
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", pwd);
        try {
            con = DriverManager.getConnection(url, props);
            if (verbose) {
                System.out.println("Successfully connected to the database "
                        + dbname + " as " + user);
            }
            con.setAutoCommit(false);
        } catch (SQLException e) {
            System.err.println("Database connection failed");
            System.err.println(e.getMessage());
            System.exit(1);
        }
        try {
            stmt1 = con.prepareStatement(staffLoader);
            stmt2 = con.prepareStatement(modelLoader);
            stmt3 = con.prepareStatement(enterpriseLoader);
            stmt4 = con.prepareStatement(centerLoader);
        } catch (SQLException e) {
            System.err.println("Insert statement failed");
            System.err.println(e.getMessage());
            closeDB();
            System.exit(1);
        }
    }

    private static void closeDB() {
        if (con != null) {
            try {
                if (stmt1 != null) {
                    stmt1.close();
                }
                if (stmt2 != null) {
                    stmt1.close();
                }
                if (stmt3 != null) {
                    stmt1.close();
                }
                if (stmt4 != null) {
                    stmt1.close();
                }
                con.close();
                con = null;
            } catch (Exception e) {
                // Forget about it
            }
        }
    }

    private static void staffLoadData(int id,
            String name,int age,String gender,int number,
            String supply_center,String mobile_phone,String type)
            throws SQLException {
        if (con != null) {
            stmt1.setInt(1, id);
            stmt1.setString(2, name);
            stmt1.setInt(3, age);
            stmt1.setString(4, gender);
            stmt1.setInt(5, number);
            stmt1.setString(6, supply_center);
            stmt1.setString(7, mobile_phone);
            stmt1.setString(8, type);
            stmt1.addBatch();
        }
    }

    private static void modelLoadData(int id,String number,String model,String name,int unit_price)
            throws SQLException {
        if (con != null) {
            stmt2.setInt(1, id);
            stmt2.setString(2, number);
            stmt2.setString(3, model);
            stmt2.setString(4, name);
            stmt2.setInt(5, unit_price);
            stmt2.addBatch();
        }
    }

    private static void enterpriseLoadData(int id,String name,String country,String city,String supply_center,String industry)
            throws SQLException {
        if (con != null) {
            stmt3.setInt(1, id);
            stmt3.setString(2, name);
            stmt3.setString(3, country);
            stmt3.setString(4, city);
            stmt3.setString(5, supply_center);
            stmt3.setString(6, industry);
            stmt3.addBatch();
        }
    }

    private static void centerLoadData(int id,String name)
            throws SQLException {
        if (con != null) {
            stmt4.setInt(1, id);
            stmt4.setString(2, name);
            stmt4.addBatch();
        }
    }

    public static void main(String[] args) {
//        String  fileName = null;
//        boolean verbose = false;
//
//        switch (args.length) {
//            case 1:
//                fileName = args[0];
//                break;
//            case 2:
//                switch (args[0]) {
//                    case "-v":
//                        verbose = true;
//                        break;
//                    default:
//                        System.err.println("Usage: java [-v] contract_Loader filename");
//                        System.exit(1);
//                }
//                fileName = args[1];
//                break;
//            default:
//                System.err.println("Usage: java [-v] contract_Loader filename");
//                System.exit(1);
//        }

//        if (propertyURL == null) {
//           System.err.println("No configuration file (loader.cnf) found");
//           System.exit(1);
//        }
        Properties defprop = new Properties();
        defprop.put("host", "localhost");
        defprop.put("user", "checker");
        defprop.put("password", "123456");
        defprop.put("database", "postgres");
        Properties prop = new Properties(defprop);
//        try (BufferedReader conf
//                = new BufferedReader(new FileReader(propertyURL.getPath()))) {
//          prop.load(conf);
//        } catch (IOException e) {
//           // Ignore
//           System.err.println("No configuration file (loader.cnf) found");
//        }
        try (BufferedReader infile
                     = new BufferedReader(new FileReader("staff.csv"))) {
            long     start;
            long     end;
            String   line;
            String[] parts;
            int      id;
            String   name;
            int      age;
            String   gender;
            int      number;
            String   supply_center;
            String   mobile_phone;
            String   type;
            int      cnt = 0;

            // Empty target table
            openDB(prop.getProperty("host"), prop.getProperty("database"),
                    prop.getProperty("user"), prop.getProperty("password"));
            Statement stmt0;
            if (con != null) {
                stmt0 = con.createStatement();
                stmt0.execute("truncate table enterprise");
                stmt0.execute("truncate table center");
                stmt0.execute("truncate table staff");
                stmt0.execute("truncate table model");
                stmt0.close();
            }
            closeDB();


            // Fill target table
            start = System.currentTimeMillis();
            openDB(prop.getProperty("host"), prop.getProperty("database"),
                    prop.getProperty("user"), prop.getProperty("password"));
            while ((line = infile.readLine()) != null) {
                parts = line.replace("\"Hong Kong, Macao and Taiwan regions of China\"","\"Hong Kong Macao and Taiwan regions of China\"").split(",");
                if (parts.length > 1) {
                    id = Integer.parseInt(parts[0]);
                    name = parts[1];
                    age = Integer.parseInt(parts[2]);
                    gender = parts[3];
                    number = Integer.parseInt(parts[4]);
                    supply_center = parts[5];
                    mobile_phone = parts[6];
                    type = parts[7];
                    staffLoadData(id,name,age,gender,number,supply_center,mobile_phone,type);
                    cnt++;
                    if (cnt % BATCH_SIZE == 0) {
                        stmt1.executeBatch();
                        stmt1.clearBatch();
                    }
                }
            }
            if (cnt % BATCH_SIZE != 0) {
                stmt1.executeBatch();
            }
            con.commit();
            stmt1.close();
            closeDB();
            end = System.currentTimeMillis();
            System.out.println(cnt + " records successfully loaded\n");
            System.out.println("Loading speed : "
                    + (cnt * 1000)/(end - start)
                    + " records/s");


        } catch (SQLException se) {
            System.err.println("SQL error: " + se.getMessage());
            try {
                con.rollback();
                stmt1.close();
            } catch (Exception e2) {
            }
            closeDB();
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Fatal error: " + e.getMessage());
            try {
                con.rollback();
                stmt1.close();
            } catch (Exception e2) {
            }
            closeDB();
            System.exit(1);
        }

        try (BufferedReader infile
                     = new BufferedReader(new FileReader("model.csv"))) {
            long     start;
            long     end;
            String   line;
            String[] parts;
            int      id;
            String   number;
            String   model;
            String   name;
            int   unit_price;
            int      cnt = 0;

            // Fill target table
            start = System.currentTimeMillis();
            openDB(prop.getProperty("host"), prop.getProperty("database"),
                    prop.getProperty("user"), prop.getProperty("password"));
            while ((line = infile.readLine()) != null) {
                parts = line.split(",");
                if (parts.length > 1) {
                    id = Integer.parseInt(parts[0]);
                    number = parts[1];
                    model = parts[2];
                    name = parts[3];
                    unit_price = Integer.parseInt(parts[4]);
                    modelLoadData(id,number,model,name,unit_price);
                    cnt++;
                    if (cnt % BATCH_SIZE == 0) {
                        stmt2.executeBatch();
                        stmt2.clearBatch();
                    }
                }
            }
            if (cnt % BATCH_SIZE != 0) {
                stmt2.executeBatch();
            }
            con.commit();
            stmt2.close();
            closeDB();
            end = System.currentTimeMillis();
            System.out.println(cnt + " records successfully loaded\n");
            System.out.println("Loading speed : "
                    + (cnt * 1000)/(end - start)
                    + " records/s");


        } catch (SQLException se) {
            System.err.println("SQL error: " + se.getMessage());
            try {
                con.rollback();
                stmt2.close();
            } catch (Exception e2) {
            }
            closeDB();
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Fatal error: " + e.getMessage());
            try {
                con.rollback();
                stmt2.close();
            } catch (Exception e2) {
            }
            closeDB();
            System.exit(1);
        }

        try (BufferedReader infile
                     = new BufferedReader(new FileReader("enterprise.csv"))) {
            long     start;
            long     end;
            String   line;
            String[] parts;
            int      id;
            String   name;
            String   country;
            String   city;
            String   supply_center;
            String   industry;
            int      cnt = 0;

            // Fill target table
            start = System.currentTimeMillis();
            openDB(prop.getProperty("host"), prop.getProperty("database"),
                    prop.getProperty("user"), prop.getProperty("password"));
            while ((line = infile.readLine()) != null) {
                parts = line.split(",");
                if (parts.length > 1) {
                    id = Integer.parseInt(parts[0]);
                    name = parts[1];
                    country = parts[2];
                    city = parts[3];
                    supply_center = parts[4];
                    industry = parts[5];
                    enterpriseLoadData(id,name,country,city,supply_center,industry);
                    cnt++;
                    if (cnt % BATCH_SIZE == 0) {
                        stmt3.executeBatch();
                        stmt3.clearBatch();
                    }
                }
            }
            if (cnt % BATCH_SIZE != 0) {
                stmt3.executeBatch();
            }
            con.commit();
            stmt3.close();
            closeDB();
            end = System.currentTimeMillis();
            System.out.println(cnt + " records successfully loaded\n");
            System.out.println("Loading speed : "
                    + (cnt * 1000)/(end - start)
                    + " records/s");


        } catch (SQLException se) {
            System.err.println("SQL error: " + se.getMessage());
            try {
                con.rollback();
                stmt3.close();
            } catch (Exception e2) {
            }
            closeDB();
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Fatal error: " + e.getMessage());
            try {
                con.rollback();
                stmt3.close();
            } catch (Exception e2) {
            }
            closeDB();
            System.exit(1);
        }

        try (BufferedReader infile
                     = new BufferedReader(new FileReader("center.csv"))) {
            long     start;
            long     end;
            String   line;
            String[] parts;
            int      id;
            String   name;
            int      cnt = 0;

            // Fill target table
            start = System.currentTimeMillis();
            openDB(prop.getProperty("host"), prop.getProperty("database"),
                    prop.getProperty("user"), prop.getProperty("password"));
            while ((line = infile.readLine()) != null) {
                parts = line.replace("\"Hong Kong, Macao and Taiwan regions of China\"","\"Hong Kong Macao and Taiwan regions of China\"").split(",");
                if (parts.length > 1) {
                    id = Integer.parseInt(parts[0]);
                    name = parts[1];
                    centerLoadData(id,name);
                    cnt++;
                    if (cnt % BATCH_SIZE == 0) {
                        stmt4.executeBatch();
                        stmt4.clearBatch();
                    }
                }
            }
            if (cnt % BATCH_SIZE != 0) {
                stmt4.executeBatch();
            }
            con.commit();
            stmt4.close();
            closeDB();
            end = System.currentTimeMillis();
            System.out.println(cnt + " records successfully loaded\n");
            System.out.println("Loading speed : "
                    + (cnt * 1000)/(end - start)
                    + " records/s");


        } catch (SQLException se) {
            System.err.println("SQL error: " + se.getMessage());
            try {
                con.rollback();
                stmt4.close();
            } catch (Exception e2) {
            }
            closeDB();
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Fatal error: " + e.getMessage());
            try {
                con.rollback();
                stmt4.close();
            } catch (Exception e2) {
            }
            closeDB();
            System.exit(1);
        }
        closeDB();
    }
}


