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
    private static final int  BATCH_SIZE = 50;
    private static URL        propertyURL = OriginalDataLoader.class
            .getResource("/loader.cnf");

    private static Connection         con = null;
    private static PreparedStatement  stmt1,stmt2,stmt3,stmt4,stmt5,stmt6,stmt7,stmt8,stmt9,stmt10 = null;
    private static boolean            verbose = false;
    private static ResultSet resultSet;

    /**
     * prepareStatement的String
     */
    private static String staffLoader =
            "insert into staff(id,name,age,gender,number,supply_center,mobile_phone,type)"
                    +" values(?,?,?,?,?,?,?,?)";
    private static String modelLoader =
            "insert into model(id,number,model,name,unit_price)"
                    +" values(?,?,?,?,?)";
    private static String enterpriseLoader =
            "insert into enterprise(id,name,country,city,supply_center,industry)"
                    +"values(?,?,?,?,?,?)";

    private static String centerLoader =
            "insert into center(id,name)"
                    +"values(?,?)";

    private static String stockInLoader =
            "insert into stockIn(id,supply_center,model,supply_staff,date,purchase_price,quantity)"
            +"values(?,?,?,?,?,?,?)";

    private static String stockLoader = "insert into stock(id,supply_center,model,quantity)"
            +"values(?,?,?,?)";

    private static String ordersLoader =
            "insert into orders(contract,product_model,quantity,salesman_number,estimated_date,lodgement_date,seq)"
                    +"values(?,?,?,?,?,?,?)";

    private static String contractLoader =
            "insert into contract(contract,enterprise,contract_manager,contract_date,contract_type)"
                    +"values(?,?,?,?,?)";
    private static String updateOrders =
            "update orders set contract = ?, product_model = ?,quantity = ?, " +
                    "salesman_number = ?, estimated_date = ?,lodgement_date = ?" +
                    "where contract = ? and product_model = ? and salesman_number = ?";
    private static String deleteOrders =
            "delete from orders where contract = ? and salesman_number = ? and seq = ?";
    /**
     * 打开数据库的方法
     */
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
        /**
         * 定义prepareStatement
         */
        try {
            stmt1 = con.prepareStatement(staffLoader);
            stmt2 = con.prepareStatement(modelLoader);
            stmt3 = con.prepareStatement(enterpriseLoader);
            stmt4 = con.prepareStatement(centerLoader);
            stmt5 = con.prepareStatement(stockInLoader);
            stmt6 = con.prepareStatement(ordersLoader);
            stmt7 = con.prepareStatement(contractLoader);
            stmt8 = con.prepareStatement(updateOrders);
            stmt9 = con.prepareStatement(deleteOrders);
            stmt10 = con.prepareStatement(stockLoader);
        } catch (SQLException e) {
            System.err.println("Insert statement failed");
            System.err.println(e.getMessage());
            closeDB();
            System.exit(1);
        }
    }
    /**
     * 关闭数据库的方法
     */
    private static void closeDB() {
        if (con != null) {
            try {
                if (stmt1 != null) {
                    stmt1.close();
                }
                if (stmt2 != null) {
                    stmt2.close();
                }
                if (stmt3 != null) {
                    stmt3.close();
                }
                if (stmt4 != null) {
                    stmt4.close();
                }
                if (stmt5 != null) {
                    stmt5.close();
                }
                if (stmt6 != null) {
                    stmt6.close();
                }
                if (stmt7 != null) {
                    stmt7.close();
                }
                if (stmt8 != null) {
                    stmt8.close();
                }
                if (stmt9 != null) {
                    stmt9.close();
                }
                if (stmt10 != null) {
                    stmt10.close();
                }
                con.close();
                con = null;
            } catch (Exception e) {
                // Forget about it
            }
        }
    }
    /**
     * 下列LoadData系列方法在后续tryCatch语句中引用，作用是用文件传入的数据填充进preparedStatement里，
     * 然后将Statement加入batch中准备执行
     */

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

    private static void stockInLoadData(int id, String supply_center,String product_model,
                                        int supply_staff, String date, int purchase_price,
                                        int quantity)
            throws SQLException {
        if (con != null) {
            stmt5.setInt(1, id);
            stmt5.setString(2, supply_center);
            stmt5.setString(3, product_model);
            stmt5.setInt(4, supply_staff);
            stmt5.setString(5, date);
            stmt5.setInt(6, purchase_price);
            stmt5.setInt(7, quantity);
            stmt5.addBatch();
        }
    }

    private static void ordersLoadData(String contract,String product_model,int quantity,
                                       int salesman_number,String estimated_date,
                                       String lodgement_date,int seq)
            throws SQLException {
        if (con != null) {
            stmt6.setString(1, contract);
            stmt6.setString(2, product_model);
            stmt6.setInt(3, quantity);
            stmt6.setInt(4, salesman_number);
            stmt6.setString(5, estimated_date);
            stmt6.setString(6, lodgement_date);
            stmt6.setInt(7, seq);
            stmt6.addBatch();
        }
    }

    private static void contractLoadData(String contract,String enterprise,int contract_manager,
                                         String contract_date,String contract_type)
            throws SQLException {
        if (con != null) {
            stmt7.setString(1, contract);
            stmt7.setString(2, enterprise);
            stmt7.setInt(3, contract_manager);
            stmt7.setString(4, contract_date);
            stmt7.setString(5, contract_type);
            stmt7.addBatch();
        }
    }

    private static void setUpdateOrders(String contract,String product_model,int quantity,
                                        int salesman_number,String estimate_delivery_date,String lodgement_date)
            throws SQLException {
        if (con != null) {
            stmt8.setString(1, contract);
            stmt8.setString(2, product_model);
            stmt8.setInt(3, quantity);
            stmt8.setInt(4, salesman_number);
            stmt8.setString(5, estimate_delivery_date);
            stmt8.setString(6, lodgement_date);
            stmt8.setString(7, contract);
            stmt8.setString(8, product_model);
            stmt8.setInt(9, salesman_number);
            stmt8.addBatch();
        }
    }
    private static void setDeleteOrders(String contract,int salesman,int seq)
            throws SQLException {
        if (con != null) {
            stmt9.setString(1, contract);
            stmt9.setInt(2, salesman);
            stmt9.setInt(3, seq);
            stmt9.addBatch();
        }
    }

    private static void stockLoadData(int id,String supply_center,String product_model, int quantity)
            throws SQLException {
        if (con != null) {
            stmt10.setInt(1, id);
            stmt10.setString(2, supply_center);
            stmt10.setString(3, product_model);
            stmt10.setInt(4, quantity);
            stmt10.addBatch();
        }
    }

    /**
     * 下列静态方法用于筛选非法数据，在DatabaseManipulation中有同款，图方便在这个类里ctrlcv了静态版本
     */
    public static String findCenterByStaff(int number) {
        StringBuilder sb = new StringBuilder();
        String sql = "select supply_center from staff where number = ?;";
        try {
            PreparedStatement preparedStatement = con.prepareStatement(sql);
            preparedStatement.setInt(1,number);
            resultSet = preparedStatement.executeQuery();
            while(resultSet.next()){
                sb.append(String.format(resultSet.getString("supply_center")));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }

    public static String findTypeByStaff(int number) {
        StringBuilder sb = new StringBuilder();
        String sql = "select type from staff where number = ?;";
        try {
            PreparedStatement preparedStatement = con.prepareStatement(sql);
            preparedStatement.setInt(1,number);
            resultSet = preparedStatement.executeQuery();
            while(resultSet.next()){
                sb.append(String.format(resultSet.getString("type")));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }

    public static int findStockInQuantity(String model, String enterprise) {
        int num = 0;
        String sql = "select quantity from stockIn s " +
                "join enterprise e on s.supply_center = e.supply_center " +
                "where s.model = ? and e.name = ? ;";
        try {
            PreparedStatement preparedStatement = con.prepareStatement(sql);
            preparedStatement.setString(1,model);
            preparedStatement.setString(2,enterprise);
            resultSet = preparedStatement.executeQuery();
            while(resultSet.next()){
                num = Integer.parseInt((String.format(resultSet.getString("quantity"))));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return num;
    }

    public static int findStockInQuantity2(String contract, String model) {
        int num = 0;
        String sql = "select quantity from stockIn s\n" +
                "    join enterprise e on s.supply_center = e.supply_center\n" +
                "    join contract c on c.enterprise = e.name\n" +
                "where c.contract = ? and s.model = ? ;";
        try {
            PreparedStatement preparedStatement = con.prepareStatement(sql);
            preparedStatement.setString(1,contract);
            preparedStatement.setString(2,model);
            resultSet = preparedStatement.executeQuery();
            while(resultSet.next()){
                num = Integer.parseInt((String.format(resultSet.getString("quantity"))));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return num;
    }

    public static int findStockQuantity(String center, String model) {
        int num = 0;
        String sql = "select quantity from stock  " +
                "where supply_center = ? and model = ? ;";
        try {
            PreparedStatement preparedStatement = con.prepareStatement(sql);
            preparedStatement.setString(1,center);
            preparedStatement.setString(2,model);
            resultSet = preparedStatement.executeQuery();
            while(resultSet.next()){
                num = Integer.parseInt((String.format(resultSet.getString("quantity"))));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return num;
    }

    public static int findOrderSalesman(String contract, String model,int salesman){
        int num = 0;
        String sql = "select salesman_number from orders " +
                "where contract = ? and product_model = ? and salesman_number = ?;";
        try {
            PreparedStatement preparedStatement = con.prepareStatement(sql);
            preparedStatement.setString(1,contract);
            preparedStatement.setString(2,model);
            preparedStatement.setInt(3,salesman);
            resultSet = preparedStatement.executeQuery();
            while(resultSet.next()){
                num = Integer.parseInt((String.format(resultSet.getString("salesman_number"))));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return num;
    }

    public static String findEnterpriseCenter(String enterprise){
        String sb ="";
        String sql = "select supply_center from enterprise " +
                "where name = ? ;";
        try {
            PreparedStatement preparedStatement = con.prepareStatement(sql);
            preparedStatement.setString(1,enterprise);
            resultSet = preparedStatement.executeQuery();
            while(resultSet.next()){
                sb = (String.format(resultSet.getString("salesman_number")));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return sb;
    }

    public static String findCenterByContract(String contract){
        String sb ="";
        String sql = "select supply_center from enterprise e " +
                "join contract c on e.name = c.enterprise " +
                "where c.contract = ? ;";
        try {
            PreparedStatement preparedStatement = con.prepareStatement(sql);
            preparedStatement.setString(1,contract);
            resultSet = preparedStatement.executeQuery();
            while(resultSet.next()){
                sb = (String.format(resultSet.getString("supply_center")));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return sb;
    }

    public static String findOrderModelByContractSalesman(String contract,int salesman){
        String sb ="";
        String sql = "select product_model from orders " +
                "where contract = ? and salesman_number = ?";
        try {
            PreparedStatement preparedStatement = con.prepareStatement(sql);
            preparedStatement.setString(1,contract);
            preparedStatement.setInt(2,salesman);
            resultSet = preparedStatement.executeQuery();
            while(resultSet.next()){
                sb = (String.format(resultSet.getString("product_model")));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return sb;
    }

    public static int findOrderQuantity(String contract, String model){
        int num = 0;
        String sql = "select quantity from orders " +
                "where contract = ? and product_model = ? ;";
        try {
            PreparedStatement preparedStatement = con.prepareStatement(sql);
            preparedStatement.setString(1,contract);
            preparedStatement.setString(2,model);
            resultSet = preparedStatement.executeQuery();
            while(resultSet.next()){
                num = Integer.parseInt((String.format(resultSet.getString("quantity"))));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return num;
    }

    public static List findOrderSalesmanByContract(String contract){
        List num = new ArrayList();
        String sql = "select salesman_number from orders " +
                "where contract = ?;";
        try {
            PreparedStatement preparedStatement = con.prepareStatement(sql);
            preparedStatement.setString(1,contract);
            resultSet = preparedStatement.executeQuery();
            while(resultSet.next()){
                num.add(Integer.parseInt((String.format(resultSet.getString("salesman_number")))));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return num;
    }


    public static void updateStock(String center, String model,int Ivalue, int Dvalue){
        String sql = "update stock set quantity = " + (Ivalue+Dvalue) +
                "where supply_center = ? and model = ?";
        try {
            PreparedStatement preparedStatement = con.prepareStatement(sql);
            preparedStatement.setString(1,center);
            preparedStatement.setString(2,model);
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void deleteOrder (String contract,String product_model,int salesman_number){
        String sql = "delete from orders where " +
                "contract = ? and product_model = ? and salesman_number = ?";
        try {
            PreparedStatement preparedStatement = con.prepareStatement(sql);
            preparedStatement.setString(1,contract);
            preparedStatement.setString(2,product_model);
            preparedStatement.setInt(3,salesman_number);
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public static String findAllCenter() {
        StringBuilder sb = new StringBuilder();
        String sql = "select name from center";
        try {
            Statement statement = con.createStatement();
            resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                sb.append(resultSet.getString("name")).append(",");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }

    public static String findAllModel() {
        StringBuilder sb = new StringBuilder();
        String sql = "select model from model";
        try {
            Statement statement = con.createStatement();
            resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                sb.append(resultSet.getString("model")).append(",");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }

    public static String findAllStaff() {
        StringBuilder sb = new StringBuilder();
        String sql = "select number from Staff";
        try {
            Statement statement = con.createStatement();
            resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                sb.append(resultSet.getString("number")).append(",");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }

    public static void main(String[] args) throws SQLException {
        /**
         * 在argument里里读取要读的文件的filename，已弃用
         */
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
        /**
         * 填入账号密码
         */
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

        // Empty target table
//        openDB(prop.getProperty("host"), prop.getProperty("database"),
//                prop.getProperty("user"), prop.getProperty("password"));
//        Statement stmt0;
//        if (con != null) {
//            stmt0 = con.createStatement();
//            stmt0.execute("truncate table enterprise");
//            stmt0.execute("truncate table center");
//            stmt0.execute("truncate table staff");
//            stmt0.execute("truncate table model");
//            stmt0.close();
//        }
//        closeDB();
        /**
         * staff表导入数据
         */
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

            // Fill target table
            start = System.currentTimeMillis();
            openDB(prop.getProperty("host"), prop.getProperty("database"),
                    prop.getProperty("user"), prop.getProperty("password"));
            while ((line = infile.readLine()) != null) {
                parts = line.replace("Hong Kong, Macao and Taiwan regions of China","Hong Kong Macao and Taiwan regions of China").split(",");
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
    /**
     * model表导入数据
     */
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
    /**
     * enterprise表导入数据
     */
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
                parts = line.replace("Hong Kong, Macao and Taiwan regions of China","Hong Kong Macao and Taiwan regions of China").split(",");
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
    /**
    * center表导入数据
    */
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
    /**
     * stockIn表导入数据,包括stockIn和stock
     */
        try (BufferedReader infile
                     = new BufferedReader(new FileReader("task1_in_stoke_test_data_publish.csv"))) {
            long     start;
            long     end;
            String   line;
            String[] parts;
            int      id;
            String   supply_center;
            String   product_model;
            int      supply_staff;
            String   date;
            int      purchase_price;
            int      quantity;
            HashSet centerModel = new HashSet();
            StringBuilder sb = new StringBuilder();
            int      cnt = 0;

            // Fill target table
            start = System.currentTimeMillis();
            openDB(prop.getProperty("host"), prop.getProperty("database"),
                    prop.getProperty("user"), prop.getProperty("password"));

            String   findAllCenter = findAllCenter();
            String   findAllModel = findAllModel();
            String   findAllStaff = findAllStaff();

            while ((line = infile.readLine()) != null) {
                parts = line.replace("Hong Kong, Macao and Taiwan regions of China","Hong Kong Macao and Taiwan regions of China").split(",");
                if (parts.length > 1) {
                    if (findAllCenter.contains(parts[1]) &&

                            parts[1].equals(findCenterByStaff(Integer.parseInt(parts[3]))) &&

                            "Supply Staff".equals(findTypeByStaff(Integer.parseInt(parts[3]))) &&

                            findAllModel.contains(parts[2]) &&

                            findAllStaff.contains(parts[3])) {

                            id = Integer.parseInt(parts[0]);
                            supply_center = parts[1];
                            product_model = parts[2];
                            supply_staff = Integer.parseInt(parts[3]);
                            date = parts[4];
                            purchase_price = Integer.parseInt(parts[5]);
                            quantity = Integer.parseInt(parts[6]);

                            stockInLoadData(id,supply_center,product_model,supply_staff,date,purchase_price,quantity);
                            cnt++;

                            sb.delete(0,sb.length());
                            sb.append(supply_center).append(product_model);

                            if(centerModel.contains(sb.toString())){
                                updateStock(supply_center,
                                        product_model,
                                        findStockQuantity(supply_center,product_model),
                                        quantity
                                        );
                            } else {
                                stockLoadData(id,supply_center,product_model,quantity);
                                centerModel.add(sb.toString());
                            }
                    }
                    if (cnt % BATCH_SIZE == 0) {
                        stmt5.executeBatch();
                        stmt5.clearBatch();
                        stmt10.executeBatch();
                        stmt10.clearBatch();
                    }

                }
            }
            if (cnt % BATCH_SIZE != 0) {
                stmt5.executeBatch();
                stmt10.executeBatch();
            }
            con.commit();
            stmt5.close();
            stmt10.close();
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
                stmt5.close();
                stmt10.close();
            } catch (Exception e2) {
            }
            closeDB();
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Fatal error: " + e.getMessage());
            try {
                con.rollback();
                stmt5.close();
                stmt10.close();
            } catch (Exception e2) {
            }
            closeDB();
            System.exit(1);
        }
        /**
         * contract表导入数据
         */
        try (BufferedReader infile
                     = new BufferedReader(new FileReader("task2_test_data_publish.csv"))) {
            long     start;
            long     end;
            String   line;
            String[] parts;
            String contract;
            String enterprise;
            int contract_manger;
            String contract_date;
            String contract_type;
            int      cnt = 0;
            HashSet contractSet = new HashSet();

            // Fill target table
            start = System.currentTimeMillis();
            openDB(prop.getProperty("host"), prop.getProperty("database"),
                    prop.getProperty("user"), prop.getProperty("password"));

            while ((line = infile.readLine()) != null) {
                parts = line.replace("Hong Kong, Macao and Taiwan regions of China","Hong Kong Macao and Taiwan regions of China").split(",");
                if (parts.length > 1) {
                    if(!contractSet.contains(parts[0]))
                    {
                        contractSet.add(parts[0]);
                        contract = parts[0];
                        enterprise = parts[1];
                        contract_manger = Integer.parseInt(parts[4]);
                        contract_date = parts[5];
                        contract_type = parts[9];

                        contractLoadData(contract,enterprise,contract_manger,
                                contract_date, contract_type);
                        cnt++;
                    }

                    if (cnt % BATCH_SIZE == 0) {
                        stmt7.executeBatch();
                        stmt7.clearBatch();
                    }

                }
            }
            if (cnt % BATCH_SIZE != 0) {
                stmt7.executeBatch();
            }
            con.commit();
            stmt7.close();
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
                stmt7.close();
            } catch (Exception e2) {
            }
            closeDB();
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Fatal error: " + e.getMessage());
            try {
                con.rollback();
                stmt7.close();
            } catch (Exception e2) {
            }
            closeDB();
            System.exit(1);
        }

        /**
         * orders表导入数据
         */
        try (BufferedReader infile
                     = new BufferedReader(new FileReader("task2_test_data_publish.csv"))) {
            long     start;
            long     end;
            String   line;
            String[] parts;
            String contract;
            String product_model;
            int quantity;
            int salesman_number;
            String estimated_date;
            String lodgement_date;
            String center;
            int seq = 1;
            StringBuilder sb = new StringBuilder();
            HashSet contractSalesman = new HashSet();
            int      cnt = 0;

            // Fill target table
            start = System.currentTimeMillis();
            openDB(prop.getProperty("host"), prop.getProperty("database"),
                    prop.getProperty("user"), prop.getProperty("password"));

            String   findAllStaff = findAllStaff();

            while ((line = infile.readLine()) != null) {
                parts = line.replace("Hong Kong, Macao and Taiwan regions of China","Hong Kong Macao and Taiwan regions of China").split(",");
                if (parts.length > 1) {
                    if ( findAllStaff.contains(parts[8]) &&
                            Integer.parseInt(parts[3]) <= findStockQuantity(findCenterByContract(parts[0]),parts[2])  &&
                            "Salesman".equals(findTypeByStaff(Integer.parseInt(parts[8])))) {

                        contract = parts[0];
                        product_model = parts[2];
                        quantity = Integer.parseInt(parts[3]);
                        salesman_number = Integer.parseInt(parts[8]);
                        estimated_date = parts[6];
                        lodgement_date = parts[7];
                        center = findCenterByContract(contract);

                        sb.delete(0,sb.length());
                        sb.append(contract).append(parts[8]);
                        if (contractSalesman.contains(sb.toString())){
                            seq++;
                        }else{
                            contractSalesman.add(sb.toString());
                        }

                        ordersLoadData(contract,product_model,quantity,salesman_number,estimated_date,
                        lodgement_date,seq);
                        updateStock(center,
                                product_model,
                                findStockQuantity(center,product_model),
                                -quantity);
                        cnt++;
                    }
                    if (cnt % BATCH_SIZE == 0) {
                        stmt6.executeBatch();
                        stmt6.clearBatch();
                    }

                }
            }
            if (cnt % BATCH_SIZE != 0) {
                stmt6.executeBatch();
            }
            con.commit();
            stmt6.close();
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
                stmt6.close();
            } catch (Exception e2) {
            }
            closeDB();
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Fatal error: " + e.getMessage());
            try {
                con.rollback();
                stmt6.close();
            } catch (Exception e2) {
            }
            closeDB();
            System.exit(1);
        }
        /**
         * 部分更新orders表
         */
        try (BufferedReader infile
                     = new BufferedReader(new FileReader("task34_update_test_data_publish.tsv"))) {
            long     start;
            long     end;
            String   line;
            String[] parts;
            String contract;
            String product_model;
            int quantity;
            int salesman_number;
            String estimated_date;
            String lodgement_date;
            String center;
            int      cnt = 0;

            // Fill target table
            start = System.currentTimeMillis();
            openDB(prop.getProperty("host"), prop.getProperty("database"),
                    prop.getProperty("user"), prop.getProperty("password"));

            while ((line = infile.readLine()) != null) {
                parts = line.split("\t");
                if (parts.length > 1) {
                    if (Integer.parseInt(parts[2])==findOrderSalesman(parts[0],parts[1], Integer.parseInt(parts[2]))) {

                        contract = parts[0];
                        product_model = parts[1];
                        quantity = Integer.parseInt(parts[3]);
                        salesman_number = Integer.parseInt(parts[2]);
                        estimated_date = parts[4];
                        lodgement_date = parts[5];
                        center = findCenterByContract(contract);

                        updateStock(findCenterByContract(contract),product_model,
                                findStockQuantity(center,product_model),
                                findOrderQuantity(contract,product_model)-quantity);
                        setUpdateOrders(contract,product_model,quantity,salesman_number,estimated_date,
                                lodgement_date);

                        if(quantity == 0){
                            System.out.println(1);
                            deleteOrder(contract,product_model,salesman_number);
                        }
                        cnt++;
                    }

                    if (cnt % BATCH_SIZE == 0) {
                        stmt8.executeBatch();
                        stmt8.clearBatch();
                    }
                }
            }
            if (cnt % BATCH_SIZE != 0) {
                stmt8.executeBatch();
            }
            con.commit();
            stmt8.close();
            closeDB();
            end = System.currentTimeMillis();
            System.out.println(cnt + " records successfully updated\n");
            System.out.println("Updating speed : "
                    + (cnt * 1000)/(end - start)
                    + " records/s");

        } catch (SQLException se) {
            System.err.println("SQL error: " + se.getMessage());
            try {
                con.rollback();
                stmt8.close();
            } catch (Exception e2) {
            }
            closeDB();
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Fatal error: " + e.getMessage());
            try {
                con.rollback();
                stmt8.close();
            } catch (Exception e2) {
            }
            closeDB();
            System.exit(1);
        }
        /**
         * 部分删除orders表
         */
        try (BufferedReader infile
                     = new BufferedReader(new FileReader("task34_delete_test_data_publish.tsv"))) {
            long     start;
            long     end;
            String   line;
            String[] parts;
            String contract;
            int salesman_number;
            String center;
            String model;
            int seq;
            int      cnt = 0;

            // Fill target table
            start = System.currentTimeMillis();
            openDB(prop.getProperty("host"), prop.getProperty("database"),
                    prop.getProperty("user"), prop.getProperty("password"));

            while ((line = infile.readLine()) != null) {
                parts = line.split("\t");
                if (parts.length > 1) {
                    if (findOrderSalesmanByContract(parts[0]).contains(Integer.parseInt(parts[1]))) {

                        contract = parts[0];
                        salesman_number = Integer.parseInt(parts[1]);
                        seq = Integer.parseInt(parts[2]);
                        center = findCenterByContract(contract);
                        model = findOrderModelByContractSalesman(contract,salesman_number);

                        updateStock(center, model,
                                findStockQuantity(center,model),
                                findOrderQuantity(contract,model));
                        setDeleteOrders(contract,salesman_number,seq);
                        cnt++;
                    }

                    if (cnt % BATCH_SIZE == 0) {
                        stmt9.executeBatch();
                        stmt9.clearBatch();
                    }
                }
            }
            if (cnt % BATCH_SIZE != 0) {
                stmt9.executeBatch();
            }
            con.commit();
            stmt9.close();
            closeDB();
            end = System.currentTimeMillis();
            System.out.println(cnt + " records successfully deleted\n");
            System.out.println("Deleting speed : "
                    + (cnt * 1000)/(end - start)
                    + " records/s");

        } catch (SQLException se) {
            System.err.println("SQL error: " + se.getMessage());
            try {
                con.rollback();
                stmt9.close();
            } catch (Exception e2) {
            }
            closeDB();
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Fatal error: " + e.getMessage());
            try {
                con.rollback();
                stmt9.close();
            } catch (Exception e2) {
            }
            closeDB();
            System.exit(1);
        }

        closeDB();
    }


}


