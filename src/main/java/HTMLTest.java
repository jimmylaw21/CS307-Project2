import java.util.HashMap;
import java.util.Map;

import io.javalin.Javalin;
import io.javalin.core.util.FileUtil;
import io.javalin.http.staticfiles.Location;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.Properties;
import java.sql.*;
import java.net.URL;

public class HTMLTest {

    private static final int  BATCH_SIZE = 1;
    private static URL        propertyURL = HTMLTest.class
            .getResource("/loader.cnf");

    private static Connection         con = null;
    private static PreparedStatement  stmt1,stmt2,stmt3,stmt4,stmt5,stmt6,stmt7,stmt8,stmt9,stmt10 = null;
    private static boolean            verbose = false;
    private static ResultSet resultSet;

    private static List<String> contractNumber ;

    static Map<String, String> reservations = new HashMap<String, String>() {{
        put("saturday", "No reservation");
        put("sunday", "No reservation");
    }};


    public static void openDB(String host, String dbname,
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
    }
    public static void closeDB() {
        if (con != null) {
            try {
                con.close();
                con = null;
            } catch (Exception e) {
                // Forget about it
            }
        }
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

    public static void deleteOrder (String contract,String product_model,int salesman_number){
        String sql = "delete from orders where " +
                "contract = ? and product_mode0l = ? and salesman_number = ?";
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

    public static String Query (String query){
        StringBuilder sb = new StringBuilder();
        String sql ="";
        String[] strings = query.split("-");
        switch (strings[0]){
            case "enterprise":
                sql = "select * from enterprise where id = ? ;";
                break;
            case "model":
                sql = "select * from model where id = ? ;";
                break;
            case "center":
                sql = "select * from center where id = ? ;";
                break;
            case "staff":
                sql = "select * from staff where number = ? ;";
                break;
            default:
                sb.append("invalid query");
                return sb.toString();
        }

        try {
            PreparedStatement preparedStatement = con.prepareStatement(sql);
            preparedStatement.setInt(1, Integer.parseInt(strings[1]));
            resultSet = preparedStatement.executeQuery();
            while(resultSet.next()){
                switch (strings[0]){
                    case "enterprise":
                        sb.append(String.format(resultSet.getString("id"))).append("\t")
                                .append(String.format(resultSet.getString("name"))).append("\t")
                                .append(String.format(resultSet.getString("country"))).append("\t")
                                .append(String.format(resultSet.getString("city"))).append("\t")
                                .append(String.format(resultSet.getString("supply_center"))).append("\t")
                                .append(String.format(resultSet.getString("industry")));
                        break;
                    case "model":
                        sb.append(String.format(resultSet.getString("id"))).append("\t")
                                .append(String.format(resultSet.getString("number"))).append("\t")
                                .append(String.format(resultSet.getString("model"))).append("\t")
                                .append(String.format(resultSet.getString("name"))).append("\t")
                                .append(String.format(resultSet.getString("unit_price")));
                        break;
                    case "center":
                        sb.append(String.format(resultSet.getString("id"))).append("\t")
                                .append(String.format(resultSet.getString("name")));
                        break;
                    case "staff":
                        sb.append(String.format(resultSet.getString("id"))).append("\t")
                                .append(String.format(resultSet.getString("name"))).append("\t")
                                .append(String.format(resultSet.getString("age"))).append("\t")
                                .append(String.format(resultSet.getString("gender"))).append("\t")
                                .append(String.format(resultSet.getString("number"))).append("\t")
                                .append(String.format(resultSet.getString("supply_center"))).append("\t")
                                .append(String.format(resultSet.getString("mobile_phone"))).append("\t")
                                .append(String.format(resultSet.getString("type")));
                        break;
                    default:
                        sb.append("invalid query");
                        return sb.toString();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }
    //Q13
    public static String getContractInfo(String contract) {
        StringBuilder sb = new StringBuilder();
        String preSql_1 = "select distinct c.contract,\n" +
                "                c.enterprise,\n" +
                "                s.name,\n" +
                "                e.supply_center\n" +
                "from contract c,\n" +
                "     staff s,\n" +
                "     enterprise e\n" +
                "where c.enterprise = e.name\n" +
                "  and c.contract_manager = s.number\n" +
                "  and c.contract = '%s';";

        String preSql_2 = "select distinct o.product_model,\n" +
                "                s.name,\n" +
                "                o.quantity,\n" +
                "                m.unit_price,\n" +
                "                o.estimated_date,\n" +
                "                o.lodgement_date\n" +
                "from contract c,\n" +
                "     orders o,\n" +
                "     staff s,\n" +
                "     model m\n" +
                "where c.contract = o.contract\n" +
                "  and o.product_model = m.model\n" +
                "  and o.salesman_number = s.number\n" +
                "  and c.contract = '%s';";
        try {
            String sql_1 = String.format(preSql_1, contract);
            String sql_2 = String.format(preSql_2, contract);
            Statement statement1 = con.createStatement();
            Statement statement2 = con.createStatement();
            resultSet = statement1.executeQuery(sql_1);
            while (resultSet.next()) {
                sb.append("contract_number: ").append(resultSet.getString("contract")).append("\n");
                sb.append("enterprise: ").append(resultSet.getString("enterprise")).append("\n");
                sb.append("manager: ").append(resultSet.getString("name")).append("\n");
                sb.append("supply_center: ").append(resultSet.getString("supply_center")).append("\n");
            }
            resultSet = statement2.executeQuery(sql_2);
            while (resultSet.next()) {
                sb.append(resultSet.getString("product_model")).append("\t").append(resultSet.getString("name")).append("\t");
                sb.append(resultSet.getInt("quantity")).append("\t").append(resultSet.getInt("unit_price")).append("\t");
                sb.append(resultSet.getString("estimated_date")).append("\t").append(resultSet.getString("lodgement_date")).append("\n");


            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }
    //Q7
    public String getContractCount() {
        StringBuilder sb = new StringBuilder();
        String sql = "select count(*)\n" +
                "from contract;";
        try {
            Statement statement = con.createStatement();
            resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                sb.append(resultSet.getString("count")).append("\n");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }
    //Q8
    public String getOrderCount() {
        StringBuilder sb = new StringBuilder();
        String sql = "select count(*)\n" +
                "from orders;\n";
        try {
            Statement statement = con.createStatement();
            resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                sb.append(resultSet.getString("count")).append("\n");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }
    //bill
    public static String sellingBillQuery(String start_date,
                                   String end_date,
                                   String model,
                                   String enterprise,
                                   String supply_center
    ) {
        StringBuilder sb1 = new StringBuilder();
        StringBuilder sb2 = new StringBuilder();
        StringBuilder sb3 = new StringBuilder();
        StringBuilder sb4 = new StringBuilder();
        StringBuilder sb5 = new StringBuilder();
        String sDay = new String();
        String eDay = new String();
        String Model = new String();
        String Enterprise = new String();
        String sc = new String();
        StringBuilder sbResult = new StringBuilder();

        if (start_date != null)
            sDay = sb1.append("'").append(start_date).append("'").toString();
        else
            sDay = "0000-00-00";

        if (end_date != null)
            eDay = sb2.append("'").append(end_date).append("'").toString();
        else
            eDay = "'9999-99-99'";

        if (model != null)
            Model = sb3.append("like ").append("'").append(model).append("'").toString();
        else
            Model = "is not null";

        if (enterprise != null)
            Enterprise = sb4.append("like ").append("'").append(enterprise).append("'").toString();
        else
            Enterprise = "is not null";

        if (supply_center != null)
            sc = sb5.append("like ").append("'").append(supply_center).append("'").toString();
        else
            sc = "is not null";

        String sql = "select *\n" +
                "from selling_bill pb\n" +
                "where model %s\n" +
                "and client %s\n" +
                "and supply_center %s\n" +
                "and to_number(pb.date, '9999a99a99') >= to_number(%s, '9999a99a99')\n" +
                "and to_number(pb.date, '9999a99a99') <= to_number(%s, '9999a99a99');";

        try {
            sql = String.format(sql, Model, Enterprise, sc, sDay, eDay);
            Statement preparedStatement = con.createStatement();

            //System.out.println(sql);
            resultSet = preparedStatement.executeQuery(sql);
            String totalPayment = "Total proceeds: ";
            int tPayment = 0;


            while (resultSet.next()) {
                sbResult.append(resultSet.getInt("orderID")).append("\t").
                        append(resultSet.getString("price")).append("\t").
                        append(resultSet.getInt("quantity")).append("\t").
                        append(resultSet.getInt("proceeds")).append("\t").
                        append(resultSet.getString("date")).append("\t").
                        append(resultSet.getString("model")).append("\t").
                        append(resultSet.getString("client")).append("\t").
                        append(resultSet.getString("supply_center")).append("\n");
                tPayment += Integer.parseInt(resultSet.getString("proceeds"));
            }
            sbResult.append(totalPayment).append(tPayment).append("\n");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return sbResult.toString();
    }

    public static void main(String[] args) throws SQLException {

        Properties defprop = new Properties();
        defprop.put("host", "localhost");
        defprop.put("user", "checker");
        defprop.put("password", "123456");
        defprop.put("database", "postgres");
        Properties prop = new Properties(defprop);

        openDB(prop.getProperty("host"), prop.getProperty("database"),
                prop.getProperty("user"), prop.getProperty("password"));

        Javalin app = Javalin.create(config -> {
            config.addStaticFiles("/public", Location.CLASSPATH);
        }).start("10.15.178.88",7777);
        //10.15.178.88

        app.post("/make-reservation", ctx -> {
            reservations.put(ctx.formParam("day"), ctx.formParam("time"));
            ctx.html("Your reservation has been saved");
        });

        app.post("/make-query", ctx -> {
            contractNumber.add(ctx.formParam("contractNumber"));
            ctx.html("Your query has been saved");
        });

        app.get("/check-reservation", ctx -> {
            ctx.html(reservations.get(ctx.queryParam("day")));
        });

        app.get("/check-query", ctx -> {
            ctx.html(findCenterByContract(ctx.queryParam("contractNumber")));
        });

        app.post("/upload-example", ctx -> {
            ctx.uploadedFiles("files").forEach(file -> {
                FileUtil.streamToFile(file.getContent(), "upload/" + file.getFilename());
            });
            ctx.html("Upload complete");
        });

        app.before("/path/*", ctx -> {
            // runs before request to /path/*
        });

        app.get("/send-S&O", ctx -> {
            ctx.result(getContractInfo(ctx.queryParam("Stock&Order")));
        });

        app.get("/send-origin", ctx -> {
            ctx.result(Query(ctx.queryParam("Origin")));
        });

        app.get("/order-delete", ctx -> {
            ctx.result(Query(ctx.queryParam("Origin")));
        });

        app.get("/send-bills", ctx -> {
            String[] strings = ctx.queryParam("Bills").split(",");
            switch (strings.length){
                case 1:
                    ctx.result(sellingBillQuery(strings[0],null,null,null,null));
                    break;
                case 2:
                    ctx.result(sellingBillQuery(strings[0],strings[1],null,null,null));
                    break;
                case 3:
                    ctx.result(sellingBillQuery(strings[0],strings[1],strings[2],null,null));
                    break;
                case 4:
                    ctx.result(sellingBillQuery(strings[0],strings[1],strings[2],strings[3],null));
                    break;
                case 5:
                    ctx.result(sellingBillQuery(strings[0],strings[1],strings[2],strings[3],strings[4]));
                    break;
                default:
                    break;
            }
        });


    }

}
