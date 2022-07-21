import java.net.URL;
import java.sql.*;
import java.util.Properties;

public class advancedAPI {
    private static Connection con = null;
    private static URL propertyURL = OriginalDataLoader.class.getResource("/loader.cnf");
    private static boolean verbose = true;
    private static ResultSet resultSet;

    public static void main(String[] args) throws SQLException {
        advancedAPI api = new advancedAPI();
        Properties defprop = new Properties();
        defprop.put("host", "localhost");
        defprop.put("user", "checker");
        defprop.put("password", "123456");
        defprop.put("database", "postgres");
        Properties prop = new Properties(defprop);

        api.openDB(prop.getProperty("host"), prop.getProperty("database"),
                prop.getProperty("user"), prop.getProperty("password"));


        /**
         * 更新订单完成情况
         */
        api.updateContractType();

        /**
         * 有条件（可选）筛选订单
         */
        System.out.println(api.conditionalQuery(0, null, "NotebookAccessoriesA7", 0, 0, "2022-01-04", null));
        System.out.println(api.conditionalQuery(0, null, null, 0, 0, "2022-01-05", null));

        /**
         * 筛选账单
         */
        System.out.println(api.purchaseBillQuery("2021-05-01", "2022-05-01", null, "Eastern China"));
        System.out.println(api.sellingBillQuery("2021-05-01", "2022-05-01", null, null, "Southwestern China"));

        //2021-05-01,2022-05-01,NotebookAccessoriesA7,Chengdu AIG,Southwestern China
        api.closeDB();
    }

    public String conditionalQuery(int id,
                                          String contract,
                                          String product_model,
                                          int quantity,
                                          int salesman_number,
                                          String estimated_date,
                                          String lodgement_date) {
        StringBuilder sb1 = new StringBuilder();
        StringBuilder sb2 = new StringBuilder();
        StringBuilder sb3 = new StringBuilder();
        StringBuilder sb4 = new StringBuilder();
        StringBuilder sb5 = new StringBuilder();
        StringBuilder sb6 = new StringBuilder();
        StringBuilder sb7 = new StringBuilder();
        StringBuilder sbResult = new StringBuilder();
        String equal = "= ";
        String ID = new String();
        String Contract = new String();
        String Model = new String();
        String Quantity = new String();
        String Salesman = new String();
        String E_day = new String();
        String L_day = new String();
        if (id != 0)
            ID = String.valueOf(sb1.append(equal).append(id));
        else
            ID = "is not null";

        if (quantity != 0)
            Quantity = String.valueOf(sb2.append(equal).append(quantity));
        else
            Quantity = "is not null";

        if (salesman_number != 0)
            Salesman = String.valueOf(sb3.append(equal).append(salesman_number));
        else
            Salesman = "is not null";

        if (contract != null)
            Contract = sb4.append("like ").append("'").append(contract).append("'").toString();
        else
            Contract = "is not null";

        if (product_model != null) {
            Model = sb5.append("like ").append("'").append(product_model).append("'").toString();
        } else
            Model = "is not null";

        if (estimated_date != null) {
            E_day = sb6.append("like ").append("'").append(estimated_date).append("'").toString();
        } else
            E_day = "is not null";

        if (lodgement_date != null)
            L_day = sb7.append("like ").append("'").append(lodgement_date).append("'").toString();
        else
            L_day = "is not null";

        String sql = "select * from orders\n" +
                "where id %s\n" +
                "and contract %s\n" +
                "and product_model %s\n" +
                "and quantity %s\n" +
                "and salesman_number %s\n" +
                "and estimated_date %s\n" +
                "and lodgement_date %s;\n";

        try {
            sql = String.format(sql, ID, Contract, Model, Quantity, Salesman, E_day, L_day);
            Statement preparedStatement = con.createStatement();

            //System.out.println(sql);
            resultSet = preparedStatement.executeQuery(sql);

            while (resultSet.next()) {
                sbResult.append(resultSet.getInt("id")).append("\t").
                        append(resultSet.getString("contract")).append("\t").
                        append(resultSet.getInt("quantity")).append("\t").
                        append(resultSet.getInt("salesman_number")).append("\t").
                        append(resultSet.getString("estimated_date")).append("\t").
                        append(resultSet.getString("lodgement_date")).append("\t").
                        append(resultSet.getString("product_model")).append("\n");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return sbResult.toString();
    }

    public String purchaseBillQuery(String start_date,
                                           String end_date,
                                           String model,
                                           String supply_center
    ) {
        StringBuilder sb1 = new StringBuilder();
        StringBuilder sb2 = new StringBuilder();
        StringBuilder sb3 = new StringBuilder();
        StringBuilder sb4 = new StringBuilder();
        String sDay = new String();
        String eDay = new String();
        String Model = new String();
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

        if (supply_center != null)
            sc = sb4.append("like ").append("'").append(supply_center).append("'").toString();
        else
            sc = "is not null";

        String sql = "select *\n" +
                "from purchase_bill pb\n" +
                "where model %s\n" +
                "and supply_center %s\n" +
                "and to_number(pb.date, '9999a99a99') >= to_number(%s, '9999a99a99')\n" +
                "and to_number(pb.date, '9999a99a99') <= to_number(%s, '9999a99a99');";

        try {
            sql = String.format(sql, Model, sc, sDay, eDay);
            Statement preparedStatement = con.createStatement();

            //System.out.println(sql);
            resultSet = preparedStatement.executeQuery(sql);
            String totalPayment = "Total payment: ";
            int tPayment = 0;


            while (resultSet.next()) {
                sbResult.append(resultSet.getInt("stockID")).append("\t").
                        append(resultSet.getString("price")).append("\t").
                        append(resultSet.getInt("quantity")).append("\t").
                        append(resultSet.getInt("payment")).append("\t").
                        append(resultSet.getString("date")).append("\t").
                        append(resultSet.getString("model")).append("\t").
                        append(resultSet.getString("supply_center")).append("\n");
                tPayment += Integer.parseInt(resultSet.getString("payment"));
            }
            sbResult.append(totalPayment).append(tPayment).append("\n");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return sbResult.toString();
    }

    public String sellingBillQuery(String start_date,
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

    public void updateContractType() {
        String sql = "update contract v\n" +
                "set contract_type = 'Finished'\n" +
                "where v.contract_type = 'Unfinished'\n" +
                "  and (select max(to_number(lodgement_date, '9999a99a99'))\n" +
                "       from orders o\n" +
                "       where o.contract = v.contract) <\n" +
                "      to_number(to_char(current_date, 'YYYYMMDD'), '99999999');";
        try {
            PreparedStatement preparedStatement = con.prepareStatement(sql);

            preparedStatement.execute();
            con.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //OriginQuery
    public String Query (String query){
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
    public String getContractInfo(String contract) {
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

    public void openDB(String host, String dbname, String user, String pwd) {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (Exception e) {
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
                        + dbname + " as " + user + "\n");
            }
            con.setAutoCommit(false);
        } catch (SQLException e) {
            System.err.println("Database connection failed");
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    public void closeDB() throws SQLException {
        if (con != null) {
            con.close();
            con = null;
        }
    }

}

