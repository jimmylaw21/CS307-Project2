package connection_pool.dao;

import connection_pool.Util.DBUtil;
import connection_pool.Util.ProxoolUtil;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DatabaseManipulation implements DataManipulation {
    private Connection con;
    private ResultSet resultSet;
    private PreparedStatement preparedStatement;
    private DBUtil util;

    public DatabaseManipulation(DBUtil util) {
        this.util = util;
    }

    public List<String> findStationsByLine(int lineId) {
        List<String> stations = new ArrayList<>();
        String sql = "select ld.num, s.english_name, s.chinese_name\n" +
                "from line_detail ld\n" +
                "         join stations s on ld.station_id = s.station_id\n" +
                "where ld.line_id = ?" +
                "order by ld.num;";
        try {
            Thread.sleep(2000);
            preparedStatement = con.prepareStatement(sql);
            preparedStatement.setInt(1, lineId);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                stations.add(String.format("%d, %s, %s", resultSet.getInt(1), resultSet.getString(2), resultSet.getString(3)));
            }
        } catch (SQLException | InterruptedException e) {
            e.printStackTrace();
        }
        return stations;
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

            System.out.println(sql);
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

    public String stackQuery(String supply_center,
                                    String model
    ) {
        StringBuilder sb1 = new StringBuilder();
        StringBuilder sb2 = new StringBuilder();
        String Model = new String();
        String sc = new String();
        StringBuilder sbResult = new StringBuilder();

        if (model != null)
            Model = sb1.append("like ").append("'").append(model).append("'").toString();
        else
            Model = "is not null";

        if (supply_center != null)
            sc = sb2.append("like ").append("'").append(supply_center).append("'").toString();
        else
            sc = "is not null";

        String sql = "select supply_center, model, quantity\n" +
                "from stock s\n" +
                "where supply_center %s\n" +
                "and model %s;";
        try {
            sql = String.format(sql, sc, Model);
            Statement preparedStatement = con.createStatement();

            System.out.println(sql);
            resultSet = preparedStatement.executeQuery(sql);


            while (resultSet.next()) {
                sbResult.append(resultSet.getString("model")).append("\t").
                        append(resultSet.getString("supply_center")).append("\t").
                        append(resultSet.getString("quantity")).append("\n");
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

            System.out.println(sql);
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

            System.out.println(sql);
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
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    //Q6
    public String getAllStaffCount() {
        StringBuilder sb = new StringBuilder();
        String sql = "select type, count(*)\n" +
                "from staff\n" +
                "group by type;";
        try {
            Statement statement = con.createStatement();
            resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                sb.append(resultSet.getString("type")).append("\t").append(resultSet.getInt("count")).append("\n");
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

    //Q9
    public String getNeverSoldProductCount() {
        StringBuilder sb = new StringBuilder();
        String sql = "select count(*)\n" +
                "from (select distinct model\n" +
                "      from stockIn\n" +
                "      where stockIn.model not in (select product_model\n" +
                "                                  from orders))as cnt;";
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

    //Q10
    public String getFavoriteProductModel() {
        StringBuilder sb = new StringBuilder();
        String sql = "select model model_name, m quantity\n" +
                "from (select model model, sum s\n" +
                "      from (select sum(quantity) sum, product_model model\n" +
                "            from orders\n" +
                "            group by product_model) as a\n" +
                "      group by model, sum) as b,\n" +
                "     (select max(sum) m\n" +
                "      from (select sum(quantity) sum, product_model model\n" +
                "            from orders\n" +
                "            group by product_model) as m) as maxInt\n" +
                "where s = m\n" +
                "group by model, m;";
        try {
            Statement statement = con.createStatement();
            resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                sb.append(resultSet.getString("model_name")).append("\t").append(resultSet.getInt("quantity")).append("\n");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }

    //Q11
    public String getAvgStockByCenter() {
        StringBuilder sb = new StringBuilder();
        String sql = "select supply_center, round(avg(s), 1)\n" +
                "from (select model, sum(quantity) s, supply_center\n" +
                "      from stock\n" +
                "      group by model, supply_center) as ss\n" +
                "group by supply_center\n" +
                "order by supply_center asc;";
        try {
            Statement statement = con.createStatement();
            resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                sb.append(resultSet.getString("supply_center")).append("\t").append(resultSet.getFloat("round")).append("\n");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }

    //Q12
    public String getProductByNumber(String model) {
        StringBuilder sb = new StringBuilder();
        String preSql = "select model.number, model.model, stock.quantity, stock.supply_center\n" +
                "from model,\n" +
                "     stock\n" +
                "where model.model = stock.model\n" +
                "  and model.number = '%s';";
        try {
            String sql = String.format(preSql, model);
            Statement statement = con.createStatement();
            resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                sb.append(resultSet.getString("model")).append("\t").append(resultSet.getInt("quantity")).append("\t").append(resultSet.getString("supply_center")).append("\n");
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

    public void getConnection() {
        con = this.util.getConnection();
        System.out.println("------Thread " + Thread.currentThread().getId() + " visiting DB!------");
        System.out.println(this.util.getConnectState());

    }

    public void closeConnection() {
        this.util.closeConnection(con, preparedStatement, resultSet);
        System.out.println("------Thread " + Thread.currentThread().getId() + " close DB!------");
    }


}
