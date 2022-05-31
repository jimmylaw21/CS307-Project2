package dao;

import java.util.List;

public interface DataManipulation {

    public void getConnection();
    public void closeConnection();
    public List<String> findStationsByLine(int lineId);

    public String getAllStaffCount();
    public String getContractCount();
    public String getOrderCount();
    public String getNeverSoldProductCount();
    public String getFavoriteProductModel();
    public String getAvgStockByCenter();
    public String getProductByNumber(String model);
    public String getContractInfo(String contract);
    public String conditionalQuery(int id, String contract, String product_model, int quantity, int salesman_number, String estimated_date, String lodgement_date);
    public String stackQuery(String supply_center,
                             String model
    );
    public String purchaseBillQuery(String start_date,
                                    String end_date,
                                    String model,
                                    String supply_center
    );
    public String sellingBillQuery(String start_date,
                                   String end_date,
                                   String model,
                                   String enterprise,
                                   String supply_center
    );
    public void updateContractType();


}
