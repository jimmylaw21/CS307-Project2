

import java.io.IOException;

public interface DataManipulation {

    public void openDatasource();
    public void closeDatasource();
    public int addOneMovie(String str);
    public String allContinentNames();
    public String continentsWithCountryCount();
    public String FullInformationOfMoviesRuntime(int min, int max);
    public String findMovieById(int id);

    public int addOneColumn(String contract_number,String client_enterprise,String supply_center,
                            String country, String city, String industry,String product_code,
                            String product_name, String product_model, int unit_price,int quantity,
                            String contract_date,String estimated_delivery_date,String lodgement_date,
                            String director, String salesman,int salesman_number,
                            int age,String gender,String mobile_phone);
    public void addManyColumn(int amount);

    public String findSalesmanBySalesmanNumber(int salesman_number);

    public String findAllSalesman();

    public String findAllSupplyCenter();

    public String findAllProductModel();

    public String findAllClientEnterprise();

    public String clientEnterpriseWithSupplyCenter();

    public String salesmanWithSupplyCenter();

    public String contractWithSupplyCenter();

    public int addOneSalesman(String str);

    public int addManySalesman(String str,int num1, int num2);

    public int addManyOrder(String str,int num1, int num2);

    public int addOneSupplyCenter(String str);

    public int addOneProductModel(String str);

    public int addOneOrder(String str);

    public String deleteSalesmanByNumber(int number) throws IOException;

    public String deleteOrderBySalesmanNumber(int number) throws IOException;

    public String deleteManySalesmenByNumber(int number,int num1,int num2);

    public int updateSalesmenSupplyCenter(String supply_center, int number) throws IOException;

    public String salesmanWithOrderCount();

    public String orderWithSalesman(int num1,int num2,int num3,int num4,int num5,int num6,int num7,int num8,int num9,int num10);





    public String findCenterByStaff(int number);

    public String findTypeByStaff(int number);

    public String findAllCenter();

    public String findAllModel();

    public String findAllStaff();

    public int findStockInQuantity(String model,String center);

    public void deleteCenter (String contract,String product_model,int salesman_number);

    public void deleteEnterprise (String contract,String product_model,int salesman_number);

    public void deleteModel (String contract,String product_model,int salesman_number);

    public void deleteStaff (String contract,String product_model,int salesman_number);

    public void Delete (String table,String condition,String value);

    public void Insert (String table,String ... values);

    public void Update (String table,String ... values);

}
