import java.util.Properties;

public class testOutput {
    public static void main(String[] args) {
        try {
            OriginalDataLoader dm = new OriginalDataLoader();

            Properties defprop = new Properties();
            defprop.put("host", "localhost");
            defprop.put("user", "checker");
            defprop.put("password", "123456");
            defprop.put("database", "postgres");
            Properties prop = new Properties(defprop);
            dm.openDB(prop.getProperty("host"), prop.getProperty("database"),
                    prop.getProperty("user"), prop.getProperty("password"));

            System.out.println(dm.getAllStaffCount());  //Q6
            System.out.println(dm.getContractCount());  //Q7
            System.out.println(dm.getOrderCount());  //Q8
            System.out.println(dm.getNeverSoldProductCount());  //Q9
            System.out.println(dm.getFavoriteProductModel());  //Q10
            System.out.println(dm.getAvgStockByCenter());  //Q11
            System.out.println(dm.getProductByNumber("A50L172"));  //Q12
            System.out.println(dm.getContractInfo("CSE0000106"));  //Q13
            System.out.println(dm.getContractInfo("CSE0000209"));  //Q13
            System.out.println(dm.getContractInfo("CSE0000306"));  //Q13
            //dm.deleteModel("LaptopC6");
            //dm.updateModel(2,"L8N0649","SA好帅","Laptop",450);
//            System.out.println(dm.Query("model-23"));
            dm.closeDB();
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
        }
    }
}
