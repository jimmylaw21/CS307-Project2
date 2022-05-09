import java.util.Properties;

public class testOutput {
    public static void main(String[] args) {
        try {
            OriginalDataLoader dm = new OriginalDataLoader();

            Properties defprop = new Properties();
            defprop.put("host", "localhost");
            defprop.put("user", "postgres");
            defprop.put("password", "Lyw1107Ypa");
            defprop.put("database", "proj_2");
            Properties prop = new Properties(defprop);
            dm.openDB(prop.getProperty("host"), prop.getProperty("database"),
                    prop.getProperty("user"), prop.getProperty("password"));

            System.out.println(dm.getAllStaffCount());  //Q6
            System.out.println(dm.getContractCount());  //Q7
            System.out.println(dm.getOrderCount());  //Q8
            System.out.println(dm.getNeverSoldProductCount());  //Q9
            System.out.println(dm.getFavoriteProductModel());  //Q10


        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
        }
    }
}
