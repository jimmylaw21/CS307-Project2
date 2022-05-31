import Util.OrdinaryUtil;
import Util.ProxoolUtil;
import dao.DataManipulation;
import dao.DatabaseManipulation;

public class Client {

    public static void main(String[] args) {

        try {
            dbRequestArrived(120);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
        }
    }

    public static void dbRequestArrived(int count) {
        for (int i = 0; i < count; i++) {
            int finalI = i;
            new Thread(() -> {
                DataManipulation dm = new DatabaseManipulation(OrdinaryUtil.getInstance());
                dm.getConnection();
                /**
                 * 更新订单完成情况
                 */
                dm.updateContractType();

                /**
                 * 有条件（可选）筛选订单
                 */
                System.out.println(dm.conditionalQuery(0, null, "NotebookAccessoriesA7", 0, 0, "2022-01-04", null));
                System.out.println(dm.conditionalQuery(0, null, null, 0, 0, "2022-01-05", null));

                /**
                 * 筛选账单
                 */
                System.out.println(dm.purchaseBillQuery("2012-01-01", null, null, "Eastern China"));
                System.out.println(dm.sellingBillQuery("2022-01-01", "2022-02-01", null, null, "Eastern China"));

                /**
                 * 查询库存
                 */
                System.out.println(dm.stackQuery(null, "DatabaseSoftwareO6"));


                /*switch (finalI) {
                    case 1:
                        System.out.println(dm.getAllStaffCount());
                        break;
                    case 2:
                        System.out.println(dm.getContractCount());
                        break;
                    case 3:
                        System.out.println();
                        break;
                    case 4:
                        System.out.println(dm.getOrderCount());  //Q8
                        break;
                    case 5:
                        System.out.println(dm.getNeverSoldProductCount());  //Q9
                        break;
                    case 6:
                        System.out.println(dm.getFavoriteProductModel());  //Q10
                        break;
                    case 7:
                        System.out.println(dm.getAvgStockByCenter());  //Q11
                        break;
                    case 8:
                        System.out.println(dm.getProductByNumber("A50L172"));  //Q12
                        break;
                    case 9:
                        System.out.println(dm.getContractInfo("CSE0000106"));  //Q13-1
                        break;
                    case 10:
                        System.out.println(dm.getContractInfo("CSE0000209"));  //Q13-2
                        break;
                    case 0:
                        System.out.println(dm.getContractInfo("CSE0000306"));  //Q13-3
                        break;
                }*/
                dm.closeConnection();
            }).start();
        }
    }

}

