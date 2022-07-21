

public class Client {

    public static void main(String[] args) {
        try {
            long     start;
            long     end;
            start = System.currentTimeMillis();
            DataManipulation dm = new DataFactory().createDataManipulation(args[0]);
            dm.openDatasource();
//            System.out.println(dm.findCenterByStaff(11311024));
//            System.out.println(dm.findTypeByStaff(11311024));
//            System.out.println(dm.findAllStaff());
            //dm.Delete("contract","name","Tencent");
            //dm.Update("contract","148","Tencent","China","Shenzhen","Southern China","Internet");
            dm.closeDatasource();
            end = System.currentTimeMillis();
            System.out.println(end-start + "ms");
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
        }
    }
}

