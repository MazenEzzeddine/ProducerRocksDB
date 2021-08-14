package org.hps;
import java.io.*;
import java.net.URISyntaxException;;
import java.util.ArrayList;
import java.util.List;

public class Workload {
    // private static final Logger log = LogManager.getLogger(KafkaProducerConfig.class);
    private static String line = "";
    private static String cvsSplitBy = ",";

    private static double inputXPointValue;
    private static double targetXPointValue;

    public ArrayList<Double> getDatay() {
        return datay;
    }
    public void setDatay(ArrayList<Double> datay) {
        this.datay = datay;
    }
    private static ArrayList<Double> datay = new ArrayList<Double>();
    public ArrayList<Double> getDatax() {
        return datax;
    }
    public void setDatax(ArrayList<Double> datax) {
        this.datax = datax;
    }
    private static ArrayList<Double> datax = new ArrayList<Double>();
    public Workload() throws IOException, URISyntaxException {
        this.loadWorkload();
    }
    private void loadWorkload() throws IOException, URISyntaxException {
        ClassLoader CLDR = this.getClass().getClassLoader();
        InputStream inputStream = CLDR.getResourceAsStream("defaultArrivalRatesm.csv");
        List<String> out = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = reader.readLine()) != null) {
                out.add(line);
            }
        }
        System.out.println(out.toString());
        for (String line : out) {
            String[] workFields = line.split(cvsSplitBy);
            inputXPointValue = Double.parseDouble(workFields[0]);
            targetXPointValue = Double.parseDouble(workFields[1]);
            datax.add(inputXPointValue);
            datay.add(targetXPointValue);
            System.out.println(inputXPointValue);
            System.out.println(targetXPointValue);
        }
    }
}



