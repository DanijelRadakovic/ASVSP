import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.hash.Hash;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

public class ProcessWebStatus {

        public static class WebLogWritable implements Writable {

        private Text ipAddress, date, time, zone, cik, accession, code;
        private DoubleWritable size;

        public WebLogWritable() {
            this.ipAddress = new Text();
            this.date = new Text();
            this.time = new Text();
            this.zone = new Text();
            this.cik = new Text();
            this.accession = new Text();
            this.code = new Text();
            this.size = new DoubleWritable();
        }


        public static Optional<WebLogWritable> parseLine(String line, String delimiter) {
            WebLogWritable log = new WebLogWritable();
            StringTokenizer s = new StringTokenizer(line, delimiter);
            String ip = s.nextToken();

            try {
                if (!ip.startsWith("ip")) { // skip header line
                    log.setIpAddress(new Text(ip));
                    log.setDate(new Text(s.nextToken()));
                    log.setTime(new Text(s.nextToken()));
                    log.setZone(new Text(s.nextToken()));
                    log.setCik(new Text(s.nextToken()));
                    log.setAccession(new Text(s.nextToken()));
                    s.nextToken(); // skip extension column
                    log.setCode(new Text(s.nextToken()));
                    log.setSize(new DoubleWritable(Double.parseDouble(s.nextToken())));
                    return Optional.of(log);
                }
                return Optional.empty();
            } catch (Exception e) {
                return Optional.empty();
            }
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            ipAddress.write(dataOutput);
            date.write(dataOutput);
            time.write(dataOutput);
            zone.write(dataOutput);
            cik.write(dataOutput);
            accession.write(dataOutput);
            code.write(dataOutput);
            size.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            ipAddress.readFields(dataInput);
            date.readFields(dataInput);
            time.readFields(dataInput);
            zone.readFields(dataInput);
            cik.readFields(dataInput);
            accession.readFields(dataInput);
            code.readFields(dataInput);
            size.readFields(dataInput);
        }

        @Override
        public int hashCode() {
            final int PRIME = 59;
            int result = 1;
            result = (result * PRIME) + super.hashCode();
            result = (result * PRIME) + (ipAddress == null ? 43 : ipAddress.hashCode());
            result = (result * PRIME) + (date == null ? 43 : date.hashCode());
            result = (result * PRIME) + (time == null ? 43 : time.hashCode());
            result = (result * PRIME) + (zone == null ? 43 : zone.hashCode());
            result = (result * PRIME) + (cik == null ? 43 : cik.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (!(obj instanceof WebLogWritable)) return false;
            WebLogWritable other = (WebLogWritable) obj;
            if (!other.canEqual(this)) return false;
            if (!Objects.equals(this.ipAddress, other.ipAddress)) return false;
            if (!Objects.equals(this.date, other.date)) return false;
            if (!Objects.equals(this.time, other.time)) return false;
            if (!Objects.equals(this.zone, other.zone)) return false;
            if (!Objects.equals(this.cik, other.cik)) return false;
            if (!Objects.equals(this.code, other.code)) return false;
            return Objects.equals(this.size, other.size);
        }

        protected boolean canEqual(Object other) {
            return other instanceof WebLogWritable;
        }

        public Text getIpAddress() {
            return ipAddress;
        }

        public void setIpAddress(Text ipAddress) {
            this.ipAddress = ipAddress;
        }

        public Text getDate() {
            return date;
        }

        public void setDate(Text date) {
            this.date = date;
        }

        public Text getTime() {
            return time;
        }

        public void setTime(Text time) {
            this.time = time;
        }

        public Text getZone() {
            return zone;
        }

        public void setZone(Text zone) {
            this.zone = zone;
        }

        public Text getCik() {
            return cik;
        }

        public void setCik(Text cik) {
            this.cik = cik;
        }

        public Text getAccession() {
            return accession;
        }

        public void setAccession(Text accession) {
            this.accession = accession;
        }

        public Text getCode() {
            return code;
        }

        public void setCode(Text code) {
            this.code = code;
        }

        public DoubleWritable getSize() {
            return size;
        }

        public void setSize(DoubleWritable size) {
            this.size = size;
        }
    }

    public static class WebLogStatusReportWritable implements Writable {

        private LongWritable hits, visitors;
        private DoubleWritable txAmount;
        private Text status;
        private static final DecimalFormat df = new DecimalFormat("###.##");

        public WebLogStatusReportWritable() {
            this.hits = new LongWritable();
            this.visitors = new LongWritable();
            this.txAmount = new DoubleWritable();
            this.status = new Text();
        }


        @Override
        public void write(DataOutput dataOutput) throws IOException {
            hits.write(dataOutput);
            visitors.write(dataOutput);
            txAmount.write(dataOutput);
            status.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            hits.readFields(dataInput);
            visitors.readFields(dataInput);
            txAmount.readFields(dataInput);
            status.readFields(dataInput);
        }

        @Override
        public int hashCode() {
            final int PRIME = 59;
            int result = 1;
            result = (result * PRIME) + super.hashCode();
            result = (result * PRIME) + (status == null ? 43 : status.hashCode());
            result = (result * PRIME) + (hits == null ? 43 : hits.hashCode());
            result = (result * PRIME) + (visitors == null ? 43 : visitors.hashCode());
            result = (result * PRIME) + (txAmount == null ? 43 : txAmount.hashCode());
            return result;
        }

       @Override
        public String toString() {
            double txAmountInMB = txAmount.get() / (1024 * 1024);
            return "hits=" + hits +
                    ",visitors=" + visitors +
                    ",txAmount=" + df.format(txAmountInMB) + // show in MB unit
                    ",txAverageAmount=" + df.format(txAmountInMB / hits.get());
        }

        public void set(Text status, LongWritable hits, LongWritable visitors, DoubleWritable txAmount) {
            this.status = status;
            this.hits = hits;
            this.visitors = visitors;
            this.txAmount = txAmount;
        }

        public LongWritable getHits() {
            return hits;
        }

        public void setHits(LongWritable hits) {
            this.hits = hits;
        }

        public LongWritable getVisitors() {
            return visitors;
        }

        public void setVisitors(LongWritable visitors) {
            this.visitors = visitors;
        }

        public DoubleWritable getTxAmount() {
            return txAmount;
        }

        public void setTxAmount(DoubleWritable txAmount) {
            this.txAmount = txAmount;
        }

        public Text getStatus() {
            return status;
        }

        public void setStatus(Text status) {
            this.status = status;
        }
    }

    public static class E_EMap extends MapReduceBase implements Mapper<LongWritable, Text, Text,
            WebLogWritable> {

        private Text status = new Text();

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, WebLogWritable> output,
                        Reporter reporter) throws IOException {
            WebLogWritable.parseLine(value.toString(), ",").ifPresent(webLogWritable -> {
                String code = webLogWritable.code.toString();
                try {
                    if (code.startsWith("2")) {
                        this.status.set("2xx Success");
                        output.collect(this.status, webLogWritable);
                    } else if (code.startsWith("3")) {
                        this.status.set("3xx Redirection");
                        output.collect(this.status, webLogWritable);
                    } else if (code.startsWith("4")) {
                        this.status.set("4xx Client errors");
                        output.collect(this.status, webLogWritable);
                    } else if (code.startsWith("5")) {
                        this.status.set("5xx Server errors");
                        output.collect(this.status, webLogWritable);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

        }
    }


    public static class E_EReduce extends MapReduceBase implements Reducer<Text, WebLogWritable,
            Text, WebLogStatusReportWritable> {

        private WebLogStatusReportWritable aggregateReport = new WebLogStatusReportWritable();
        private LongWritable hits = new LongWritable();
        private LongWritable visitors = new LongWritable();
        private DoubleWritable txAmount = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterator<WebLogWritable> values, OutputCollector<Text,
                WebLogStatusReportWritable> output, Reporter reporter) throws IOException {
            long hits = 0;
            double txAmount = 0;
            HashSet<WebLogWritable> visitors = new HashSet<>();
            WebLogWritable log;

            while (values.hasNext()) {
                log = values.next();
                hits++;
                visitors.add(log);
                txAmount += log.getSize().get();
            }

            this.hits.set(hits);
            this.visitors.set(visitors.size());
            this.txAmount.set(txAmount);
            aggregateReport.set(key, this.hits, this.visitors, this.txAmount);

            output.collect(key, aggregateReport);
        }
    }


    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(ProcessWebStatus.class);

        conf.setJobName("web_status_report");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(WebLogWritable.class);
        conf.setMapperClass(E_EMap.class);
        conf.setReducerClass(E_EReduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[1]));
        FileOutputFormat.setOutputPath(conf, new Path(args[2]));

        JobClient.runJob(conf);
    }
}
