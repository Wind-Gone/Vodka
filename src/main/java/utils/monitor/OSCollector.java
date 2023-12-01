package utils.monitor;/*
 * OSCollector.java
 *
 *
 *
 */

import org.apache.log4j.*;

import java.lang.*;
import java.io.*;
import java.util.*;

public class OSCollector {
    private String script;
    private int interval;
    private String sshAddress;
    private String devices;
    private File outputDir;
    private Logger log;

    private Thread collectorThread = null;
    private boolean endCollection = false;
    private Process collProc;

    private BufferedWriter[] resultCSVs;
    private ArrayList<Thread> threadList = new ArrayList<>();

    public OSCollector(String script, int runID, int interval,
                       String sshAddress, String devices, File outputDir,
                       Logger log) throws IOException {


        this.script = script;
        this.interval = interval;
        this.sshAddress = sshAddress;
        this.devices = devices;
        this.outputDir = outputDir;
        this.log = log;

        File sshFileDir;
        String[] sshAddresses = sshAddress.split(",");
        String[] deviceNames = devices.split(",");
        for (String address : sshAddresses) {
            // each server machine owns sshAddress and several csv files
            sshFileDir = new File(outputDir, address);
            if (!sshFileDir.mkdir()) {
                throw new IOException("fail to create directory '" + sshFileDir.getPath() + "'");
            }

            // organize cmd line(need to copy ssh key to each server first)
            ArrayList<String> cmdLine = new ArrayList<>();
            cmdLine.add("ssh");
            cmdLine.add(address);
            cmdLine.add("python");
            cmdLine.add(script);
            BufferedWriter[] resultCSVs = new BufferedWriter[deviceNames.length + 1];
            resultCSVs[0] = new BufferedWriter(new FileWriter(new File(sshFileDir, "sys_info.csv")));
            for (int i = 0; i < deviceNames.length; i++) {
                cmdLine.add(deviceNames[i]);
                resultCSVs[i + 1] = new BufferedWriter(new FileWriter(new File(sshFileDir, deviceNames[i] + ".csv")));
            }
            threadList.add(new Thread(new CmdRun(script, cmdLine, resultCSVs)));
        }

    }

    // client send start signal to oscollector
    public void start() {
        for (Thread thread : threadList) {
            thread.start();
        }
    }

    // client send end signal to oscollector
    public void stop() {
        endCollection = true;
        try {
            for (Thread thread : threadList) {
                thread.join();
            }
        } catch (InterruptedException ie) {
            log.error("get interruption while waiting for os collector workers finish " + ie.getMessage());
        }
    }

    private class CmdRun implements Runnable {
        private Process collProc;
        private String script;
        private ArrayList<String> cmdLine;
        private BufferedWriter[] resultCSVs;

        public CmdRun(String script, ArrayList<String> cmdLine, BufferedWriter[] resultCSVs) {
            this.script = script;
            this.cmdLine = cmdLine;
            this.resultCSVs = resultCSVs;
        }

        public void run() {
            // generate the system information from the server, start a process to avoid the
            // impact on result recording

            try {
                ProcessBuilder pb = new ProcessBuilder(cmdLine);
                pb.redirectError(ProcessBuilder.Redirect.INHERIT);
                collProc = pb.start();
                // send script file to ssh standard input
                BufferedReader scriptReader = new BufferedReader(new FileReader(script));
                BufferedWriter scriptWriter = new BufferedWriter(new OutputStreamWriter(collProc.getOutputStream()));
                String line;
                while ((line = scriptReader.readLine()) != null) {
                    scriptWriter.write(line);
                    scriptWriter.newLine();
                }
                scriptWriter.close();
                scriptReader.close();
            } catch (IOException ioe) {
                log.error("fail to start helper process, " + ioe.getMessage() + ", thread terminated");
                throw new RuntimeException("fail to start helper process");
            }

            // record the system information according to ssh standard output
            BufferedReader osData;
            String line;
            int resultIdx = 0;
            osData = new BufferedReader(new InputStreamReader(collProc.getInputStream()));

            // resultIdx not equal zero to ensure integrate results
            while (!endCollection || resultIdx != 0) {
                try {
                    line = osData.readLine();
                    if (line == null) {
                        log.error("unexpected EOF while reading from external helper process");
                        break;
                    }
                    resultCSVs[resultIdx].write(line);
                    resultCSVs[resultIdx].newLine();
                    resultCSVs[resultIdx].flush();
                    if (++resultIdx >= resultCSVs.length) {
                        resultIdx = 0;
                    }
                } catch (IOException ioe) {
                    log.error("fail to record data from helper process, " + ioe.getMessage());
                    break;
                }
            }

            try {
                osData.close();
                for (BufferedWriter resultCSV : resultCSVs) {
                    resultCSV.close();
                }
            } catch (IOException ioe) {
                log.error("fail to close file reader or writer, " + ioe.getMessage());
            }
        }
    }
}


