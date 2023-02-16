package org.apache.flink.util.event_gens;

import org.apache.flink.util.io.JsonDataRead;
import org.apache.flink.util.listener.DataReadListener;

import java.io.IOException;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class DataTimerEventGen {

    // num of threads to read data
    private final int numThreads = 1;

    private DataReadListener dataReadListener;

    // some parameters to control the data input
    private double scalingFactor; // num of reading data
    private int inputRate;
    private int period = 1000;
    private int batchSize;

    public DataTimerEventGen(double scalingFactor, int inputRate) {
        this.dataReadListener = null;
        this.scalingFactor = scalingFactor;
        this.inputRate = inputRate;
        this.batchSize = this.inputRate / (1000 / period);
        // System.out.println("############# batch size : " + batchSize);
    }

    public void setDataReadListener(DataReadListener listener) {
        this.dataReadListener = listener;
    }

    public void launch(String csvFileName) {
        launch(csvFileName, "SENML");
    }

    public void launch(String csvFileName, String dataSetType) {
        // data lists from different reading threads
        try {
            List<List<List<String>>> dataReadList = JsonDataRead.roundRobinSplitJsonToMemory(csvFileName, numThreads, scalingFactor);
            for (int i = 0; i < numThreads; i++) {
                Timer timer = new Timer("dataEventGen", true);
                int tsIndex = 0;
                if (dataSetType.equals("TRAIN")){
                    tsIndex = 1;
                }
                TimerTask task = new EventGenTimerTask(dataReadList.get(i), tsIndex);
                TimerTask task2 = new WorkKillerTimerTask(dataReadList.get(i), tsIndex);
                timer.scheduleAtFixedRate(task, 0, 100);
                // timer.schedule(task2, 20*1000);
                // timer.scheduleAtFixedRate(task2, 0, period);
                // System.out.println("+++++++++++++++");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    class WorkKillerTimerTask extends TimerTask {

        private List<List<String>> dataRows;

        private int rowLen;
        private int rowIndex;
        private int tsIndex;

        WorkKillerTimerTask(List<List<String>> dataRows, int tsIndex) {
            this.dataRows = dataRows;
            this.rowLen = dataRows.size();
            this.rowIndex = 0;
            this.tsIndex = tsIndex;
        }

        @Override
        public void run() {
            if (rowIndex == rowLen) {
                rowIndex = 0;
            }
            List<String> data = dataRows.get(rowIndex);
            data.set(tsIndex, "kill");
            dataReadListener.receive(data);
            rowIndex++;
            // for (int i = 1; i < batchSize; i++) {
            //     if (rowIndex == rowLen) {
            //         rowIndex = 0;
            //     }
            //     data = dataRows.get(rowIndex);
            //     data.set(tsIndex, String.valueOf(Instant.now().toEpochMilli()));
            //     dataReadListener.receive(data);
            //     rowIndex++;
            // }
        }
    }

    class EventGenTimerTask extends TimerTask {

        private List<List<String>> dataRows;

        private int rowLen;
        private int rowIndex;
        private int tsIndex;

        EventGenTimerTask(List<List<String>> dataRows, int tsIndex) {
            this.dataRows = dataRows;
            this.rowLen = dataRows.size();
            this.rowIndex = 0;
            this.tsIndex = tsIndex;
        }

        @Override
        public void run() {
            for (int i = 0; i < batchSize; i++) {
                if (rowIndex == rowLen) {
                    rowIndex = 0;
                }
                List<String> data = dataRows.get(rowIndex);
                data.set(tsIndex, "others");
                dataReadListener.receive(data);
                rowIndex++;
            }
        }
    }
}

