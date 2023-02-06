package org.apache.flink.util.listener;

import java.util.List;

public interface DataReadListener {
    public void receive(List<String> data);
}
