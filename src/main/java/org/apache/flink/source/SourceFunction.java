package org.apache.flink.source;
// package com.toscan.source;

// import org.apache.flink.api.common.state.ListState;
// import org.apache.flink.api.common.state.ListStateDescriptor;
// import org.apache.flink.api.common.typeinfo.TypeInformation;
// import org.apache.flink.api.common.typeutils.base.LongSerializer;
// import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
// import org.apache.flink.configuration.Configuration;
// import org.apache.flink.runtime.state.FunctionInitializationContext;
// import org.apache.flink.runtime.state.FunctionSnapshotContext;
// import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
// import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
// import org.apache.flink.util.Preconditions;

// import com.toscan.generator.DataGenerator;

// import java.util.ArrayList;
// import java.util.List;

// public class SourceFunction<T> extends RichParallelSourceFunction<T>
// implements CheckpointedFunction, ResultTypeQueryable<T> {

//     private transient DataGenerator generator;

//     private final String deserializer;

// 	private final TypeInformation<T> resultType;

//     /** Flag to make the source cancelable. */
// 	private volatile boolean isRunning = true;

//     private transient ListState<Long> checkpointedState;

//     public SourceFunction(String deserializer, TypeInformation<T> resultType) {
//         this.deserializer = deserializer;
//         this.resultType = resultType;
// 	}

//     @Override
// 	public void open(Configuration parameters) throws Exception {
// 		super.open(parameters);
// 		this.generator = new DataGenerator(getSubGeneratorConfig());
// 	}


//     @Override
//     public void run(SourceContext<T> ctx) throws Exception {
//         // TODO Auto-generated method stub
        
//     }

//     @Override
//     public void cancel() {
//         // TODO Auto-generated method stub
        
//     }

//     @Override
//     public TypeInformation<T> getProducedType() {
//         // TODO Auto-generated method stub
//         return null;
//     }

//     @Override
//     public void snapshotState(FunctionSnapshotContext context) throws Exception {
//         // TODO Auto-generated method stub
        
//     }

//     @Override
//     public void initializeState(FunctionInitializationContext context) throws Exception {
//         // TODO Auto-generated method stub
        
//     }
    
// }
