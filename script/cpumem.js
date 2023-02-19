"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const os = __importStar(require("os"));
const fs_1 = __importDefault(require("fs"));
class SysMetrics {
    timesBefore = os.cpus().map(c => c.times);
    getCpuUsage() {
        const timesAfter = os.cpus().map(c => c.times);
        const timeDeltas = timesAfter.map((t, i) => ({
            user: t.user - this.timesBefore[i].user,
            sys: t.sys - this.timesBefore[i].sys,
            idle: t.idle - this.timesBefore[i].idle
        }));
        this.timesBefore = timesAfter;
        return timeDeltas.map(times => 1 - times.idle / (times.user + times.sys + times.idle));
    }
    benchmarkingInterval = null;
    startBenchmarking(path, interval) {
        this.timesBefore = os.cpus().map(c => c.times);
        this.benchmarkingInterval = setInterval(() => {
            const cpuUsage = this.getCpuUsage();
            const memUsage = os.totalmem();
            const currentTimeStamp = new Date().getTime();
            const data = [currentTimeStamp, memUsage, ...cpuUsage];
            fs_1.default.appendFileSync(path, data.join(",") + '\n');
        }, interval);
    }
    stopBenchmarking() {
        if (this.benchmarkingInterval)
            clearInterval(this.benchmarkingInterval);
    }
    constructor(path, interval) {
        this.startBenchmarking(path, interval);
    }
}
new SysMetrics('test-1.csv', 100);
