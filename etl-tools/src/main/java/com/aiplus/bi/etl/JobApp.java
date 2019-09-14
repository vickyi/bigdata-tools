package com.aiplus.bi.etl;

import sun.misc.Signal;
import sun.misc.SignalHandler;

/**
 * 程序入口.
 *
 * @author dev
 */
public class JobApp {

    public static void main(String[] args) {

        String jobName = args[0];
        String[] jobArgs = new String[args.length - 1];
        System.arraycopy(args, 1, jobArgs, 0, jobArgs.length);

        // 关闭程序：https://www.cnblogs.com/nuccch/p/10903162.html
        Signal sg = new Signal("TERM"); // kill -15 pid
        // 监听信号量
        Signal.handle(sg, new SignalHandler() {
            @Override
            public void handle(Signal signal) {
                System.out.println("signal handle: " + signal.getName());
                System.exit(0);
            }
        });

        // 注册关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // 在关闭钩子中执行收尾工作
                // 注意事项：
                // 1.在这里执行的动作不能耗时太久
                // 2.不能在这里再执行注册，移除关闭钩子的操作
                // 3 不能在这里调用System.exit()
                System.out.println("do shutdown hook：程序异常退出");
            }
        });

        ConfigurableJob job = JobRegistry.getInstance().createRegisterJob(jobName);
        int sgn = JobTools.submitJob(job, jobArgs);
        System.exit(sgn);
    }
}
