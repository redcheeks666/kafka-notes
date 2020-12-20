package com.zll.kafka.delay;

public  class TaskEntry extends Task {
    private String msg;

    public TaskEntry(String msg){
        this.msg=msg;
    }


    @Override
    public void run() {
        System.out.println(msg);
    }

}
