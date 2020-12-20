package com.zll.kafka.delay;

public abstract class Task extends Thread {

    //延时时间
    private int key;
    //周期数
    private int cycleNum;
    //所在时间轮下标
    private int index;
    @Override
    public void run() {

    }
    public int getKey(){
        return key;
    }

    public void setKey(int key){
        this.key=key;
    }
    public int getCycleNum(){
        return cycleNum;
    }

    public void setCycleNum(int cycleNum){
        this.cycleNum=cycleNum;
    }
    public int getIndex(){
        return index;
    }

    public void setIndex(int cycleNum){
        this.index=index;
    }
}
