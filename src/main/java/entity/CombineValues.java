package entity;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CombineValues implements WritableComparable<CombineValues> {

    private Text joinKey; //连接键 on
    private Text flag; //文件来源标志，左表和右表
    private Text secondPart; //除了on 字段的其他字段

    public Text getJoinKey() {
        return joinKey;
    }

    public void setJoinKey(Text joinKey) {
        this.joinKey = joinKey;
    }

    public Text getFlag() {
        return flag;
    }

    public void setFlag(Text flag) {
        this.flag = flag;
    }

    public Text getSecondPart() {
        return secondPart;
    }

    public void setSecondPart(Text secondPart) {
        this.secondPart = secondPart;
    }

    @Override
    public int compareTo(CombineValues o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}
