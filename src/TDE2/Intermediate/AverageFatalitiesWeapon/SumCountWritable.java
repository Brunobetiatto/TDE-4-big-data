package TDE2.medium.AverageFatalitiesWeapon;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SumCountWritable implements Writable {
    private int sum;
    private int count;

    public SumCountWritable() {}

    public SumCountWritable(int sum, int count) {
        this.sum = sum;
        this.count = count;
    }

    public int getSum() {
        return sum;
    }

    public int getCount() {
        return count;
    }

    public void setSum(int sum) {
        this.sum = sum;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(sum);
        dataOutput.writeInt(count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.sum = dataInput.readInt();
        this.count = dataInput.readInt();
    }
}
