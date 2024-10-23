package TDE2.Intermediate.AverageAgeByWeaponAndLocation;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AgeCountWritable implements Writable {
    private int ageSum;
    private int count;

    // Construtor padrão (necessário)
    public AgeCountWritable() {
    }

    public AgeCountWritable(int ageSum, int count) {
        this.ageSum = ageSum;
        this.count = count;
    }

    // Getters e Setters
    public int getAgeSum() {
        return ageSum;
    }

    public void setAgeSum(int ageSum) {
        this.ageSum = ageSum;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    // Métodos de serialização
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(ageSum);
        out.writeInt(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        ageSum = in.readInt();
        count = in.readInt();
    }

    @Override
    public String toString() {
        return "Average Age: " + ((double) ageSum / count);
    }
}
