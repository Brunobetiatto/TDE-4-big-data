package ExemplosAntonio.Intermediate;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FireAvgTempWritable implements Writable {
    private float sumTemp;
    private float sumN;

    public FireAvgTempWritable() {}

    public FireAvgTempWritable(float sumTemp, float sumN) {
        this.sumTemp = sumTemp;
        this.sumN = sumN;
    }

    public float getSumTemp() {
        return sumTemp;
    }
    public void setSumTemp(float sumTemp) {
        this.sumTemp = sumTemp;
    }
    public float getSumN() {
        return sumN;
    }
    public void setSumN(float sumN) {
        this.sumN = sumN;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(sumTemp);
        dataOutput.writeFloat(sumN);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.sumTemp = dataInput.readFloat();
        this.sumN = dataInput.readFloat();
    }
}
