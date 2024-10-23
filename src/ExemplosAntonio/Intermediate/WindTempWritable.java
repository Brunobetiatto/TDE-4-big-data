package ExemplosAntonio.Intermediate;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WindTempWritable implements Writable {
    private float wind;
    private float temp;

    public WindTempWritable() {

    }

    public WindTempWritable(float wind, float temp) {
        this.wind = wind;
        this.temp = temp;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(this.wind);
        dataOutput.writeFloat(this.temp);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.wind = dataInput.readFloat();
        this.temp = dataInput.readFloat();
    }

    public float getWind() {
        return wind;
    }

    public void setWind(float wind) {
        this.wind = wind;
    }

    public float getTemp() {
        return temp;
    }

    public void setTemp(float temp) {
        this.temp = temp;
    }

    @Override
    public String toString() {
        return "WindTempWritable{" +
                "wind=" + wind +
                ", temp=" + temp +
                '}';
    }
}

