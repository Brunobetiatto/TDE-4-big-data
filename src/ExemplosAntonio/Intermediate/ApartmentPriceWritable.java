package ExemplosAntonio.Intermediate;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ApartmentPriceWritable implements Writable {
    private float price;
    private int count;

    public ApartmentPriceWritable() {
    }

    public ApartmentPriceWritable(float price, int count) {
        this.price = price;
        this.count = count;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(this.price);
        dataOutput.writeInt(this.count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.price = dataInput.readFloat();
        this.count = dataInput.readInt();
    }

    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "Average Price: " + price;
    }
}
