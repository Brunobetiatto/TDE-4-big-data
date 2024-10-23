package ExemplosAntonio.Advanced;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class EntropyWritable implements Writable {
    private String characters;
    private int count;

    public EntropyWritable() {
    }

    public EntropyWritable(String characters, int count) {
        this.characters = characters;
        this.count = count;
    }

    public String getCharacters() {
        return characters;
    }

    public int getCount() {
        return count;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.characters);
        dataOutput.writeInt(this.count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.characters = dataInput.readUTF();
        this.count = dataInput.readInt();
    }
}
