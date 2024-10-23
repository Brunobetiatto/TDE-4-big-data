package TDE2.Advanced.MostFrequentlyUsedWeaponByLocation;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WeaponCountWritable implements WritableComparable<WeaponCountWritable> {
    private String weaponType;
    private int count;

    // Construtor padrão
    public WeaponCountWritable() {
    }

    public WeaponCountWritable(String weaponType, int count) {
        this.weaponType = weaponType;
        this.count = count;
    }

    // Getters e Setters
    public String getWeaponType() {
        return weaponType;
    }

    public void setWeaponType(String weaponType) {
        this.weaponType = weaponType;
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
        out.writeUTF(weaponType);
        out.writeInt(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        weaponType = in.readUTF();
        count = in.readInt();
    }

    // Implementação de compareTo
    @Override
    public int compareTo(WeaponCountWritable o) {
        int result = weaponType.compareTo(o.weaponType);
        if (result == 0) {
            result = Integer.compare(count, o.count);
        }
        return result;
    }

    @Override
    public String toString() {
        return weaponType + "\t" + count;
    }
}
