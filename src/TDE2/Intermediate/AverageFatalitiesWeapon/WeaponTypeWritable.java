package TDE2.medium.AverageFatalitiesWeapon;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WeaponTypeWritable implements WritableComparable<WeaponTypeWritable> {
    private String weaponType;

    public WeaponTypeWritable() {}

    public WeaponTypeWritable(String weaponType) {
        this.weaponType = weaponType;
    }

    public String getWeaponType() {
        return weaponType;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(weaponType);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.weaponType = dataInput.readUTF();
    }

    @Override
    public int compareTo(WeaponTypeWritable o) {
        return this.weaponType.compareTo(o.weaponType);
    }

    @Override
    public String toString() {
        return weaponType;
    }

    @Override
    public int hashCode() {
        return weaponType.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        WeaponTypeWritable that = (WeaponTypeWritable) obj;
        return this.weaponType.equals(that.weaponType);
    }
}
