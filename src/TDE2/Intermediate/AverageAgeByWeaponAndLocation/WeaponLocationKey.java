package TDE2.Intermediate.AverageAgeByWeaponAndLocation;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WeaponLocationKey implements WritableComparable<WeaponLocationKey> {
    private String weaponType;
    private String state;

    // Construtor padrão
    public WeaponLocationKey() {
    }

    public WeaponLocationKey(String weaponType, String state) {
        this.weaponType = weaponType;
        this.state = state;
    }

    // Getters e Setters
    public String getWeaponType() {
        return weaponType;
    }

    public void setWeaponType(String weaponType) {
        this.weaponType = weaponType;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    // Métodos de serialização
    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, weaponType);
        WritableUtils.writeString(out, state);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        weaponType = WritableUtils.readString(in);
        state = WritableUtils.readString(in);
    }

    // Método de comparação
    @Override
    public int compareTo(WeaponLocationKey other) {
        int cmp = this.weaponType.compareTo(other.weaponType);
        if (cmp != 0) {
            return cmp;
        }
        return this.state.compareTo(other.state);
    }

    @Override
    public String toString() {
        return weaponType + "\t" + state;
    }

    @Override
    public int hashCode() {
        return weaponType.hashCode() * 163 + state.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof WeaponLocationKey) {
            WeaponLocationKey other = (WeaponLocationKey) o;
            return weaponType.equals(other.weaponType) && state.equals(other.state);
        }
        return false;
    }
}