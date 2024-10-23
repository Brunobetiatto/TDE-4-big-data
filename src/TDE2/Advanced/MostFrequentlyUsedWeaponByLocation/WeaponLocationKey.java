package TDE2.Advanced.MostFrequentlyUsedWeaponByLocation;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WeaponLocationKey implements WritableComparable<WeaponLocationKey> {
    private String location;
    private String weaponType;

    // Construtor padrão
    public WeaponLocationKey() {
    }

    public WeaponLocationKey(String location, String weaponType) {
        this.location = location;
        this.weaponType = weaponType;
    }

    // Getters e Setters
    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getWeaponType() {
        return weaponType;
    }

    public void setWeaponType(String weaponType) {
        this.weaponType = weaponType;
    }

    // Métodos de serialização
    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, location);
        WritableUtils.writeString(out, weaponType);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        location = WritableUtils.readString(in);
        weaponType = WritableUtils.readString(in);
    }

    // Implementação de compareTo
    @Override
    public int compareTo(WeaponLocationKey o) {
        int cmp = this.location.compareTo(o.location);
        if (cmp != 0) {
            return cmp;
        }
        return this.weaponType.compareTo(o.weaponType);
    }

    @Override
    public String toString() {
        return location + "\t" + weaponType;
    }

    @Override
    public int hashCode() {
        return location.hashCode() * 163 + weaponType.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof WeaponLocationKey) {
            WeaponLocationKey other = (WeaponLocationKey) o;
            return location.equals(other.location) && weaponType.equals(other.weaponType);
        }
        return false;
    }
}