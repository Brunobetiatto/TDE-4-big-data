package TDE2.medium.Gender;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LocationGenderWritable implements WritableComparable<LocationGenderWritable> {
    private String location;
    private String gender;

    public LocationGenderWritable() {}

    public LocationGenderWritable(String location, String gender) {
        this.location = location;
        this.gender = gender;
    }

    public String getLocation() {
        return location;
    }

    public String getGender() {
        return gender;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(location);
        dataOutput.writeUTF(gender);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.location = dataInput.readUTF();
        this.gender = dataInput.readUTF();
    }

    @Override
    public int compareTo(LocationGenderWritable o) {
        int result = this.location.compareTo(o.location);
        if (result != 0) {
            return result;
        }
        return this.gender.compareTo(o.gender);
    }

    @Override
    public String toString() {
        return location + "\t" + gender;
    }

    @Override
    public int hashCode() {
        return location.hashCode() * 163 + gender.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        LocationGenderWritable that = (LocationGenderWritable) obj;
        return this.location.equals(that.location) && this.gender.equals(that.gender);
    }
}
