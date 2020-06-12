package com.gusi.demo.mapreduce.sorted;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * PariDTO <br>
 *
 * @author yydeng
 * @since 2020/6/5
 */
public class PariDTO implements WritableComparable<PariDTO> {
	private String first;
	private int second;

	@Override
	public int compareTo(PariDTO o) {
		int comp = this.first.compareTo(o.first);
		if (comp != 0) {
			return comp;
		} else {
			return Integer.valueOf(this.second).compareTo(Integer.valueOf(o.second));
		}
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeUTF(first);
		dataOutput.writeInt(second);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.first = dataInput.readUTF();
		this.second = dataInput.readInt();
	}

	public String getFirst() {
		return first;
	}

	public void setFirst(String first) {
		this.first = first;
	}

	public int getSecond() {
		return second;
	}

	public void setSecond(int second) {
		this.second = second;
	}

	@Override
	public String toString() {
		return "PariDTO{" + "first='" + first + '\'' + ", second='" + second + '\'' + '}';
	}
}
