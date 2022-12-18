package index;

import storage.TableRecord;

public abstract class BaseUnorderedIndex implements Iterable<TableRecord> {
    protected final int partition_num;
    protected final int num_items;

    private final int delta;
    public abstract TableRecord SearchRecord(String primary_key);

    public abstract boolean InsertRecord(String key, TableRecord record, int partition_id);

    public abstract boolean InsertRecord(String key, TableRecord record);

    public BaseUnorderedIndex(int partition_num, int num_items) {
        this.partition_num = partition_num;
        this.num_items = num_items;
        this.delta = num_items / partition_num;
    }

    public int getPartitionId(String primary_key) {
        int key = Integer.parseInt(primary_key);
        return key / delta;
    }
}
