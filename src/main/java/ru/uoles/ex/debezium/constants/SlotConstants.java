package ru.uoles.ex.debezium.constants;

/**
 * debezium-test
 * Created by Intellij IDEA.
 * Developer: uoles (Kulikov Maksim)
 * Date: 24.10.2024
 * Time: 4:54
 */
public final class SlotConstants {

    public static final String SLOT_NAME_PARAM = "slotName";
    public static final String SLOT_ACTIVE_COLUMN = "active";
    public static final String SLOT_STATUS_SELECT =
            "SELECT " +
            "   slot_name, " +
            "   active, " +
            "   pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) as slot_lag, " +
            "   pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) as confirmed_lag " +
            " FROM pg_replication_slots " +
            " WHERE slot_name = :slotName ";
}
