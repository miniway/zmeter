import os
import re
import time
import zmeter
import subprocess

class Mssql(zmeter.Metric):

    def __init__(self):
        super(Mssql, self).__init__()

    def fetchWindows(self):
        conf = self._config.get('mssql', {})
        name = conf.get("name", "SQLServer")

        ITEMS = {
            "Locks" : [
                    ("Average Wait Time (ms)", "avg.lock_waited"),
                    ("Lock Requests/sec", "lock_rps"),
                    ("Lock Timeouts (timeout > 0)/sec", "lock_timeout0_ps"),
                    ("Lock Timeouts/sec", "lock_timeout_ps"),
                    ("Lock Wait Time (ms)", "lock_waited"),
                    ("Lock Waits/sec", "lock_waits_ps"),
                    ("Number of Deadlocks/sec", "deadlocks"),
                ],
            "Databases" : [
                    ("Data File(s) Size (KB)", "data_size"),
                    ("Log File(s) Size (KB)", "log_size"),
                    ("Log File(s) Used Size (KB)", "log_used"),
                    ("Percent Log Used", "log_pused"),
                    ("Active Transactions", "txns"),
                    ("Transactions/sec", "tps"),
                    ("Repl. Pending Xacts", "repl_pending_xacts"),
                    ("Repl. Trans. Rate", "repl_txns_rage"),
                    ("Log Cache Reads/sec", "log_cache_reads_ps"),
                    ("Log Cache Hit Ratio", "log_cache_hit_ratio"),
                    ("Bulk Copy Rows/sec", "bulk_copy_rows_ps"),
                    ("Bulk Copy Throughput/sec", "bulk_copy_throughput_ps"),
                    ("Backup/Restore Throughput/sec", "backup_restore_throughput_ps"),
                    ("DBCC Logical Scan Bytes/sec", "dbcc_logical_scan_bps"),
                    ("Shrink Data Movement Bytes/sec", "shrink_data_mov_bps"),
                    ("Log Flushes/sec", "log_flushes_ps"),
                    ("Log Bytes Flushed/sec", "log_flushed_bps"),
                    ("Log Flush Waits/sec", "log_flush_waits_ps"),
                    ("Log Flush Wait Time", "log_flush_waited"),
                    ("Log Truncations", "log_truncations"),
                    ("Log Growths", "log_growth"),
                    ("Log Shrinks", "log_shrinks")
                ],
            "SQL Errors" : [ ("Errors/sec", "errors") ],
            "Plan Cache" : [
                    ("Cache Hit Ratio", "plan_cache_hit_ratio"),
                    ("Cache Pages", "plan_cache_pages"),
                    ("Cache Object Counts", "plan_cache_objects"),
                    ("Cache Objects in use", "plan_cache_objects_in_use"),
                ],
            "Cursor Manager by Type" : [
                    ("Cache Hit Ratio", "cursor_cache_hit_ratio"),
                    ("Cached Cursor Counts", "cursor_cached"),
                    ("Cursor Cache Use Counts/sec", "cursor_cache_use_counts_ps"),
                    ("Cursor Requests/sec", "cursor_rps"),
                    ("Active cursors", "cursors"),
                    ("Cursor memory usage", "cursor_memory"),
                    ("Cursor worktable usage", "cursor_worktable"),
                    ("Number of active cursor plans", "cursor_plans"),
                ],
            "Broker Activation" : [
                    ("Tasks Started/sec", "tasks_started_ps"),
                    ("Tasks Running", "tasks_running"),
                    ("Tasks Aborted/sec", "tasks_aborted_ps"),
                    ("Task Limit Reached/sec", "task_limit_reached_ps"),
                    ("Task Limit Reached", "task_limit_reached"),
                    ("Stored Procedures Invoked/sec", "stored_procedures_invoiked_ps")
                ],
            "Catalog Metadata" : [
                    ("Cache Hit Ratio", "catalog_cache_hit_ratio"),
                    ("Cache Entries Count", "catalog_cache_entiries"),
                    ("Cache Entries Pinned Count", "catalog_cache_entries_pinned")
                ],
        }
        import win32pdh
        stat_handles = {}
        query_handle = win32pdh.OpenQuery()
        for key, items in ITEMS.items():
            parent_key = "MSSQL$%s:%s" % (name, key)
            for item in items:
                path = win32pdh.MakeCounterPath(
                    (None, parent_key, "_Total", None, -1, item[0]))
                counter_handle = win32pdh.AddCounter(query_handle, path)
                stat_handles[item[1]] = counter_handle
        win32pdh.CollectQueryData(query_handle)
        time.sleep(1)
        win32pdh.CollectQueryData(query_handle)

        stat = {}
        for key, handle in stat_handles.items():
            data = win32pdh.GetFormattedCounterValue(handle, win32pdh.PDH_FMT_DOUBLE)
            stat[key] = round(data[1], 2)
        win32pdh.CloseQuery(query_handle)
        
        return stat

            
