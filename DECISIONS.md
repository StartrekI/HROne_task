# DECISIONS.md

| # | Question | Decision | Why |
|---|---|---|---|
| 1 | Indexes | `employees`: unique `emp_code`; `department+emp_code`; `department+joined_on`; `joined_on`. `attendance_logs`: unique `emp_code+date`; `date desc+emp_code` plus the fields analytics read; `status+date+emp_code`; `emp_code+punch_in desc`. Rejected: a separate `emp_code` index on logs. | Unique indexes block duplicates. The others serve the department filter, headcount, list sort and filters, and punch-out. The analytics index lets reports read only index keys. `emp_code+date` already covers `emp_code` alone. |
| 2 | Punch-in race | Both requests compute the same `(emp_code, date)` and call `insert_one`. | The unique index lets one through (201); the other gets `DuplicateKeyError`, returned as 409. No read-then-insert check. |
| 3 | Ties | `$rank`, then filter `rank <= limit`. | Tied employees share a rank (1, 2, 2, 4), so everyone tied at the cutoff is returned, even beyond `limit` rows. |
| 4 | Headcount | Count from `employees`, not logs. | Each employee who joined by month end adds one row, merged with log totals, so zero-log employees count. |
| 5 | 100x data | Shard `attendance_logs` on `emp_code`. | The unique `emp_code+date` index starts with it, as a sharded unique index must, and an employee's reads stay on one shard. |
