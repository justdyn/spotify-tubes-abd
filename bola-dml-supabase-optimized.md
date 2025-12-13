# Optimized bola-dml-supabase.sql - Run in Sections

## ⚠️ Timeout Prevention Strategy

If you encounter timeouts, run the script in smaller sections:

### Section 1: Leagues and Teams (Fast)
```sql
-- Run STEP 1 and STEP 3 only
-- (Leagues and Teams)
```

### Section 2: Players (Fast)
```sql
-- Run STEP 4 only
-- (Players)
```

### Section 3: Games (Medium)
```sql
-- Run STEP 2 and STEP 5 only
-- (Games temp table and Games)
```

### Section 4: Team Stats (Medium)
```sql
-- Run STEP 6 only
-- (Team Stats)
```

### Section 5: Appearances (SLOW - Run separately)
```sql
-- Run STEP 7 only
-- (Appearances - this is the slowest part)
```

### Section 6: Shots (Medium)
```sql
-- Run STEP 8 only
-- (Shots)
```

### Section 7: Team Players and Cleanup (Fast)
```sql
-- Run STEP 9, STEP 10, STEP 11
-- (Team Players, Statistics, Cleanup)
```

## Performance Optimizations Applied

1. ✅ **Triggers Disabled**: Temporarily disabled during bulk inserts
2. ✅ **Optimized JOINs**: Replaced correlated subqueries with JOINs
3. ✅ **Materialized Views**: Made optional (commented out)
4. ✅ **Batch Processing**: Script processes data in logical chunks

## If Still Timing Out

1. **Increase Statement Timeout** (in Supabase Dashboard):
   - Go to Settings → Database
   - Increase "Statement Timeout" to 300 seconds (5 minutes)

2. **Run Appearances Separately**:
   - The appearances INSERT is the slowest operation
   - Run it alone, then continue with the rest

3. **Use Direct Database Connection**:
   - Connect via psql or pgAdmin for longer timeouts
   - Supabase SQL Editor has shorter timeouts

## Verification After Each Section

After each section, verify data was inserted:
```sql
SELECT COUNT(*) FROM [table_name];
```

