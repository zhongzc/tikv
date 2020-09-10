// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use kvproto::coprocessor::KeyRange;
use tidb_query_datatype::EvalType;
use tipb::ColumnInfo;
use tipb::FieldType;
use tipb::IndexScan;

use super::util::scan_executor::*;
use crate::interface::*;
use codec::prelude::NumberDecoder;
use tidb_query_common::storage::{IntervalRange, Storage};
use tidb_query_common::Result;
use tidb_query_datatype::codec::batch::{LazyBatchColumn, LazyBatchColumnVec};
use tidb_query_datatype::codec::table::{check_index_key, MAX_OLD_ENCODED_VALUE_LEN};
use tidb_query_datatype::codec::{datum, table};
use tidb_query_datatype::expr::{EvalConfig, EvalContext};
use tikv_util::minitrace::{self, Event};

use DecodeHandleStrategy::*;

pub struct BatchIndexScanExecutor<S: Storage>(ScanExecutor<S, IndexScanExecutorImpl>);

// We assign a dummy type `Box<dyn Storage<Statistics = ()>>` so that we can omit the type
// when calling `check_supported`.
impl BatchIndexScanExecutor<Box<dyn Storage<Statistics = ()>>> {
    /// Checks whether this executor can be used.
    #[inline]
    pub fn check_supported(descriptor: &IndexScan) -> Result<()> {
        check_columns_info_supported(descriptor.get_columns())
    }
}

impl<S: Storage> BatchIndexScanExecutor<S> {
    pub fn new(
        storage: S,
        config: Arc<EvalConfig>,
        columns_info: Vec<ColumnInfo>,
        key_ranges: Vec<KeyRange>,
        primary_column_ids_len: usize,
        is_backward: bool,
        unique: bool,
        is_scanned_range_aware: bool,
    ) -> Result<Self> {
        // Note 1: `unique = true` doesn't completely mean that it is a unique index scan. Instead
        // it just means that we can use point-get for this index. In the following scenarios
        // `unique` will be `false`:
        // - scan from a non-unique index
        // - scan from a unique index with like: where unique-index like xxx
        //
        // Note 2: Unlike table scan executor, the accepted `columns_info` of index scan executor is
        // strictly stipulated. The order of columns in the schema must be the same as index data
        // stored and if PK handle is needed it must be placed as the last one.
        //
        // Note 3: Currently TiDB may send multiple PK handles to TiKV (but only the last one is
        // real). We accept this kind of request for compatibility considerations, but will be
        // forbidden soon.

        let is_int_handle = columns_info.last().map_or(false, |ci| ci.get_pk_handle());
        let is_common_handle = primary_column_ids_len > 0;
        let (decode_handle_strategy, handle_column_cnt) = match (is_int_handle, is_common_handle) {
            (false, false) => (NoDecode, 0),
            (false, true) => (DecodeCommonHandle, primary_column_ids_len),
            (true, false) => (DecodeIntHandle, 1),
            // TiDB may accidentally push down both int handle or common handle.
            // However, we still try to decode int handle.
            _ => {
                return Err(other_err!(
                    "Both int handle and common handle are push downed"
                ));
            }
        };

        if handle_column_cnt > columns_info.len() {
            return Err(other_err!(
                "The number of handle columns exceeds the length of `columns_info`"
            ));
        }

        let schema: Vec<_> = columns_info
            .iter()
            .map(|ci| field_type_from_column_info(&ci))
            .collect();

        let columns_id_without_handle: Vec<_> = columns_info
            [..columns_info.len() - handle_column_cnt]
            .iter()
            .map(|ci| ci.get_column_id())
            .collect();

        let imp = IndexScanExecutorImpl {
            context: EvalContext::new(config),
            schema,
            columns_id_without_handle,
            decode_handle_strategy,
        };
        let wrapper = ScanExecutor::new(ScanExecutorOptions {
            imp,
            storage,
            key_ranges,
            is_backward,
            is_key_only: false,
            accept_point_range: unique,
            is_scanned_range_aware,
        })?;
        Ok(Self(wrapper))
    }
}

impl<S: Storage> BatchExecutor for BatchIndexScanExecutor<S> {
    type StorageStats = S::Statistics;

    #[inline]
    fn schema(&self) -> &[FieldType] {
        self.0.schema()
    }

    #[inline]
    fn next_batch(&mut self, scan_rows: usize) -> BatchExecuteResult {
        let _guard = minitrace::new_span(Event::TiKvCoprIndexScanExecutorNextBatch as u32);
        self.0.next_batch(scan_rows)
    }

    #[inline]
    fn collect_exec_stats(&mut self, dest: &mut ExecuteStats) {
        self.0.collect_exec_stats(dest);
    }

    #[inline]
    fn collect_storage_stats(&mut self, dest: &mut Self::StorageStats) {
        self.0.collect_storage_stats(dest);
    }

    #[inline]
    fn take_scanned_range(&mut self) -> IntervalRange {
        self.0.take_scanned_range()
    }

    #[inline]
    fn can_be_cached(&self) -> bool {
        self.0.can_be_cached()
    }
}

#[derive(PartialEq, Debug)]
enum DecodeHandleStrategy {
    NoDecode,
    DecodeIntHandle,
    DecodeCommonHandle,
}

struct IndexScanExecutorImpl {
    /// See `TableScanExecutorImpl`'s `context`.
    context: EvalContext,

    /// See `TableScanExecutorImpl`'s `schema`.
    schema: Vec<FieldType>,

    /// ID of interested columns (exclude PK handle column).
    columns_id_without_handle: Vec<i64>,

    /// The strategy to decode handles.
    /// Handle will be always placed in the last column.
    decode_handle_strategy: DecodeHandleStrategy,
}

impl ScanExecutorImpl for IndexScanExecutorImpl {
    #[inline]
    fn schema(&self) -> &[FieldType] {
        &self.schema
    }

    #[inline]
    fn mut_context(&mut self) -> &mut EvalContext {
        &mut self.context
    }

    /// Constructs empty columns, with PK containing int handle in decoded format and the rest in raw format.
    ///
    /// Note: the structure of the constructed column is the same as table scan executor but due
    /// to different reasons.
    fn build_column_vec(&self, scan_rows: usize) -> LazyBatchColumnVec {
        let columns_len = self.schema.len();
        let mut columns = Vec::with_capacity(columns_len);

        for _ in 0..self.columns_id_without_handle.len() {
            columns.push(LazyBatchColumn::raw_with_capacity(scan_rows));
        }

        match self.decode_handle_strategy {
            NoDecode => {}
            DecodeIntHandle => {
                columns.push(LazyBatchColumn::decoded_with_capacity_and_tp(
                    scan_rows,
                    EvalType::Int,
                ));
            }
            DecodeCommonHandle => {
                for _ in self.columns_id_without_handle.len()..columns_len {
                    columns.push(LazyBatchColumn::raw_with_capacity(scan_rows));
                }
            }
        }

        assert_eq!(columns.len(), columns_len);
        LazyBatchColumnVec::from(columns)
    }

    // Currently, we have 6 foramts of index value.
    // Value layout:
    // +--With Restore Data(for indices on string columns)
    // |  |
    // |  +--Non Unique (TailLen = len(PaddingData) + len(Flag), TailLen < 8 always)
    // |  |  |
    // |  |  |  Layout: TailLen |      RestoreData  |      PaddingData
    // |  |  |  Length: 1       | size(RestoreData) | size(paddingData)
    // |  |  |
    // |  |  |  The length >= 10 always because of padding.
    // |  |
    // |  |
    // |  +--Unique Common Handle
    // |  |  |
    // |  |  |
    // |  |  |  Layout: 0x00 | CHandle Flag | CHandle Len | CHandle       | RestoreData
    // |  |  |  Length: 1    | 1            | 2           | size(CHandle) | size(RestoreData)
    // |  |  |
    // |  |  |  The length > 10 always because of CHandle size.
    // |  |
    // |  |
    // |  +--Unique Integer Handle (TailLen = len(Handle) + len(Flag), TailLen == 8 || TailLen == 9)
    // |     |
    // |     |  Layout: 0x08 |    RestoreData    |  Handle
    // |     |  Length: 1    | size(RestoreData) |   8
    // |     |
    // |     |  The length >= 10 always since size(RestoreData) > 0.
    // |
    // |
    // +--Without Restore Data
    // |
    // +--Non Unique
    // |  |
    // |  |  Layout: '0'
    // |  |  Length:  1
    // |
    // +--Unique Common Handle
    // |  |
    // |  |  Layout: 0x00 | CHandle Flag | CHandle Len | CHandle
    //    |  Length: 1    | 1            | 2           | size(CHandle)
    // |
    // |
    // +--Unique Integer Handle
    // |
    // |  Layout: Handle
    // |  Length:   8
    // For more intuitive explanation, check the docs: https://docs.google.com/document/d/1Co5iMiaxitv3okJmLYLJxZYCNChcjzswJMRr-_45Eqg/edit?usp=sharing
    #[inline]
    fn process_kv_pair(
        &mut self,
        mut key: &[u8],
        value: &[u8],
        columns: &mut LazyBatchColumnVec,
    ) -> Result<()> {
        check_index_key(key)?;
        key = &key[table::PREFIX_LEN + table::ID_LEN..];
        if value.len() > MAX_OLD_ENCODED_VALUE_LEN {
            if value[0] <= 1 && value[1] == table::INDEX_VALUE_COMMON_HANDLE_FLAG {
                if self.decode_handle_strategy != DecodeCommonHandle {
                    return Err(other_err!("Expect to decode index values with common handles in `DecodeCommonHandle` mode."));
                }
                self.process_unique_common_handle_kv(key, value, columns)
            } else {
                self.process_new_collation_kv(key, value, columns)
            }
        } else {
            self.process_old_collation_kv(key, value, columns)
        }
    }
}

impl IndexScanExecutorImpl {
    #[inline]
    fn decode_handle_from_value(&self, mut value: &[u8]) -> Result<i64> {
        // NOTE: it is not `number::decode_i64`.
        value
            .read_u64()
            .map_err(|_| other_err!("Failed to decode handle in value as i64"))
            .map(|x| x as i64)
    }

    #[inline]
    fn decode_handle_from_key(&self, key: &[u8]) -> Result<i64> {
        let flag = key[0];
        let mut val = &key[1..];

        // TODO: Better to use `push_datum`. This requires us to allow `push_datum`
        // receiving optional time zone first.

        match flag {
            datum::INT_FLAG => val
                .read_i64()
                .map_err(|_| other_err!("Failed to decode handle in key as i64")),
            datum::UINT_FLAG => val
                .read_u64()
                .map_err(|_| other_err!("Failed to decode handle in key as u64"))
                .map(|x| x as i64),
            _ => Err(other_err!("Unexpected handle flag {}", flag)),
        }
    }

    fn extract_columns_from_row_format(
        &mut self,
        value: &[u8],
        columns: &mut LazyBatchColumnVec,
    ) -> Result<()> {
        use tidb_query_datatype::codec::row::v2::{RowSlice, V1CompatibleEncoder};

        let row = RowSlice::from_bytes(value)?;
        for (idx, col_id) in self.columns_id_without_handle.iter().enumerate() {
            if let Some((start, offset)) = row.search_in_non_null_ids(*col_id)? {
                let mut buffer_to_write = columns[idx].mut_raw().begin_concat_extend();
                buffer_to_write
                    .write_v2_as_datum(&row.values()[start..offset], &self.schema[idx])?;
            } else if row.search_in_null_ids(*col_id) {
                columns[idx].mut_raw().push(datum::DATUM_DATA_NULL);
            } else {
                return Err(other_err!("Unexpected missing column {}", col_id));
            }
        }
        Ok(())
    }

    fn extract_columns_from_datum_format(
        datum: &mut &[u8],
        columns: &mut [LazyBatchColumn],
    ) -> Result<()> {
        for (i, column) in columns.iter_mut().enumerate() {
            if datum.is_empty() {
                return Err(other_err!("{}th column is missing value", i));
            }
            let (value, remaining) = datum::split_datum(datum, false)?;
            column.mut_raw().push(value);
            *datum = remaining;
        }
        Ok(())
    }

    // Process index values that contain common handle flags.
    // NOTE: If there is no restore data in index value, we need to extract the index column from the key.
    // 1. Unique common handle in new collation
    // |  Layout: 0x00 | CHandle Flag | CHandle Len | CHandle       | RestoreData
    // |  Length: 1    | 1            | 2           | size(CHandle) | size(RestoreData)
    //
    // 2. Unique common handle in old collation
    // |  Layout: 0x00 | CHandle Flag | CHandle Len | CHandle       |
    // |  Length: 1    | 1            | 2           | size(CHandle) |
    fn process_unique_common_handle_kv(
        &mut self,
        mut key_payload: &[u8],
        value: &[u8],
        columns: &mut LazyBatchColumnVec,
    ) -> Result<()> {
        let handle_len = (&value[2..]).read_u16().map_err(|_| {
            other_err!(
                "Fail to read common handle's length from value: {}",
                hex::encode_upper(value)
            )
        })? as usize;
        let handle_end_offset = 4 + handle_len;

        if handle_end_offset > value.len() {
            return Err(other_err!("`handle_len` is corrupted: {}", handle_len));
        }

        // If there are some restore data, the index value is in new collation.
        if handle_end_offset < value.len() {
            let restore_values = &value[handle_end_offset..];
            self.extract_columns_from_row_format(restore_values, columns)?;
        } else {
            // Otherwise, the index value is in old collation, we should extract the index columns from the key.
            Self::extract_columns_from_datum_format(
                &mut key_payload,
                &mut columns[..self.columns_id_without_handle.len()],
            )?;
        }

        // Decode the common handles.
        let mut common_handle = &value[4..handle_end_offset];
        Self::extract_columns_from_datum_format(
            &mut common_handle,
            &mut columns[self.columns_id_without_handle.len()..],
        )?;

        Ok(())
    }

    // Process index values that are in new collation but don't contain common handle.
    // NOTE: We should extract the index columns from the key if there are common handles in the key or the tail length of the value is less than 8.
    // These index values have 2 types.
    // 1. Non-unique index
    //      * Key contains a int handle.
    //      * Key contains a common handle.
    // 2. Unique index
    //      * Value contains a int handle.
    fn process_new_collation_kv(
        &mut self,
        mut key_payload: &[u8],
        value: &[u8],
        columns: &mut LazyBatchColumnVec,
    ) -> Result<()> {
        let tail_len = value[0] as usize;

        if tail_len > value.len() {
            return Err(other_err!("`tail_len`: {} is corrupted", tail_len));
        }

        let restore_values = &value[1..value.len() - tail_len];
        self.extract_columns_from_row_format(restore_values, columns)?;

        match self.decode_handle_strategy {
            NoDecode => {}
            // This is a non-unique index value, we should extract the int handle from the key.
            DecodeIntHandle if tail_len < 8 => {
                datum::skip_n(&mut key_payload, self.columns_id_without_handle.len())?;
                let handle = self.decode_handle_from_key(key_payload)?;
                columns[self.columns_id_without_handle.len()]
                    .mut_decoded()
                    .push_int(Some(handle));
            }
            // This is a unique index value, we should extract the int handle from the value.
            DecodeIntHandle => {
                let handle = self.decode_handle_from_value(&value[value.len() - tail_len..])?;
                columns[self.columns_id_without_handle.len()]
                    .mut_decoded()
                    .push_int(Some(handle));
            }
            // This is a non-unique index value, we should extract the common handle from the key.
            DecodeCommonHandle => {
                datum::skip_n(&mut key_payload, self.columns_id_without_handle.len())?;
                Self::extract_columns_from_datum_format(
                    &mut key_payload,
                    &mut columns[self.columns_id_without_handle.len()..],
                )?;
            }
        }
        Ok(())
    }

    // Process index values that are in old collation but don't contain common handles.
    // NOTE: We should extract the index columns from the key first, and extract the handles from value if there is no handle in the key.
    // Otherwise, extract the handles from the key.
    fn process_old_collation_kv(
        &mut self,
        mut key_payload: &[u8],
        value: &[u8],
        columns: &mut LazyBatchColumnVec,
    ) -> Result<()> {
        Self::extract_columns_from_datum_format(
            &mut key_payload,
            &mut columns[..self.columns_id_without_handle.len()],
        )?;

        match self.decode_handle_strategy {
            NoDecode => {}
            // For normal index, it is placed at the end and any columns prior to it are
            // ensured to be interested. For unique index, it is placed in the value.
            DecodeIntHandle if key_payload.is_empty() => {
                // This is a unique index, and we should look up PK int handle in the value.
                let handle_val = self.decode_handle_from_value(value)?;
                columns[self.columns_id_without_handle.len()]
                    .mut_decoded()
                    .push_int(Some(handle_val));
            }
            DecodeIntHandle => {
                // This is a normal index, and we should look up PK handle in the key.
                let handle_val = self.decode_handle_from_key(key_payload)?;
                columns[self.columns_id_without_handle.len()]
                    .mut_decoded()
                    .push_int(Some(handle_val));
            }
            DecodeCommonHandle => {
                // Otherwise, if the handle is common handle, we extract it from the key.
                Self::extract_columns_from_datum_format(
                    &mut key_payload,
                    &mut columns[self.columns_id_without_handle.len()..],
                )?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use codec::prelude::NumberEncoder;
    use kvproto::coprocessor::KeyRange;
    use tidb_query_datatype::{FieldTypeAccessor, FieldTypeTp};
    use tipb::ColumnInfo;

    use tidb_query_common::storage::test_fixture::FixtureStorage;
    use tidb_query_common::util::convert_to_prefix_next;
    use tidb_query_datatype::codec::data_type::*;
    use tidb_query_datatype::codec::{
        datum,
        row::v2::encoder_for_test::{Column, RowEncoder},
        table, Datum,
    };
    use tidb_query_datatype::expr::EvalConfig;

    #[test]
    fn test_basic() {
        const TABLE_ID: i64 = 3;
        const INDEX_ID: i64 = 42;
        let mut ctx = EvalContext::default();

        // Index schema: (INT, FLOAT)

        // the elements in data are: [int index, float index, handle id].
        let data = vec![
            [Datum::I64(-5), Datum::F64(0.3), Datum::I64(10)],
            [Datum::I64(5), Datum::F64(5.1), Datum::I64(5)],
            [Datum::I64(5), Datum::F64(10.5), Datum::I64(2)],
        ];

        // The column info for each column in `data`. Used to build the executor.
        let columns_info = vec![
            {
                let mut ci = ColumnInfo::default();
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci
            },
            {
                let mut ci = ColumnInfo::default();
                ci.as_mut_accessor().set_tp(FieldTypeTp::Double);
                ci
            },
            {
                let mut ci = ColumnInfo::default();
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci.set_pk_handle(true);
                ci
            },
        ];

        // The schema of these columns. Used to check executor output.
        let schema = vec![
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::Double.into(),
            FieldTypeTp::LongLong.into(),
        ];

        // Case 1. Normal index.

        // For a normal index, the PK handle is stored in the key and nothing interesting is stored
        // in the value. So let's build corresponding KV data.

        let store = {
            let kv: Vec<_> = data
                .iter()
                .map(|datums| {
                    let index_data = datum::encode_key(&mut ctx, datums).unwrap();
                    let key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &index_data);
                    let value = vec![];
                    (key, value)
                })
                .collect();
            FixtureStorage::from(kv)
        };

        {
            // Case 1.1. Normal index, without PK, scan total index in reverse order.

            let key_ranges = vec![{
                let mut range = KeyRange::default();
                let start_data = datum::encode_key(&mut ctx, &[Datum::Min]).unwrap();
                let start_key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &start_data);
                range.set_start(start_key);
                let end_data = datum::encode_key(&mut ctx, &[Datum::Max]).unwrap();
                let end_key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &end_data);
                range.set_end(end_key);
                range
            }];

            let mut executor = BatchIndexScanExecutor::new(
                store.clone(),
                Arc::new(EvalConfig::default()),
                vec![columns_info[0].clone(), columns_info[1].clone()],
                key_ranges,
                0,
                true,
                false,
                false,
            )
            .unwrap();

            let mut result = executor.next_batch(10);
            assert!(result.is_drained.as_ref().unwrap());
            assert_eq!(result.physical_columns.columns_len(), 2);
            assert_eq!(result.physical_columns.rows_len(), 3);
            assert!(result.physical_columns[0].is_raw());
            result.physical_columns[0]
                .ensure_all_decoded_for_test(&mut ctx, &schema[0])
                .unwrap();
            assert_eq!(
                result.physical_columns[0].decoded().to_int_vec(),
                &[Some(5), Some(5), Some(-5)]
            );
            assert!(result.physical_columns[1].is_raw());
            result.physical_columns[1]
                .ensure_all_decoded_for_test(&mut ctx, &schema[1])
                .unwrap();
            assert_eq!(
                result.physical_columns[1].decoded().to_real_vec(),
                &[
                    Real::new(10.5).ok(),
                    Real::new(5.1).ok(),
                    Real::new(0.3).ok()
                ]
            );
        }

        {
            // Case 1.2. Normal index, with PK, scan index prefix.

            let key_ranges = vec![{
                let mut range = KeyRange::default();
                let start_data = datum::encode_key(&mut ctx, &[Datum::I64(2)]).unwrap();
                let start_key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &start_data);
                range.set_start(start_key);
                let end_data = datum::encode_key(&mut ctx, &[Datum::I64(6)]).unwrap();
                let end_key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &end_data);
                range.set_end(end_key);
                range
            }];

            let mut executor = BatchIndexScanExecutor::new(
                store,
                Arc::new(EvalConfig::default()),
                vec![
                    columns_info[0].clone(),
                    columns_info[1].clone(),
                    columns_info[2].clone(),
                ],
                key_ranges,
                0,
                false,
                false,
                false,
            )
            .unwrap();

            let mut result = executor.next_batch(10);
            assert!(result.is_drained.as_ref().unwrap());
            assert_eq!(result.physical_columns.columns_len(), 3);
            assert_eq!(result.physical_columns.rows_len(), 2);
            assert!(result.physical_columns[0].is_raw());
            result.physical_columns[0]
                .ensure_all_decoded_for_test(&mut ctx, &schema[0])
                .unwrap();
            assert_eq!(
                result.physical_columns[0].decoded().to_int_vec(),
                &[Some(5), Some(5)]
            );
            assert!(result.physical_columns[1].is_raw());
            result.physical_columns[1]
                .ensure_all_decoded_for_test(&mut ctx, &schema[1])
                .unwrap();
            assert_eq!(
                result.physical_columns[1].decoded().to_real_vec(),
                &[Real::new(5.1).ok(), Real::new(10.5).ok()]
            );
            assert!(result.physical_columns[2].is_decoded());
            assert_eq!(
                result.physical_columns[2].decoded().to_int_vec(),
                &[Some(5), Some(2)]
            );
        }

        // Case 2. Unique index.

        // For a unique index, the PK handle is stored in the value.

        let store = {
            let kv: Vec<_> = data
                .iter()
                .map(|datums| {
                    let index_data = datum::encode_key(&mut ctx, &datums[0..2]).unwrap();
                    let key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &index_data);
                    // PK handle in the value
                    let mut value = vec![];
                    value
                        .write_u64(datums[2].as_int().unwrap().unwrap() as u64)
                        .unwrap();
                    (key, value)
                })
                .collect();
            FixtureStorage::from(kv)
        };

        {
            // Case 2.1. Unique index, prefix range scan.

            let key_ranges = vec![{
                let mut range = KeyRange::default();
                let start_data = datum::encode_key(&mut ctx, &[Datum::I64(5)]).unwrap();
                let start_key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &start_data);
                range.set_start(start_key);
                range.set_end(range.get_start().to_vec());
                convert_to_prefix_next(range.mut_end());
                range
            }];

            let mut executor = BatchIndexScanExecutor::new(
                store.clone(),
                Arc::new(EvalConfig::default()),
                vec![
                    columns_info[0].clone(),
                    columns_info[1].clone(),
                    columns_info[2].clone(),
                ],
                key_ranges,
                0,
                false,
                false,
                false,
            )
            .unwrap();

            let mut result = executor.next_batch(10);
            assert!(result.is_drained.as_ref().unwrap());
            assert_eq!(result.physical_columns.columns_len(), 3);
            assert_eq!(result.physical_columns.rows_len(), 2);
            assert!(result.physical_columns[0].is_raw());
            result.physical_columns[0]
                .ensure_all_decoded_for_test(&mut ctx, &schema[0])
                .unwrap();
            assert_eq!(
                result.physical_columns[0].decoded().to_int_vec(),
                &[Some(5), Some(5)]
            );
            assert!(result.physical_columns[1].is_raw());
            result.physical_columns[1]
                .ensure_all_decoded_for_test(&mut ctx, &schema[1])
                .unwrap();
            assert_eq!(
                result.physical_columns[1].decoded().to_real_vec(),
                &[Real::new(5.1).ok(), Real::new(10.5).ok()]
            );
            assert!(result.physical_columns[2].is_decoded());
            assert_eq!(
                result.physical_columns[2].decoded().to_int_vec(),
                &[Some(5), Some(2)]
            );
        }

        {
            // Case 2.2. Unique index, point scan.

            let key_ranges = vec![{
                let mut range = KeyRange::default();
                let start_data =
                    datum::encode_key(&mut ctx, &[Datum::I64(5), Datum::F64(5.1)]).unwrap();
                let start_key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &start_data);
                range.set_start(start_key);
                range.set_end(range.get_start().to_vec());
                convert_to_prefix_next(range.mut_end());
                range
            }];

            let mut executor = BatchIndexScanExecutor::new(
                store,
                Arc::new(EvalConfig::default()),
                vec![
                    columns_info[0].clone(),
                    columns_info[1].clone(),
                    columns_info[2].clone(),
                ],
                key_ranges,
                0,
                false,
                true,
                false,
            )
            .unwrap();

            let mut result = executor.next_batch(10);
            assert!(result.is_drained.as_ref().unwrap());
            assert_eq!(result.physical_columns.columns_len(), 3);
            assert_eq!(result.physical_columns.rows_len(), 1);
            assert!(result.physical_columns[0].is_raw());
            result.physical_columns[0]
                .ensure_all_decoded_for_test(&mut ctx, &schema[0])
                .unwrap();
            assert_eq!(
                result.physical_columns[0].decoded().to_int_vec(),
                &[Some(5)]
            );
            assert!(result.physical_columns[1].is_raw());
            result.physical_columns[1]
                .ensure_all_decoded_for_test(&mut ctx, &schema[1])
                .unwrap();
            assert_eq!(
                result.physical_columns[1].decoded().to_real_vec(),
                &[Real::new(5.1).ok()]
            );
            assert!(result.physical_columns[2].is_decoded());
            assert_eq!(
                result.physical_columns[2].decoded().to_int_vec(),
                &[Some(5)]
            );
        }
    }

    #[test]
    fn test_unique_common_handle_index() {
        const TABLE_ID: i64 = 3;
        const INDEX_ID: i64 = 42;
        let columns_info = vec![
            {
                let mut ci = ColumnInfo::default();
                ci.set_column_id(1);
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci
            },
            {
                let mut ci = ColumnInfo::default();
                ci.set_column_id(2);
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci
            },
            {
                let mut ci = ColumnInfo::default();
                ci.set_column_id(3);
                ci.as_mut_accessor().set_tp(FieldTypeTp::Double);
                ci
            },
        ];

        // The schema of these columns. Used to check executor output.
        let schema: Vec<FieldType> = vec![
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::Double.into(),
        ];

        let columns = vec![Column::new(1, 2), Column::new(2, 3), Column::new(3, 4.0)];
        let datums = vec![Datum::U64(2), Datum::U64(3), Datum::F64(4.0)];

        let mut value_prefix = vec![];
        let mut restore_data = vec![];
        let common_handle =
            datum::encode_value(&mut EvalContext::default(), &[Datum::F64(4.0)]).unwrap();

        restore_data
            .write_row(&mut EvalContext::default(), columns)
            .unwrap();

        // Tail length
        value_prefix.push(0);
        // Common handle flag
        value_prefix.push(127);
        // Common handle length
        value_prefix.write_u16(common_handle.len() as u16).unwrap();

        // Common handle
        value_prefix.extend(common_handle);

        let index_data = datum::encode_key(&mut EvalContext::default(), &datums[0..2]).unwrap();
        let key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &index_data);

        let key_ranges = vec![{
            let mut range = KeyRange::default();
            let start_key = key.clone();
            range.set_start(start_key);
            range.set_end(range.get_start().to_vec());
            convert_to_prefix_next(range.mut_end());
            range
        }];

        // 1. New collation unique common handle.
        let mut value = value_prefix.clone();
        value.extend(restore_data);
        let store = FixtureStorage::from(vec![(key.clone(), value)]);
        let mut executor = BatchIndexScanExecutor::new(
            store,
            Arc::new(EvalConfig::default()),
            columns_info.clone(),
            key_ranges.clone(),
            1,
            false,
            true,
            false,
        )
        .unwrap();

        let mut result = executor.next_batch(10);
        assert!(result.is_drained.as_ref().unwrap());
        assert_eq!(result.physical_columns.columns_len(), 3);
        assert_eq!(result.physical_columns.rows_len(), 1);
        assert!(result.physical_columns[0].is_raw());
        result.physical_columns[0]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schema[0])
            .unwrap();
        assert_eq!(
            result.physical_columns[0].decoded().to_int_vec(),
            &[Some(2)]
        );
        assert!(result.physical_columns[1].is_raw());
        result.physical_columns[1]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schema[1])
            .unwrap();
        assert_eq!(
            result.physical_columns[1].decoded().to_int_vec(),
            &[Some(3)]
        );
        assert!(result.physical_columns[2].is_raw());
        result.physical_columns[2]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schema[2])
            .unwrap();
        assert_eq!(
            result.physical_columns[2].decoded().to_real_vec(),
            &[Real::new(4.0).ok()]
        );

        let value = value_prefix;
        let store = FixtureStorage::from(vec![(key, value)]);
        let mut executor = BatchIndexScanExecutor::new(
            store,
            Arc::new(EvalConfig::default()),
            columns_info,
            key_ranges,
            1,
            false,
            true,
            false,
        )
        .unwrap();

        let mut result = executor.next_batch(10);
        assert!(result.is_drained.as_ref().unwrap());
        assert_eq!(result.physical_columns.columns_len(), 3);
        assert_eq!(result.physical_columns.rows_len(), 1);
        assert!(result.physical_columns[0].is_raw());
        result.physical_columns[0]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schema[0])
            .unwrap();
        assert_eq!(
            result.physical_columns[0].decoded().to_int_vec(),
            &[Some(2)]
        );
        assert!(result.physical_columns[1].is_raw());
        result.physical_columns[1]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schema[1])
            .unwrap();
        assert_eq!(
            result.physical_columns[1].decoded().to_int_vec(),
            &[Some(3)]
        );
        assert!(result.physical_columns[2].is_raw());
        result.physical_columns[2]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schema[2])
            .unwrap();
        assert_eq!(
            result.physical_columns[2].decoded().to_real_vec(),
            &[Real::new(4.0).ok()]
        );
    }

    #[test]
    fn test_old_collation_non_unique_common_handle_index() {
        const TABLE_ID: i64 = 3;
        const INDEX_ID: i64 = 42;
        let columns_info = vec![
            {
                let mut ci = ColumnInfo::default();
                ci.set_column_id(1);
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci
            },
            {
                let mut ci = ColumnInfo::default();
                ci.set_column_id(2);
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci
            },
            {
                let mut ci = ColumnInfo::default();
                ci.set_column_id(3);
                ci.as_mut_accessor().set_tp(FieldTypeTp::Double);
                ci
            },
        ];

        // The schema of these columns. Used to check executor output.
        let schema: Vec<FieldType> = vec![
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::Double.into(),
        ];

        let datums = vec![Datum::U64(2), Datum::U64(3), Datum::F64(4.0)];

        let common_handle = datum::encode_key(
            &mut EvalContext::default(),
            &[Datum::U64(3), Datum::F64(4.0)],
        )
        .unwrap();

        let index_data = datum::encode_key(&mut EvalContext::default(), &datums[0..1]).unwrap();
        let mut key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &index_data);
        key.extend(common_handle);

        let key_ranges = vec![{
            let mut range = KeyRange::default();
            let start_key = key.clone();
            range.set_start(start_key);
            range.set_end(range.get_start().to_vec());
            convert_to_prefix_next(range.mut_end());
            range
        }];

        let store = FixtureStorage::from(vec![(key, vec![])]);
        let mut executor = BatchIndexScanExecutor::new(
            store,
            Arc::new(EvalConfig::default()),
            columns_info,
            key_ranges,
            2,
            false,
            false,
            false,
        )
        .unwrap();

        let mut result = executor.next_batch(10);
        assert!(result.is_drained.as_ref().unwrap());
        assert_eq!(result.physical_columns.columns_len(), 3);
        assert_eq!(result.physical_columns.rows_len(), 1);
        assert!(result.physical_columns[0].is_raw());
        result.physical_columns[0]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schema[0])
            .unwrap();
        assert_eq!(
            result.physical_columns[0].decoded().to_int_vec(),
            &[Some(2)]
        );
        assert!(result.physical_columns[1].is_raw());
        result.physical_columns[1]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schema[1])
            .unwrap();
        assert_eq!(
            result.physical_columns[1].decoded().to_int_vec(),
            &[Some(3)]
        );
        assert!(result.physical_columns[2].is_raw());
        result.physical_columns[2]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schema[2])
            .unwrap();
        assert_eq!(
            result.physical_columns[2].decoded().to_real_vec(),
            &[Real::new(4.0).ok()]
        );
    }

    #[test]
    fn test_new_collation_unique_int_handle_index() {
        const TABLE_ID: i64 = 3;
        const INDEX_ID: i64 = 42;
        let columns_info = vec![
            {
                let mut ci = ColumnInfo::default();
                ci.set_column_id(1);
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci
            },
            {
                let mut ci = ColumnInfo::default();
                ci.set_column_id(2);
                ci.as_mut_accessor().set_tp(FieldTypeTp::Double);
                ci
            },
            {
                let mut ci = ColumnInfo::default();
                ci.set_column_id(3);
                ci.set_pk_handle(true);
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci
            },
        ];

        // The schema of these columns. Used to check executor output.
        let schema: Vec<FieldType> = vec![
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::Double.into(),
            FieldTypeTp::LongLong.into(),
        ];

        let columns = vec![Column::new(1, 2), Column::new(2, 3.0), Column::new(3, 4)];
        let datums = vec![Datum::U64(2), Datum::F64(3.0), Datum::U64(4)];
        let index_data = datum::encode_key(&mut EvalContext::default(), &datums[0..2]).unwrap();
        let key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &index_data);

        let mut restore_data = vec![];
        restore_data
            .write_row(&mut EvalContext::default(), columns)
            .unwrap();
        let mut value = vec![8];
        value.extend(restore_data);
        value
            .write_u64(datums[2].as_int().unwrap().unwrap() as u64)
            .unwrap();

        let key_ranges = vec![{
            let mut range = KeyRange::default();
            let start_key = key.clone();
            range.set_start(start_key);
            range.set_end(range.get_start().to_vec());
            convert_to_prefix_next(range.mut_end());
            range
        }];

        let store = FixtureStorage::from(vec![(key, value)]);
        let mut executor = BatchIndexScanExecutor::new(
            store,
            Arc::new(EvalConfig::default()),
            columns_info,
            key_ranges,
            0,
            false,
            true,
            false,
        )
        .unwrap();

        let mut result = executor.next_batch(10);
        assert!(result.is_drained.as_ref().unwrap());
        assert_eq!(result.physical_columns.columns_len(), 3);
        assert_eq!(result.physical_columns.rows_len(), 1);
        assert!(result.physical_columns[0].is_raw());
        result.physical_columns[0]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schema[0])
            .unwrap();
        assert_eq!(
            result.physical_columns[0].decoded().to_int_vec(),
            &[Some(2)]
        );
        assert!(result.physical_columns[1].is_raw());
        result.physical_columns[1]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schema[1])
            .unwrap();
        assert_eq!(
            result.physical_columns[1].decoded().to_real_vec(),
            &[Real::new(3.0).ok()]
        );
        assert!(result.physical_columns[2].is_decoded());
        assert_eq!(
            result.physical_columns[2].decoded().to_int_vec(),
            &[Some(4)]
        );
    }

    #[test]
    fn test_new_collation_non_unique_int_handle_index() {
        const TABLE_ID: i64 = 3;
        const INDEX_ID: i64 = 42;
        let columns_info = vec![
            {
                let mut ci = ColumnInfo::default();
                ci.set_column_id(1);
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci
            },
            {
                let mut ci = ColumnInfo::default();
                ci.set_column_id(2);
                ci.as_mut_accessor().set_tp(FieldTypeTp::Double);
                ci
            },
            {
                let mut ci = ColumnInfo::default();
                ci.set_column_id(3);
                ci.set_pk_handle(true);
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci
            },
        ];

        // The schema of these columns. Used to check executor output.
        let schema: Vec<FieldType> = vec![
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::Double.into(),
            FieldTypeTp::LongLong.into(),
        ];

        let columns = vec![Column::new(1, 2), Column::new(2, 3.0), Column::new(3, 4)];
        let datums = vec![Datum::U64(2), Datum::F64(3.0), Datum::U64(4)];
        let index_data = datum::encode_key(&mut EvalContext::default(), &datums).unwrap();
        let key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &index_data);

        let mut restore_data = vec![];
        restore_data
            .write_row(&mut EvalContext::default(), columns)
            .unwrap();
        let mut value = vec![0];
        value.extend(restore_data);

        let key_ranges = vec![{
            let mut range = KeyRange::default();
            let start_key = key.clone();
            range.set_start(start_key);
            range.set_end(range.get_start().to_vec());
            convert_to_prefix_next(range.mut_end());
            range
        }];

        let store = FixtureStorage::from(vec![(key, value)]);
        let mut executor = BatchIndexScanExecutor::new(
            store,
            Arc::new(EvalConfig::default()),
            columns_info,
            key_ranges,
            0,
            false,
            true,
            false,
        )
        .unwrap();

        let mut result = executor.next_batch(10);
        assert!(result.is_drained.as_ref().unwrap());
        assert_eq!(result.physical_columns.columns_len(), 3);
        assert_eq!(result.physical_columns.rows_len(), 1);
        assert!(result.physical_columns[0].is_raw());
        result.physical_columns[0]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schema[0])
            .unwrap();
        assert_eq!(
            result.physical_columns[0].decoded().to_int_vec(),
            &[Some(2)]
        );
        assert!(result.physical_columns[1].is_raw());
        result.physical_columns[1]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schema[1])
            .unwrap();
        assert_eq!(
            result.physical_columns[1].decoded().to_real_vec(),
            &[Real::new(3.0).ok()]
        );
        assert!(result.physical_columns[2].is_decoded());
        assert_eq!(
            result.physical_columns[2].decoded().to_int_vec(),
            &[Some(4)]
        );
    }

    #[test]
    fn test_new_collation_non_unique_common_handle_index() {
        const TABLE_ID: i64 = 3;
        const INDEX_ID: i64 = 42;
        let columns_info = vec![
            {
                let mut ci = ColumnInfo::default();
                ci.set_column_id(1);
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci
            },
            {
                let mut ci = ColumnInfo::default();
                ci.set_column_id(2);
                ci.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
                ci
            },
            {
                let mut ci = ColumnInfo::default();
                ci.set_column_id(3);
                ci.as_mut_accessor().set_tp(FieldTypeTp::Double);
                ci
            },
        ];

        // The schema of these columns. Used to check executor output.
        let schema: Vec<FieldType> = vec![
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::Double.into(),
        ];

        let columns = vec![Column::new(1, 2), Column::new(2, 3), Column::new(3, 4.0)];
        let datums = vec![Datum::U64(2), Datum::U64(3), Datum::F64(4.0)];
        let index_data = datum::encode_key(&mut EvalContext::default(), &datums).unwrap();
        let key = table::encode_index_seek_key(TABLE_ID, INDEX_ID, &index_data);

        let mut restore_data = vec![];
        restore_data
            .write_row(&mut EvalContext::default(), columns)
            .unwrap();
        let mut value = vec![0];
        value.extend(restore_data);

        let key_ranges = vec![{
            let mut range = KeyRange::default();
            let start_key = key.clone();
            range.set_start(start_key);
            range.set_end(range.get_start().to_vec());
            convert_to_prefix_next(range.mut_end());
            range
        }];

        let store = FixtureStorage::from(vec![(key, value)]);
        let mut executor = BatchIndexScanExecutor::new(
            store,
            Arc::new(EvalConfig::default()),
            columns_info,
            key_ranges,
            1,
            false,
            true,
            false,
        )
        .unwrap();

        let mut result = executor.next_batch(10);
        assert!(result.is_drained.as_ref().unwrap());
        assert_eq!(result.physical_columns.columns_len(), 3);
        assert_eq!(result.physical_columns.rows_len(), 1);
        assert!(result.physical_columns[0].is_raw());
        result.physical_columns[0]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schema[0])
            .unwrap();
        assert_eq!(
            result.physical_columns[0].decoded().to_int_vec(),
            &[Some(2)]
        );
        assert!(result.physical_columns[1].is_raw());
        result.physical_columns[1]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schema[1])
            .unwrap();
        assert_eq!(
            result.physical_columns[1].decoded().to_int_vec(),
            &[Some(3)]
        );
        assert!(result.physical_columns[2].is_raw());
        result.physical_columns[2]
            .ensure_all_decoded_for_test(&mut EvalContext::default(), &schema[2])
            .unwrap();
        assert_eq!(
            result.physical_columns[2].decoded().to_real_vec(),
            &[Real::new(4.0).ok()]
        );
    }
}
