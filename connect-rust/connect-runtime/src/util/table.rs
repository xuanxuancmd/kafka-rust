// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Two-dimensional table structure.
//!
//! This corresponds to `org.apache.kafka.connect.util.Table` in Java.

use std::collections::HashMap;

/// A two-dimensional table structure with row and column keys.
///
/// This corresponds to `org.apache.kafka.connect.util.Table` in Java.
/// Provides a sparse table implementation where values can be stored
/// and retrieved using row and column keys.
#[derive(Debug, Clone)]
pub struct Table<R, C, V> {
    data: HashMap<R, HashMap<C, V>>,
}

impl<R, C, V> Table<R, C, V>
where
    R: Eq + std::hash::Hash + Clone,
    C: Eq + std::hash::Hash + Clone,
    V: Clone,
{
    /// Creates a new empty table.
    pub fn new() -> Self {
        Table {
            data: HashMap::new(),
        }
    }

    /// Inserts a value at the given row and column.
    ///
    /// Returns the previous value if one existed at this position.
    pub fn put(&mut self, row: R, column: C, value: V) -> Option<V> {
        let row_data = self.data.entry(row).or_insert_with(HashMap::new);
        row_data.insert(column, value)
    }

    /// Gets the value at the given row and column.
    ///
    /// Returns None if no value exists at this position.
    pub fn get(&self, row: &R, column: &C) -> Option<&V> {
        self.data.get(row).and_then(|row_data| row_data.get(column))
    }

    /// Removes the entire row from the table.
    ///
    /// Returns the removed row data as a HashMap, or None if the row didn't exist.
    pub fn remove_row(&mut self, row: &R) -> Option<HashMap<C, V>> {
        self.data.remove(row)
    }

    /// Removes a single cell from the table.
    ///
    /// Returns the removed value, or None if the cell didn't exist.
    pub fn remove(&mut self, row: &R, column: &C) -> Option<V> {
        if let Some(row_data) = self.data.get_mut(row) {
            let result = row_data.remove(column);
            if row_data.is_empty() {
                self.data.remove(row);
            }
            result
        } else {
            None
        }
    }

    /// Gets all values in a row as a copy of the row's HashMap.
    ///
    /// Returns an empty HashMap if the row doesn't exist.
    pub fn row(&self, row: &R) -> HashMap<C, V> {
        self.data.get(row).cloned().unwrap_or_default()
    }

    /// Returns true if the table is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Returns the number of rows in the table.
    pub fn row_count(&self) -> usize {
        self.data.len()
    }

    /// Returns the total number of cells in the table.
    pub fn cell_count(&self) -> usize {
        self.data.values().map(|row| row.len()).sum()
    }

    /// Clears all data from the table.
    pub fn clear(&mut self) {
        self.data.clear();
    }

    /// Returns an iterator over all rows.
    pub fn rows(&self) -> impl Iterator<Item = &R> {
        self.data.keys()
    }

    /// Returns an iterator over all (row, column, value) triples.
    pub fn iter(&self) -> impl Iterator<Item = (&R, &C, &V)> + '_ {
        self.data
            .iter()
            .flat_map(|(row, row_data)| row_data.iter().map(move |(col, val)| (row, col, val)))
    }
}

impl<R, C, V> Default for Table<R, C, V>
where
    R: Eq + std::hash::Hash + Clone,
    C: Eq + std::hash::Hash + Clone,
    V: Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_put_and_get() {
        let mut table: Table<String, String, i32> = Table::new();
        table.put("row1".to_string(), "col1".to_string(), 10);

        assert_eq!(
            table.get(&"row1".to_string(), &"col1".to_string()),
            Some(&10)
        );
        assert_eq!(table.get(&"row1".to_string(), &"col2".to_string()), None);
        assert_eq!(table.get(&"row2".to_string(), &"col1".to_string()), None);
    }

    #[test]
    fn test_table_overwrite() {
        let mut table: Table<String, String, i32> = Table::new();
        let old = table.put("row1".to_string(), "col1".to_string(), 10);
        assert!(old.is_none());

        let old = table.put("row1".to_string(), "col1".to_string(), 20);
        assert_eq!(old, Some(10));
        assert_eq!(
            table.get(&"row1".to_string(), &"col1".to_string()),
            Some(&20)
        );
    }

    #[test]
    fn test_table_remove_cell() {
        let mut table: Table<String, String, i32> = Table::new();
        table.put("row1".to_string(), "col1".to_string(), 10);
        table.put("row1".to_string(), "col2".to_string(), 20);

        let removed = table.remove(&"row1".to_string(), &"col1".to_string());
        assert_eq!(removed, Some(10));
        assert!(table
            .get(&"row1".to_string(), &"col1".to_string())
            .is_none());

        // Row should still exist
        assert!(table.data.contains_key(&"row1".to_string()));
    }

    #[test]
    fn test_table_remove_last_cell_in_row() {
        let mut table: Table<String, String, i32> = Table::new();
        table.put("row1".to_string(), "col1".to_string(), 10);

        let removed = table.remove(&"row1".to_string(), &"col1".to_string());
        assert_eq!(removed, Some(10));

        // Row should be removed since it's empty
        assert!(!table.data.contains_key(&"row1".to_string()));
        assert!(table.is_empty());
    }

    #[test]
    fn test_table_remove_row() {
        let mut table: Table<String, String, i32> = Table::new();
        table.put("row1".to_string(), "col1".to_string(), 10);
        table.put("row1".to_string(), "col2".to_string(), 20);
        table.put("row2".to_string(), "col1".to_string(), 30);

        let removed_row = table.remove_row(&"row1".to_string());
        assert!(removed_row.is_some());
        assert_eq!(removed_row.unwrap().len(), 2);

        assert!(!table.data.contains_key(&"row1".to_string()));
        assert!(table.data.contains_key(&"row2".to_string()));
    }

    #[test]
    fn test_table_row() {
        let mut table: Table<String, String, i32> = Table::new();
        table.put("row1".to_string(), "col1".to_string(), 10);
        table.put("row1".to_string(), "col2".to_string(), 20);

        let row_data = table.row(&"row1".to_string());
        assert_eq!(row_data.len(), 2);
        assert_eq!(row_data.get(&"col1".to_string()), Some(&10));

        // Non-existent row returns empty
        let empty_row = table.row(&"row2".to_string());
        assert!(empty_row.is_empty());
    }

    #[test]
    fn test_table_is_empty() {
        let mut table: Table<String, String, i32> = Table::new();
        assert!(table.is_empty());

        table.put("row1".to_string(), "col1".to_string(), 10);
        assert!(!table.is_empty());

        table.clear();
        assert!(table.is_empty());
    }

    #[test]
    fn test_table_counts() {
        let mut table: Table<String, String, i32> = Table::new();
        assert_eq!(table.row_count(), 0);
        assert_eq!(table.cell_count(), 0);

        table.put("row1".to_string(), "col1".to_string(), 10);
        table.put("row1".to_string(), "col2".to_string(), 20);
        table.put("row2".to_string(), "col1".to_string(), 30);

        assert_eq!(table.row_count(), 2);
        assert_eq!(table.cell_count(), 3);
    }
}
