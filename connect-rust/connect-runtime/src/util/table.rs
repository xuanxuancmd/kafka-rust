/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::collections::HashMap;

/// A generic table data structure with row and column keys
pub struct Table<R, C, V> {
    table: HashMap<R, HashMap<C, V>>,
}

impl<R, C, V> Table<R, C, V>
where
    R: Eq + std::hash::Hash + Clone,
    C: Eq + std::hash::Hash + Clone,
    V: Clone,
{
    /// Create a new empty table
    pub fn new() -> Self {
        Self {
            table: HashMap::new(),
        }
    }

    /// Put a value into the table
    pub fn put(&mut self, row: R, column: C, value: V) -> Option<V> {
        let columns = self.table.entry(row).or_insert_with(HashMap::new);
        columns.insert(column, value)
    }

    /// Get a value from the table
    pub fn get(&self, row: &R, column: &C) -> Option<&V> {
        self.table.get(row).and_then(|columns| columns.get(column))
    }

    /// Remove an entire row from the table
    pub fn remove_row(&mut self, row: &R) -> Option<HashMap<C, V>> {
        self.table.remove(row)
    }

    /// Remove a single cell from the table
    pub fn remove(&mut self, row: &R, column: &C) -> Option<V> {
        if let Some(columns) = self.table.get_mut(row) {
            let value = columns.remove(column);
            if columns.is_empty() {
                self.table.remove(row);
            }
            value
        } else {
            None
        }
    }

    /// Get a copy of a row
    pub fn row(&self, row: &R) -> HashMap<C, V> {
        self.table
            .get(row)
            .map(|columns| columns.clone())
            .unwrap_or_else(|| HashMap::new())
    }

    /// Check if the table is empty
    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
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
