/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.scrambledb;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.*;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.*;
import org.apache.calcite.schema.impl.AbstractTableQueryable;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public class ScrambledbInMemoryTable
    extends AbstractQueryableTable implements ScannableTable, ModifiableTable {

  private final String tableName;
  private List<@Nullable Object[]> list = new ArrayList<Object[]>();
  private final RelProtoDataType protoRowType;

  public ScrambledbInMemoryTable(String name, RelProtoDataType protoRowType, List<@Nullable Object[]> data) {
    super(Object[].class);
    this.tableName = name;
    this.protoRowType = protoRowType;
    this.list = data;
  }

  @Override public String toString() {
    return "inMemoryTable {" + this.tableName + "}";
  }

  public Collection<@Nullable Object[]> getModifiableCollection() {
    return list;
  }

  @Override
  public TableModify toModificationRel(RelOptCluster cluster, RelOptTable table,
      Prepare.CatalogReader catalogReader, RelNode child, TableModify.Operation operation,
      @Nullable List<String> updateColumnList, @Nullable List<RexNode> sourceExpressionList,
      boolean flattened) {
    return new LogicalTableModify(cluster, cluster.traitSetOf(Convention.NONE),
        table, catalogReader, child, operation, updateColumnList,
        sourceExpressionList, flattened);
  }

  @Override public <T> Queryable<T> asQueryable(@NonNull QueryProvider queryProvider,
      @NonNull SchemaPlus schema, @NonNull String tableName) {
    return new AbstractTableQueryable<T>(queryProvider, schema, this,
        tableName) {
      @Override public Enumerator<T> enumerator() {
        //noinspection unchecked
        return (Enumerator<T>) Linq4j.enumerator(list);
      }
    };
  }

  @Override public Type getElementType() {
    return Object[].class;
  }

  @Override
  public Expression getExpression(SchemaPlus schema, String tableName, Class clazz) {
    return Schemas.tableExpression(schema, elementType, tableName, Queryable.class);
  }

  @Override public RelDataType getRowType(@NonNull RelDataTypeFactory typeFactory) {
    return protoRowType.apply(typeFactory);
  }

  @Override
  public <C> C unwrapOrThrow(Class<C> aClass) {
    return super.unwrapOrThrow(aClass);
  }

  @Override
  public <C> Optional<C> maybeUnwrap(Class<C> aClass) {
    return super.maybeUnwrap(aClass);
  }

  @Override
  public Enumerable<@Nullable Object[]> scan(DataContext root) {
    return Linq4j.asEnumerable(this.list);
  }
}
