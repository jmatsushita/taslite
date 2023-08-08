import stream from "stream";
import Database, * as sqlite from "better-sqlite3";

import * as tasl from "tasl";

import { Decoder } from "./decoder.js";
import { Encoder } from "./encoder.js";
import {
  getTableName,
  getPropertyName,
  getPropertyType,
  getProperties,
  parseValue,
  serializeValue,
  Values,
} from "./utils.js";
import { applyExpression } from "./apply.js";

export class DB {
  readonly #schema: tasl.Schema;
  readonly #database: sqlite.Database;

  readonly #has: sqlite.Statement[] = [];
  readonly #get: sqlite.Statement[] = [];
  readonly #count: sqlite.Statement[] = [];
  readonly #insert: sqlite.Statement[] = [];
  readonly #upsert: sqlite.Statement[] = [];

  private constructor(schema: tasl.Schema, database: sqlite.Database) {
    this.#schema = schema;
    this.#database = database;

    for (const [key, type, index] of schema.entries()) {
      const tableName = getTableName(index);

      // TODO: Set the sequence id to start at 0 https://stackoverflow.com/a/26332544

      this.#has.push(
        this.#database.prepare(`SELECT id FROM ${tableName} WHERE id = :id`)
      );

      this.#get.push(
        this.#database.prepare(`SELECT * FROM ${tableName} WHERE id = :id`)
      );

      this.#count.push(
        this.#database.prepare(`SELECT COUNT(*) AS count FROM ${tableName}`)
      );

      const updates: string[] = [];
      const insertNames: string[] = [];
      const insertValues: string[] = [];
      for (const [path] of getProperties(type, [])) {
        const name = getPropertyName(path);
        updates.push(`${name} = :${name}`);
        insertNames.push(name);
        insertValues.push(`:${name}`);
      }

      const names = insertNames.join(", ");
      const values = insertValues.join(", ");
      this.#insert.push(
        this.#database.prepare(
          `INSERT INTO ${tableName} (${names}) VALUES (${values}) RETURNING id`
        )
      );

      this.#upsert.push(
        this.#database.prepare(
          `INSERT INTO ${tableName} VALUES (:id, ${values}) ON CONFLICT (id) DO UPDATE SET ${updates}`
        )
      );
    }

    this.#database.pragma("foreign_keys = ON");
  }

  private static schemaId = 0;

  private static setSchema(
    database: sqlite.Database,
    schema: tasl.Schema
  ): Promise<void> {
    const createSchemaTable = database.prepare(
      "CREATE TABLE schemas (id INTEGER PRIMARY KEY NOT NULL, value BLOB NOT NULL)"
    );

    createSchemaTable.run();

    const insertIntoSchemaTable = database.prepare(
      "INSERT INTO schemas VALUES (?, ?)"
    );

    insertIntoSchemaTable.run(DB.schemaId, tasl.encodeSchema(schema));

    for (const [_, type, index] of schema.entries()) {
      const tableName = getTableName(index);

      const columns: string[] = [
        "id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL",
      ];

      for (const [path, property, optional] of getProperties(type)) {
        const name = getPropertyName(path);
        const propertyType = getPropertyType(schema, property);
        columns.push(
          optional
            ? `${name} ${propertyType}`
            : `${name} ${propertyType} NOT NULL`
        );
      }

      database
        .prepare(`CREATE TABLE ${tableName} (${columns.join(", ")})`)
        .run();
    }

    return Promise.resolve();
  }

  private static getSchema(db: sqlite.Database): tasl.Schema {
    const row = db
      .prepare("SELECT value FROM schemas WHERE id = ?")
      .get(DB.schemaId) as { value: Buffer };
    return tasl.decodeSchema(row.value);
  }

  public static open(path: string, options: { readOnly?: boolean } = {}): DB {
    const database = new Database(path, {
      fileMustExist: true,
      readonly: options.readOnly,
    });

    const schema = DB.getSchema(database);
    return new DB(schema, database);
  }

  public static create(path: string | null, schema: tasl.Schema): DB {
    const database = new Database(path === null ? ":memory:" : path);
    this.setSchema(database, schema);
    return new DB(schema, database);
  }

  public static async import(
    path: string | null,
    schema: tasl.Schema,
    stream: AsyncIterable<Buffer>
  ): Promise<DB> {
    const db = DB.create(path, schema);
    db.#database.pragma("foreign_keys = OFF");

    const decoder = new Decoder(stream);

    const version = await decoder.decodeUnsignedVarint();
    if (version !== tasl.version) {
      throw new Error("unsupported encoding verison");
    }

    for (const [key, type, index] of schema.entries()) {
      for await (const row of decoder.forRows(type)) {
        db.#upsert[index].run(row);
      }
    }

    const { value, done } = await decoder.iterator.next();
    if (!done || value !== undefined) {
      throw new Error("stream not closed when expected");
    }

    db.#database.pragma("foreign_keys = ON");
    return db;
  }

  public close() {
    this.#database.close();
  }

  public get schema(): tasl.Schema {
    return this.#schema;
  }

  public async *export(
    options: { chunkSize?: number } = {}
  ): AsyncIterable<Buffer> {
    const encoder = new Encoder(options);
    yield* encoder.encodeUnsignedVarint(tasl.version);
    for (const [key, type, index] of this.#schema.entries()) {
      const name = getTableName(index);
      const count = this.count(key);
      yield* encoder.encodeUnsignedVarint(count);

      const statement = this.#database.prepare(
        `SELECT * FROM ${name} ORDER BY id ASC`
      );
      let delta = 0;
      for (const row of statement.iterate() as any) {
        const id = row.id as number;
        yield* encoder.encodeUnsignedVarint(id - delta);
        yield* encoder.encodeValue(type, [], row);
        delta = id + 1;
      }
    }

    yield* encoder.close();
  }

  public get(key: string, id: number): tasl.values.Value {
    const index = this.#schema.indexOfKey(key);
    const type = this.#schema.get(key);

    const row = this.#get[index].get({ id }) as Values;
    if (row === undefined) {
      throw new Error(`no element in ${key} with id ${id}`);
    }

    return parseValue(type, [], row);
  }

  public *keys(key: string): Iterable<number> {
    const index = this.#schema.indexOfKey(key);
    const tableName = getTableName(index);
    const statement = this.#database.prepare(
      `SELECT id FROM ${tableName} ORDER BY id ASC`
    );
    for (const row of statement.iterate() as any) {
      yield row.id;
    }
  }

  public *values(key: string): Iterable<tasl.values.Value> {
    const index = this.#schema.indexOfKey(key);
    const type = this.#schema.get(key);
    const tableName = getTableName(index);

    const statement = this.#database.prepare(
      `SELECT * FROM ${tableName} ORDER BY id ASC`
    );
    for (const row of statement.iterate()) {
      yield parseValue(type, [], row as Values);
    }
  }

  public *entries(key: string): Iterable<[number, tasl.values.Value]> {
    const index = this.#schema.indexOfKey(key);
    const type = this.#schema.get(key);
    const tableName = getTableName(index);

    const statement = this.#database.prepare(
      `SELECT * FROM ${tableName} ORDER BY id ASC`
    );
    for (const row of statement.iterate() as any) {
      yield [row.id, parseValue(type, [], row)];
    }
  }

  public has(key: string, id: number): boolean {
    const index = this.#schema.indexOfKey(key);
    const rows = this.#has[index].get({ id });
    return rows !== undefined;
  }

  public count(key: string): number {
    const index = this.#schema.indexOfKey(key);
    const { count } = this.#count[index].get() as { count: number };
    return count;
  }

  public push(key: string, value: tasl.values.Value): number {
    const index = this.#schema.indexOfKey(key);
    const type = this.#schema.get(key);
    const params = serializeValue(type, value);
    const row = this.#insert[index].get(params) as { id: number };
    if (row === undefined || typeof row.id !== "number") {
      throw new Error("internal error inserting value");
    } else {
      return row.id;
    }
  }

  public set(key: string, id: number, value: tasl.values.Value) {
    const index = this.#schema.indexOfKey(key);
    const type = this.#schema.get(key);
    const params = serializeValue(type, value);
    this.#upsert[index].run({ id, ...params });
  }

  public merge(elements: Record<string, tasl.values.Element[]>) {
    // TODO: clean this up
    this.#database.pragma("foreign_keys = OFF");
    const txn = this.#database.transaction<
      (elements: Record<string, tasl.values.Element[]>) => void
    >((elements) => {
      for (const key of Object.keys(elements)) {
        const index = this.#schema.indexOfKey(key);
        const type = this.#schema.get(key);
        for (const element of elements[key]) {
          const params = serializeValue(type, element.value);
          this.#upsert[index].run({ id: element.id, ...params });
        }
      }
    });

    txn(elements);
    this.#database.pragma("foreign_keys = ON");
  }

  public async migrate(
    mapping: tasl.Mapping,
    targetPath: string | null
  ): Promise<DB> {
    if (!mapping.source.isEqualTo(this.#schema)) {
      throw new Error(
        "database schema is not equal to the mapping source schema"
      );
    }

    const target = DB.create(targetPath, mapping.target);

    for (const map of mapping.values()) {
      const sourceType = this.schema.get(map.source);
      const targetType = target.schema.get(map.target);
      for (const [id, sourceValue] of this.entries(map.source)) {
        const targetValue = applyExpression(
          (key, id) => [this.#schema.get(key), this.get(key, id)],
          map.value,
          targetType,
          { [map.id]: [sourceType, sourceValue] }
        );
        target.set(map.target, id, targetValue);
      }
    }

    return target;
  }
}
