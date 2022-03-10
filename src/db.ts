import sqlite3 from "sqlite3"

import * as tasl from "tasl"
import { makeDecodeState } from "tasl/lib/utils.js"
import { decodeValue } from "tasl/lib/instance/decodeInstance.js"

import { Decoder } from "./decoder.js"
import { Encoder } from "./encoder.js"
import {
	elementTableName,
	finalize,
	getQuery,
	getStatement,
	runQuery,
} from "./utils.js"

export class DB {
	readonly #schema: tasl.Schema
	readonly #db: sqlite3.Database

	private constructor(schema: tasl.Schema, db: sqlite3.Database) {
		this.#schema = schema
		this.#db = db
	}

	private static async setSchema(db: sqlite3.Database, schema: tasl.Schema) {
		const value = tasl.encodeSchema(schema)
		await runQuery(
			db,
			"CREATE TABLE schemas (id INTEGER PRIMARY KEY NOT NULL, value BLOB NOT NULL)"
		)

		await runQuery(db, "INSERT INTO schemas VALUES ($id, $value)", {
			$id: 0,
			$value: value,
		})

		for (const [key, type, index] of schema.entries()) {
			const name = elementTableName(index)
			await runQuery(
				db,
				`CREATE TABLE ${name} (id INTEGER PRIMARY KEY NOT NULL, value BLOB NOT NULL)`
			)
		}
	}

	private static getSchema(db: sqlite3.Database): Promise<tasl.Schema> {
		return new Promise((resolve, reject) => {
			db.get(
				"SELECT value FROM schemas WHERE id = $id",
				{ $id: 0 },
				(err, row) =>
					err ? reject(err) : resolve(tasl.decodeSchema(row.value))
			)
		})
	}

	public static async openDB(
		path: string | null,
		options: { readonly?: boolean } = {}
	): Promise<DB> {
		const mode = options.readonly
			? sqlite3.OPEN_READONLY
			: sqlite3.OPEN_READWRITE
		const db = new sqlite3.Database(path === null ? ":memory:" : path, mode)
		const schema = await DB.getSchema(db)
		return new DB(schema, db)
	}

	public static async createDB(
		path: string | null,
		schema: tasl.Schema
	): Promise<DB> {
		const mode = sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE
		const db = new sqlite3.Database(path === null ? ":memory:" : path, mode)
		await DB.setSchema(db, schema)
		return new DB(schema, db)
	}

	public static async import(
		path: string | null,
		schema: tasl.Schema,
		stream: AsyncIterable<Buffer>
	): Promise<DB> {
		const db = await DB.createDB(path, schema)

		const decoder = new Decoder(stream)

		const version = await decoder.decodeUnsignedVarint()
		if (version !== tasl.version) {
			throw new Error("unsupported encoding verison")
		}

		for (const [_, type, index] of schema.entries()) {
			const name = elementTableName(index)
			const statement = db.#db.prepare(
				`INSERT INTO ${name} VALUES ($id, $value)`
			)

			for await (const [id, value] of decoder.forElements(type)) {
				statement.run({ $id: id, $value: value })
			}

			await finalize(statement)
		}

		const { value, done } = await decoder.iterator.next()
		if (!done || value !== undefined) {
			throw new Error("stream not closed when expected")
		}

		return db
	}

	public close(): Promise<void> {
		return new Promise((resolve, reject) =>
			this.#db.close((err) => (err ? reject(err) : resolve()))
		)
	}

	public get schema(): tasl.Schema {
		return this.#schema
	}

	private static defaultChunkSize = 1024

	public async *export(
		options: { chunkSize?: number } = {}
	): AsyncIterable<Buffer> {
		const chunkSize = options.chunkSize || DB.defaultChunkSize
		const encoder = new Encoder(chunkSize)
		yield* encoder.encodeUnsignedVarint(tasl.version)
		for (const [key, type, index] of this.#schema.entries()) {
			const name = elementTableName(index)
			const { count } = await getQuery(
				this.#db,
				`SELECT COUNT(*) AS count FROM ${name}`
			)

			yield* encoder.encodeUnsignedVarint(count as number)

			const statement = this.#db.prepare(`SELECT id, value FROM ${name}`)
			let row = await getStatement(statement)
			let delta = 0
			while (row !== undefined) {
				const id = row.id as number
				yield* encoder.encodeUnsignedVarint(id - delta)
				yield* encoder.encodeValue(row.value as Buffer)
				delta = id + 1
				row = await getStatement(statement)
			}

			await finalize(statement)
		}

		yield* encoder.close()
	}

	async get(key: string, id: number): Promise<tasl.values.Value> {
		const index = this.#schema.indexOfKey(key)
		const name = elementTableName(index)
		const row = await getQuery(
			this.#db,
			`SELECT value FROM ${name} WHERE id = $id`,
			{ $id: id }
		)

		if (row === undefined) {
			throw new Error(`no element in ${key} with id ${id}`)
		} else if (row.value instanceof Uint8Array) {
			const state = makeDecodeState(row.value)
			return decodeValue(state, this.#schema.get(key))
		} else {
			throw new Error("internal error: unexpected type")
		}
	}

	async has(key: string, id: number): Promise<boolean> {
		const index = this.#schema.indexOfKey(key)
		const name = elementTableName(index)
		const row = await getQuery(
			this.#db,
			`SELECT id FROM ${name} WHERE id = $id`,
			{ $id: id }
		)

		return row !== undefined
	}

	async count(key: string): Promise<number> {
		const index = this.#schema.indexOfKey(key)
		const name = elementTableName(index)

		const { count } = await getQuery(
			this.#db,
			`SELECT COUNT(*) AS count FROM ${name}`
		)

		if (typeof count === "number") {
			return count
		} else {
			throw new Error("internal error: unexpected type")
		}
	}

	async match(key: string, path: number[], value: string): Promise<void> {}
}
