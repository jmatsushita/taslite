import Database, * as sqlite3 from "better-sqlite3"

import * as tasl from "tasl"
import { floatToString } from "tasl/lib/utils.js"

import { xsd } from "@underlay/namespaces"

import { Decoder } from "./decoder.js"
import { Encoder } from "./encoder.js"
import { getTableName, getPropertyName, Values } from "./utils.js"
import { optionAtIndex } from "tasl"

export class DB {
	readonly #schema: tasl.Schema
	readonly #database: sqlite3.Database

	readonly #getElements: sqlite3.Statement[]
	readonly #hasElements: sqlite3.Statement[]
	readonly #insertElements: sqlite3.Statement[]
	readonly #countElements: sqlite3.Statement[]

	private constructor(schema: tasl.Schema, database: sqlite3.Database) {
		this.#schema = schema
		this.#database = database

		this.#getElements = []
		this.#hasElements = []
		this.#insertElements = []
		this.#countElements = []
		for (const [key, type, index] of schema.entries()) {
			const tableName = getTableName(index)

			this.#getElements.push(
				this.#database.prepare(`SELECT * FROM ${tableName} WHERE id = ?`)
			)

			this.#hasElements.push(
				this.#database.prepare(`SELECT id FROM ${tableName} WHERE id = ?`)
			)

			this.#countElements.push(
				this.#database.prepare(`SELECT COUNT(*) AS count FROM ${tableName}`)
			)

			const properties: string[] = [":id"]
			for (const [path] of DB.getProperties(type, [])) {
				properties.push(`:${getPropertyName(path)}`)
			}

			this.#insertElements.push(
				this.#database.prepare(
					`INSERT INTO ${tableName} VALUES (${properties.join(", ")})`
				)
			)
		}
	}

	private static schemaId = 0

	private static setSchema(
		database: sqlite3.Database,
		schema: tasl.Schema
	): Promise<void> {
		const createSchemaTable = database.prepare(
			"CREATE TABLE schemas (id INTEGER PRIMARY KEY NOT NULL, value BLOB NOT NULL)"
		)

		createSchemaTable.run()

		const insertIntoSchemaTable = database.prepare(
			"INSERT INTO schemas VALUES (?, ?)"
		)

		insertIntoSchemaTable.run(DB.schemaId, tasl.encodeSchema(schema))

		for (const [_, type, index] of schema.entries()) {
			const tableName = getTableName(index)

			const columns: string[] = ["id INTEGER PRIMARY KEY NOT NULL"]

			for (const [path, property, optional] of DB.getProperties(type)) {
				const name = getPropertyName(path)
				const propertyType = DB.getPropertyType(property)
				columns.push(
					optional
						? `${name} ${propertyType}`
						: `${name} ${propertyType} NOT NULL`
				)
			}

			database
				.prepare(`CREATE TABLE ${tableName} (${columns.join(", ")})`)
				.run()
		}

		return Promise.resolve()
	}

	private static integerDatatypes = new Set<string>([
		xsd.boolean,
		xsd.long,
		xsd.int,
		xsd.short,
		xsd.byte,
		xsd.unsignedLong,
		xsd.unsignedInt,
		xsd.unsignedShort,
		xsd.unsignedByte,
	])

	private static getPropertyType(property: tasl.types.Type): string {
		if (property.kind === "uri") {
			return "TEXT"
		} else if (property.kind === "literal") {
			if (DB.integerDatatypes.has(property.datatype)) {
				return "INTEGER"
			} else if (
				property.datatype === xsd.double ||
				property.datatype === xsd.float
			) {
				return "REAL"
			} else if (property.datatype === xsd.hexBinary) {
				return "BLOB"
			} else {
				return "TEXT"
			}
		} else if (property.kind === "coproduct") {
			return "INTEGER"
		} else if (property.kind === "reference") {
			return "INTEGER"
		} else {
			throw new Error("invalid property type")
		}
	}

	private static *getProperties(
		type: tasl.types.Type,
		path: number[] = [],
		optional = false
	): Iterable<[number[], tasl.types.Type, boolean]> {
		if (type.kind === "uri") {
			yield [path, type, optional]
		} else if (type.kind === "literal") {
			yield [path, type, optional]
		} else if (type.kind === "product") {
			for (const [_, component, index] of tasl.forComponents(type)) {
				yield* DB.getProperties(component, [...path, index], optional)
			}
		} else if (type.kind === "coproduct") {
			yield [path, type, optional]
			for (const [_, option, index] of tasl.forOptions(type)) {
				yield* DB.getProperties(option, [...path, index], true)
			}
		} else if (type.kind === "reference") {
			yield [path, type, optional]
		} else {
			throw new Error("invalid type")
		}
	}

	private static getSchema(db: sqlite3.Database): tasl.Schema {
		const row = db
			.prepare("SELECT value FROM schemas WHERE id = ?")
			.get(DB.schemaId)
		return tasl.decodeSchema(row.value)
	}

	public static openDB(path: string, options: { readOnly?: boolean } = {}): DB {
		const database = new Database(path, {
			fileMustExist: true,
			readonly: options.readOnly,
		})

		const schema = DB.getSchema(database)
		return new DB(schema, database)
	}

	public static createDB(path: string | null, schema: tasl.Schema): DB {
		const database = new Database(path === null ? ":memory:" : path)
		this.setSchema(database, schema)
		return new DB(schema, database)
	}

	public static async import(
		path: string | null,
		schema: tasl.Schema,
		stream: AsyncIterable<Buffer>
	): Promise<DB> {
		const db = DB.createDB(path, schema)

		const decoder = new Decoder(stream)

		const version = await decoder.decodeUnsignedVarint()
		if (version !== tasl.version) {
			throw new Error("unsupported encoding verison")
		}

		for (const [key, type, index] of schema.entries()) {
			for await (const row of decoder.forRows(type)) {
				for (const [path] of DB.getProperties(type)) {
					const name = getPropertyName(path)
					if (name in row) {
						continue
					} else {
						row[name] = null
					}
				}
				db.#insertElements[index].run(row)
			}
		}

		const { value, done } = await decoder.iterator.next()
		if (!done || value !== undefined) {
			throw new Error("stream not closed when expected")
		}

		return db
	}

	public close() {
		this.#database.close()
	}

	public get schema(): tasl.Schema {
		return this.#schema
	}

	public async *export(
		options: { chunkSize?: number } = {}
	): AsyncIterable<Buffer> {
		const encoder = new Encoder(options)
		yield* encoder.encodeUnsignedVarint(tasl.version)
		for (const [key, type, index] of this.#schema.entries()) {
			const name = getTableName(index)
			const count = this.count(key)
			yield* encoder.encodeUnsignedVarint(count)

			const statement = this.#database.prepare(`SELECT * FROM ${name}`)
			let delta = 0
			for (const row of statement.iterate()) {
				const id = row.id as number
				yield* encoder.encodeUnsignedVarint(id - delta)
				yield* encoder.encodeValue(type, [], row)
				delta = id + 1
			}
		}

		yield* encoder.close()
	}

	public get(key: string, id: number): tasl.values.Value {
		const index = this.#schema.indexOfKey(key)
		const type = this.#schema.get(key)
		const tableName = getTableName(index)

		const row = this.#database
			.prepare(`SELECT * FROM ${tableName} WHERE id = ?`)
			.get(id)

		if (row === undefined) {
			throw new Error(`no element in ${key} with id ${id}`)
		}

		return this.parseValue(type, [], row)
	}

	public *entries(key: string): Iterable<[number, tasl.values.Value]> {
		const index = this.#schema.indexOfKey(key)
		const type = this.#schema.get(key)
		const tableName = getTableName(index)

		const statement = this.#database.prepare(`SELECT * FROM ${tableName}`)
		for (const row of statement.iterate()) {
			yield [row.id, this.parseValue(type, [], row)]
		}

		// for (let row = statement.get(); row !== undefined; row = statement.get()) {
		// 	yield [row.id, this.parseValue(type, [], row)]
		// }
	}

	private parseValue(
		type: tasl.types.Type,
		path: number[],
		row: Values
	): tasl.values.Value {
		const name = getPropertyName(path)

		if (type.kind === "uri") {
			const value = row[name]
			if (typeof value !== "string") {
				throw new Error(
					`internal error parsing value: invalid property ${name}`
				)
			}

			return tasl.values.uri(value)
		} else if (type.kind === "literal") {
			const value = DB.parseLiteralValue(type.datatype, row[name])
			return tasl.values.literal(value)
		} else if (type.kind === "product") {
			const components: Record<string, tasl.values.Value> = {}
			for (const [key, component, index] of tasl.forComponents(type)) {
				components[key] = this.parseValue(component, [...path, index], row)
			}
			return tasl.values.product(components)
		} else if (type.kind === "coproduct") {
			const name = getPropertyName(path)
			const index = row[name]

			if (typeof index !== "number") {
				throw new Error(
					`internal error parsing value: invalid property ${name}`
				)
			}

			const [key, option] = optionAtIndex(type, index)
			return tasl.values.coproduct(
				key,
				this.parseValue(option, [...path, index], row)
			)
		} else if (type.kind === "reference") {
			const name = getPropertyName(path)
			const value = row[name]
			if (typeof value !== "number") {
				throw new Error(
					`internal error parsing value: invalid property ${name}`
				)
			}
			return tasl.values.reference(value)
		} else {
			throw new Error("invalid type")
		}
	}

	private static parseLiteralValue(
		datatype: string,
		value: string | number | Buffer | null
	): string {
		if (DB.integerDatatypes.has(datatype)) {
			if (typeof value !== "number") {
				throw new Error(`internal error parsing property value`)
			}

			if (datatype === xsd.boolean) {
				if (value === 0) {
					return "false"
				} else if (value === 1) {
					return "true"
				} else {
					throw new Error("interal error: invalid boolean value")
				}
			} else {
				return value.toString()
			}
		} else if (datatype === xsd.double || datatype === xsd.float) {
			if (typeof value !== "number") {
				throw new Error(`internal error parsing property value`)
			}

			return floatToString(value)
		} else if (datatype === xsd.hexBinary) {
			if (!Buffer.isBuffer(value)) {
				throw new Error(`internal error parsing property value`)
			}

			return value.toString("hex")
		} else {
			if (typeof value !== "string") {
				throw new Error(`internal error parsing property value`)
			}

			return value
		}
	}

	public has(key: string, id: number): boolean {
		const index = this.#schema.indexOfKey(key)
		const rows = this.#hasElements[index].get(id)
		return rows !== undefined
	}

	public count(key: string): number {
		const index = this.#schema.indexOfKey(key)
		const { count } = this.#countElements[index].get()
		return count
	}

	public async migrate(
		mapping: tasl.Mapping,
		targetPath: string | null
	): Promise<DB> {
		if (!mapping.source.isEqualTo(this.#schema)) {
			throw new Error(
				"database schema is not equal to the mapping source schema"
			)
		}

		const target = await DB.createDB(targetPath, mapping.target)

		return target
	}
}
