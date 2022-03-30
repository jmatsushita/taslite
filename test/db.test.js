import test from "ava"
import fs from "node:fs"
import * as tasl from "tasl"

import { DB } from "../lib/index.js"
import { microInstance, microSchema } from "./instances/micro.js"

test("DB.has", async (t) => {
	const data = Buffer.from(tasl.encodeInstance(microInstance))
	async function* stream() {
		yield data
	}

	const db = await DB.import(null, microSchema, stream())

	t.true(db.has("http://example.com/a", 0))
	t.false(db.has("http://example.com/a", 1))
	t.false(db.has("http://example.com/a", 3))

	t.true(db.has("http://example.com/b", 0))
	t.true(db.has("http://example.com/b", 1))
	t.true(db.has("http://example.com/b", 2))
	t.true(db.has("http://example.com/b", 3))
	t.false(db.has("http://example.com/b", 4))
	t.false(db.has("http://example.com/b", 5))
})

test("DB.count", async (t) => {
	const data = Buffer.from(tasl.encodeInstance(microInstance))
	async function* stream() {
		yield data
	}

	const db = await DB.import(null, microSchema, stream())

	t.is(db.count("http://example.com/a"), 1)
	t.is(db.count("http://example.com/b"), 4)
})

test("DB.get", async (t) => {
	const data = Buffer.from(tasl.encodeInstance(microInstance))
	async function* stream() {
		yield data
	}

	const db = await DB.import(null, microSchema, stream())

	for (const key of microSchema.keys()) {
		for (const [id, value] of microInstance.entries(key)) {
			t.deepEqual(db.get(key, id), value)
		}
	}

	t.pass()
})

test("DB.entries", async (t) => {
	const data = Buffer.from(tasl.encodeInstance(microInstance))
	async function* stream() {
		yield data
	}

	const db = await DB.import(null, microSchema, stream())

	for (const key of microSchema.keys()) {
		for (const [id, value] of db.entries(key)) {
			t.deepEqual(microInstance.get(key, id), value)
		}
	}

	t.pass()
})

test("DB.merge", (t) => {
	const db = DB.create(null, microSchema)

	db.merge(microInstance.toJSON())

	for (const key of microSchema.keys()) {
		for (const [id, value] of microInstance.entries(key)) {
			t.true(db.has(key, id))
			t.deepEqual(db.get(key, id), value)
		}
	}
})

test("DB.set", (t) => {
	const db = DB.create(null, microSchema)

	for (const key of microSchema.keys()) {
		for (const [id, value] of microInstance.entries(key)) {
			db.set(key, id, value)
		}
	}

	for (const key of microSchema.keys()) {
		for (const [id, value] of microInstance.entries(key)) {
			t.true(db.has(key, id))
			t.deepEqual(db.get(key, id), value)
		}
	}
})

test("DB.push", (t) => {
	const db = DB.create(null, microSchema)

	for (const key of microSchema.keys()) {
		for (const [id, value] of microInstance.entries(key)) {
			t.is(id + 1, db.push(key, value))
		}
	}

	for (const key of microSchema.keys()) {
		for (const [id, value] of microInstance.entries(key)) {
			t.true(db.has(key, id + 1))
			t.deepEqual(db.get(key, id + 1), value)
		}
	}
})

test("DB.migrate", (t) => {
	const source = tasl.parseSchema(
		fs.readFileSync("./test/source.tasl", "utf-8")
	)

	const target = tasl.parseSchema(
		fs.readFileSync("./test/target.tasl", "utf-8")
	)

	const sourceToTarget = tasl.parseMapping(
		source,
		target,
		fs.readFileSync("./test/source-to-target.taslx", "utf-8")
	)

	const sourceDB = DB.create(null, source)

	sourceDB.merge(
		JSON.parse(fs.readFileSync("./test/source.instance.json", "utf-8"))
	)

	const targetDB = sourceDB.migrate(sourceToTarget, null)

	t.pass()
})

test("DB.migrate 2", async (t) => {
	const sourceSchema = new tasl.Schema({
		"http://schema.org/Person": tasl.types.product({
			"http://schema.org/name": tasl.types.string,
			"http://schema.org/gender": tasl.types.coproduct({
				"http://schema.org/Male": tasl.types.unit,
				"http://schema.org/Female": tasl.types.unit,
				"http://schema.org/value": tasl.types.string,
			}),
		}),
	})

	const sourceDB = DB.create(null, sourceSchema)
	sourceDB.merge({
		"http://schema.org/Person": [
			{
				id: 0,
				value: tasl.values.product({
					"http://schema.org/name": tasl.values.string("John Doe"),
					"http://schema.org/gender": tasl.values.coproduct(
						"http://schema.org/Male",
						tasl.values.unit()
					),
				}),
			},
			{
				id: 1,
				value: tasl.values.product({
					"http://schema.org/name": tasl.values.string("Jane Doe"),
					"http://schema.org/gender": tasl.values.coproduct(
						"http://schema.org/Female",
						tasl.values.unit()
					),
				}),
			},
		],
	})

	const targetSchema = new tasl.Schema({
		"http://example.com/person": tasl.types.product({
			"http://example.com/name": tasl.types.string,
			"http://example.com/gender": tasl.types.string,
		}),
	})

	const mapping = new tasl.Mapping(sourceSchema, targetSchema, [
		{
			target: "http://example.com/person",
			source: "http://schema.org/Person",
			id: "person",
			value: tasl.expressions.product({
				"http://example.com/name": tasl.expressions.term("person", [
					tasl.expressions.projection("http://schema.org/name"),
				]),
				"http://example.com/gender": tasl.expressions.match(
					"person",
					[tasl.expressions.projection("http://schema.org/gender")],
					{
						"http://schema.org/Male": {
							id: "gender",
							value: tasl.expressions.literal("Male"),
						},
						"http://schema.org/Female": {
							id: "gender",
							value: tasl.expressions.literal("Female"),
						},
						"http://schema.org/value": {
							id: "gender",
							value: tasl.expressions.term("gender", []),
						},
					}
				),
			}),
		},
	])

	const targetDB = await sourceDB.migrate(mapping, null)

	const result = {
		"http://example.com/person": [
			{
				id: 0,
				value: tasl.values.product({
					"http://example.com/name": tasl.values.string("John Doe"),
					"http://example.com/gender": tasl.values.string("Male"),
				}),
			},
			{
				id: 1,
				value: tasl.values.product({
					"http://example.com/name": tasl.values.string("Jane Doe"),
					"http://example.com/gender": tasl.values.string("Female"),
				}),
			},
		],
	}

	for (const key of targetSchema.keys()) {
		for (const { id, value } of result[key]) {
			t.deepEqual(targetDB.get(key, id), value)
		}
	}
})
