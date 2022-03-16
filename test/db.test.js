import test from "ava"
import * as tasl from "tasl"

import { DB } from "../lib/db.js"
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
