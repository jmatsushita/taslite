import test from "ava"

import sqlite3 from "sqlite3"

import { DB } from "../lib/db.js"

import { microSchema } from "./instances/micro.js"
import { nanoSchema } from "./instances/nano.js"

test("Nano", async (t) => {
	const db = new sqlite3.Database(
		":memory:",
		sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE
	)

	await DB.setSchema(db, nanoSchema)

	const s = await DB.getSchema(db)
	t.true(s.isEqualTo(nanoSchema))
})

test("Micro", async (t) => {
	const db = new sqlite3.Database(
		":memory:",
		sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE
	)

	await DB.setSchema(db, microSchema)

	const s = await DB.getSchema(db)
	t.true(s.isEqualTo(microSchema))
})
